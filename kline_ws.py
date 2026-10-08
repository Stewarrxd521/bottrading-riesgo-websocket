"""
kline_ws.py — Velas de Binance USDⓈ-M Futures SOLO por WebSocket (cero REST).

Sustituye a KlineWebSocketCache_v4, que hacía backfill, relleno de huecos y
"safety refresh" contra /fapi/v1/klines y, además, se reiniciaba cada vez que
cambiaba el radar. Esas ráfagas REST eran las que provocaban el baneo de IP.

Funcionamiento
  • Se suscribe a <symbol>@kline_<interval> de TODOS los símbolos indicados,
    repartidos en varias conexiones (KLINE_STREAMS_PER_CONN por conexión).
  • Guarda por símbolo las últimas `history` velas CERRADAS en un deque
    (por ahora 2: última y penúltima). Subir `history` (p. ej. a 1500 para
    EMA/RSI/MACD) no cambia nada más del módulo.
  • No hay backfill: tras arrancar, la primera vela cerrada llega al cerrar
    el minuto en curso y la segunda un minuto después.
  • Binance manda un mensaje de kline cada ~250 ms por símbolo; los que no
    son de cierre ("x":false) se descartan ANTES de parsear el JSON, así que
    seguir cientos de símbolos cuesta muy poca CPU.

API
  start() / stop()
  ensure_symbols(symbols)      — añade símbolos sin reconectar
  closed(symbol)               — lista de velas cerradas (vieja → reciente)
  last_closed(symbol, fresh)   — última vela cerrada (None si no hay o es vieja)
  get_stats()
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import threading
import time
from collections import deque
from typing import Callable, Deque, Dict, Iterable, List, NamedTuple, Optional

import websockets

try:
    import orjson as _orjson
    _loads = _orjson.loads
except ImportError:  # pragma: no cover
    _loads = json.loads


WS_URL           = os.getenv("WS_KLINE_URL", os.getenv("WS_FSTREAM_URL", "wss://fstream.binance.com/market/stream"))
STREAMS_PER_CONN = int(os.getenv("KLINE_STREAMS_PER_CONN", "200"))
SUB_CHUNK_SIZE   = 100     # streams por mensaje SUBSCRIBE
SUB_CHUNK_GAP_S  = 0.25    # Binance cierra la conexión con > 10 mensajes/s entrantes
CONN_STAGGER_S   = 1.0     # separación entre aperturas de conexiones

_INTERVAL_MS = {"1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000,
                "30m": 1_800_000, "1h": 3_600_000}


class Candle(NamedTuple):
    open_time:  int     # ms
    close_time: int     # ms
    open:       float
    high:       float
    low:        float
    close:      float
    volume:     float

    @property
    def bullish(self) -> bool:
        return self.close >= self.open


def parse_kline_message(raw) -> Optional[tuple]:
    """(symbol, Candle) si `raw` es el CIERRE de una vela; None en otro caso.
    Descarta las velas en formación sin parsear el JSON."""
    if isinstance(raw, (bytes, bytearray)):
        if b'"x":true' not in raw:
            return None
    elif '"x":true' not in raw:
        return None
    try:
        msg = _loads(raw)
    except Exception:
        return None
    data = msg.get("data", msg) if isinstance(msg, dict) else None
    if not isinstance(data, dict) or data.get("e") != "kline":
        return None
    k = data.get("k") or {}
    if not k.get("x"):
        return None
    try:
        return str(k["s"]).upper(), Candle(
            int(k["t"]), int(k["T"]), float(k["o"]), float(k["h"]),
            float(k["l"]), float(k["c"]), float(k["v"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


class KlineStore:
    """Velas cerradas por símbolo. Thread-safe; sin red."""

    def __init__(self, history: int = 2, interval: str = "1m") -> None:
        if interval not in _INTERVAL_MS:
            raise ValueError(f"Intervalo no soportado: {interval}")
        self.history     = max(1, int(history))
        self.interval    = interval
        self.interval_ms = _INTERVAL_MS[interval]
        self._data: Dict[str, Deque[Candle]] = {}
        self._lock = threading.Lock()
        self.closed_count = 0

    def add(self, symbol: str, candle: Candle) -> None:
        with self._lock:
            dq = self._data.get(symbol)
            if dq is None:
                dq = self._data[symbol] = deque(maxlen=self.history)
            if dq and dq[-1].open_time == candle.open_time:
                dq[-1] = candle                     # duplicado tras reconexión
            elif dq and dq[-1].open_time > candle.open_time:
                return                              # llegó tarde, ya hay una más nueva
            else:
                dq.append(candle)
            self.closed_count += 1

    def closed(self, symbol: str) -> List[Candle]:
        with self._lock:
            dq = self._data.get(symbol)
            return list(dq) if dq else []

    def last_closed(self, symbol: str, fresh: bool = True,
                    now_ms: Optional[int] = None, grace_ms: int = 5_000) -> Optional[Candle]:
        """Última vela cerrada. Con fresh=True solo la devuelve si es la del
        periodo inmediatamente anterior (si se perdió un cierre por una
        reconexión, el dato viejo no se usa para decidir)."""
        with self._lock:
            dq = self._data.get(symbol)
            last = dq[-1] if dq else None
        if last is None or not fresh:
            return last
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        if now_ms - last.close_time > self.interval_ms + grace_ms:
            return None
        return last

    def symbols_with_data(self) -> int:
        with self._lock:
            return len(self._data)


class KlineWebSocketStream:
    """Mantiene las suscripciones <symbol>@kline_<interval> y llena un KlineStore."""

    def __init__(self, symbols: Iterable[str], history: int = 2, interval: str = "1m",
                 streams_per_conn: int = STREAMS_PER_CONN,
                 on_close: Optional[Callable[[str, Candle], None]] = None) -> None:
        self.store    = KlineStore(history=history, interval=interval)
        self.interval = interval
        self.per_conn = max(1, min(int(streams_per_conn), 1000))
        self.on_close = on_close
        self._groups: List[List[str]] = []          # símbolos asignados a cada conexión
        self._sockets: Dict[int, object] = {}
        self._subscribed: Dict[int, set] = {}
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._tasks: list = []
        self.running = False
        self.messages = 0
        self.reconnects = 0
        self.last_error = ""
        self._assign(symbols)

    # ── Reparto de símbolos ──────────────────────────────────────────────────

    def _assign(self, symbols: Iterable[str]) -> List[int]:
        """Reparte símbolos nuevos entre conexiones. Devuelve los grupos tocados."""
        touched: List[int] = []
        with self._lock:
            known = {s for g in self._groups for s in g}
            for sym in sorted({s.upper() for s in symbols if s}):
                if sym in known:
                    continue
                if not self._groups or len(self._groups[-1]) >= self.per_conn:
                    self._groups.append([])
                self._groups[-1].append(sym)
                known.add(sym)
                gid = len(self._groups) - 1
                if gid not in touched:
                    touched.append(gid)
        return touched

    def _streams(self, gid: int) -> List[str]:
        with self._lock:
            return [f"{s.lower()}@kline_{self.interval}" for s in self._groups[gid]]

    # ── Conexiones ───────────────────────────────────────────────────────────

    async def _subscribe(self, gid: int, ws) -> None:
        done = self._subscribed.setdefault(gid, set())
        todo = [s for s in self._streams(gid) if s not in done]
        for i in range(0, len(todo), SUB_CHUNK_SIZE):
            if self._sockets.get(gid) is not ws:
                return
            part = todo[i:i + SUB_CHUNK_SIZE]
            await ws.send(json.dumps({"method": "SUBSCRIBE", "params": part, "id": gid * 100_000 + i}))
            done.update(part)
            await asyncio.sleep(SUB_CHUNK_GAP_S)

    async def _conn_loop(self, gid: int) -> None:
        await asyncio.sleep(gid * CONN_STAGGER_S)
        delay = 1.0
        while self.running:
            connected_at = 0.0
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20,
                                              close_timeout=5, max_queue=1024) as ws:
                    connected_at = time.time()
                    self._sockets[gid] = ws
                    self._subscribed[gid] = set()
                    await self._subscribe(gid, ws)
                    print(f"✅ [KLINE] conexión {gid} activa — {len(self._subscribed[gid])} streams", flush=True)
                    delay = 1.0
                    async for raw in ws:
                        self.messages += 1
                        parsed = parse_kline_message(raw)
                        if parsed is None:
                            continue
                        sym, candle = parsed
                        self.store.add(sym, candle)
                        cb = self.on_close
                        if cb is not None:
                            try:
                                cb(sym, candle)
                            except Exception:
                                pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"conn {gid}: {exc}"
            finally:
                self._sockets.pop(gid, None)
            if not self.running:
                break
            self.reconnects += 1
            # Conexión que aguantó (Binance corta cada 24 h) → reconexión rápida.
            delay = 1.0 if connected_at and time.time() - connected_at > 60 else min(delay * 2, 60.0)
            wait = delay + random.uniform(0, 1.0)
            print(f"🔴 [KLINE] conexión {gid} caída ({self.last_error}) — reconectando en {wait:.1f}s", flush=True)
            await asyncio.sleep(wait)

    # ── API pública ──────────────────────────────────────────────────────────

    def ensure_symbols(self, symbols: Iterable[str]) -> None:
        touched = self._assign(symbols)
        loop = self._loop
        if not touched or loop is None or not self.running:
            return
        for gid in touched:
            ws = self._sockets.get(gid)
            if ws is not None:
                asyncio.run_coroutine_threadsafe(self._subscribe(gid, ws), loop)
            elif gid >= len(self._tasks):
                self._tasks.append(asyncio.run_coroutine_threadsafe(self._conn_loop(gid), loop))

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, daemon=True, name="ws-kline").start()
        for gid in range(len(self._groups)):
            self._tasks.append(asyncio.run_coroutine_threadsafe(self._conn_loop(gid), self._loop))
        total = sum(len(g) for g in self._groups)
        print(f"✅ [KLINE] {total} símbolos ({self.interval}) en {len(self._groups)} conexión(es), "
              f"guardando {self.store.history} velas cerradas — sin REST", flush=True)

    def stop(self) -> None:
        self.running = False
        loop = self._loop
        if loop is None:
            return
        for ws in list(self._sockets.values()):
            try:
                asyncio.run_coroutine_threadsafe(ws.close(), loop).result(timeout=3)
            except Exception:
                pass
        for t in self._tasks:
            t.cancel()
        loop.call_soon_threadsafe(loop.stop)

    # ── Atajos ───────────────────────────────────────────────────────────────

    def closed(self, symbol: str) -> List[Candle]:
        return self.store.closed(symbol)

    def last_closed(self, symbol: str, fresh: bool = True) -> Optional[Candle]:
        return self.store.last_closed(symbol, fresh=fresh)

    def get_stats(self) -> dict:
        with self._lock:
            total = sum(len(g) for g in self._groups)
            conns = len(self._groups)
        return {
            "pairs_with_data":    self.store.symbols_with_data(),
            "symbols":            total,
            "total_messages":     self.messages,
            "closed_candles":     self.store.closed_count,
            "active_connections": len(self._sockets),
            "connections":        conns,
            "reconnects":         self.reconnects,
            "history":            self.store.history,
            "last_error":         self.last_error,
        }
