"""
kline_ws.py — Velas de Binance USDⓈ-M Futures SOLO por WebSocket (cero REST).

Conexiones
  • UNA conexión WebSocket por intervalo con TODOS los símbolos suscritos
    (<symbol>@kline_1m en una, <symbol>@kline_5m en otra, …). Binance admite
    hasta 1024 streams por conexión; solo si hubiera más símbolos que eso se
    abre una segunda conexión para ese intervalo.
  • No hay backfill REST: la primera vela cerrada llega al cerrar el periodo
    en curso.
  • Binance manda un mensaje de kline cada ~250 ms por símbolo. Los que no son
    de cierre ("x":false) se descartan con una búsqueda de texto ANTES de
    parsear el JSON, así que solo se parsea 1 mensaje por símbolo y periodo.

Almacenamiento (buffer circular numpy, sin objetos Python por vela)
  Por intervalo hay un único bloque contiguo de memoria:
      ohlcv[símbolo, slot, 5]  (open, high, low, close, volume)
      times[símbolo, slot]     (open_time en ms, int64)
  Cada vela ocupa 48 bytes en float64 (28 en float32) frente a ~300 bytes de
  una tupla Python con sus floats. Subir KLINE_HISTORY a 1500 para EMA/RSI/MACD
  no cambia el código, y el formato permite calcular indicadores de todos los
  símbolos a la vez con operaciones vectorizadas.

API
  start() / stop()
  ensure_symbols(symbols)                 — añade símbolos sin reconectar
  closed(symbol, interval="1m")           — array (n, 5) de velas cerradas, vieja → reciente
  last_closed(symbol, interval="1m")      — Candle de la última vela cerrada (None si no hay o es vieja)
  get_stats()
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import threading
import time
from typing import Callable, Dict, Iterable, List, NamedTuple, Optional

import numpy as np
import websockets

try:
    import orjson as _orjson
    _loads = _orjson.loads
except ImportError:  # pragma: no cover
    _loads = json.loads


WS_URL           = os.getenv("WS_KLINE_URL", os.getenv("WS_FSTREAM_URL", "wss://fstream.binance.com/market/stream"))
MAX_STREAMS      = 1024    # límite de Binance por conexión
STREAMS_PER_CONN = min(int(os.getenv("KLINE_STREAMS_PER_CONN", str(MAX_STREAMS))), MAX_STREAMS)
# Compresión permessage-deflate: menos tráfico de entrada pero más CPU para
# descomprimir ~3.300 mensajes/s por intervalo. Por defecto apagada (prioriza CPU).
WS_COMPRESSION   = os.getenv("KLINE_WS_COMPRESSION", "false").lower() == "true"
SUB_CHUNK_SIZE   = 100     # streams por mensaje SUBSCRIBE
SUB_CHUNK_GAP_S  = 0.25    # Binance cierra la conexión con > 10 mensajes/s entrantes
CONN_STAGGER_S   = 1.0     # separación entre aperturas de conexiones

INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000,
}

O, H, L, C, V = range(5)    # columnas de ohlcv


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
    """(symbol, interval, open_time, o, h, l, c, v) si `raw` es el CIERRE de una
    vela; None en otro caso. Descarta las velas en formación sin parsear JSON."""
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
        return (str(k["s"]).upper(), str(k["i"]), int(k["t"]), float(k["o"]),
                float(k["h"]), float(k["l"]), float(k["c"]), float(k["v"]))
    except (KeyError, TypeError, ValueError):
        return None


class KlineRing:
    """Buffer circular de velas cerradas de UN intervalo para muchos símbolos.
    Thread-safe; sin red."""

    def __init__(self, interval: str = "1m", history: int = 2, dtype: str = "float64",
                 capacity: int = 64) -> None:
        if interval not in INTERVAL_MS:
            raise ValueError(f"Intervalo no soportado: {interval}")
        self.interval    = interval
        self.interval_ms = INTERVAL_MS[interval]
        self.history     = max(1, int(history))
        self.dtype       = np.dtype(dtype)
        self._rows: Dict[str, int] = {}
        self._lock = threading.Lock()
        self.closed_count = 0
        self._alloc(max(1, capacity))

    def _alloc(self, cap: int) -> None:
        old = getattr(self, "ohlcv", None)
        ohlcv = np.zeros((cap, self.history, 5), dtype=self.dtype)
        times = np.zeros((cap, self.history), dtype=np.int64)
        head  = np.zeros(cap, dtype=np.int32)    # próximo slot a escribir
        count = np.zeros(cap, dtype=np.int32)    # slots llenos (≤ history)
        if old is not None:
            n = old.shape[0]
            ohlcv[:n], times[:n] = self.ohlcv, self.times
            head[:n], count[:n]  = self.head, self.count
        self.ohlcv, self.times, self.head, self.count = ohlcv, times, head, count

    def _row(self, symbol: str) -> int:
        row = self._rows.get(symbol)
        if row is None:
            row = len(self._rows)
            if row >= self.ohlcv.shape[0]:
                self._alloc(self.ohlcv.shape[0] * 2)
            self._rows[symbol] = row
        return row

    def reserve(self, symbols: Iterable[str]) -> None:
        """Pre-asigna filas (evita copias al crecer mientras llegan velas)."""
        with self._lock:
            new = [s for s in symbols if s not in self._rows]
            need = len(self._rows) + len(new)
            if need > self.ohlcv.shape[0]:
                self._alloc(need)
            for s in new:
                self._row(s)

    def add(self, symbol: str, open_time: int, o: float, h: float,
            l: float, c: float, v: float) -> None:
        with self._lock:
            r = self._row(symbol)
            n = int(self.count[r])
            if n:
                last = (int(self.head[r]) - 1) % self.history
                last_t = int(self.times[r, last])
                if open_time < last_t:
                    return                          # llegó tarde, ya hay una más nueva
                if open_time == last_t:
                    self.ohlcv[r, last] = (o, h, l, c, v)    # duplicado tras reconexión
                    return
            slot = int(self.head[r])
            self.ohlcv[r, slot] = (o, h, l, c, v)
            self.times[r, slot] = open_time
            self.head[r]  = (slot + 1) % self.history
            self.count[r] = min(n + 1, self.history)
            self.closed_count += 1

    def _order(self, r: int) -> np.ndarray:
        n = int(self.count[r])
        return (int(self.head[r]) - n + np.arange(n)) % self.history

    def closed(self, symbol: str) -> np.ndarray:
        """Copia (n, 5) de las velas cerradas, de la más vieja a la más reciente."""
        with self._lock:
            r = self._rows.get(symbol)
            if r is None:
                return np.empty((0, 5), dtype=self.dtype)
            return self.ohlcv[r, self._order(r)]

    def open_times(self, symbol: str) -> np.ndarray:
        with self._lock:
            r = self._rows.get(symbol)
            if r is None:
                return np.empty(0, dtype=np.int64)
            return self.times[r, self._order(r)]

    def last_closed(self, symbol: str, fresh: bool = True,
                    now_ms: Optional[int] = None, grace_ms: int = 5_000) -> Optional[Candle]:
        """Última vela cerrada. Con fresh=True solo se devuelve si es la del
        periodo inmediatamente anterior (si se perdió un cierre por una
        reconexión, el dato viejo no se usa para decidir)."""
        with self._lock:
            r = self._rows.get(symbol)
            if r is None or not self.count[r]:
                return None
            last = (int(self.head[r]) - 1) % self.history
            t = int(self.times[r, last])
            o, h, l, c, v = (float(x) for x in self.ohlcv[r, last])
        candle = Candle(t, t + self.interval_ms - 1, o, h, l, c, v)
        if fresh:
            now_ms = int(time.time() * 1000) if now_ms is None else now_ms
            if now_ms - candle.close_time > self.interval_ms + grace_ms:
                return None
        return candle

    def symbols_with_data(self) -> int:
        with self._lock:
            return int(np.count_nonzero(self.count[:len(self._rows)]))

    @property
    def nbytes(self) -> int:
        return self.ohlcv.nbytes + self.times.nbytes + self.head.nbytes + self.count.nbytes


class _Conn:
    __slots__ = ("cid", "interval", "symbols", "subscribed", "ws", "messages", "reconnects")

    def __init__(self, cid: int, interval: str) -> None:
        self.cid        = cid
        self.interval   = interval
        self.symbols: List[str] = []
        self.subscribed: set    = set()
        self.ws         = None
        self.messages   = 0
        self.reconnects = 0


class KlineWebSocketStream:
    """Una conexión por intervalo con todos los símbolos; llena un KlineRing por intervalo."""

    def __init__(self, symbols: Iterable[str], intervals: Iterable[str] = ("1m",),
                 history: int = 2, dtype: str = "float64",
                 streams_per_conn: int = STREAMS_PER_CONN,
                 on_close: Optional[Callable[[str, str, Candle], None]] = None) -> None:
        self.intervals = list(dict.fromkeys(intervals))
        symbols = sorted({s.upper() for s in symbols if s})
        self.rings: Dict[str, KlineRing] = {
            iv: KlineRing(iv, history=history, dtype=dtype, capacity=max(64, len(symbols) + 64))
            for iv in self.intervals
        }
        self.per_conn = max(1, min(int(streams_per_conn), MAX_STREAMS))
        self.on_close = on_close
        self._conns: List[_Conn] = []
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._tasks: Dict[int, object] = {}
        self.running = False
        self.last_error = ""
        self._assign(symbols)

    # ── Reparto de símbolos ──────────────────────────────────────────────────

    def _assign(self, symbols: Iterable[str]) -> List[_Conn]:
        """Mete los símbolos nuevos en la conexión de cada intervalo (abre otra
        solo si se superan los 1024 streams). Devuelve las conexiones tocadas."""
        symbols = [s.upper() for s in symbols if s]
        touched: List[_Conn] = []
        with self._lock:
            for iv in self.intervals:
                conns = [c for c in self._conns if c.interval == iv]
                known = {s for c in conns for s in c.symbols}
                new = [s for s in dict.fromkeys(symbols) if s not in known]
                if not new:
                    continue
                self.rings[iv].reserve(new)
                for sym in new:
                    if not conns or len(conns[-1].symbols) >= self.per_conn:
                        conns.append(_Conn(len(self._conns), iv))
                        self._conns.append(conns[-1])
                    conns[-1].symbols.append(sym)
                    if conns[-1] not in touched:
                        touched.append(conns[-1])
        return touched

    # ── Conexiones ───────────────────────────────────────────────────────────

    async def _subscribe(self, conn: _Conn, ws) -> None:
        with self._lock:
            todo = [f"{s.lower()}@kline_{conn.interval}" for s in conn.symbols]
        todo = [s for s in todo if s not in conn.subscribed]
        for i in range(0, len(todo), SUB_CHUNK_SIZE):
            if conn.ws is not ws:
                return
            part = todo[i:i + SUB_CHUNK_SIZE]
            await ws.send(json.dumps({"method": "SUBSCRIBE", "params": part,
                                      "id": conn.cid * 100_000 + i}))
            conn.subscribed.update(part)
            await asyncio.sleep(SUB_CHUNK_GAP_S)

    async def _conn_loop(self, conn: _Conn) -> None:
        await asyncio.sleep(conn.cid * CONN_STAGGER_S)
        rings = self.rings
        delay = 1.0
        while self.running:
            connected_at = 0.0
            try:
                async with websockets.connect(
                    WS_URL, ping_interval=20, ping_timeout=20, close_timeout=5,
                    max_queue=4096, compression="deflate" if WS_COMPRESSION else None,
                ) as ws:
                    connected_at = time.time()
                    conn.ws = ws
                    conn.subscribed = set()
                    await self._subscribe(conn, ws)
                    print(f"✅ [KLINE {conn.interval}] conexión {conn.cid} activa — "
                          f"{len(conn.subscribed)} streams", flush=True)
                    delay = 1.0
                    async for raw in ws:
                        conn.messages += 1
                        p = parse_kline_message(raw)
                        if p is None:
                            continue
                        ring = rings.get(p[1])
                        if ring is None:
                            continue
                        ring.add(p[0], p[2], p[3], p[4], p[5], p[6], p[7])
                        cb = self.on_close
                        if cb is not None:
                            try:
                                cb(p[0], p[1], Candle(p[2], p[2] + ring.interval_ms - 1, *p[3:]))
                            except Exception:
                                pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"{conn.interval}/{conn.cid}: {exc}"
            finally:
                conn.ws = None
            if not self.running:
                break
            conn.reconnects += 1
            # Conexión que aguantó (Binance corta cada 24 h) → reconexión rápida.
            delay = 1.0 if connected_at and time.time() - connected_at > 60 else min(delay * 2, 60.0)
            wait = delay + random.uniform(0, 1.0)
            print(f"🔴 [KLINE {conn.interval}] conexión {conn.cid} caída ({self.last_error}) — "
                  f"reconectando en {wait:.1f}s", flush=True)
            await asyncio.sleep(wait)

    def _launch(self, conn: _Conn) -> None:
        self._tasks[conn.cid] = asyncio.run_coroutine_threadsafe(self._conn_loop(conn), self._loop)

    # ── API pública ──────────────────────────────────────────────────────────

    def ensure_symbols(self, symbols: Iterable[str]) -> None:
        touched = self._assign(symbols)
        if not touched or self._loop is None or not self.running:
            return
        for conn in touched:
            if conn.cid not in self._tasks:
                self._launch(conn)
            elif conn.ws is not None:
                asyncio.run_coroutine_threadsafe(self._subscribe(conn, conn.ws), self._loop)
            # si está reconectando, al conectar se suscribe a todos sus símbolos

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, daemon=True, name="ws-kline").start()
        for conn in list(self._conns):
            self._launch(conn)
        for iv in self.intervals:
            conns = [c for c in self._conns if c.interval == iv]
            print(f"✅ [KLINE {iv}] {sum(len(c.symbols) for c in conns)} símbolos en "
                  f"{len(conns)} conexión(es), {self.rings[iv].history} velas cerradas "
                  f"por símbolo — sin REST", flush=True)

    def stop(self) -> None:
        self.running = False
        loop = self._loop
        if loop is None:
            return
        for conn in self._conns:
            ws = conn.ws
            if ws is not None:
                try:
                    asyncio.run_coroutine_threadsafe(ws.close(), loop).result(timeout=3)
                except Exception:
                    pass
        for t in self._tasks.values():
            t.cancel()
        loop.call_soon_threadsafe(loop.stop)

    # ── Lectura ──────────────────────────────────────────────────────────────

    def closed(self, symbol: str, interval: str = "1m") -> np.ndarray:
        return self.rings[interval].closed(symbol)

    def last_closed(self, symbol: str, interval: str = "1m", fresh: bool = True) -> Optional[Candle]:
        return self.rings[interval].last_closed(symbol, fresh=fresh)

    def get_stats(self) -> dict:
        with self._lock:
            conns = list(self._conns)
        per_iv = {}
        for iv, ring in self.rings.items():
            cs = [c for c in conns if c.interval == iv]
            per_iv[iv] = {
                "symbols":        sum(len(c.symbols) for c in cs),
                "connections":    len(cs),
                "active":         sum(1 for c in cs if c.ws is not None),
                "messages":       sum(c.messages for c in cs),
                "closed_candles": ring.closed_count,
                "with_data":      ring.symbols_with_data(),
                "memory_kb":      round(ring.nbytes / 1024, 1),
            }
        return {
            "pairs_with_data":    sum(v["with_data"] for v in per_iv.values()),
            "total_messages":     sum(v["messages"] for v in per_iv.values()),
            "active_connections": sum(v["active"] for v in per_iv.values()),
            "connections":        len(conns),
            "reconnects":         sum(c.reconnects for c in conns),
            "last_error":         self.last_error,
            "intervals":          per_iv,
        }
