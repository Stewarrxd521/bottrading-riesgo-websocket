"""
Prueba de punta a punta de la recuperación tras reinicio (no la ejecuta pytest).

    python -m tests.e2e_restart

Levanta un Binance falso (WebSocket con ticker 24h, mark price, bookTicker y
velas 1m) y un Upstash falso (API REST sobre un redis-server real, para que los
scripts Lua se ejecuten de verdad), y arranca el bot como proceso real con el
disco "efímero" (un directorio nuevo por arranque, como en Render free):

  1. A abre un short en FAKEUSDT (+60 % en 24h), le pone un SL manual y todo
     queda en Upstash.
  2. A muere de golpe (kill -9). B arranca con disco vacío, espera a que caduque
     el control de A y recupera la posición (mismo trade_id, tramos y SL), sin
     abrir duplicados.
  3. El precio cae, B cierra por TP: la posición desaparece del documento y
     el cierre queda en el historial, con su cooldown.
  4. C arranca: no hay posición y el cooldown impide reentrar.
  5. D arranca con C vivo (deploy con solapamiento): D espera sin operar (un
     cambio de SL en D se rechaza) y C sigue al mando. C recibe SIGTERM, guarda
     y libera; D toma el control enseguida, sin esperar a que caduque.
  6. Upstash caído al arrancar: E no opera hasta recuperar el estado; cuando
     Upstash vuelve, lo recupera y abre.
  7. SIGTERM a E (gunicorn): guardado final y control liberado.
  0. (antes de todo) Con la URL de otro producto (404, como QStash) el panel
     muestra "error de configuración", el log explica qué variable cambiar y el
     bot no opera.

Todo corre SIN la variable RENDER: la persistencia funciona fuera de Render.
"""

import asyncio
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

import websockets

from tests.fake_upstash import FakeRedis, serve

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORK = tempfile.mkdtemp(prefix="e2e-bot-")
PRICES = {"FAKEUSDT": {"price": 0.80, "open": 0.50}, "BTCUSDT": {"price": 60000.0, "open": 59000.0}}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ── Binance falso ─────────────────────────────────────────────────────────────

async def _ws_handler(ws, path=None):
    subs = set()

    async def feed():
        while True:
            now_ms = int(time.time() * 1000)
            t = now_ms // 60_000 * 60_000 - 60_000
            snap = {s: dict(v) for s, v in PRICES.items()}
            msgs = []
            if "!ticker@arr" in subs:
                msgs.append({"stream": "!ticker@arr", "data": [
                    {"e": "24hrTicker", "s": s, "c": str(v["price"]), "o": str(v["open"]),
                     "P": str((v["price"] / v["open"] - 1) * 100), "p": str(v["price"] - v["open"]),
                     "h": str(v["price"]), "l": str(v["open"]), "v": "1000", "q": "1000"}
                    for s, v in snap.items()]})
            if "!markPrice@arr@1s" in subs:
                msgs.append({"stream": "!markPrice@arr@1s", "data": [
                    {"e": "markPriceUpdate", "s": s, "p": str(v["price"])} for s, v in snap.items()]})
            for st in list(subs):
                sym = st.split("@")[0].upper()
                v = snap.get(sym)
                if v is None:
                    continue
                if st.endswith("@bookTicker"):
                    msgs.append({"stream": st, "data": {"e": "bookTicker", "s": sym,
                                 "b": str(v["price"] * 0.9999), "a": str(v["price"] * 1.0001)}})
                elif "@kline_" in st:
                    p = v["price"]
                    msgs.append({"stream": st, "data": {"e": "kline", "s": sym, "k": {
                        "t": t, "T": t + 59_999, "s": sym, "i": "1m", "o": str(p * 0.99),
                        "h": str(p), "l": str(p * 0.98), "c": str(p), "v": "10", "x": True}}})
            for m in msgs:
                await ws.send(json.dumps(m, separators=(",", ":")))
            await asyncio.sleep(0.5)

    task = asyncio.create_task(feed())
    try:
        async for raw in ws:
            d = json.loads(raw)
            if d.get("method") == "SUBSCRIBE":
                subs.update(d["params"])
            elif d.get("method") == "UNSUBSCRIBE":
                subs.difference_update(d["params"])
            await ws.send(json.dumps({"result": None, "id": d.get("id")}))
    except websockets.ConnectionClosed:
        pass
    finally:
        task.cancel()


def start_fake_binance(port: int) -> None:
    async def main():
        async with websockets.serve(_ws_handler, "127.0.0.1", port, compression=None):
            await asyncio.Future()
    threading.Thread(target=lambda: asyncio.run(main()), daemon=True).start()


# ── Bot como proceso ──────────────────────────────────────────────────────────

class Bot:
    def __init__(self, name: str, ws_port: int, up_port: int, gunicorn: bool = False, **extra_env):
        self.name = name
        self.port = free_port()
        disk = os.path.join(WORK, name)          # disco nuevo: efímero como en Render
        os.makedirs(disk)
        env = {k: v for k, v in os.environ.items() if "proxy" not in k.lower() and k != "RENDER"}
        env.update({                             # sin RENDER: también debe funcionar fuera de Render
            "UPSTASH_REDIS_REST_URL": f"http://127.0.0.1:{up_port}",
            "UPSTASH_REDIS_REST_TOKEN": "test-token",
            "STATE_HEARTBEAT_S": "2",
            "STATE_OWNER_LEASE_S": "6",
            "WS_FSTREAM_URL": f"ws://127.0.0.1:{ws_port}",
            "WS_KLINE_URL": f"ws://127.0.0.1:{ws_port}",
            "BASE_URL": "http://127.0.0.1:9",      # exchangeInfo falla rápido → INITIAL_SYMBOLS
            "REST_PROXY_URL": "",
            "INITIAL_SYMBOLS": "FAKEUSDT,BTCUSDT",
            "EXECUTOR_URL": "",
            "PAPER_MODE": "true",
            "STATS_FILE": os.path.join(disk, "stats.jsonl"),
            "SETTINGS_FILE": os.path.join(disk, "settings.json"),
            "STATE_FILE": os.path.join(disk, "state.json"),
            "RECOVERY_FILE": os.path.join(disk, "recovery.json"),
            "SYMBOLS_CACHE_FILE": os.path.join(disk, "symbols.json"),
        })
        env.update(extra_env)
        self.log = open(os.path.join(WORK, f"{name}.log"), "w")
        self.log_path = os.path.join(WORK, f"{name}.log")
        cmd = ([sys.executable, "-m", "gunicorn", "app:app", "--bind", f"127.0.0.1:{self.port}",
                "--worker-class", "gthread", "--workers", "1", "--threads", "8", "--timeout", "0"]
               if gunicorn else
               [sys.executable, "-c",
                f"import app; app.app.run(host='127.0.0.1', port={self.port}, threaded=True)"])
        self.proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=self.log, stderr=subprocess.STDOUT)

    def get(self, path: str) -> dict:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as r:
            return json.loads(r.read())

    def post(self, path: str, body: dict) -> dict:
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", method="POST",
                                     data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read())

    def status(self) -> dict:
        return self.get("/api/status")

    def wait(self, cond, what: str, timeout: float = 60.0):
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                last = self.status()
                if cond(last):
                    return last
            except Exception:
                pass
            time.sleep(0.5)
        raise AssertionError(f"[{self.name}] no se cumplió: {what}\núltimos eventos: "
                             f"{(last or {}).get('events', [])[:15]}")

    def kill(self, sig=signal.SIGKILL):
        self.proc.send_signal(sig)
        self.proc.wait(timeout=20)

    def logs(self) -> str:
        with open(self.log_path) as fh:
            return fh.read()


def positions(s: dict) -> dict:
    return {p["symbol"]: p for p in s.get("positions", [])}


def main() -> None:
    ws_port, up_port = free_port(), free_port()
    start_fake_binance(ws_port)
    redis = FakeRedis()
    serve(redis, up_port)
    LEASE = 6.0
    bots = []
    try:
        # 0. URL de otro producto de Upstash (QStash responde 404 sin cuerpo)
        nf_port = free_port()
        serve(redis, nf_port, not_found=True)
        q = Bot("Q", ws_port, up_port, STATE_BOOT_WAIT_S="3",
                UPSTASH_REDIS_REST_URL=f"http://127.0.0.1:{nf_port}"); bots.append(q)
        s = q.wait(lambda s: s.get("persistence", {}).get("mode") == "error de configuración",
                   "Q muestra el error de configuración", 30)
        assert "QStash" in s["persistence"]["config_error"]
        time.sleep(3)
        assert not positions(q.status()), "Q abrió una posición sin poder guardarla"
        assert "⛔" in q.logs() and "Upstash mal configurado" in q.logs()
        try:
            q.post("/api/set-default-sl", {"sl_usd": -3.0})
            raise AssertionError("Q aceptó un cambio de SL sin poder guardarlo")
        except urllib.error.HTTPError as exc:
            assert exc.code == 409 and "QStash" in json.loads(exc.read()).get("error", ""), exc.code
        q.kill()
        print("✓ Con la URL de otro producto (404) el panel y el log explican el error y el bot no opera")

        # 1. A abre la posición y la guarda en Upstash
        a = Bot("A", ws_port, up_port); bots.append(a)
        a.wait(lambda s: "FAKEUSDT" in positions(s), "A abre short en FAKEUSDT")
        deadline = time.time() + 10
        while time.time() < deadline and "FAKEUSDT" not in ((redis.state() or {}).get("positions") or {}):
            time.sleep(0.2)
        saved = redis.state()["positions"]["FAKEUSDT"]
        print(f"✓ A abrió FAKEUSDT trade_id={saved['trade_id']} tramos={len(saved['fills'])} "
              f"SL={saved['sl_usd']:.4f} y quedó en Upstash")
        a.post("/api/set-sl/FAKEUSDT", {"sl_usd": -2.5})       # SL manual desde el dashboard
        deadline = time.time() + 10
        while time.time() < deadline and not redis.state()["positions"]["FAKEUSDT"].get("sl_manual"):
            time.sleep(0.2)
        saved = redis.state()["positions"]["FAKEUSDT"]
        assert saved["sl_manual"] and saved["sl_usd"] == -2.5

        # 2. A muere de golpe; B arranca con disco vacío, espera a que caduque el control y recupera
        a.kill()
        b = Bot("B", ws_port, up_port); bots.append(b)
        s = b.wait(lambda s: "FAKEUSDT" in positions(s), "B recupera FAKEUSDT")
        assert "controla el estado. Espero" in b.logs(), "B no esperó a que caducara el control de A"
        pos_b = positions(s)["FAKEUSDT"]
        assert pos_b["trade_id"] == saved["trade_id"], (pos_b["trade_id"], saved["trade_id"])
        assert len(pos_b["fills"]) == len(saved["fills"]) == 1, pos_b["fills"]
        assert abs(pos_b["stop_loss_usd"] - saved["sl_usd"]) < 1e-9 and pos_b["sl_mode"] == "manual"
        assert abs(pos_b["fills"][0]["entry_price"] - saved["fills"][0]["entry_price"]) < 1e-12
        assert any("Recuperado desde upstash" in e for e in s["events"]), s["events"][:10]
        time.sleep(4)                                    # unos ticks: no debe abrir duplicados
        s = b.status()
        assert len(positions(s)["FAKEUSDT"]["fills"]) == 1
        assert sum("SHORT FAKEUSDT" in e for e in s["events"]) == 0, "B abrió un tramo duplicado"
        print("✓ B recuperó la posición tras kill -9 (mismo trade_id, tramos, entrada y SL manual), sin duplicados")

        # 3. TP: la posición sale del documento y queda en el historial
        PRICES["FAKEUSDT"]["price"] = 0.65
        b.wait(lambda s: "FAKEUSDT" not in positions(s), "B cierra por TP")
        deadline = time.time() + 10
        while time.time() < deadline and (redis.state() or {}).get("positions"):
            time.sleep(0.2)
        st = redis.state()
        assert st["positions"] == {}, st["positions"]
        assert "FAKEUSDT" in st["cooldowns"]
        trades = redis.trades()
        assert [t["reason"] for t in trades] == ["TP"] and trades[0]["trade_id"] == saved["trade_id"]
        print(f"✓ B cerró por TP (PnL {trades[0]['pnl']:.4f}); la posición salió del documento "
              f"y el cierre quedó en el historial con cooldown")

        # 4. C: sin posición y con cooldown (no reentra aunque vuelva a +60 %)
        PRICES["FAKEUSDT"]["price"] = 0.80
        b.kill()
        c = Bot("C", ws_port, up_port, gunicorn=True); bots.append(c)
        s = c.wait(lambda s: any("Recuperado desde upstash" in e for e in s["events"]), "C arranca")
        time.sleep(6)
        s = c.status()
        assert "FAKEUSDT" not in positions(s), "C reentró pese al cooldown"
        assert len(c.get("/api/trades")) == 1
        print("✓ C arrancó sin la posición cerrada, conservó el historial y respetó el cooldown")

        # 5. D arranca con C vivo: D espera sin operar; C sigue al mando hasta su SIGTERM
        d = Bot("D", ws_port, up_port); bots.append(d)
        d.wait(lambda s: s.get("persistence", {}).get("mode") == "esperando control", "D espera el control")
        time.sleep(LEASE + 2)                          # más que un lease: C lo renueva y no lo pierde
        assert d.status()["persistence"]["mode"] == "esperando control"
        assert c.status()["persistence"]["mode"] == "ok"
        try:
            d.post("/api/set-default-sl", {"sl_usd": -3.0})
            raise AssertionError("D aceptó un cambio de SL sin controlar el estado")
        except urllib.error.HTTPError as exc:
            assert exc.code == 409, exc.code
        c.kill(signal.SIGTERM)                         # Render apaga la instancia vieja
        released_at = time.time()
        assert "guardado final enviado y control liberado" in c.logs(), "C no liberó el control"
        d.wait(lambda s: s.get("persistence", {}).get("mode") == "ok", "D toma el control", 20)
        took = time.time() - released_at
        assert took < LEASE, f"D tardó {took:.1f}s: esperó a que caducara en vez de usar la liberación"
        s = d.status()
        assert "FAKEUSDT" not in positions(s) and len(d.get("/api/trades")) == 1
        print(f"✓ Deploy con solapamiento: D esperó sin operar (SL rechazado con 409) mientras C seguía; "
              f"C guardó y liberó al recibir SIGTERM y D tomó el control en {took:.1f}s")

        # 6. Upstash caído al arrancar: E arranca degradado (sin entradas) y se
        #    recupera solo cuando Upstash vuelve.
        d.kill()
        redis.cmd("DEL", "botshort:state")             # sin cooldown: FAKEUSDT sería una entrada
        redis.fail = True
        e = Bot("E", ws_port, up_port, gunicorn=True, STATE_BOOT_WAIT_S="3"); bots.append(e)
        s = e.wait(lambda s: any("Aún no controlo el estado" in x for x in s["events"]),
                   "E arranca sin el estado")
        time.sleep(4)
        s = e.status()
        assert "FAKEUSDT" not in positions(s), "E abrió una posición sin persistencia"
        redis.fail = False
        e.wait(lambda s: "FAKEUSDT" in positions(s), "E opera tras recuperar Upstash", 90)
        assert e.status()["persistence"]["mode"] == "ok"
        print("✓ Con Upstash caído E no abrió posiciones; al volver recuperó el estado y retomó")

        # 7. Apagado ordenado (SIGTERM de Render a gunicorn): guardado final en Upstash
        e.kill(signal.SIGTERM)
        assert "guardado final enviado y control liberado" in e.logs(), "sin guardado final al apagar"
        assert "FAKEUSDT" in redis.state()["positions"] and redis.owner().endswith("|released")
        print("✓ E (gunicorn) recibió SIGTERM, hizo el guardado final y liberó el control")
        print("\nTODO OK")
    finally:
        for b in bots:
            if b.proc.poll() is None:
                b.kill()
        redis.close()
        if os.getenv("KEEP_E2E") != "1":
            shutil.rmtree(WORK, ignore_errors=True)
        else:
            print("logs en", WORK)


if __name__ == "__main__":
    main()
