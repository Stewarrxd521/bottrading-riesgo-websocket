"""Upstash falso para tests: un redis-server real (los scripts Lua de
state_store.py se ejecutan de verdad) detrás de la misma API REST de Upstash.
Requiere el binario redis-server; los tests se saltan si no está instalado."""

import json
import shutil
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from state_store import StoreError

HAVE_REDIS = shutil.which("redis-server") is not None


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class FakeRedis:
    """redis-server propio en un puerto libre + cliente RESP mínimo.
    fail=True simula que Upstash no responde."""

    def __init__(self):
        self.port = _free_port()
        self.proc = subprocess.Popen(
            ["redis-server", "--port", str(self.port), "--bind", "127.0.0.1",
             "--save", "", "--appendonly", "no"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.lock = threading.Lock()
        self.fail = False
        self.calls = 0
        deadline = time.time() + 10
        while True:
            try:
                self.sock = socket.create_connection(("127.0.0.1", self.port), timeout=5)
                break
            except OSError:
                if time.time() > deadline:
                    raise
                time.sleep(0.05)
        self.rfile = self.sock.makefile("rb")

    def close(self):
        try:
            self.sock.close()
        finally:
            self.proc.terminate()
            self.proc.wait(timeout=10)

    def cmd(self, *args):
        if self.fail:
            raise StoreError("Upstash no responde: simulado")
        parts = [a if isinstance(a, str) else str(a) for a in args]
        out = [f"*{len(parts)}\r\n".encode()]
        for p in parts:
            b = p.encode("utf-8")
            out.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
        with self.lock:
            self.calls += 1
            self.sock.sendall(b"".join(out))
            return self._read()

    def _read(self):
        line = self.rfile.readline()
        kind, rest = line[:1], line[1:-2].decode("utf-8")
        if kind == b"+":
            return rest
        if kind == b"-":
            raise StoreError(rest)
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self.rfile.read(n + 2)[:-2]
            return data.decode("utf-8")
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise StoreError(f"respuesta RESP desconocida: {line!r}")

    # ── ayudas para los asserts ──
    def state(self, prefix="botshort"):
        raw = self.cmd("GET", f"{prefix}:state")
        return json.loads(raw) if raw else None

    def trades(self, prefix="botshort"):
        return [json.loads(x) for x in self.cmd("LRANGE", f"{prefix}:trades", 0, -1)]

    def owner(self, prefix="botshort"):
        return self.cmd("GET", f"{prefix}:owner")


def serve(redis: FakeRedis, port: int, token: str = "test-token", not_found: bool = False,
          reply=None):
    """Expone FakeRedis con la misma interfaz HTTP que la API REST de Upstash.
    not_found=True imita un producto que no es Redis (p. ej. QStash): 404 sin cuerpo.
    reply=(código, cuerpo) responde eso a todo; se puede cambiar luego con srv.reply."""
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            canned = (404, "") if not_found else self.server.reply
            if canned is not None:
                code, text = canned
                data = text.encode()
                self.send_response(code)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            if self.headers.get("Authorization") != f"Bearer {token}":
                self.send_response(401)
                self.end_headers()
                self.wfile.write(b'{"error":"Unauthorized"}')
                return
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            try:
                out = {"result": redis.cmd(*body)}
                code = 200
            except StoreError as exc:
                out, code = {"error": str(exc)}, 400
            data = json.dumps(out).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    srv.reply = reply
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv
