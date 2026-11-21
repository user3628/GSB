"""Local deterministic HTTP test endpoints.

Three endpoint flavours are provided:
  * HealthyServer   -> returns parseable JSON; honors the ``X-Fault:
                       corrupt-body`` header by emitting truncated JSON.
  * BlackholeServer -> accepts TCP connections and never sends a single
                       byte back (a real non-responding socket).
  * unlistened port -> simply nothing bound; connection attempts are
                       refused by the OS.
"""

from __future__ import annotations

import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class _HealthyHandler(BaseHTTPRequestHandler):
    endpoint_id = "healthy"

    def _send(self, body, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/data"):
            body = json.dumps({
                "endpoint": self.endpoint_id,
                "status": "ok",
                "payload": {"samples": [1, 2, 3]},
            }).encode()
            if self.headers.get("X-Fault") == "corrupt-body":
                body = body[: max(len(body) // 2, 1)]
            self._send(body)
        elif self.path.startswith("/health"):
            self._send(b'{"status": "ok"}')
        else:
            self._send(b'{"error": "not found"}', status=404)

    def log_message(self, *args):
        pass


class HealthyServer:
    def __init__(self, port, endpoint_id="healthy", host="127.0.0.1"):
        handler = type("Handler", (_HealthyHandler,), {"endpoint_id": endpoint_id})
        self._server = ThreadingHTTPServer((host, port), handler)
        self._server.daemon_threads = True
        self.host, self.port = self._server.server_address[:2]
        self._thread = threading.Thread(target=self._server.serve_forever,
                                        kwargs={"poll_interval": 0.05},
                                        daemon=True)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()


class BlackholeServer:
    """Accepts connections and holds them open without ever responding."""

    def __init__(self, port, host="127.0.0.1"):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))
        self._sock.listen(64)
        self._sock.settimeout(0.1)
        self.host, self.port = self._sock.getsockname()[:2]
        self._stop = threading.Event()
        self._connections = []
        self._conn_lock = threading.Lock()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self._conn_lock:
                self._connections.append(conn)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)
        with self._conn_lock:
            conns, self._connections = self._connections, []
        for conn in conns:
            try:
                conn.close()
            except OSError:
                pass
        self._sock.close()

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()


def port_is_free(port, host="127.0.0.1"):
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((host, port))
    except OSError:
        return False
    finally:
        probe.close()
    return True


def free_port(host="127.0.0.1"):
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind((host, 0))
    port = probe.getsockname()[1]
    probe.close()
    return port
