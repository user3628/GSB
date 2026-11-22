"""Deterministic local test service: echo or hang TCP mode plus admin metrics."""

from __future__ import annotations

import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class ProbeTestService:
    def __init__(
        self,
        *,
        mode: str = "echo",
        probe_port: int = 0,
        admin_port: int = 0,
        host: str = "127.0.0.1",
        hang_seconds: float = 30.0,
    ) -> None:
        if mode not in {"echo", "hang", "refuse"}:
            raise ValueError("mode must be echo, hang or refuse")
        self.mode = mode
        self.host = host
        self.requested_probe_port = probe_port
        self.requested_admin_port = admin_port
        self.hang_seconds = hang_seconds
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._listener: socket.socket | None = None
        self._reported_probe_port: int | None = None
        self._accept_thread: threading.Thread | None = None
        self._admin: ThreadingHTTPServer | None = None
        self._admin_thread: threading.Thread | None = None
        self._started_monotonic = time.monotonic()
        self._metrics: dict[str, Any] = {
            "accepted_connections": 0,
            "completed_probes": 0,
            "hung_connections": 0,
            "bytes_in": 0,
            "bytes_out": 0,
            "errors": 0,
        }

    @property
    def probe_port(self) -> int:
        if self._listener is not None:
            return int(self._listener.getsockname()[1])
        if self._reported_probe_port is not None:
            return self._reported_probe_port
        raise RuntimeError("service not started")

    @property
    def admin_port(self) -> int:
        if self._admin is None:
            raise RuntimeError("service not started")
        return int(self._admin.server_address[1])

    def start(self) -> "ProbeTestService":
        if self.mode == "refuse":
            # Reserve then release a loopback port. No listener remains, so a
            # probe receives a real ECONNREFUSED from the local TCP stack.
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe_socket:
                probe_socket.bind((self.host, self.requested_probe_port))
                self._reported_probe_port = int(probe_socket.getsockname()[1])
        else:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.host, self.requested_probe_port))
            listener.listen(64)
            listener.settimeout(0.2)
            self._listener = listener
            self._accept_thread = threading.Thread(
                target=self._accept_loop, name=f"diaglab-{self.mode}-listener", daemon=True
            )
            self._accept_thread.start()

        service = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/health":
                    payload = {"ok": True, "mode": service.mode}
                    service._send_json(self, 200, payload)
                elif self.path == "/metrics":
                    service._send_json(self, 200, service.metrics())
                else:
                    service._send_json(self, 404, {"ok": False, "code": "E_NOT_FOUND"})

            def log_message(self, format: str, *args: object) -> None:
                return

        admin = ThreadingHTTPServer((self.host, self.requested_admin_port), Handler)
        admin.daemon_threads = True
        self._admin = admin
        self._admin_thread = threading.Thread(
            target=admin.serve_forever, name="diaglab-admin", daemon=True
        )
        self._admin_thread.start()
        self._started_monotonic = time.monotonic()
        return self

    @staticmethod
    def _send_json(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(data)))
        handler.send_header("Connection", "close")
        handler.end_headers()
        handler.wfile.write(data)

    def _accept_loop(self) -> None:
        assert self._listener is not None
        while not self._stop.is_set():
            try:
                connection, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self._lock:
                self._metrics["accepted_connections"] += 1
            threading.Thread(
                target=self._handle_connection,
                args=(connection,),
                name="diaglab-probe-connection",
                daemon=True,
            ).start()

    def _handle_connection(self, connection: socket.socket) -> None:
        try:
            connection.settimeout(1.0)
            if self.mode == "hang":
                with self._lock:
                    self._metrics["hung_connections"] += 1
                deadline = time.monotonic() + self.hang_seconds
                while not self._stop.is_set() and time.monotonic() < deadline:
                    time.sleep(0.05)
                return

            received = bytearray()
            while True:
                try:
                    chunk = connection.recv(65_536)
                except socket.timeout:
                    if received:
                        break
                    continue
                if not chunk:
                    break
                received += chunk
                if len(received) >= 65_507:
                    break
            with self._lock:
                self._metrics["bytes_in"] += len(received)
            if received:
                connection.sendall(bytes(received))
                with self._lock:
                    self._metrics["bytes_out"] += len(received)
                    self._metrics["completed_probes"] += 1
        except (BrokenPipeError, ConnectionResetError, OSError):
            with self._lock:
                self._metrics["errors"] += 1
        finally:
            try:
                connection.close()
            except OSError:
                pass

    def metrics(self) -> dict[str, Any]:
        with self._lock:
            metrics = dict(self._metrics)
        uptime_ms = max(1, int((time.monotonic() - self._started_monotonic) * 1_000))
        throughput_bytes_per_sec = (metrics["bytes_in"] + metrics["bytes_out"]) * 1_000.0 / uptime_ms
        return {
            "mode": self.mode,
            "uptime_ms": uptime_ms,
            **metrics,
            "throughput_bytes_per_sec": throughput_bytes_per_sec,
            "error_count": metrics["errors"],
            "probe_port": self.probe_port,
            "admin_port": self.admin_port,
        }

    def stop(self) -> None:
        self._stop.set()
        if self._admin is not None:
            self._admin.shutdown()
            self._admin.server_close()
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2)
        if self._admin_thread is not None:
            self._admin_thread.join(timeout=2)

    def __enter__(self) -> "ProbeTestService":
        return self.start()

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()


def free_loopback_port() -> int:
    """Return a currently unused loopback TCP port (used to demonstrate refusal)."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
