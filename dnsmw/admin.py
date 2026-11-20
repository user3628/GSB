"""Read-only loopback HTTP administration interface."""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlparse

if TYPE_CHECKING:
    from .server import MiddlewareService


class AdminHttpServer:
    def __init__(self, service: "MiddlewareService", host: str, port: int) -> None:
        self.service = service
        handler = self._handler_class()
        self.httpd = ThreadingHTTPServer((host, int(port)), handler)
        self.httpd.daemon_threads = True
        self.port = int(self.httpd.server_address[1])
        self._thread: threading.Thread | None = None

    def _handler_class(self) -> type[BaseHTTPRequestHandler]:
        service = self.service

        class Handler(BaseHTTPRequestHandler):
            server_version = "dnsmw-admin/1.0"

            def log_message(self, format: str, *args: object) -> None:
                return

            def _send(self, status: int, payload: Any) -> None:
                body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                path = urlparse(self.path).path.rstrip("/") or "/"
                try:
                    if path in ("/", "/health"):
                        self._send(HTTPStatus.OK, service.health())
                    elif path == "/stats":
                        self._send(HTTPStatus.OK, service.admin_snapshot())
                    elif path == "/cache":
                        self._send(HTTPStatus.OK, service.cache.snapshot())
                    elif path.startswith("/cache/"):
                        key = unquote(path[len("/cache/") :])
                        rows = [row for row in service.cache.snapshot() if row["key"] == key]
                        self._send(HTTPStatus.OK if rows else HTTPStatus.NOT_FOUND, rows)
                    elif path == "/sessions":
                        self._send(HTTPStatus.OK, service.cache.sessions())
                    elif path == "/intents":
                        self._send(HTTPStatus.OK, service.cache.intents())
                    elif path == "/recovery":
                        self._send(HTTPStatus.OK, service.cache.latest_recovery_report())
                    else:
                        self._send(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                except Exception as exc:
                    self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

        return Handler

    def start(self) -> "AdminHttpServer":
        self._thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)


def fetch_json(url: str, timeout: float = 2.0) -> Any:
    import urllib.request

    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))
