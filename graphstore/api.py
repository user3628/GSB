"""基于标准库的 HTTP API（线程化 HTTPServer，仅监听本地回环）。"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING

from .errors import GraphStoreError

if TYPE_CHECKING:
    from .engine import GraphEngine


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "LocalGraphStore/1.0"

    @property
    def engine(self) -> "GraphEngine":
        return self.server.engine

    def log_message(self, fmt, *args):
        self.server.access_log.append("%s - %s" % (self.address_string(), fmt % args))

    def _write_json(self, status: int, payload: dict):
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _error(self, exc: GraphStoreError):
        status = 409 if exc.code in ("E_VERSION_RETIRED", "E_CONFLICTING_REQUEST") else 400
        self._write_json(status, {"ok": False, "error": exc.to_dict()})

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise GraphStoreError("请求体不是合法 JSON", {"detail": exc.msg})
        if not isinstance(body, dict):
            raise GraphStoreError("请求体必须是 JSON 对象", {})
        return body

    def do_GET(self):
        try:
            if self.path == "/health":
                self._write_json(200, {"ok": True, "status": "up",
                                       "version": self.engine.current_version})
            elif self.path == "/version":
                self._write_json(200, {"ok": True,
                                       "current_version": self.engine.current_version,
                                       "versions": self.engine.registry.versions()})
            elif self.path == "/recovery":
                self._write_json(200, {"ok": True,
                                       "report": self.engine.recovery_report})
            else:
                self._write_json(404, {"ok": False, "error": {
                    "code": "E_GRAPH_INCONSISTENT",
                    "message": f"未知路径 {self.path}", "context": {}}})
        except GraphStoreError as exc:
            self._error(exc)

    def do_POST(self):
        try:
            body = self._read_body()
            if self.path == "/snapshots/load":
                result = self.engine.load_snapshot(body["path"])
            elif self.path == "/updates":
                result = self.engine.apply_delta(body)
            elif self.path == "/query":
                result = self.engine.query(body)
            elif self.path == "/query/audit":
                result = self.engine.audit_modes(body)
            elif self.path == "/verify":
                result = self.engine.verify_integrity(body.get("version"))
            else:
                self._write_json(404, {"ok": False, "error": {
                    "code": "E_GRAPH_INCONSISTENT",
                    "message": f"未知路径 {self.path}", "context": {}}})
                return
            self._write_json(200, {"ok": True, "data": result})
        except GraphStoreError as exc:
            self._error(exc)
        except KeyError as exc:
            self._error(GraphStoreError("请求缺少必填字段", {"field": str(exc)}))


def build_server(engine: "GraphEngine", host: str = "127.0.0.1",
                 port: int = 8088) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), ApiHandler)
    server.engine = engine
    server.access_log = []
    return server


def serve(engine: "GraphEngine", host: str = "127.0.0.1",
          port: int = 8088) -> ThreadingHTTPServer:
    server = build_server(engine, host, port)
    thread = threading.Thread(target=server.serve_forever, name="graphstore-http",
                              daemon=True)
    thread.start()
    return server
