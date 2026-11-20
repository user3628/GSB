from __future__ import annotations

import json
import os
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from .engine import ShardRouteService
from .errors import BusinessError, error
from .snapshot import finalize_snapshot_document
from .util import atomic_write_json


MAX_REQUEST_BYTES = 16 * 1024 * 1024


class ShardRouteHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], service: ShardRouteService):
        self.service = service
        self.stop_event = threading.Event()
        super().__init__(server_address, RouteRequestHandler)


class RouteRequestHandler(BaseHTTPRequestHandler):
    server: ShardRouteHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format_string: str, *args: Any) -> None:
        if os.environ.get("SHARDROUTE_HTTP_LOG") == "1":
            super().log_message(format_string, *args)

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, exc: BusinessError, request_id: str | None = None) -> None:
        payload = exc.to_envelope()
        payload["request_id"] = request_id or exc.context.get("request_id")
        self._send_json(exc.http_status or 400, payload)

    def _read_json_body(self) -> Any:
        content_length = self.headers.get("Content-Length")
        if content_length is None:
            raise error("E_BAD_REQUEST", "Content-Length is required")
        try:
            length = int(content_length)
        except ValueError as exc:
            raise error("E_BAD_REQUEST", "Content-Length must be an integer") from exc
        if length < 0 or length > MAX_REQUEST_BYTES:
            raise error(
                "E_BAD_REQUEST",
                "request body size is outside the allowed range",
                content_length=length,
                max_bytes=MAX_REQUEST_BYTES,
            )
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise error("E_BAD_REQUEST", "request body is not valid UTF-8 JSON", failure=str(exc)) from exc

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        parsed = urlsplit(self.path)
        path = unquote(parsed.path)
        try:
            if path == "/health":
                state = self.server.service._current_state_optional()
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "service": "shardroute-local",
                        "pid": os.getpid(),
                        "run_id": self.server.service.run_id,
                        "active_version": state.version if state else None,
                    },
                )
                return
            if path == "/state":
                self._send_json(200, self.server.service.state_summary())
                return
            if path == "/cache":
                self._send_json(
                    200,
                    {"ok": True, "cache": self.server.service.cache_summary()},
                )
                return
            if path == "/recovery/report":
                report = self.server.service.latest_recovery_report()
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "facts_available": report is not None,
                        "report": report,
                    },
                )
                return
            if path == "/snapshots":
                state_summary = self.server.service.state_summary()
                self._send_json(
                    200, {"ok": True, "snapshots": state_summary["snapshots"]}
                )
                return
            if path.startswith("/snapshots/"):
                raw_version = path[len("/snapshots/") :]
                try:
                    version = int(raw_version)
                except ValueError as exc:
                    raise error("E_BAD_REQUEST", "snapshot version must be an integer") from exc
                row = self.server.service.store.get_snapshot_row(version)
                if row is None:
                    raise error("E_NOT_FOUND", "snapshot version not found", version=version)
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "snapshot": json.loads(row["document_json"]),
                        "committed_at": row["committed_at"],
                    },
                )
                return
            if path == "/queries":
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "queries": self.server.service.store.list_query_batches(),
                    },
                )
                return
            raise error("E_NOT_FOUND", "route not found", path=path)
        except BusinessError as exc:
            self._send_error(exc)
        except Exception as exc:
            self._send_error(
                error("E_INTERNAL", "unexpected server error", failure=repr(exc))
            )

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        parsed = urlsplit(self.path)
        path = unquote(parsed.path)
        query = parse_qs(parsed.query)
        request_id: str | None = None
        try:
            body = self._read_json_body()
            if not isinstance(body, dict):
                raise error("E_BAD_REQUEST", "request body must be a JSON object")
            if isinstance(body.get("request_id"), str):
                request_id = body["request_id"]
            if path == "/snapshot":
                document = body.get("snapshot", body)
                if not isinstance(document, dict):
                    raise error("E_BAD_REQUEST", "snapshot must be a JSON object")
                crash = query.get("crash_after_prepare", ["0"])[0] in {"1", "true", "yes"}
                result = self.server.service.load_snapshot(
                    document, crash_after_prepare=crash
                )
                self._send_json(200, result)
                return
            if path == "/snapshot/finalize":
                self._send_json(200, {"ok": True, "snapshot": finalize_snapshot_document(body)})
                return
            if path == "/snapshot/patch":
                crash = query.get("crash_after_prepare", ["0"])[0] in {"1", "true", "yes"}
                result = self.server.service.apply_patch(body, crash_after_prepare=crash)
                self._send_json(200, result)
                return
            if path == "/query":
                result = self.server.service.execute_query(body)
                self._send_json(200, result)
                return
            if path == "/injections":
                injection = body.get("type") or body.get("inject")
                if not isinstance(injection, str):
                    raise error("E_BAD_REQUEST", "injection type is required")
                state = self.server.service.current_state()
                default_table = sorted(state.snapshot.tables)[0]
                request = {
                    "request_id": body.get(
                        "request_id", f"inject-{uuid.uuid4().hex[:16]}"
                    ),
                    "logical_table": body.get("logical_table", default_table),
                    "query_type": body.get("query_type", "scan"),
                    "routing_version": body.get(
                        "routing_version", state.version
                    ),
                    "limit": body.get("limit", 0),
                    "inject": injection,
                }
                self._send_json(200, self.server.service.execute_query(request))
                return
            if path == "/shutdown":
                self.server.service.close(graceful=True)
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "message": "graceful shutdown requested",
                        "run_id": self.server.service.run_id,
                    },
                )
                self.server.stop_event.set()
                return
            raise error("E_NOT_FOUND", "route not found", path=path)
        except BusinessError as exc:
            self._send_error(exc, request_id=request_id)
        except Exception as exc:
            self._send_error(
                error("E_INTERNAL", "unexpected server error", failure=repr(exc)),
                request_id=request_id,
            )


def create_http_server(
    service: ShardRouteService,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
) -> ShardRouteHTTPServer:
    return ShardRouteHTTPServer((host, port), service)


def write_port_file(path: str | Path, server: ShardRouteHTTPServer) -> None:
    atomic_write_json(
        path,
        {
            "host": server.server_address[0],
            "port": server.server_address[1],
            "pid": os.getpid(),
        },
    )

