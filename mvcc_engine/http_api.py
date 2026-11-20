"""Loopback-only JSON HTTP API."""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import unquote, urlsplit

from .engine import MVCCEngine
from .errors import AppError, fail


MAX_BODY_BYTES = 2 * 1024 * 1024


class MVCCApiServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        engine: MVCCEngine,
        quiet: bool = False,
    ) -> None:
        super().__init__(server_address, MVCCRequestHandler)
        self.engine = engine
        self.quiet = quiet
        self.started_at = time.time()


class MVCCRequestHandler(BaseHTTPRequestHandler):
    server: MVCCApiServer
    server_version = "MVCCEngine/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        if not self.server.quiet:
            sys.stderr.write(
                "%s - - [%s] %s\n"
                % (self.client_address[0], self.log_date_time_string(), fmt % args)
            )

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def do_DELETE(self) -> None:
        self._dispatch("DELETE")

    def _dispatch(self, method: str) -> None:
        try:
            body = self._read_json_body() if method in {"POST", "DELETE"} else {}
            parts = self._path_parts()
            payload = self._route(method, parts, body)
            self._send_json(200, payload)
        except AppError as error:
            self._send_json(int(error.http_status or 400), error.as_dict())
        except BrokenPipeError:
            return
        except Exception:  # pragma: no cover - exercised by fault injection only
            traceback.print_exc(file=sys.stderr)
            self._send_json(
                500,
                {
                    "ok": False,
                    "code": "E_INTERNAL",
                    "message": "unexpected internal error",
                    "context": {"path": self.path, "method": method},
                },
            )

    def _route(self, method: str, parts: list[str], body: dict[str, Any]) -> dict[str, Any]:
        engine = self.server.engine

        if method == "GET" and parts == ["health"]:
            return {
                "ok": True,
                "status": "ready",
                "pid": os.getpid(),
                "host": self.server.server_address[0],
                "port": int(self.server.server_address[1]),
                "uptime_seconds": round(time.time() - self.server.started_at, 3),
                "recovery": engine.get_recovery_report(),
            }
        if method == "GET" and parts == ["stats"]:
            return engine.stats()
        if method == "GET" and parts == ["locks"]:
            return engine.locks_snapshot()
        if method == "GET" and parts == ["tables"]:
            return engine.list_tables()
        if method == "GET" and parts == ["recovery", "report"]:
            return {"ok": True, "report": engine.get_recovery_report()}
        if method == "GET" and parts == ["config"]:
            settings = engine.settings
            return {
                "ok": True,
                "host": settings.host,
                "port": settings.port,
                "max_active_txns": settings.max_active_txns,
                "max_waiters": settings.max_waiters,
                "lock_wait_ms": settings.lock_wait_ms,
                "edit_budget_ms": settings.edit_budget_ms,
            }

        if method == "POST" and parts == ["txns"]:
            return engine.open_transaction(
                body.get("isolation"),
                body.get("txn_id") or body.get("client_txn_id"),
            )

        if len(parts) >= 2 and parts[0] == "txns":
            txn_id = unquote(parts[1])
            tail = parts[2:]
            if method == "GET" and not tail:
                return engine.get_transaction(txn_id)
            if method == "POST" and tail == ["commit"]:
                return engine.commit_transaction(txn_id, request_context=body)
            if method == "POST" and tail == ["rollback"]:
                return engine.rollback_transaction(txn_id, str(body.get("reason") or "client_request"))
            if method == "POST" and tail == ["read"]:
                return engine.read_row(txn_id, body.get("table"), body.get("row_id"))
            if method == "POST" and tail == ["query"]:
                return engine.query_rows(txn_id, body.get("table"))
            if method == "POST" and tail == ["edit-sessions"]:
                return engine.open_edit_session(txn_id, body.get("table"), body.get("row_id"))
            if method == "POST" and tail == ["save"]:
                session_id = body.get("edit_session_id") or body.get("session_id")
                return engine.save_edit_session(
                    txn_id,
                    str(session_id or ""),
                    body.get("row_version"),
                    body.get("data", body.get("edit")),
                    bool(body.get("delete", False)),
                )
            if method == "POST" and tail == ["savepoints"]:
                return engine.create_savepoint(txn_id, body.get("name"))
            if method == "POST" and tail in (["rollback-to"], ["savepoints", "rollback"]):
                return engine.rollback_to_savepoint(txn_id, body.get("name"))

            if len(tail) >= 3 and tail[0] == "rows":
                table = unquote(tail[1])
                row_id = unquote(tail[2])
                action = tail[3] if len(tail) >= 4 else ""
                if method == "GET" and len(tail) == 3:
                    return engine.read_row(txn_id, table, row_id)
                if method == "POST" and action == "update":
                    return engine.update_row(
                        txn_id,
                        table,
                        row_id,
                        body.get("row_version"),
                        body.get("data", body.get("edit")),
                    )
                if method == "POST" and action == "delete":
                    return engine.delete_row(
                        txn_id,
                        table,
                        row_id,
                        body.get("row_version"),
                    )

            if len(tail) >= 2 and tail[0] == "edit-sessions":
                session_id = unquote(tail[1])
                action = tail[2] if len(tail) >= 3 else ""
                if len(tail) == 2 and method == "GET":
                    return engine.get_edit_session(txn_id, session_id)
                if method == "POST" and action == "save":
                    return engine.save_edit_session(
                        txn_id,
                        session_id,
                        body.get("row_version"),
                        body.get("data", body.get("edit")),
                        bool(body.get("delete", False)),
                    )
                if method == "POST" and action == "delete":
                    return engine.save_edit_session(
                        txn_id,
                        session_id,
                        body.get("row_version"),
                        None,
                        True,
                    )

        raise fail(
            "E_ROUTE_NOT_FOUND",
            "no route matches this request",
            method=method,
            path="/" + "/".join(parts),
        )

    def _path_parts(self) -> list[str]:
        path = urlsplit(self.path).path
        return [unquote(part) for part in path.strip("/").split("/") if part]

    def _read_json_body(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            return {}
        try:
            length = int(raw_length)
        except ValueError:
            raise fail("E_INVALID_REQUEST", "invalid Content-Length", content_length=raw_length)
        if length <= 0:
            return {}
        if length > MAX_BODY_BYTES:
            raise fail(
                "E_INVALID_REQUEST",
                "request body is too large",
                max_body_bytes=MAX_BODY_BYTES,
                content_length=length,
            )
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise fail(
                "E_INVALID_JSON",
                "request body must be a UTF-8 JSON object",
                error=str(error),
            )
        if not isinstance(value, dict):
            raise fail(
                "E_INVALID_REQUEST",
                "request JSON body must be an object",
                received_type=type(value).__name__,
            )
        return value

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        data = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)


def build_server(
    engine: MVCCEngine,
    host: str = "127.0.0.1",
    port: int = 0,
    quiet: bool = False,
) -> MVCCApiServer:
    if host not in {"127.0.0.1", "::1", "localhost"}:
        raise ValueError("the demo server may only bind to a loopback address")
    return MVCCApiServer((host, int(port)), engine, quiet=quiet)

