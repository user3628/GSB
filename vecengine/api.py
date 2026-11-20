from __future__ import annotations

import json
import logging
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .engine import ExecutionEngine
from .errors import (
    BusinessError,
    MethodNotAllowedError,
    PlanError,
    RouteNotFoundError,
)


LOGGER = logging.getLogger("vecengine.api")
MAX_REQUEST_BYTES = 4 * 1024 * 1024


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "VecEngine/1.0"
    engine: ExecutionEngine

    def do_GET(self) -> None:  # noqa: N802 - http.server API
        try:
            self._dispatch_get()
        except BusinessError as exc:
            self._send_error(exc)
        except Exception as exc:  # defensive API boundary
            LOGGER.error("unclassified API error\n%s", traceback.format_exc())
            self._send_error(
                BusinessError(
                    f"unclassified server error: {type(exc).__name__}",
                    details={"exception_type": type(exc).__name__},
                )
            )

    def do_POST(self) -> None:  # noqa: N802 - http.server API
        try:
            self._dispatch_post()
        except BusinessError as exc:
            self._send_error(exc)
        except Exception as exc:  # defensive API boundary
            LOGGER.error("unclassified API error\n%s", traceback.format_exc())
            self._send_error(
                BusinessError(
                    f"unclassified server error: {type(exc).__name__}",
                    details={"exception_type": type(exc).__name__},
                )
            )

    def do_PUT(self) -> None:  # noqa: N802 - http.server API
        self._send_error(MethodNotAllowedError("method not allowed"))

    def do_DELETE(self) -> None:  # noqa: N802 - http.server API
        self._send_error(MethodNotAllowedError("method not allowed"))

    def _dispatch_get(self) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)
        if path == "/health":
            self._send_success(
                {
                    "status": "ok",
                    "datasets": [spec.name for spec in self.engine.registry.specs()],
                }
            )
            return
        if path == "/datasets":
            self._send_success(
                {"datasets": [spec.to_dict() for spec in self.engine.registry.specs()]}
            )
            return
        if path in ("/recovery/report", "/recovery/reports"):
            self._send_success(self.engine.recovery_reports())
            return
        parts = [part for part in path.split("/") if part]
        if len(parts) >= 2 and parts[0] == "sessions":
            session_id = parts[1]
            if len(parts) == 2:
                self._send_success(self.engine.get_session(session_id))
                return
            if len(parts) == 3 and parts[2] == "batches":
                self._send_success(self.engine.get_batches(session_id))
                return
            if len(parts) == 3 and parts[2] == "recovery":
                self._send_success(self.engine.recovery_report(session_id))
                return
            if len(parts) == 3 and parts[2] == "intents":
                self._send_success(self.engine.intents(session_id))
                return
            if len(parts) == 3 and parts[2] == "result":
                output_format = _query_one(query, "format", "json")
                if output_format == "npz":
                    path, digest = self.engine.get_result_file(session_id)
                    body = path.read_bytes()
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Disposition", f'attachment; filename="{session_id}.npz"')
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("X-Content-SHA256", digest)
                    self.end_headers()
                    self.wfile.write(body)
                    return
                if output_format != "json":
                    raise PlanError("result format must be json or npz")
                offset = _parse_nonnegative_int(_query_one(query, "offset", "0"), "offset")
                limit = _parse_nonnegative_int(_query_one(query, "limit", "100"), "limit")
                self._send_success(self.engine.get_result_page(session_id, offset, limit))
                return
        raise RouteNotFoundError("route not found", details={"path": path})

    def _dispatch_post(self) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path != "/sessions":
            raise RouteNotFoundError("route not found", details={"path": path})
        payload = self._read_json_body()
        if not isinstance(payload, dict):
            raise PlanError("request body must be a JSON object")
        unknown = set(payload) - {"request_id", "plan", "execution"}
        if unknown:
            raise PlanError("request body contains unknown fields", details={"fields": sorted(unknown)})
        if "request_id" not in payload or "plan" not in payload:
            raise PlanError("request body requires request_id and plan")
        result = self.engine.submit(
            payload["request_id"], payload["plan"], payload.get("execution", {})
        )
        status = HTTPStatus.OK if result.get("reused") else HTTPStatus.CREATED
        self._send_success(result, status=status)

    def _read_json_body(self) -> Any:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise PlanError("Content-Length is required")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise PlanError("Content-Length must be an integer") from exc
        if length <= 0 or length > MAX_REQUEST_BYTES:
            raise PlanError("request body size is invalid", details={"max_bytes": MAX_REQUEST_BYTES})
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PlanError("request body is not valid UTF-8 JSON") from exc

    def _send_success(self, data: Any, *, status: HTTPStatus = HTTPStatus.OK) -> None:
        self._send_json(status, {"ok": True, "data": data})

    def _send_error(self, error: BusinessError) -> None:
        self._send_json(
            HTTPStatus(error.http_status),
            {"ok": False, "error": error.to_dict()},
        )

    def _send_json(self, status: HTTPStatus, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        LOGGER.info("%s - %s", self.address_string(), fmt % args)


def make_handler(engine: ExecutionEngine) -> type[ApiHandler]:
    class BoundApiHandler(ApiHandler):
        pass

    BoundApiHandler.engine = engine
    return BoundApiHandler


def create_server(
    engine: ExecutionEngine,
    host: str = "127.0.0.1",
    port: int = 8765,
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), make_handler(engine))
    server.daemon_threads = True
    return server


def _query_one(query: dict[str, list[str]], name: str, default: str) -> str:
    values = query.get(name)
    if not values:
        return default
    if len(values) != 1:
        raise PlanError(f"query parameter {name!r} must appear once")
    return values[0]


def _parse_nonnegative_int(value: str, name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise PlanError(f"query parameter {name!r} must be an integer") from exc
    if parsed < 0:
        raise PlanError(f"query parameter {name!r} must be non-negative")
    return parsed
