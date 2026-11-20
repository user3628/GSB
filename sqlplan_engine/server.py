"""Local standard-library HTTP service."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from .business import BadRequestError, EngineError, NotFoundError
from .expressions import canonical_json
from .service import PlanService, build_injection_from_request


MAX_BODY_BYTES = 2 * 1024 * 1024


class PlanHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], service: PlanService) -> None:
        super().__init__(server_address, PlanRequestHandler)
        self.service = service
        self.shutdown_requested = False

    def request_shutdown(self) -> None:
        if self.shutdown_requested:
            return
        self.shutdown_requested = True
        threading.Thread(target=self.shutdown, name="http-shutdown", daemon=True).start()


class PlanRequestHandler(BaseHTTPRequestHandler):
    server: PlanHTTPServer
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._send_error(EngineError("E_BAD_REQUEST", "HTTP method is not supported", {"method": "PUT"}))

    def do_DELETE(self) -> None:  # noqa: N802
        self._send_error(
            EngineError("E_BAD_REQUEST", "HTTP method is not supported", {"method": "DELETE"})
        )

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _dispatch(self, method: str) -> None:
        request_id: str | None = None
        try:
            parsed = urlsplit(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query, keep_blank_values=True)
            if method == "GET":
                self._handle_get(path, query)
                return
            body = self._read_json_body()
            request_id = (
                body.get("request_id")
                or query.get("request_id", [None])[0]
                or self.headers.get("X-Request-ID")
            )
            self._handle_post(path, query, body, request_id)
        except BrokenPipeError:
            return
        except EngineError as exc:
            self._send_error(exc, request_id=request_id)
        except Exception as exc:
            wrapped = EngineError(
                "E_INTERNAL",
                "unhandled local service failure",
                {"exception_type": type(exc).__name__, "detail": str(exc)[:500]},
            )
            self._send_error(wrapped, request_id=request_id)

    def _handle_get(self, path: str, query: dict[str, list[str]]) -> None:
        service = self.server.service
        if path == "/health":
            self._send_json(200, service.health())
            return
        if path == "/v1/samples":
            self._send_json(200, service.samples())
            return
        if path == "/v1/recovery-report":
            self._send_json(200, service.recovery())
            return
        prefix = "/v1/plans/"
        if path.startswith(prefix):
            request_id = unquote(path[len(prefix) :])
            if not request_id or "/" in request_id:
                raise NotFoundError("request", request_id)
            self._send_json(200, service.get_request(request_id))
            return
        prefix = "/v1/requests/"
        if path.startswith(prefix):
            request_id = unquote(path[len(prefix) :])
            if not request_id or "/" in request_id:
                raise NotFoundError("request", request_id)
            self._send_json(200, service.get_request(request_id))
            return
        prefix = "/v1/intents/"
        if path.startswith(prefix):
            intent_id = unquote(path[len(prefix) :])
            if not intent_id or "/" in intent_id:
                raise NotFoundError("intent", intent_id)
            self._send_json(200, service.get_intent(intent_id))
            return
        if path == "/v1/intents":
            request_id = query.get("request_id", [None])[0]
            if not request_id:
                raise BadRequestError("request_id query parameter is required")
            self._send_json(200, service.intents_for_request(request_id))
            return
        raise NotFoundError("route", path)

    def _handle_post(
        self,
        path: str,
        query: dict[str, list[str]],
        body: dict[str, Any],
        request_id: str | None,
    ) -> None:
        if path == "/v1/admin/shutdown":
            if body.get("confirm") is not True:
                raise BadRequestError("graceful shutdown requires {\"confirm\": true}")
            self._send_json(
                200,
                {
                    "ok": True,
                    "business_code": "OK",
                    "message": "graceful shutdown scheduled; a complete exit summary is emitted by the process",
                },
            )
            self.server.request_shutdown()
            return
        if path != "/v1/plans":
            raise NotFoundError("route", path)
        sql = body.get("sql", query.get("sql", [None])[0])
        if not isinstance(sql, str):
            raise BadRequestError("sql must be provided as a JSON string")
        injection = build_injection_from_request(body, query)
        if request_id is None:
            request_id = query.get("request_id", [None])[0] or body.get("request_id")
        response = self.server.service.submit(
            sql,
            request_id=request_id if isinstance(request_id, str) else None,
            injection=injection,
        )
        self._send_json(200, response)

    def _read_json_body(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length", "0")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise BadRequestError("invalid Content-Length") from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise BadRequestError(
                "request body is missing or too large", max_bytes=MAX_BODY_BYTES
            )
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BadRequestError("request body must be valid UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise BadRequestError("request body must be a JSON object")
        return payload

    def _send_error(self, error: EngineError, request_id: str | None = None) -> None:
        self._send_json(error.http_status, error.to_dict(request_id=request_id))

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        raw = canonical_json(payload).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return


def create_server(service: PlanService, host: str = "127.0.0.1", port: int = 0) -> PlanHTTPServer:
    return PlanHTTPServer((host, port), service)

