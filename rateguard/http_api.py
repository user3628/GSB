"""Standard-library HTTP API for RateGuard."""

from __future__ import annotations

import json
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .engine import Engine
from .errors import BusinessError, validation_error


MAX_BODY_BYTES = 16 * 1024 * 1024


def _json_default(value: Any) -> Any:
    return str(value)


class RateGuardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], engine: Engine) -> None:
        self.engine = engine
        super().__init__(server_address, RateGuardRequestHandler)


class RateGuardRequestHandler(BaseHTTPRequestHandler):
    server: RateGuardHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"http {self.address_string()} {fmt % args}", flush=True)

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=_json_default,
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise validation_error("Content-Length is required")
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise validation_error("Content-Length must be an integer") from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise BusinessError(
                "E_VALIDATION",
                "request body is too large",
                413,
                {"max_bytes": MAX_BODY_BYTES},
            )
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise validation_error("request body must be valid UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise validation_error("request body must be a JSON object")
        return payload

    def _handle(self, method: str) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = {key: values[-1] for key, values in parse_qs(parsed.query).items()}
            if method == "GET" and path == "/health":
                return self._send(200, self.server.engine.health())
            if method == "POST" and path == "/v1/events/batches":
                return self._send(200, self.server.engine.ingest_batch(self._read_json()))
            if method == "POST" and path == "/v1/limit/decision":
                return self._send(200, self.server.engine.decide(self._read_json()))
            if method == "POST" and path == "/v1/circuit/transition":
                return self._send(
                    200, self.server.engine.manual_breaker_transition(self._read_json())
                )
            if method == "GET" and path.startswith("/v1/events/batches/"):
                parts = path.split("/")
                if len(parts) != 6:
                    raise BusinessError(
                        "E_NOT_FOUND", "batch route must be /v1/events/batches/{id}/{sequence}", 404
                    )
                batch_id = unquote(parts[4])
                try:
                    sequence = int(parts[5])
                except ValueError as exc:
                    raise validation_error("batch sequence must be an integer") from exc
                return self._send(200, self.server.engine.get_batch(batch_id, sequence))
            if method == "GET" and path.startswith("/v1/batches/"):
                parts = path.split("/")
                if len(parts) != 5:
                    raise BusinessError(
                        "E_NOT_FOUND", "batch route must be /v1/batches/{id}/{sequence}", 404
                    )
                batch_id = unquote(parts[3])
                try:
                    sequence = int(parts[4])
                except ValueError as exc:
                    raise validation_error("batch sequence must be an integer") from exc
                return self._send(200, self.server.engine.get_batch(batch_id, sequence))
            if method == "GET" and path == "/v1/circuit/state":
                route = _required_query(query, "route")
                as_of = _optional_int(query.get("as_of_ms"), "as_of_ms")
                return self._send(200, self.server.engine.get_circuit_state(route, as_of))
            if method == "GET" and path == "/v1/circuit/transitions":
                route = _required_query(query, "route")
                limit = _optional_int(query.get("limit"), "limit") or 100
                return self._send(200, self.server.engine.list_transitions(route, limit))
            if method == "GET" and path == "/v1/windows":
                route = _required_query(query, "route")
                limit = _optional_int(query.get("limit"), "limit") or 20
                return self._send(200, self.server.engine.list_windows(route, limit))
            if method == "GET" and path == "/v1/windows/stats":
                route = _required_query(query, "route")
                start = _optional_int(query.get("window_start_ms"), "window_start_ms")
                version = _optional_int(query.get("version"), "version")
                strict: str | bool | None = query.get("strict")
                return self._send(
                    200,
                    self.server.engine.get_window_stats(
                        route,
                        window_start_ms=start,
                        version=version,
                        strict=strict,
                    ),
                )
            if method == "GET" and path == "/v1/quota/derivation":
                route = _required_query(query, "route")
                version = _optional_int(query.get("composite_version"), "composite_version")
                return self._send(
                    200, self.server.engine.get_quota_derivation(route, composite_version=version)
                )
            if method == "GET" and path == "/v1/recovery/report":
                run_id = _optional_int(query.get("run_id"), "run_id")
                return self._send(200, self.server.engine.recovery_report(run_id))
            known_paths = {
                "/health",
                "/v1/events/batches",
                "/v1/limit/decision",
                "/v1/circuit/transition",
                "/v1/circuit/state",
                "/v1/circuit/transitions",
                "/v1/windows",
                "/v1/windows/stats",
                "/v1/quota/derivation",
                "/v1/recovery/report",
            }
            if path in known_paths or path.startswith("/v1/events/batches/") or path.startswith(
                "/v1/batches/"
            ):
                raise BusinessError(
                    "E_METHOD_NOT_ALLOWED",
                    f"method {method} is not allowed for this path",
                    405,
                    {"path": path, "method": method},
                )
            raise BusinessError("E_NOT_FOUND", "unknown path", 404, {"path": path})
        except BusinessError as exc:
            self._send(exc.http_status, exc.as_dict())
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as exc:  # noqa: BLE001 - last-resort stable envelope
            traceback.print_exc()
            self._send(
                500,
                BusinessError(
                    "E_INTERNAL",
                    "internal server error",
                    500,
                    {"type": type(exc).__name__},
                ).as_dict(),
            )

    def do_GET(self) -> None:  # noqa: N802
        self._handle("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._handle("POST")

    def do_PUT(self) -> None:  # noqa: N802
        self._handle("PUT")

    def do_DELETE(self) -> None:  # noqa: N802
        self._handle("DELETE")

    def do_PATCH(self) -> None:  # noqa: N802
        self._handle("PATCH")


def _required_query(query: dict[str, str], name: str) -> str:
    value = query.get(name)
    if value is None or value == "":
        raise validation_error(f"query parameter {name} is required")
    return value


def _optional_int(value: str | None, name: str) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise validation_error(f"query parameter {name} must be an integer") from exc

