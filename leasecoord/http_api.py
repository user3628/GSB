"""Local JSON-over-HTTP adapter for the coordinator."""

from __future__ import annotations

import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .coordinator import Coordinator
from .errors import BusinessError, business_error

MAX_BODY_BYTES = 1_048_576


class CoordinatorHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        coordinator: Coordinator,
        *,
        allow_time_override: bool = False,
    ) -> None:
        super().__init__(server_address, CoordinatorRequestHandler)
        self.coordinator = coordinator
        self.allow_time_override = allow_time_override


class CoordinatorRequestHandler(BaseHTTPRequestHandler):
    server: CoordinatorHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("LEASE_HTTP_LOG", "0") in {"1", "true", "TRUE"}:
            super().log_message(fmt, *args)

    def _send(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any]:
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            return {}
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise business_error(
                "E_BAD_REQUEST", "invalid Content-Length", content_length=raw_length
            ) from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise business_error(
                "E_BAD_REQUEST",
                "request body is too large",
                max_body_bytes=MAX_BODY_BYTES,
                content_length=length,
            )
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise business_error(
                "E_BAD_REQUEST", "request body must be valid UTF-8 JSON"
            ) from exc
        if not isinstance(value, dict):
            raise business_error("E_BAD_REQUEST", "request JSON must be an object")
        return value

    def _test_now(self, body: dict[str, Any], query: dict[str, list[str]]) -> int | None:
        value: Any = body.pop("now_ms", None)
        if value is None and query.get("now_ms"):
            value = query["now_ms"][-1]
        header = self.headers.get("X-Coordinator-Now-Ms")
        if value is None and header is not None:
            value = header
        if value is None:
            return None
        if not self.server.allow_time_override:
            raise business_error(
                "E_BAD_REQUEST",
                "time override is disabled on this server",
                hint="start with --allow-time-override for deterministic local tests",
            )
        try:
            now = int(value)
        except (TypeError, ValueError) as exc:
            raise business_error(
                "E_BAD_REQUEST", "now_ms must be an integer", now_ms=value
            ) from exc
        if now < 0:
            raise business_error(
                "E_BAD_REQUEST", "now_ms must be non-negative", now_ms=now
            )
        return now

    @staticmethod
    def _required(body: dict[str, Any], field: str) -> Any:
        if field not in body:
            raise business_error(
                "E_BAD_REQUEST", f"missing required field: {field}", field=field
            )
        return body[field]

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path.rstrip("/") or "/"
        coordinator = self.server.coordinator
        try:
            now_ms = self._test_now({}, query)
            if path == "/healthz":
                self._send(
                    200,
                    {
                        "ok": True,
                        "status": "ok",
                        "instance_id": coordinator.instance_id,
                    },
                )
                return
            if path == "/v1/config":
                self._send(200, {"ok": True, **coordinator.get_config()})
                return
            if path == "/v1/leader":
                self._send(200, {"ok": True, **coordinator.get_leader(now_ms=now_ms)})
                return
            if path == "/v1/lease":
                self._send(200, {"ok": True, **coordinator.get_lease(now_ms=now_ms)})
                return
            if path == "/v1/nodes":
                nodes = coordinator.list_nodes(now_ms=now_ms)
                self._send(
                    200,
                    {"ok": True, "node_count": len(nodes), "nodes": nodes},
                )
                return
            if path == "/v1/recovery":
                self._send(
                    200,
                    {"ok": True, **coordinator.get_recovery_report()},
                )
                return
            if path == "/v1/events":
                raw_limit = query.get("limit", ["100"])[-1]
                try:
                    limit = int(raw_limit)
                except ValueError as exc:
                    raise business_error(
                        "E_BAD_REQUEST", "limit must be an integer", limit=raw_limit
                    ) from exc
                self._send(
                    200,
                    {"ok": True, "events": coordinator.get_events(limit)},
                )
                return
            node_match = re.fullmatch(r"/v1/nodes/([^/]+)", path)
            if node_match:
                node_id = unquote(node_match.group(1))
                self._send(
                    200,
                    {"ok": True, **coordinator.get_node(node_id, now_ms=now_ms)},
                )
                return
            self._send(
                404,
                {
                    "ok": False,
                    "error": {
                        "code": "E_NOT_FOUND",
                        "message": "unknown endpoint",
                        "context": {"method": "GET", "path": path},
                    },
                },
            )
        except BusinessError as exc:
            self._send(exc.http_status, {"ok": False, "error": exc.to_dict()})
        except Exception as exc:  # pragma: no cover - last-resort HTTP boundary
            self._send(
                500,
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": "internal server error",
                        "context": {"exception_type": type(exc).__name__},
                    },
                },
            )

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        path = parsed.path.rstrip("/") or "/"
        coordinator = self.server.coordinator
        try:
            body = self._read_json()
            now_ms = self._test_now(body, query)
            request_id = body.pop("request_id", None)
            if request_id is not None and not isinstance(request_id, str):
                raise business_error(
                    "E_BAD_REQUEST", "request_id must be a string"
                )

            if path == "/v1/nodes":
                result = coordinator.register_node(
                    self._required(body, "id"),
                    self._required(body, "priority"),
                    self._required(body, "lease_ttl_ms"),
                    last_heartbeat_ms=body.get("last_heartbeat_ms"),
                    status=body.get("status"),
                    now_ms=now_ms,
                )
                self._send(200, {"ok": True, **result, "request_id": request_id})
                return

            node_heartbeat = re.fullmatch(r"/v1/nodes/([^/]+)/heartbeat", path)
            if node_heartbeat:
                node_id = unquote(node_heartbeat.group(1))
                result = coordinator.heartbeat(
                    node_id,
                    heartbeat_ms=body.get("heartbeat_ms"),
                    now_ms=now_ms,
                )
                self._send(200, {"ok": True, **result, "request_id": request_id})
                return

            node_transition = re.fullmatch(
                r"/v1/nodes/([^/]+)/(?:admin-)?transition", path
            )
            if node_transition:
                node_id = unquote(node_transition.group(1))
                result = coordinator.admin_transition(
                    node_id,
                    self._required(body, "target_status"),
                    expected_lease_version=body.get("expected_lease_version"),
                    now_ms=now_ms,
                )
                self._send(200, {"ok": True, **result, "request_id": request_id})
                return

            if path in {"/v1/elections/campaign", "/v1/campaign"}:
                result = coordinator.campaign(
                    self._required(body, "node_id"),
                    expected_lease_version=body.get("expected_lease_version"),
                    now_ms=now_ms,
                    request_id=request_id,
                )
                self._send(200, {"ok": True, **result, "request_id": request_id})
                return

            if path == "/v1/lease/renew":
                result = coordinator.renew(
                    self._required(body, "node_id"),
                    expected_lease_version=body.get("expected_lease_version"),
                    now_ms=now_ms,
                    request_id=request_id,
                )
                self._send(200, {"ok": True, **result, "request_id": request_id})
                return

            if path == "/v1/lease/resign":
                result = coordinator.resign(
                    self._required(body, "node_id"),
                    expected_lease_version=body.get("expected_lease_version"),
                    now_ms=now_ms,
                    request_id=request_id,
                )
                self._send(200, {"ok": True, **result, "request_id": request_id})
                return

            if path == "/v1/lease/failover":
                result = coordinator.failover(
                    reason=body.get("reason"),
                    expected_lease_version=body.get("expected_lease_version"),
                    force=body.get("force", False),
                    now_ms=now_ms,
                    request_id=request_id,
                )
                self._send(200, {"ok": True, **result, "request_id": request_id})
                return

            self._send(
                404,
                {
                    "ok": False,
                    "error": {
                        "code": "E_NOT_FOUND",
                        "message": "unknown endpoint",
                        "context": {"method": "POST", "path": path},
                    },
                },
            )
        except BusinessError as exc:
            self._send(exc.http_status, {"ok": False, "error": exc.to_dict()})
        except Exception as exc:  # pragma: no cover - last-resort HTTP boundary
            self._send(
                500,
                {
                    "ok": False,
                    "error": {
                        "code": "E_INTERNAL",
                        "message": "internal server error",
                        "context": {"exception_type": type(exc).__name__},
                    },
                },
            )


def create_server(
    coordinator: Coordinator,
    host: str = "127.0.0.1",
    port: int = 8765,
    *,
    allow_time_override: bool | None = None,
) -> CoordinatorHTTPServer:
    if allow_time_override is None:
        allow_time_override = coordinator.config.allow_time_override
    return CoordinatorHTTPServer(
        (host, port),
        coordinator,
        allow_time_override=allow_time_override,
    )
