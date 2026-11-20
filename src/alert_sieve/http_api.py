from __future__ import annotations

import json
import traceback
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from .errors import BusinessError, snapshot_invalid
from .store import Store

MAX_BODY_BYTES = 16 * 1024 * 1024


def _json_bytes(body: Any) -> bytes:
    return (json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _bool_query(values: dict[str, list[str]], key: str, default: bool = False) -> bool:
    raw = values.get(key, [None])[0]
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    raise BusinessError("E_SNAPSHOT_INVALID", f"{key} must be a boolean")


def _int_query(values: dict[str, list[str]], key: str, default: int) -> int:
    raw = values.get(key, [None])[0]
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise BusinessError("E_SNAPSHOT_INVALID", f"{key} must be an integer") from exc


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "AlertSieve/0.1"
    protocol_version = "HTTP/1.1"
    store: Store
    enable_fault_injection = False

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        return

    def _request_id(self) -> str:
        raw = self.headers.get("X-Request-ID", "")
        if raw and len(raw) <= 128:
            return raw
        return uuid.uuid4().hex

    def _send(self, status: int, body: Any) -> None:
        payload = _json_bytes(body)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _send_success(self, request_id: str, data: Any, status: int = 200) -> None:
        self._send(status, {"ok": True, "request_id": request_id, "data": data})

    def _send_error(self, request_id: str, error: BusinessError) -> None:
        self._send(error.http_status, error.as_body(request_id))

    def _read_json(self) -> Any:
        length_raw = self.headers.get("Content-Length")
        if length_raw is None:
            raise snapshot_invalid("Content-Length is required")
        try:
            length = int(length_raw)
        except ValueError as exc:
            raise snapshot_invalid("Content-Length must be an integer") from exc
        if length < 0 or length > MAX_BODY_BYTES:
            raise snapshot_invalid("request body is too large", max_bytes=MAX_BODY_BYTES)
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise snapshot_invalid("request body must be valid UTF-8 JSON", reason=str(exc)) from exc

    def _fault_options(self) -> tuple[bool, int]:
        if not self.enable_fault_injection:
            return False, 0
        crash = self.headers.get("X-Debug-Crash-After-Products", "") in ("1", "true", "yes")
        sleep_raw = self.headers.get("X-Debug-Sleep-After-Products-Ms", "0")
        try:
            sleep_ms = int(sleep_raw) if sleep_raw else 0
        except ValueError as exc:
            raise snapshot_invalid("X-Debug-Sleep-After-Products-Ms must be an integer") from exc
        if sleep_ms < 0 or sleep_ms > 60_000:
            raise snapshot_invalid("fault sleep must be between 0 and 60000 ms")
        return crash, sleep_ms

    def do_GET(self) -> None:  # noqa: N802
        request_id = self._request_id()
        try:
            parsed = urlsplit(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query, keep_blank_values=False)
            if path == "/v1/health":
                self._send_success(request_id, self.store.health())
                return
            if path == "/v1/state":
                self._send_success(request_id, self.store.query_state())
                return
            if path == "/v1/reduction":
                include_alerts = _bool_query(query, "include_alerts", False)
                self._send_success(request_id, self.store.query_reduction(include_alerts=include_alerts))
                return
            if path == "/v1/suppression-index":
                fingerprint = query.get("fingerprint", [None])[0]
                self._send_success(request_id, self.store.query_suppression_index(fingerprint))
                return
            if path == "/v1/root-causes":
                alert_uid = query.get("alert_uid", [None])[0]
                service = query.get("service", [None])[0]
                limit = _int_query(query, "limit", 200)
                self._send_success(
                    request_id,
                    self.store.query_root_causes(alert_uid=alert_uid, service=service, limit=limit),
                )
                return
            if path == "/v1/invalidations":
                limit = _int_query(query, "limit", 100)
                self._send_success(request_id, self.store.query_invalidations(limit=limit))
                return
            if path.startswith("/v1/invalidations/"):
                raw_id = unquote(path.rsplit("/", 1)[-1])
                try:
                    event_id = int(raw_id)
                except ValueError as exc:
                    raise BusinessError("E_SNAPSHOT_INVALID", "invalidation event id must be an integer") from exc
                self._send_success(request_id, self.store.query_invalidation(event_id))
                return
            if path == "/v1/recovery/report":
                self._send_success(request_id, self.store.query_recovery_report())
                return
            if path.startswith("/v1/batches/"):
                batch_id = unquote(path.rsplit("/", 1)[-1])
                self._send_success(request_id, self.store.query_batch(batch_id))
                return
            raise BusinessError(
                "E_ORDER_NOT_FOUND",
                "route was not found",
                details={"method": "GET", "path": parsed.path},
            )
        except BusinessError as exc:
            self._send_error(request_id, exc)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            traceback.print_exc()
            self._send_error(
                request_id,
                BusinessError("E_REBUILD_CONFLICT", "unexpected server failure", details={"error": str(exc)}),
            )


    def do_PUT(self) -> None:  # noqa: N802
        request_id = self._request_id()
        self._send_error(
            request_id,
            BusinessError(
                "E_ORDER_NOT_FOUND",
                "HTTP method is not supported by this local API",
                details={"method": "PUT", "path": urlsplit(self.path).path},
            ),
        )

    def do_PATCH(self) -> None:  # noqa: N802
        request_id = self._request_id()
        self._send_error(
            request_id,
            BusinessError(
                "E_ORDER_NOT_FOUND",
                "HTTP method is not supported by this local API",
                details={"method": "PATCH", "path": urlsplit(self.path).path},
            ),
        )

    def do_DELETE(self) -> None:  # noqa: N802
        request_id = self._request_id()
        self._send_error(
            request_id,
            BusinessError(
                "E_ORDER_NOT_FOUND",
                "HTTP method is not supported by this local API",
                details={"method": "DELETE", "path": urlsplit(self.path).path},
            ),
        )

    def do_POST(self) -> None:  # noqa: N802
        request_id = self._request_id()
        try:
            parsed = urlsplit(self.path)
            path = parsed.path.rstrip("/") or "/"
            body = self._read_json()
            crash, sleep_ms = self._fault_options()
            common = {
                "request_id": request_id,
                "fault_crash_after_products": crash,
                "fault_sleep_after_products_ms": sleep_ms,
            }
            if path == "/v1/alerts/snapshot":
                result = self.store.submit_alert_snapshot(body, **common)
            elif path in ("/v1/alerts/increments", "/v1/alerts/batch"):
                result = self.store.submit_alert_increment(body, **common)
            elif path == "/v1/topology/snapshot":
                result = self.store.submit_topology_snapshot(body, **common)
            elif path == "/v1/topology/increments":
                result = self.store.submit_topology_increment(body, **common)
            elif path == "/v1/rules/snapshot":
                result = self.store.submit_rules_snapshot(body, **common)
            elif path == "/v1/rules/increments":
                result = self.store.submit_rules_increment(body, **common)
            else:
                raise BusinessError(
                    "E_ORDER_NOT_FOUND",
                    "route was not found",
                    details={"method": "POST", "path": parsed.path},
                )
            self._send_success(request_id, result)
        except BusinessError as exc:
            self._send_error(request_id, exc)
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            traceback.print_exc()
            self._send_error(
                request_id,
                BusinessError("E_REBUILD_CONFLICT", "unexpected server failure", details={"error": str(exc)}),
            )


def make_server(
    store: Store,
    host: str = "127.0.0.1",
    port: int = 8765,
    *,
    enable_fault_injection: bool = False,
) -> ThreadingHTTPServer:
    handler = type(
        "BoundApiHandler",
        (ApiHandler,),
        {
            "store": store,
            "enable_fault_injection": enable_fault_injection,
        },
    )
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    return server
