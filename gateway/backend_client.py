from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit


@dataclass(slots=True)
class BackendOutcome:
    success: bool
    result: dict[str, Any] | None = None
    error_code: str | None = None
    error_detail: dict[str, Any] = field(default_factory=dict)
    timed_out: bool = False
    cancelled: bool = False
    http_status: int | None = None
    elapsed_ms: float = 0.0


class ActiveCallRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._connections: dict[str, http.client.HTTPConnection] = {}

    def register(self, request_id: str, connection: http.client.HTTPConnection) -> None:
        with self._lock:
            self._connections[request_id] = connection

    def unregister(self, request_id: str, connection: http.client.HTTPConnection) -> None:
        with self._lock:
            if self._connections.get(request_id) is connection:
                self._connections.pop(request_id, None)

    def cancel(self, request_id: str) -> bool:
        with self._lock:
            connection = self._connections.get(request_id)
            if connection is None:
                return False
            sock = getattr(connection, "sock", None)
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            try:
                connection.close()
            except OSError:
                pass
            return True

    def active_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._connections)


class BackendClient:
    """One short-lived HTTP connection per dispatch.

    The timeout is a real socket read timeout.  On timeout or cancellation we
    close/shutdown the connection before returning; we never claim to terminate
    arbitrary code running inside the backend process.
    """

    def __init__(self, registry: ActiveCallRegistry, *, max_response_bytes: int = 2_000_000) -> None:
        self.registry = registry
        self.max_response_bytes = max_response_bytes

    @staticmethod
    def _infer_path(endpoint: str) -> tuple[str, int, str, bool]:
        parsed = urlsplit(endpoint)
        if parsed.scheme not in {"http", "https"} or parsed.hostname is None:
            raise ValueError("Unsupported backend endpoint")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        path = parsed.path.rstrip("/")
        if not path.endswith("/infer"):
            path = (path or "") + "/infer"
        if parsed.query:
            path += "?" + parsed.query
        return parsed.hostname, port, path, parsed.scheme == "https"

    def infer(
        self,
        *,
        endpoint: str,
        timeout_ms: int,
        request_id: str,
        tenant_id: str,
        payload: dict[str, Any],
        fault_mode: str | None,
        cancelled: bool = False,
    ) -> BackendOutcome:
        started = time.monotonic()
        if cancelled:
            return BackendOutcome(
                success=False,
                error_code="E_REQUEST_CANCELLED",
                error_detail={"reason": "cancel_requested_before_connect"},
                cancelled=True,
            )
        try:
            host, port, path, use_tls = self._infer_path(endpoint)
        except ValueError as exc:
            return BackendOutcome(
                success=False,
                error_code="E_BACKEND_UNAVAILABLE",
                error_detail={"reason": str(exc), "endpoint": endpoint},
            )

        timeout_s = max(0.001, timeout_ms / 1000.0)
        connection_cls = http.client.HTTPSConnection if use_tls else http.client.HTTPConnection
        connection = connection_cls(host, port, timeout=timeout_s)
        self.registry.register(request_id, connection)
        timed_out_event = threading.Event()

        def expire_connection() -> None:
            timed_out_event.set()
            sock = getattr(connection, "sock", None)
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            try:
                connection.close()
            except OSError:
                pass

        timeout_timer = threading.Timer(timeout_s, expire_connection)
        timeout_timer.daemon = True
        timeout_timer.start()
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Connection": "close",
            "X-Request-Id": request_id,
            "X-Tenant-Id": tenant_id,
        }
        if fault_mode:
            headers["X-Fault-Mode"] = fault_mode

        try:
            connection.request("POST", path, body=body, headers=headers)
            response = connection.getresponse()
            raw = response.read(self.max_response_bytes + 1)
            if len(raw) > self.max_response_bytes:
                return BackendOutcome(
                    success=False,
                    error_code="E_BACKEND_RESPONSE_INVALID",
                    error_detail={"reason": "response_too_large", "http_status": response.status},
                    http_status=response.status,
                    elapsed_ms=(time.monotonic() - started) * 1000.0,
                )
            if timed_out_event.is_set():
                return BackendOutcome(
                    success=False,
                    error_code="E_BACKEND_TIMEOUT",
                    error_detail={
                        "reason": "request_wall_timeout",
                        "timeout_ms": timeout_ms,
                    },
                    timed_out=True,
                    elapsed_ms=(time.monotonic() - started) * 1000.0,
                )
            try:
                parsed = json.loads(raw.decode("utf-8")) if raw else {}
            except (UnicodeDecodeError, json.JSONDecodeError):
                parsed = {"raw": raw.decode("utf-8", errors="replace")[:500]}
            if not 200 <= response.status < 300:
                code = parsed.get("code") if isinstance(parsed, dict) else None
                return BackendOutcome(
                    success=False,
                    error_code=str(code or "E_BACKEND_FAILURE"),
                    error_detail={
                        "http_status": response.status,
                        "backend_response": parsed,
                    },
                    http_status=response.status,
                    elapsed_ms=(time.monotonic() - started) * 1000.0,
                )
            if isinstance(parsed, dict) and parsed.get("ok") is False:
                return BackendOutcome(
                    success=False,
                    error_code=str(parsed.get("code") or "E_BACKEND_FAILURE"),
                    error_detail={"http_status": response.status, "backend_response": parsed},
                    http_status=response.status,
                    elapsed_ms=(time.monotonic() - started) * 1000.0,
                )
            return BackendOutcome(
                success=True,
                result=parsed if isinstance(parsed, dict) else {"value": parsed},
                http_status=response.status,
                elapsed_ms=(time.monotonic() - started) * 1000.0,
            )
        except (socket.timeout, TimeoutError) as exc:
            return BackendOutcome(
                success=False,
                error_code="E_BACKEND_TIMEOUT",
                error_detail={
                    "reason": "socket_read_timeout",
                    "timeout_ms": timeout_ms,
                    "exception": type(exc).__name__,
                },
                timed_out=True,
                elapsed_ms=(time.monotonic() - started) * 1000.0,
            )
        except (ConnectionError, OSError, http.client.HTTPException) as exc:
            if timed_out_event.is_set():
                return BackendOutcome(
                    success=False,
                    error_code="E_BACKEND_TIMEOUT",
                    error_detail={
                        "reason": "request_wall_timeout_connection_closed",
                        "timeout_ms": timeout_ms,
                        "exception": type(exc).__name__,
                    },
                    timed_out=True,
                    elapsed_ms=(time.monotonic() - started) * 1000.0,
                )
            return BackendOutcome(
                success=False,
                error_code="E_BACKEND_UNAVAILABLE",
                error_detail={
                    "reason": "connection_closed_or_failed",
                    "exception": type(exc).__name__,
                },
                cancelled=isinstance(exc, OSError) and "cancel" in type(exc).__name__.lower(),
                elapsed_ms=(time.monotonic() - started) * 1000.0,
            )
        finally:
            timeout_timer.cancel()
            self.registry.unregister(request_id, connection)
            try:
                connection.close()
            except OSError:
                pass

