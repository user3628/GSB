from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        detail: dict[str, Any] | None = None,
        http_status: int = 400,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail or {}
        self.http_status = http_status


E_SPEC_INVALID = "E_SPEC_INVALID"
E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_CONCURRENCY_LIMIT = "E_CONCURRENCY_LIMIT"
E_QUOTA_DENIED = "E_QUOTA_DENIED"
E_START_TIMEOUT = "E_START_TIMEOUT"
E_EXEC_TIMEOUT = "E_EXEC_TIMEOUT"
E_START_FAILED = "E_START_FAILED"
E_EXEC_FAILED = "E_EXEC_FAILED"
E_CONFLICTING_CALL = "E_CONFLICTING_CALL"
E_TRACE_OUT_OF_ORDER = "E_TRACE_OUT_OF_ORDER"
E_QUEUE_TIMEOUT = "E_QUEUE_TIMEOUT"
E_RECOVERED_INTERRUPTED = "E_RECOVERED_INTERRUPTED"
E_NOT_FOUND = "E_NOT_FOUND"
E_INTERNAL = "E_INTERNAL"


def error_payload(exc: BusinessError) -> dict[str, Any]:
    return {
        "ok": False,
        "business_code": exc.code,
        "message": exc.message,
        "detail": exc.detail,
    }

