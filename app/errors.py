from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """A stable, machine-readable failure exposed by the HTTP API."""

    def __init__(
        self,
        code: str,
        message: str,
        context: dict[str, Any] | None = None,
        status_code: int = 409,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}
        self.status_code = status_code

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "context": self.context}


STATUS_BY_CODE = {
    "E_SESSION_CONFLICT": 409,
    "E_PART_SEQ_INVALID": 422,
    "E_PART_MISMATCH": 409,
    "E_SCHED_QUEUE_FULL": 429,
    "E_BACKEND_TIMEOUT": 504,
    "E_READBACK_MISMATCH": 502,
    "E_QUOTA_EXCEEDED": 409,
    "E_CONFLICTING_REQUEST": 409,
    "E_BACKEND_UNAVAILABLE": 502,
    "E_BACKEND_BUSY": 503,
    "E_SESSION_NOT_ACTIVE": 409,
    "E_PARTS_INCOMPLETE": 409,
    "E_NOT_FOUND": 404,
    "E_INVALID_REQUEST": 422,
    "E_REQUEST_IN_PROGRESS": 409,
}


def business_error(code: str, message: str, **context: Any) -> BusinessError:
    return BusinessError(code, message, context, STATUS_BY_CODE.get(code, 409))
