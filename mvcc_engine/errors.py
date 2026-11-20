"""Business errors and their stable JSON wire representation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


ERROR_HTTP_STATUS = {
    "E_INVALID_ISOLATION": 400,
    "E_INVALID_REQUEST": 400,
    "E_INVALID_JSON": 400,
    "E_TXN_NOT_FOUND": 404,
    "E_TXN_NOT_ACTIVE": 409,
    "E_ROW_NOT_FOUND": 404,
    "E_EDIT_SESSION_NOT_FOUND": 404,
    "E_SAVEPOINT_NOT_FOUND": 404,
    "E_EDIT_STALE": 409,
    "E_EDIT_EXPIRED": 409,
    "E_WRITE_CONFLICT": 409,
    "E_LOCK_TIMEOUT": 408,
    "E_LOCK_QUEUE_FULL": 429,
    "E_TXN_LIMIT": 429,
    "E_DEADLOCK_VICTIM": 409,
    "E_CONFLICTING_TXN": 409,
    "E_TXN_CANCELED": 409,
    "E_ROUTE_NOT_FOUND": 404,
    "E_METHOD_NOT_ALLOWED": 405,
    "E_INTERNAL": 500,
}


@dataclass(slots=True)
class AppError(Exception):
    """An expected business failure.

    Every error carries a stable business code and machine-readable context.  The
    HTTP layer deliberately does not leak Python exception text in responses.
    """

    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None

    def __post_init__(self) -> None:
        Exception.__init__(self, f"{self.code}: {self.message}")
        if self.http_status is None:
            self.http_status = ERROR_HTTP_STATUS.get(self.code, 400)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


def fail(code: str, message: str, **context: Any) -> AppError:
    return AppError(code=code, message=message, context=context)

