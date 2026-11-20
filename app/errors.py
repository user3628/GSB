from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """An error that is safe to expose through the local HTTP API."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}

    def to_body(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


E_STALE_SNAPSHOT = "E_STALE_SNAPSHOT"
E_SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
E_SNAPSHOT_NOT_FOUND = "E_SNAPSHOT_NOT_FOUND"
E_BATCH_NOT_FOUND = "E_BATCH_NOT_FOUND"
E_BATCH_INCOMPLETE = "E_BATCH_INCOMPLETE"
E_ORDER_NOT_FOUND = "E_ORDER_NOT_FOUND"
E_ROUTE_AMBIGUOUS = "E_ROUTE_AMBIGUOUS"
E_INVALID_ARGUMENT = "E_INVALID_ARGUMENT"
E_INTERNAL_ERROR = "E_INTERNAL_ERROR"
