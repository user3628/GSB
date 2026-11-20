from __future__ import annotations

from typing import Any


HTTP_STATUS = {
    "E_INVALID_REQUEST": 400,
    "E_SNAPSHOT_INVALID": 400,
    "E_DATASET_NOT_FOUND": 404,
    "E_REQUEST_NOT_FOUND": 404,
    "E_REQUEST_CONFLICT": 409,
    "E_REQUEST_NOT_COMMITTED": 409,
    "E_EXECUTION_ABORTED": 409,
    "E_SKEW_LIMIT": 422,
    "E_PRECISION_BUDGET": 422,
    "E_NUMERIC_OVERFLOW": 422,
    "E_INTERNAL": 500,
}


class EngineError(Exception):
    """A business error that is safe to expose through the local API."""

    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    @property
    def http_status(self) -> int:
        return HTTP_STATUS.get(self.code, 500)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }

    @classmethod
    def from_stored(cls, stored: dict[str, Any]) -> "EngineError":
        error = stored.get("error", stored)
        return cls(
            str(error.get("code", "E_INTERNAL")),
            str(error.get("message", "execution failed")),
            error.get("details") if isinstance(error.get("details"), dict) else {},
        )
