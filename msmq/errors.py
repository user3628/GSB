from __future__ import annotations

from typing import Any


class MQError(Exception):
    """A business error that is safe to expose through the local HTTP API."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        http_status: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


E_BAD_REQUEST = "E_BAD_REQUEST"
E_NOT_FOUND = "E_NOT_FOUND"
E_QUEUE_EXISTS = "E_QUEUE_EXISTS"
E_QUEUE_NOT_FOUND = "E_QUEUE_NOT_FOUND"
E_QUEUE_MODE_MISMATCH = "E_QUEUE_MODE_MISMATCH"
E_QUEUE_BUSY = "E_QUEUE_BUSY"
E_MESSAGE_EXISTS = "E_MESSAGE_EXISTS"
E_MESSAGE_NOT_FOUND = "E_MESSAGE_NOT_FOUND"
E_MESSAGE_EXPIRED = "E_MESSAGE_EXPIRED"
E_LEASE_MISMATCH = "E_LEASE_MISMATCH"
E_DUPLICATE_ACK = "E_DUPLICATE_ACK"
E_ALREADY_ACKED = "E_ALREADY_ACKED"
E_FORMAT_UNKNOWN = "E_FORMAT_UNKNOWN"
E_FORMAT_MISMATCH = "E_FORMAT_MISMATCH"
E_FORMAT_INVALID = "E_FORMAT_INVALID"
E_FORMAT_INCOMPATIBLE = "E_FORMAT_INCOMPATIBLE"
E_RECORD_TOO_LARGE = "E_RECORD_TOO_LARGE"
E_BATCH_SPLIT_REQUIRED = "E_BATCH_SPLIT_REQUIRED"
E_CONFLICT_AMBIGUOUS = "E_CONFLICT_AMBIGUOUS"
E_TIME_PRECISION_UNSUPPORTED = "E_TIME_PRECISION_UNSUPPORTED"
E_INTERNAL = "E_INTERNAL"

