from __future__ import annotations

from typing import Any


class ErrorCode:
    CHECKSUM_MISMATCH = "E_CHECKSUM_MISMATCH"
    INVALID_TRANSITION = "E_INVALID_TRANSITION"
    WRITER_BUSY = "E_WRITER_BUSY"
    CHECKPOINT_NOT_FOUND = "E_CHECKPOINT_NOT_FOUND"
    PRUNE_CONFLICT = "E_PRUNE_CONFLICT"
    INVALID_REQUEST = "E_INVALID_REQUEST"
    WRITE_FAILED = "E_WRITE_FAILED"
    PRUNE_IO_ERROR = "E_PRUNE_IO_ERROR"
    PROCESS_TERMINATED = "E_PROCESS_TERMINATED"
    INTERNAL = "E_INTERNAL"


HTTP_STATUS = {
    ErrorCode.CHECKSUM_MISMATCH: 409,
    ErrorCode.INVALID_TRANSITION: 409,
    ErrorCode.WRITER_BUSY: 429,
    ErrorCode.CHECKPOINT_NOT_FOUND: 404,
    ErrorCode.PRUNE_CONFLICT: 409,
    ErrorCode.INVALID_REQUEST: 422,
    ErrorCode.WRITE_FAILED: 500,
    ErrorCode.PRUNE_IO_ERROR: 500,
    ErrorCode.PROCESS_TERMINATED: 410,
    ErrorCode.INTERNAL: 500,
}


class ServiceError(Exception):
    """A stable business error safe to expose through the local HTTP API."""

    def __init__(
        self,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.http_status = http_status if http_status is not None else HTTP_STATUS.get(code, 500)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ServiceError":
        return cls(
            code=value.get("code", ErrorCode.INTERNAL),
            message=value.get("message", "unknown error"),
            details=value.get("details") or {},
        )

