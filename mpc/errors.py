from __future__ import annotations

from typing import Any


BUSINESS_CODES = {
    "E_CHECKSUM_MISMATCH": 409,
    "E_STALE_BASE": 409,
    "E_STALE_SNAPSHOT": 409,
    "E_CONFLICTING_COMMIT": 409,
    "E_INVALID_PRECISION": 422,
    "E_INVALID_REQUEST": 400,
    "E_INVALID_JSON": 400,
    "E_INVALID_POLICY": 422,
    "E_INVALID_LOSS_SCALE": 422,
    "E_SNAPSHOT_VERSION": 409,
    "E_INVALID_SNAPSHOT": 422,
    "E_UNKNOWN_SHARD": 404,
    "E_SHARD_NOT_FULL": 409,
    "E_SAMPLE_INTEGRITY": 409,
    "E_POLICY_REQUIRED": 409,
    "E_LOSS_SCALE_MISMATCH": 409,
    "E_NOT_FOUND": 404,
    "E_INTERNAL": 500,
}


class BusinessError(Exception):
    """A deliberately small, JSON-safe error type for the HTTP boundary."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.status = status if status is not None else BUSINESS_CODES.get(code, 400)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }
