from __future__ import annotations

from typing import Any


STATUS_BY_CODE: dict[str, int] = {
    "E_REQUEST_INVALID": 400,
    "E_PART_INVALID": 400,
    "E_PART_OUT_OF_RANGE": 400,
    "E_PART_MISMATCH": 409,
    "E_PART_MISSING": 409,
    "E_SIZE_MISMATCH": 409,
    "E_SESSION_NOT_FOUND": 404,
    "E_SESSION_EXISTS": 409,
    "E_SESSION_CONFLICT": 409,
    "E_SESSION_STALE": 409,
    "E_SESSION_NOT_ACTIVE": 409,
    "E_OBJECT_NOT_FOUND": 404,
    "E_CONTENT_NOT_FOUND": 404,
    "E_OBJECT_EXISTS": 409,
    "E_VERSION_NOT_FOUND": 404,
    "E_VERSION_RETIRED": 410,
    "E_LEASE_NOT_FOUND": 404,
    "E_CRASH_DRILL_DISABLED": 403,
    "E_RECOVERY_NOT_FOUND": 404,
    "E_ROUTE_NOT_FOUND": 404,
    "E_HTTP_ERROR": 400,
    "E_INTERNAL": 500,
}


class BusinessError(Exception):
    """A stable, machine-readable domain error."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.http_status = http_status if http_status is not None else STATUS_BY_CODE.get(code, 400)

    def as_dict(self) -> dict[str, Any]:
        error: dict[str, Any] = {"code": self.code, "message": self.message}
        error.update(self.details)
        return {"ok": False, "error": error}

