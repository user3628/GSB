from __future__ import annotations

from typing import Any


ERROR_HTTP_STATUS: dict[str, int] = {
    "E_ALERT_INVALID": 400,
    "E_STALE_BASE": 409,
    "E_CONFLICTING_ALERT": 409,
    "E_SNAPSHOT_INVALID": 400,
    "E_ORDER_NOT_FOUND": 404,
    "E_REBUILD_CONFLICT": 409,
}


class BusinessError(Exception):
    """A stable public business error with a machine-readable code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: Any = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details
        self.http_status = http_status or ERROR_HTTP_STATUS.get(code, 400)

    def as_body(self, request_id: str | None = None) -> dict[str, Any]:
        error: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details is not None:
            error["details"] = self.details
        body: dict[str, Any] = {"ok": False, "error": error}
        if request_id:
            body["request_id"] = request_id
        return body


def alert_invalid(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_ALERT_INVALID", message, details=details or None)


def snapshot_invalid(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_SNAPSHOT_INVALID", message, details=details or None)


def stale_base(base_version: int, current_version: int) -> BusinessError:
    return BusinessError(
        "E_STALE_BASE",
        "base_version does not match the current committed version",
        details={"base_version": base_version, "current_version": current_version},
    )


def conflicting_alert(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_CONFLICTING_ALERT", message, details=details or None)


def order_not_found(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_ORDER_NOT_FOUND", message, details=details or None)


def rebuild_conflict(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_REBUILD_CONFLICT", message, details=details or None)
