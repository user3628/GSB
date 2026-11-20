from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    http_status: int = 409
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def body(self, request_id: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "code": self.code,
            "message": self.message,
        }
        if request_id is not None:
            payload["request_id"] = request_id
        if self.details:
            payload["details"] = self.details
        return payload


def error(
    code: str,
    message: str,
    http_status: int = 409,
    **details: Any,
) -> BusinessError:
    return BusinessError(code=code, message=message, http_status=http_status, details=details)


ERROR_HTTP_STATUS = {
    "E_VOLUME_NOT_FOUND": 404,
    "E_VOLUME_CORRUPT": 409,
    "E_BLOCK_NOT_FOUND": 404,
    "E_BLOCK_CORRUPT": 409,
    "E_SNAPSHOT_NOT_FOUND": 404,
    "E_SNAPSHOT_ALREADY_EXISTS": 409,
    "E_SNAPSHOT_REFERENCED": 409,
    "E_SNAPSHOT_CORRUPT": 409,
    "E_SNAPSHOT_NOT_ADJACENT": 409,
    "E_INVALID_TRANSITION": 409,
    "E_OPERATION_ACTIVE": 409,
    "E_CONFLICTING_REQUEST": 409,
    "E_OPERATION_ABORTED": 409,
    "E_FAULT_INJECTION_DISABLED": 403,
    "E_RECOVERY_REPORT_NOT_FOUND": 404,
    "E_ROUTE_NOT_FOUND": 404,
    "E_INVALID_REQUEST": 422,
    "E_INTERNAL": 500,
}

