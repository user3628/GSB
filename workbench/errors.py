from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class WorkbenchError(Exception):
    code: str
    message: str
    http_status: int = 400
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "business_code": self.code,
            "code": self.code,
            "message": self.message,
        }
        if self.details:
            payload["details"] = self.details
        return payload


ERROR_HTTP_STATUS: dict[str, int] = {
    "E_KEY_INVALID": 400,
    "E_INVALID_TRANSITION": 409,
    "E_STALE_SNAPSHOT": 409,
    "E_SNAPSHOT_INVALID": 422,
    "E_POLICY_MISSING": 409,
    "E_NODE_NOT_FOUND": 404,
    "E_NODE_NOT_COVERED": 409,
    "E_ORIGIN_TIMEOUT": 504,
    "E_ORIGIN_5XX": 502,
    "E_ORIGIN_CONNECT": 502,
    "E_ORIGIN_PROTOCOL": 502,
    "E_INJECT_INVALID": 400,
    "E_PURGE_BROADCAST_FAIL": 503,
    "E_REQUEST_CONFLICT": 409,
    "E_REQUEST_INCOMPLETE": 409,
    "E_METHOD_NOT_ALLOWED": 405,
    "E_NOT_FOUND": 404,
    "E_CONFLICT": 409,
    "E_INTERNAL": 500,
}


def wb_error(code: str, message: str, *, http_status: int | None = None, **details: Any) -> WorkbenchError:
    return WorkbenchError(
        code=code,
        message=message,
        http_status=http_status if http_status is not None else ERROR_HTTP_STATUS.get(code, 400),
        details=details,
    )

