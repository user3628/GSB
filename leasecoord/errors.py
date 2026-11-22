"""Business errors shared by the coordinator and HTTP adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class BusinessError(Exception):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int = 409

    def __post_init__(self) -> None:
        Exception.__init__(self, f"{self.code}: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


ERROR_HTTP_STATUS: dict[str, int] = {
    "E_NODE_INVALID": 422,
    "E_LEASE_HELD": 409,
    "E_NO_ELIGIBLE_CANDIDATE": 409,
    "E_STALE_LEASE": 409,
    "E_CONFLICTING_REQUEST": 409,
    "E_INVALID_TRANSITION": 409,
    "E_BAD_REQUEST": 400,
    "E_NOT_FOUND": 404,
    "E_INTERNAL": 500,
}


def business_error(code: str, message: str, **context: Any) -> BusinessError:
    return BusinessError(
        code=code,
        message=message,
        context=context,
        http_status=ERROR_HTTP_STATUS.get(code, 409),
    )
