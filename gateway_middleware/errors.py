from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

E_AUTH_DENIED = "E_AUTH_DENIED"
E_TOKEN_EXPIRED = "E_TOKEN_EXPIRED"
E_RATE_LIMITED = "E_RATE_LIMITED"
E_CONFLICTING_REQUEST = "E_CONFLICTING_REQUEST"
E_BATCH_NOT_FOUND = "E_BATCH_NOT_FOUND"
E_REQUEST_NOT_FOUND = "E_REQUEST_NOT_FOUND"
E_SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_INVALID_INPUT = "E_INVALID_INPUT"
E_INTERNAL = "E_INTERNAL"


_CODE_STATUS: dict[str, int] = {
    E_AUTH_DENIED: 403,
    E_TOKEN_EXPIRED: 403,
    E_RATE_LIMITED: 429,
    E_CONFLICTING_REQUEST: 409,
    E_BATCH_NOT_FOUND: 404,
    E_REQUEST_NOT_FOUND: 404,
    E_SNAPSHOT_INVALID: 409,
    E_INVALID_TRANSITION: 409,
    E_INVALID_INPUT: 422,
    E_INTERNAL: 500,
}


@dataclass(slots=True)
class DomainError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    status_code: int | None = None

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)
        if self.status_code is None:
            self.status_code = _CODE_STATUS.get(self.code, 400)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def failure(code: str, message: str, **details: Any) -> dict[str, Any]:
    return {
        "ok": False,
        "error": {
            "code": code,
            "message": message,
            "details": details,
        },
    }
