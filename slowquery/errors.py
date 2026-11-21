from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


HTTP_STATUS_BY_CODE = {
    "E_SAMPLE_INVALID": 400,
    "E_SNAPSHOT_INVALID": 400,
    "E_REQUEST_INVALID": 400,
    "E_SNAPSHOT_RETIRED": 409,
    "E_INVALID_TRANSITION": 409,
    "E_SESSION_CONFLICT": 409,
    "E_SESSION_NOT_FOUND": 404,
    "E_VERIFY_MISMATCH": 422,
    "E_VERIFY_ERROR": 422,
    "E_INTERNAL_ERROR": 500,
}


@dataclass(slots=True)
class BusinessError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)
        if self.http_status is None:
            self.http_status = HTTP_STATUS_BY_CODE.get(self.code, 400)

    def as_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


def invalid_sample(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_SAMPLE_INVALID", message, details)


def invalid_snapshot(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_SNAPSHOT_INVALID", message, details)


def invalid_transition(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_INVALID_TRANSITION", message, details)


def session_not_found(session_id: str) -> BusinessError:
    return BusinessError(
        "E_SESSION_NOT_FOUND",
        f"Diagnosis session {session_id!r} was not found.",
        {"session_id": session_id},
    )
