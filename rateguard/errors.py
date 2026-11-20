"""Business errors and the stable HTTP error envelope."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class BusinessError(Exception):
    code: str
    message: str
    http_status: int = 422
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def validation_error(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_VALIDATION", message, 400, details)

