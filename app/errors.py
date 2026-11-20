from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_EVENT_INVALID = "E_EVENT_INVALID"
E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_LATE_EVENT = "E_LATE_EVENT"
E_SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
E_CONFLICTING_BATCH = "E_CONFLICTING_BATCH"
E_CAPACITY_EXCEEDED = "E_CAPACITY_EXCEEDED"
E_REQUEST_INVALID = "E_REQUEST_INVALID"
E_CURSOR_EXPIRED = "E_CURSOR_EXPIRED"
E_NOT_FOUND = "E_NOT_FOUND"
E_INTERNAL = "E_INTERNAL"


@dataclass(slots=True)
class BusinessError(Exception):
    code: str
    message: str
    status_code: int = 400
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def as_response(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }
