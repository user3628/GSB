from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
E_BLOCK_CORRUPT = "E_BLOCK_CORRUPT"
E_KEY_SET_TOO_LARGE = "E_KEY_SET_TOO_LARGE"
E_MEMORY_BUDGET_EXCEEDED = "E_MEMORY_BUDGET_EXCEEDED"
E_PAGE_CONSUMED = "E_PAGE_CONSUMED"
E_BATCH_NOT_FOUND = "E_BATCH_NOT_FOUND"
E_INVALID_REQUEST = "E_INVALID_REQUEST"
E_PAGE_OUT_OF_RANGE = "E_PAGE_OUT_OF_RANGE"
E_BATCH_FAILED = "E_BATCH_FAILED"
E_INTERNAL_ERROR = "E_INTERNAL_ERROR"


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int = 400

    def __post_init__(self) -> None:
        Exception.__init__(self, f"{self.code}: {self.message}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            },
        }


class SimulatedCrash(RuntimeError):
    """Raised by recovery tests to stop a materialization between transactions."""

