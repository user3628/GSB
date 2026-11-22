"""Stable business errors exposed by the CLI and HTTP API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_ALIGNMENT_FAILED = "E_ALIGNMENT_FAILED"
E_STALE_MAPPING = "E_STALE_MAPPING"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_INVALID_INPUT = "E_INVALID_INPUT"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_INVALID_RECORD = "E_INVALID_RECORD"
E_INTERNAL = "E_INTERNAL"


HTTP_STATUS = {
    E_ALIGNMENT_FAILED: 409,
    E_STALE_MAPPING: 409,
    E_STALE_CHECKPOINT: 409,
    E_INVALID_STAGE: 409,
    E_INVALID_INPUT: 400,
    E_INVALID_CONFIG: 400,
    E_INVALID_RECORD: 422,
    E_INTERNAL: 500,
}


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def http_status(self) -> int:
        return HTTP_STATUS.get(self.code, 500)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


class StopRequested(Exception):
    """Internal control flow used for a graceful, checkpoint-safe stop."""

    def __init__(self, completed_sentences: int, reason: str = "stop_requested") -> None:
        super().__init__(reason)
        self.completed_sentences = completed_sentences
        self.reason = reason

