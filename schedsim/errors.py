"""Business errors shared by the scheduler API and engine."""

from __future__ import annotations

from typing import Any


class SchedError(Exception):
    """An expected, machine-readable scheduler failure."""

    def __init__(
        self,
        code: str,
        message: str,
        context: dict[str, Any] | None = None,
        http_status: int = 400,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}
        self.http_status = http_status

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


E_SPEC_INVALID = "E_SPEC_INVALID"
E_TOPOLOGY_KEY_MISSING = "E_TOPOLOGY_KEY_MISSING"
E_SCHED_QUEUE_FULL = "E_SCHED_QUEUE_FULL"
E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_BATCH_NOT_FOUND = "E_BATCH_NOT_FOUND"

