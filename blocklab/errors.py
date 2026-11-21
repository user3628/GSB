"""Business error types shared by the CLI, HTTP API, and test suite."""

from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """An expected, machine-readable business failure."""

    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


E_NOT_FOUND = "E_NOT_FOUND"
E_INPUT = "E_INPUT"
E_CONFIG = "E_CONFIG"
E_PATH = "E_PATH"
E_SHAPE_MISMATCH = "E_SHAPE_MISMATCH"
E_NUMERIC = "E_NUMERIC"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_BUSY = "E_BUSY"
E_STOPPED = "E_STOPPED"

