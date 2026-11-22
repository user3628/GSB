"""Business errors with stable codes and machine-readable details."""

from __future__ import annotations

from typing import Any


class EngineError(Exception):
    """A stable, serializable business error.

    The CLI never leaks a traceback for this class.  Library callers can inspect
    ``code`` and ``details`` directly.
    """

    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }
