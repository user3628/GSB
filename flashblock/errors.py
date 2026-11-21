"""Business errors used by the block attention component."""

from __future__ import annotations

from typing import Any


class FlashBlockError(Exception):
    """A stable, JSON-serializable error with a business error code."""

    def __init__(self, code: str, message: str, **context: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


def fail(code: str, message: str, **context: Any) -> None:
    raise FlashBlockError(code, message, **context)

