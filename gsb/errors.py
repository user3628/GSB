"""Stable error codes used by the CLI and HTTP API."""

from __future__ import annotations

from typing import Any


class WorkbenchError(Exception):
    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {"ok": False, "error": {"code": self.code, "message": self.message, "details": self.details}}


class StopRequested(Exception):
    """Raised after the current indivisible boosting round has been persisted."""

    def __init__(self, stage: str, round_number: int | None = None):
        super().__init__(f"stop requested during {stage}")
        self.stage = stage
        self.round_number = round_number


def fail(code: str, message: str, **details: Any) -> None:
    raise WorkbenchError(code, message, details)


def require(condition: bool, code: str, message: str, **details: Any) -> None:
    if not condition:
        fail(code, message, **details)
