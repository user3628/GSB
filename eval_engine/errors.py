"""Stable business errors used by both the Python API and the CLI."""

from __future__ import annotations

from typing import Any


class EngineError(Exception):
    """An error with a stable machine-readable business code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class StopRequested(Exception):
    """Raised internally after the current indivisible unit has completed."""

    def __init__(self, completed_units: int, stage: int | None = None) -> None:
        super().__init__("graceful stop requested")
        self.completed_units = completed_units
        self.stage = stage
