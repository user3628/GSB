"""Typed errors exposed by both the CLI and HTTP API."""

from __future__ import annotations

from typing import Any


class WorkbenchError(Exception):
    """A stable, machine-readable workbench error.

    The API and CLI never return a silent default decision.  Every rejection
    carries a stable code, the failing stage (when known), and structured
    diagnostic context.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        stage: str | None = None,
        sample_id: str | int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.stage = stage
        self.sample_id = sample_id
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "stage": self.stage,
            "sample_id": self.sample_id,
            "details": self.details,
        }


def error_payload(exc: WorkbenchError) -> dict[str, Any]:
    return {"ok": False, "error": exc.to_dict()}
