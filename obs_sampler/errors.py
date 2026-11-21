"""Business errors shared by the service and HTTP layer."""

from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """An expected failure with a stable, machine-readable business code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


class PolicyError(ValueError):
    """Raised while validating a policy snapshot before it reaches the store."""
