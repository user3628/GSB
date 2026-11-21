from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """An error that is safe to expose as a stable business code."""

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
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


class SimulatedCrash(RuntimeError):
    """Used by tests and crash injection to stop before the next durable step."""


def fail(
    code: str,
    message: str,
    *,
    status_code: int = 400,
    details: dict[str, Any] | None = None,
) -> None:
    raise BusinessError(code, message, status_code=status_code, details=details)
