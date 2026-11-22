"""Business errors shared by the domain, HTTP server and CLI."""

from __future__ import annotations

from typing import Any


class AppError(Exception):
    """An expected failure with a stable business code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        http_status: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
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


class PlanInvalid(AppError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__("E_PLAN_INVALID", message, http_status=422, details=details)


class InvalidTransition(AppError):
    def __init__(
        self, message: str, *, details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(
            "E_INVALID_TRANSITION", message, http_status=409, details=details
        )


class RoundActive(AppError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__("E_ROUND_ACTIVE", message, http_status=409, details=details)


class InsufficientSamples(AppError):
    def __init__(
        self, message: str, *, details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(
            "E_INSUFFICIENT_SAMPLES", message, http_status=422, details=details
        )


class NotFound(AppError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__("E_NOT_FOUND", message, http_status=404, details=details)


class BadRequest(AppError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__("E_BAD_REQUEST", message, http_status=400, details=details)


class Conflict(AppError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__("E_CONFLICT", message, http_status=409, details=details)


class RoundAborted(AppError):
    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__("E_ROUND_ABORTED", message, http_status=409, details=details)

