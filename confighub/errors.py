from __future__ import annotations

from typing import Any


class ErrorCode:
    E_STALE_SNAPSHOT = "E_STALE_SNAPSHOT"
    E_SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
    E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
    E_PUSH_TIMEOUT = "E_PUSH_TIMEOUT"
    E_CONFLICTING_PUSH = "E_CONFLICTING_PUSH"
    E_STALE_ACK = "E_STALE_ACK"
    E_CONFLICTING_ACK = "E_CONFLICTING_ACK"
    E_PUSH_FAILED = "E_PUSH_FAILED"
    E_RESULT_INVALID = "E_RESULT_INVALID"
    E_NOT_FOUND = "E_NOT_FOUND"
    E_VALIDATION = "E_VALIDATION"
    E_CLIENT_UNREACHABLE = "E_CLIENT_UNREACHABLE"
    E_EXTERNAL_TARGET = "E_EXTERNAL_TARGET"
    E_BATCH_IN_PROGRESS = "E_BATCH_IN_PROGRESS"


class ConfigHubError(Exception):
    """A stable error carrying a machine-readable code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 409,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }

