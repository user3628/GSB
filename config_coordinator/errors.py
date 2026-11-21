"""Stable business errors and their JSON representation."""

from __future__ import annotations

from typing import Any


class DomainError(Exception):
    """A business failure that is safe to return to an API caller."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        http_status: int = 409,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.http_status = http_status

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


E_STALE_SNAPSHOT = "E_STALE_SNAPSHOT"
E_STALE_BASE = "E_STALE_BASE"
E_SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
E_DEPENDENCY_CYCLE = "E_DEPENDENCY_CYCLE"
E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_PUSH_TIMEOUT = "E_PUSH_TIMEOUT"
E_PUSH_REJECTED = "E_PUSH_REJECTED"
E_PUSH_CONNECTION = "E_PUSH_CONNECTION"
E_CONFLICTING_PUSH = "E_CONFLICTING_PUSH"
E_COMPONENT_NOT_FOUND = "E_COMPONENT_NOT_FOUND"
E_SNAPSHOT_NOT_FOUND = "E_SNAPSHOT_NOT_FOUND"
E_SNAPSHOT_NOT_CURRENT = "E_SNAPSHOT_NOT_CURRENT"
E_DISPATCH_IN_PROGRESS = "E_DISPATCH_IN_PROGRESS"
E_DISPATCH_NOT_FOUND = "E_DISPATCH_NOT_FOUND"
E_UNKNOWN_CONFIG_KEY = "E_UNKNOWN_CONFIG_KEY"
E_VALIDATION = "E_VALIDATION"
E_INTERNAL = "E_INTERNAL"

