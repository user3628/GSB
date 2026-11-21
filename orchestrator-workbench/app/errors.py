"""Business error codes and the workbench exception type.

Every user-facing failure maps to a stable business code so that the HTTP
layer and the end-to-end demo can reason about failures deterministically.
"""
from __future__ import annotations


class ErrorCode:
    E_FORMAT_INVALID = "E_FORMAT_INVALID"
    E_VERSION_UNSUPPORTED = "E_VERSION_UNSUPPORTED"
    E_SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
    E_DEPENDENCY_CYCLE = "E_DEPENDENCY_CYCLE"
    E_STALE_BASE = "E_STALE_BASE"
    E_NOT_FOUND = "E_NOT_FOUND"
    E_CONFLICT = "E_CONFLICT"


class WorkbenchError(Exception):
    """Exception carrying a business code plus structured details."""

    def __init__(self, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        return {"ok": False, "error": {"code": self.code, "message": self.message, "details": self.details}}
