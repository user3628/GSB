"""Business error codes and the unified JSON error body."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


class ErrorCode:
    """Stable string business codes returned in every error body."""

    RECORD_CORRUPT = "E_RECORD_CORRUPT"
    FORMAT_UNSUPPORTED = "E_FORMAT_UNSUPPORTED"
    CONFLICTING_RUN = "E_CONFLICTING_RUN"
    SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
    STALE_BASE = "E_STALE_BASE"
    VERSION_RETIRED = "E_VERSION_RETIRED"
    NOT_FOUND = "E_NOT_FOUND"
    INVALID_REQUEST = "E_INVALID_REQUEST"


@dataclass
class TrackerError(Exception):
    """Exception whose payload maps 1:1 to the HTTP JSON error body."""

    code: str
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    http_status: int = 400

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_body(self) -> Dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            }
        }


def error_body(code: str, message: str, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Helper to build a unified error body without raising."""
    return {
        "error": {
            "code": code,
            "message": message,
            "context": context or {},
        }
    }
