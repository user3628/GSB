"""Stable business errors used by the library, CLI, and HTTP API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


OK = "OK"
E_INVALID_SHAPE = "E_INVALID_SHAPE"
E_INVALID_AXIS = "E_INVALID_AXIS"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_MISSING_FRAGMENT = "E_MISSING_FRAGMENT"
E_NUMERIC_MISMATCH = "E_NUMERIC_MISMATCH"
E_STALE_VERSION = "E_STALE_VERSION"
E_STALE_PUBLISH = "E_STALE_PUBLISH"
E_MEMORY_BUDGET = "E_MEMORY_BUDGET"
E_ZERO_ROW = "E_ZERO_ROW"
E_NONFINITE = "E_NONFINITE"
E_INVALID_STATE = "E_INVALID_STATE"
E_BAD_REQUEST = "E_BAD_REQUEST"
E_NOT_FOUND = "E_NOT_FOUND"
E_IO = "E_IO"


@dataclass
class ChunkOpsError(Exception):
    """Exception carrying a stable business code and structured context."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(f"{self.code}: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


def as_error(exc: Exception) -> ChunkOpsError:
    if isinstance(exc, ChunkOpsError):
        return exc
    return ChunkOpsError(E_IO, str(exc), {"exception_type": type(exc).__name__})
