from __future__ import annotations

from enum import Enum
from typing import Any


class BusinessCode(str, Enum):
    OK = "OK"
    E_INVALID_SHAPE = "E_INVALID_SHAPE"
    E_BATCH_MISMATCH = "E_BATCH_MISMATCH"
    E_NUMERIC_MISMATCH = "E_NUMERIC_MISMATCH"
    E_STALE_CONFIG = "E_STALE_CONFIG"
    E_STALE_PUBLISH = "E_STALE_PUBLISH"
    E_MEMORY_BUDGET = "E_MEMORY_BUDGET"


class GemmError(Exception):
    """Stable business error with machine-readable details."""

    def __init__(
        self,
        code: BusinessCode | str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code = BusinessCode(code)
        self.message = message
        self.details = details or {}
        super().__init__(f"{self.code.value}: {message}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code.value,
            "message": self.message,
            "details": self.details,
        }


def fail(
    code: BusinessCode | str,
    message: str,
    **details: Any,
) -> None:
    raise GemmError(code, message, details=details)

