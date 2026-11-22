"""Typed application errors with stable, machine-readable error codes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


ERROR_HTTP_STATUS = {
    "E_TIME_INVALID": 422,
    "E_GRID_INVALID": 422,
    "E_CONFIG_INVALID": 422,
    "E_SOURCE_INVALID": 422,
    "E_INVALID_STAGE": 409,
    "E_STALE_CHECKPOINT": 409,
    "E_STALE_RESULT": 409,
    "E_RUN_ACTIVE": 409,
    "E_NOT_FOUND": 404,
    "E_PORT_IN_USE": 409,
    "E_IO": 500,
    "E_INTERNAL": 500,
}


@dataclass
class TelemetryError(Exception):
    """An error that can be safely serialized by the CLI and HTTP layer."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.code}: {self.message}"

    @property
    def http_status(self) -> int:
        return ERROR_HTTP_STATUS.get(self.code, 500)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def error_payload(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, TelemetryError):
        return exc.to_dict()
    return {
        "ok": False,
        "error": {
            "code": "E_INTERNAL",
            "message": str(exc),
            "details": {"exception_type": type(exc).__name__},
        },
    }

