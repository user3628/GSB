"""Stable business errors used by the command line and HTTP surfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


ERROR_HTTP_STATUS = {
    "E_INVALID_CONFIG": 400,
    "E_ZONE_OFFSET_OUT_OF_BOUNDS": 422,
    "E_LEXICON_REFERENCE_MISSING": 422,
    "E_NONFINITE_SCORE": 422,
    "E_STALE_CHECKPOINT": 409,
    "E_INCREMENTAL_MISMATCH": 500,
    "E_INVALID_MANIFEST": 422,
    "E_INVALID_EDIT": 422,
    "E_INVALID_WAV": 422,
    "E_BUILD_IN_PROGRESS": 409,
    "E_RUN_STATE_CHANGED": 409,
    "E_NO_CHECKPOINT": 404,
    "E_NOT_FOUND": 404,
    "E_BAD_REQUEST": 400,
}


@dataclass
class CabinVoiceError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:  # pragma: no cover - trivial formatting
        suffix = f" {self.details}" if self.details else ""
        return f"{self.code}: {self.message}{suffix}"

    def as_dict(self) -> dict[str, Any]:
        return {"ok": False, "code": self.code, "message": self.message, "details": self.details}

    @property
    def http_status(self) -> int:
        return ERROR_HTTP_STATUS.get(self.code, 500)
