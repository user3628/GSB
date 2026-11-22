"""Stable business error codes and the structured workbench error type.

The same code/message/details triple is used in JSON responses, exception
stringification and log lines so that all surfaces stay consistent.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from .util import canonical_json

logger = logging.getLogger("sft_workbench")

E_EMPTY_SAMPLE = "E_EMPTY_SAMPLE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_UNSUPPORTED_VERSION = "E_UNSUPPORTED_VERSION"
E_DECODE_SAMPLE = "E_DECODE_SAMPLE"
E_ENCODE_SAMPLE = "E_ENCODE_SAMPLE"
E_INVALID_INPUT = "E_INVALID_INPUT"


@dataclass
class WorkbenchError(Exception):
    """Structured error carrying a stable code and machine-readable details."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:  # identical wording in logs and responses
        return f"{self.code}: {self.message} details={canonical_json(self.details)}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }

    def log(self) -> None:
        logger.error("code=%s message=%s details=%s", self.code, self.message, canonical_json(self.details))
