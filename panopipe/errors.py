"""Stable business error codes used by the CLI and HTTP interfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_UNKNOWN_COLOR = "E_UNKNOWN_COLOR"
E_UNSUPPORTED_SCHEMA = "E_UNSUPPORTED_SCHEMA"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_SEGMENT_ID_OVERFLOW = "E_SEGMENT_ID_OVERFLOW"
E_PALETTE_CAPACITY = "E_PALETTE_CAPACITY"
E_FORMAT_MISSING = "E_FORMAT_MISSING"
E_FORMAT_ERROR = "E_FORMAT_ERROR"
E_NOT_FOUND = "E_NOT_FOUND"
E_CHECKPOINT_CORRUPT = "E_CHECKPOINT_CORRUPT"
E_BUSY = "E_BUSY"
E_INTERNAL = "E_INTERNAL"


HTTP_STATUS = {
    E_UNKNOWN_COLOR: 422,
    E_UNSUPPORTED_SCHEMA: 422,
    E_INVALID_STAGE: 409,
    E_INVALID_CONFIG: 400,
    E_STALE_CHECKPOINT: 409,
    E_SEGMENT_ID_OVERFLOW: 422,
    E_PALETTE_CAPACITY: 422,
    E_FORMAT_MISSING: 409,
    E_FORMAT_ERROR: 422,
    E_NOT_FOUND: 404,
    E_CHECKPOINT_CORRUPT: 500,
    E_BUSY: 409,
    E_INTERNAL: 500,
}


@dataclass
class PipelineError(Exception):
    """An error with a stable machine-readable code and structured details."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }

    @property
    def http_status(self) -> int:
        return HTTP_STATUS.get(self.code, 400)


def error_payload(exc: PipelineError) -> dict[str, Any]:
    return exc.to_dict()
