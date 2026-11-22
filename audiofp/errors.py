"""Stable, machine-readable business errors for audiofp."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_NO_PEAK = "E_NO_PEAK"
E_BUCKET_COLLISION = "E_BUCKET_COLLISION"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_INVALID_WAV = "E_INVALID_WAV"


@dataclass
class FingerprintError(Exception):
    """An expected business failure.

    The numeric payload is deliberately JSON friendly and contains no paths,
    tracebacks, timestamps, or Python object representations.  This makes the
    same failure reproducible at the byte level.
    """

    code: str
    message: str
    shard_id: str | None = None
    stage: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.shard_id is not None:
            payload["shard_id"] = self.shard_id
        if self.stage is not None:
            payload["stage"] = self.stage
        if self.details:
            payload["details"] = self.details
        return payload

