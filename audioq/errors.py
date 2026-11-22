"""Stable, structured business errors used by every interface."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


CODES = {
    "E_EMPTY_AUDIO": "audio is empty, silent, or shorter than one frame",
    "E_CLIPPED_AUDIO": "clipping exceeds the configured allowed ratio",
    "E_LENGTH_MISMATCH": "manifest sample count differs from the WAV sample count",
    "E_SAMPLE_RATE": "manifest sample rate differs from the WAV sample rate",
    "E_NUMERIC": "a metric or fused score is NaN or infinite",
    "E_INVALID_WEIGHT": "weights are invalid, all metrics are unavailable, or their sum is zero",
    "E_STALE_CHECKPOINT": "checkpoint sequence or publisher identity is stale",
    "E_INVALID_STAGE": "a prerequisite stage artifact is missing or invalid",
    "E_AUDIO_FORMAT": "the audio container is not mono 16-bit PCM WAV",
    "E_CONFIG": "the configuration is malformed",
    "E_MANIFEST": "the shard manifest is malformed",
    "E_IO": "a local file operation failed",
}


@dataclass
class AudioQError(Exception):
    """An exception whose serialized payload is always a stable JSON object."""

    code: str
    message: str
    shard_id: str | None = None
    stage: str | None = None
    observed: Mapping[str, Any] | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.code not in CODES:
            raise ValueError(f"unknown business error code: {self.code}")
        Exception.__init__(self, f"{self.code}: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "code": self.code,
            "message": self.message,
        }
        if self.shard_id is not None:
            payload["shard_id"] = self.shard_id
        if self.stage is not None:
            payload["stage"] = self.stage
        if self.observed is not None:
            payload["observed"] = dict(self.observed)
        if self.details:
            payload["details"] = dict(self.details)
        return payload


def as_error(exc: BaseException, *, shard_id: str | None = None, stage: str | None = None) -> AudioQError:
    if isinstance(exc, AudioQError):
        return exc
    return AudioQError(
        code="E_IO",
        message=str(exc) or exc.__class__.__name__,
        shard_id=shard_id,
        stage=stage,
        observed={"exception": exc.__class__.__name__},
    )
