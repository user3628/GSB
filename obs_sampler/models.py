"""HTTP request models and stable API representations."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class _RecordCommon(StrictModel):
    timestamp_ms: int = Field(ge=0, strict=True)
    service: str = Field(min_length=1)
    level: str = Field(min_length=1, max_length=32)
    trace_id: str = Field(min_length=1)
    span_id: str = Field(min_length=1)
    parent_span_id: str | None = None
    sample_weight: float = Field(default=1.0, gt=0.0, allow_inf_nan=False)

    @field_validator("level", mode="before")
    @classmethod
    def normalize_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value


class LogRecordIn(_RecordCommon):
    message_fingerprint: str = Field(min_length=1)


class SpanRecordIn(_RecordCommon):
    message_fingerprint: str | None = None
    latency_us: int = Field(ge=0, strict=True)
    error: bool = False


class BatchRequest(StrictModel):
    batch_id: str = Field(min_length=1, max_length=128)
    policy_version: int | None = Field(default=None, ge=1)
    logs: list[LogRecordIn] = Field(default_factory=list)
    spans: list[SpanRecordIn] = Field(default_factory=list)

    @field_validator("batch_id")
    @classmethod
    def batch_id_must_be_safe(cls, value: str) -> str:
        if any(character.isspace() for character in value):
            raise ValueError("batch_id must not contain whitespace")
        return value
