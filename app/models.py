from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EventIn(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    timestamp_ms: int
    group: str
    value: float | None = None

    @field_validator("timestamp_ms")
    @classmethod
    def validate_timestamp(cls, value: int) -> int:
        if value < 0:
            raise ValueError("timestamp_ms must be a non-negative integer")
        return value

    @field_validator("group")
    @classmethod
    def validate_group(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("group must be a non-empty string")
        return value

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: float | None) -> float | None:
        if value is None:
            return None
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("value must be finite or null")
        return float(value)


class BatchIn(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    batch_id: str = Field(min_length=1)
    events: list[EventIn]

    @field_validator("batch_id")
    @classmethod
    def validate_batch_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("batch_id must be non-empty")
        return value


class SnapshotTriggerIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: str | None = Field(default=None, max_length=200)


class SnapshotLoadIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int | None = None
    path: str | None = None


class WindowQueryIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start_ms: int = 0
    end_ms: int = 9_223_372_036_854_775_807
    group: str | None = None
    window_types: list[Literal["tumbling", "sliding", "session"]] = Field(
        default_factory=lambda: ["tumbling", "sliding", "session"]
    )
    quantiles: list[float] | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    cursor: str | None = None

    @field_validator("end_ms")
    @classmethod
    def validate_range(cls, value: int, info) -> int:
        start = info.data.get("start_ms", 0)
        if value <= start:
            raise ValueError("end_ms must be greater than start_ms")
        return value

    @field_validator("window_types")
    @classmethod
    def validate_types(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("window_types cannot be empty")
        return list(dict.fromkeys(value))

    @field_validator("quantiles")
    @classmethod
    def validate_quantiles(cls, value: list[float] | None) -> list[float] | None:
        if value is None:
            return None
        if not value:
            raise ValueError("quantiles cannot be empty")
        for probability in value:
            if not 0.0 < probability <= 1.0:
                raise ValueError("quantiles must satisfy 0 < p <= 1")
        return list(dict.fromkeys(value))


class TransitionValidateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    current: Literal["OPEN", "CLOSED", "EMITTED"]
    target: Literal["OPEN", "CLOSED", "EMITTED"]
