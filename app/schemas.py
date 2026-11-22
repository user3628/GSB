from __future__ import annotations

import hashlib
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictFloat, StrictInt, StrictStr, field_validator, model_validator

from .errors import EngineError

Boundary = Literal["left_closed_right_open", "left_closed_right_closed"]


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class MeasureType(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["integer", "decimal"]
    scale: int | None = Field(default=None, ge=0, le=18)

    @model_validator(mode="after")
    def validate_scale(self) -> "MeasureType":
        if self.kind == "integer":
            if self.scale not in (None, 0):
                raise ValueError("integer measure type cannot declare a decimal scale")
            self.scale = 0
        elif self.scale is None:
            raise ValueError("decimal measure type requires scale")
        return self


class WindowSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["tumbling", "sliding", "session"]
    boundary: Boundary = "left_closed_right_open"
    width_ms: int | None = Field(default=None, gt=0)
    step_ms: int | None = Field(default=None, gt=0)
    gap_ms: int | None = Field(default=None, gt=0)
    quantiles: list[float] = Field(default_factory=list)

    @field_validator("quantiles")
    @classmethod
    def validate_quantiles(cls, values: list[float]) -> list[float]:
        for value in values:
            if not 0 <= value <= 1:
                raise ValueError("quantiles must be between 0 and 1")
        return sorted(set(values))

    @model_validator(mode="after")
    def validate_shape(self) -> "WindowSpec":
        if self.kind == "tumbling":
            if self.width_ms is None:
                raise ValueError("tumbling window requires width_ms")
            self.step_ms = None
            self.gap_ms = None
        elif self.kind == "sliding":
            if self.width_ms is None or self.step_ms is None:
                raise ValueError("sliding window requires width_ms and step_ms")
            if self.step_ms > self.width_ms:
                raise ValueError("sliding step_ms must be <= width_ms to avoid uncovered timestamps")
            if self.width_ms // self.step_ms > 5000:
                raise ValueError("sliding window would generate more than 5000 overlaps per event")
            self.gap_ms = None
        else:
            if self.gap_ms is None:
                raise ValueError("session window requires gap_ms")
            self.width_ms = None
            self.step_ms = None
        return self

    @property
    def boundary_echo(self) -> dict[str, str]:
        end = "open" if self.boundary == "left_closed_right_open" else "closed"
        return {"start": "closed", "end": end}

    @property
    def end_inclusive(self) -> bool:
        return self.boundary == "left_closed_right_closed"

    @property
    def spec_hash(self) -> str:
        return hashlib.sha256(canonical_json(self.model_dump(mode="json")).encode()).hexdigest()

    def canonical(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class EventInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1, max_length=160)
    timestamp_ms: StrictInt
    group_key: str = Field(min_length=1, max_length=160)
    measure: StrictInt | StrictFloat | StrictStr | None = None
    labels: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp_ms")
    @classmethod
    def validate_timestamp(cls, value: int) -> int:
        if value < -(2**63) or value > 2**63 - 1:
            raise ValueError("timestamp_ms must fit a signed 64-bit millisecond value")
        return value

    @field_validator("event_id", "group_key")
    @classmethod
    def no_blank_keys(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("keys cannot be blank")
        return value


class BatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: str = Field(min_length=1, max_length=160)
    window_spec: WindowSpec
    measure_type: MeasureType
    events: list[EventInput] = Field(min_length=1)
    timestamp_order: Literal["sort", "reject"] = "sort"
    late_policy: Literal["queue", "reject"] = "queue"

    def request_hash(self) -> str:
        return hashlib.sha256(canonical_json(self.model_dump(mode="json")).encode()).hexdigest()


class CloseWindowsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    through_ms: StrictInt
    window_kind: Literal["tumbling", "sliding", "session"] | None = None
    group_key: str | None = None
    target_state: Literal["CLOSED"] = "CLOSED"


class EmitWindowsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    through_ms: StrictInt
    window_kind: Literal["tumbling", "sliding", "session"] | None = None
    group_key: str | None = None


class TransitionWindowRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    window_id: str = Field(min_length=1)
    target_state: Literal["OPEN", "CLOSED", "EMITTED"]


class LoadSnapshotRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    snapshot: dict[str, Any]


class RecoveryScanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = "manual"


class DemoCrashRequest(BatchRequest):
    crash_point: Literal["after_intent", "mid_transaction"] = "mid_transaction"


def spec_from_dict(value: dict[str, Any]) -> WindowSpec:
    try:
        return WindowSpec.model_validate(value)
    except Exception as exc:
        raise EngineError(
            "E_INVALID_WINDOW_SPEC",
            "window specification is invalid",
            {"error": str(exc), "spec": value},
        ) from exc
