from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MeterMode(StrEnum):
    BYTES = "BYTES"
    REQUESTS = "REQUESTS"
    COMPOSITE = "COMPOSITE"


class IODirection(StrEnum):
    READ = "READ"
    WRITE = "WRITE"


class TenantStatus(StrEnum):
    ACTIVE = "ACTIVE"
    THROTTLED = "THROTTLED"
    QUOTA_BLOCKED = "QUOTA_BLOCKED"
    SUSPENDED = "SUSPENDED"


class TenantSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    capacity_bytes: Decimal
    requests_per_second: Decimal
    burst_capacity: Decimal
    weight: Decimal = Decimal("1")
    parent_id: str | None = None


class PolicySnapshotIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int
    generated_at_ms: int
    tenants: list[TenantSpec]
    checksum: str | None = None


class EventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_time_ms: int
    tenant_id: str

    @field_validator("event_time_ms", mode="before")
    @classmethod
    def floor_submillisecond_timestamp(cls, value: Any) -> int:
        try:
            timestamp = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise ValueError("event_time_ms must be a finite integer-millisecond timestamp")
        if not timestamp.is_finite():
            raise ValueError("event_time_ms must be finite")
        return int(timestamp.to_integral_value(rounding=ROUND_FLOOR))
    meter_mode: MeterMode
    io_direction: IODirection
    payload_bytes: Decimal = Decimal("0")
    request_weight: Decimal = Decimal("0")


class BatchFaults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    invalid_payload: bool = False
    unknown_tenant: bool = False
    clock_rewind: bool = False
    abort_midway: bool = False
    crash_after_intent: bool = False


class BatchIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: str | None = None
    events: list[EventIn] = Field(default_factory=list)
    faults: BatchFaults = Field(default_factory=BatchFaults)


class TenantPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    capacity_bytes: Decimal | None = None
    requests_per_second: Decimal | None = None
    burst_capacity: Decimal | None = None
    weight: Decimal | None = None
    parent_id: str | None = None


class PatchFaults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rebuild_failure: bool = False
    version_rewind: bool = False
    crash_during_rebuild: bool = False


class PolicyPatchIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_version: int
    generated_at_ms: int | None = None
    tenant_patches: list[TenantPatch] = Field(default_factory=list)
    upsert_tenants: list[TenantSpec] = Field(default_factory=list)
    remove_tenant_ids: list[str] = Field(default_factory=list)
    faults: PatchFaults = Field(default_factory=PatchFaults)


class TenantPatchIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_version: int
    generated_at_ms: int | None = None
    capacity_bytes: Decimal | None = None
    requests_per_second: Decimal | None = None
    burst_capacity: Decimal | None = None
    weight: Decimal | None = None
    parent_id: str | None = None
    faults: PatchFaults = Field(default_factory=PatchFaults)


class SuspendIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str = "manual suspension"


class ResumeIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    at_ms: int | None = None


class SimulationIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: list[EventIn] = Field(default_factory=list)
    label: str | None = None
    baseline: Literal["POLICY", "CURRENT"] = "POLICY"


def dump_model(value: BaseModel) -> dict[str, Any]:
    return value.model_dump(mode="json")
