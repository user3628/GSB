from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SnapshotRequest(StrictModel):
    base_version: int | None = None
    snapshot_version: str
    batch_id: str
    records: list[dict[str, Any]] = Field(min_length=1)
    policy: dict[str, Any]


class SampleBatchRequest(StrictModel):
    base_version: int
    batch_id: str
    records: list[dict[str, Any]] = Field(min_length=1)


class PolicyIncrementRequest(StrictModel):
    base_version: int
    base_policy_version: str
    policy_version: str
    patch: dict[str, Any]


class ReplayRequest(StrictModel):
    source_version: int
    batch_id: str | None = None
    timeout_ms: int | None = Field(default=None, ge=1, le=600000)
    fault_mode: str = "mirror"
    fault_status: int = Field(default=200, ge=100, le=599)
    hang_ms: int = Field(default=0, ge=0, le=600000)
    raise_on_timeout: bool = False
