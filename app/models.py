from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


MAX_BYTES = 2**63 - 1


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class LayerPolicyIn(StrictModel):
    id: str = Field(min_length=1, max_length=128)
    capacity_bytes: int = Field(ge=0, le=MAX_BYTES)


class SnapshotIn(StrictModel):
    version: int = Field(ge=1)
    generated_at: str = Field(min_length=1)
    layers: list[LayerPolicyIn] = Field(min_length=1)
    heat_threshold: int = Field(ge=0, le=255)
    batch_limit: int = Field(ge=1, le=100_000)
    cooldown_seconds: int = Field(ge=0, le=31_536_000)
    checksum: str = Field(min_length=64, max_length=64)


class AdjustmentIn(StrictModel):
    base_version: int = Field(ge=1)
    version: int = Field(ge=1)
    generated_at: str | None = None
    layer_capacity_updates: dict[str, int] = Field(default_factory=dict)
    heat_threshold: int | None = Field(default=None, ge=0, le=255)
    batch_limit: int | None = Field(default=None, ge=1, le=100_000)
    cooldown_seconds: int | None = Field(default=None, ge=0, le=31_536_000)

    @model_validator(mode="after")
    def require_adjustment(self) -> "AdjustmentIn":
        if (
            not self.layer_capacity_updates
            and self.heat_threshold is None
            and self.batch_limit is None
            and self.cooldown_seconds is None
        ):
            raise ValueError("at least one incremental adjustment is required")
        for layer_id, capacity in self.layer_capacity_updates.items():
            if not layer_id:
                raise ValueError("layer id must not be empty")
            if capacity < 0 or capacity > MAX_BYTES:
                raise ValueError(f"capacity for {layer_id!r} is out of range")
        return self


class BatchPreviewIn(StrictModel):
    snapshot_version: int = Field(ge=1)


class BatchRunIn(StrictModel):
    batch_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    snapshot_version: int = Field(ge=1)


class TransitionCheckIn(StrictModel):
    from_state: str
    to_state: str


class FaultInjectionIn(StrictModel):
    point: Literal[
        "after_intent",
        "after_transfer_before_commit",
        "after_commit_before_response",
    ]
    mode: Literal["raise", "hard_exit"] = "raise"
    batch_id: str | None = Field(default=None, min_length=1, max_length=128)


class SafeShutdownIn(StrictModel):
    reason: str = "operator_requested"
