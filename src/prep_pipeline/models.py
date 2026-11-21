from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


StageName = Literal["parse_clean", "filter", "shard", "sample", "tensorize"]
STAGE_ORDER: tuple[StageName, ...] = (
    "parse_clean",
    "filter",
    "shard",
    "sample",
    "tensorize",
)

_BATCH_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


class InjectionOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    invalid_label: bool = False
    missing_shard_key: bool = False
    sample_ratio: float | None = None
    duplicate_conflict: bool = False
    duplicate_sample_id: str | None = None
    interrupt_at_stage: StageName | None = None
    interrupt_after_records: int = Field(default=1, ge=1)
    target_record_index: int = Field(default=0, ge=0)

    def primary_count(self) -> int:
        return sum(
            (
                self.invalid_label,
                self.missing_shard_key,
                self.sample_ratio is not None,
                self.duplicate_conflict,
            )
        )


class BatchSubmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: str = Field(min_length=1, max_length=128)
    policy_version: int = Field(ge=1)
    input_path: str = Field(min_length=1)
    injections: InjectionOptions = Field(default_factory=InjectionOptions)

    @model_validator(mode="after")
    def validate_injections(self) -> "BatchSubmitRequest":
        if not _BATCH_ID_RE.fullmatch(self.batch_id):
            raise ValueError("batch_id must match [A-Za-z0-9_.:-]{1,128}")
        if self.injections.primary_count() > 1:
            raise ValueError("at most one primary anomaly injection may be requested")
        return self


class VerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    policy_version: int | None = Field(default=None, ge=1)
    baseline_batch_id: str | None = None
