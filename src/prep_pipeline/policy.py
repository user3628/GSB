from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .canonical import canonical_json_bytes, digest_object
from .errors import PipelineError


class CleaningPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trim_whitespace: bool = True
    lowercase_labels: bool = True
    allowed_labels: list[str] = Field(min_length=1)
    min_text_length: int = Field(ge=0)
    max_text_length: int = Field(gt=0)


class FilteringPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_timestamp_ms: int = Field(ge=0)
    max_timestamp_ms: int = Field(ge=0)
    min_text_length: int = Field(ge=0)
    max_text_length: int = Field(gt=0)
    drop_labels: list[str] = Field(default_factory=list)


class ShardingPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    shard_count: int = Field(ge=1, le=4096)
    algorithm: Literal["sha256_mod_v1"]
    salt: str = "prep-pipeline"
    sample_skew_threshold: float = Field(ge=0, le=1)
    byte_skew_threshold: float = Field(ge=0, le=1)


class NormalizationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    length_min: float
    length_max: float
    dtype: Literal["float32"]
    clip: bool = True


class PolicySnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1)
    base_version: int = Field(ge=0)
    generated_at: str
    cleaning: CleaningPolicy
    filtering: FilteringPolicy
    sharding: ShardingPolicy
    normalization: NormalizationPolicy
    seed: int = Field(ge=0)
    sample_ratio: float = Field(gt=0, le=1)
    checksum: str

    @field_validator("generated_at")
    @classmethod
    def validate_generated_at(cls, value: str) -> str:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("generated_at must be an ISO-8601 timestamp") from exc
        if parsed.tzinfo is None:
            raise ValueError("generated_at must include a timezone")
        return value

    def unsigned_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload.pop("checksum", None)
        return payload

    def expected_checksum(self) -> str:
        return digest_object(self.unsigned_payload())

    def validate_business(self) -> None:
        if self.version <= self.base_version:
            raise PipelineError(
                "E_SNAPSHOT_INVALID",
                "policy version must be greater than base_version",
                {"version": self.version, "base_version": self.base_version},
            )
        if self.checksum != self.expected_checksum():
            raise PipelineError(
                "E_SNAPSHOT_INVALID",
                "policy checksum does not match canonical snapshot payload",
                {
                    "version": self.version,
                    "declared_checksum": self.checksum,
                    "expected_checksum": self.expected_checksum(),
                },
            )
        labels = self.cleaning.allowed_labels
        if len(set(labels)) != len(labels) or any(not label for label in labels):
            raise PipelineError(
                "E_SNAPSHOT_INVALID",
                "allowed_labels must be non-empty and unique",
                {"allowed_labels": labels},
            )
        if self.cleaning.min_text_length > self.cleaning.max_text_length:
            raise PipelineError(
                "E_SNAPSHOT_INVALID",
                "cleaning text length bounds are inverted",
                {"cleaning": self.cleaning.model_dump(mode="json")},
            )
        if self.filtering.min_timestamp_ms > self.filtering.max_timestamp_ms:
            raise PipelineError(
                "E_SNAPSHOT_INVALID",
                "filtering timestamp bounds are inverted",
                {"filtering": self.filtering.model_dump(mode="json")},
            )
        if self.filtering.min_text_length > self.filtering.max_text_length:
            raise PipelineError(
                "E_SNAPSHOT_INVALID",
                "filtering text length bounds are inverted",
                {"filtering": self.filtering.model_dump(mode="json")},
            )
        if self.normalization.length_max <= self.normalization.length_min:
            raise PipelineError(
                "E_SNAPSHOT_INVALID",
                "normalization length_max must be greater than length_min",
                {"normalization": self.normalization.model_dump(mode="json")},
            )
        if not (0 < self.sample_ratio <= 1):
            raise PipelineError(
                "E_SAMPLE_RATIO_INVALID",
                "sample_ratio must be in (0, 1]",
                {"sample_ratio": self.sample_ratio},
            )

    def canonical_json(self) -> str:
        return canonical_json_bytes(self.model_dump(mode="json")).decode("utf-8")


def parse_policy_snapshot(value: dict[str, Any]) -> PolicySnapshot:
    declared_ratio = value.get("sample_ratio")
    if isinstance(declared_ratio, (int, float)) and not isinstance(declared_ratio, bool):
        if not (0 < float(declared_ratio) <= 1):
            raise PipelineError(
                "E_SAMPLE_RATIO_INVALID",
                "sample_ratio must be in (0, 1]",
                {"sample_ratio": declared_ratio},
            )
    try:
        snapshot = PolicySnapshot.model_validate(value)
    except ValidationError as exc:
        raise PipelineError(
            "E_SNAPSHOT_INVALID",
            "policy snapshot schema is invalid",
            {"validation_errors": exc.errors(include_url=False)},
        ) from exc
    snapshot.validate_business()
    return snapshot


_SAMPLE_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")


def validate_sample_id(value: str) -> bool:
    return bool(_SAMPLE_ID_RE.fullmatch(value))
