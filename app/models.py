from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


NAME_PATTERN = r"^[a-z][a-z0-9_-]{0,31}$"
DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class DeploymentUnitInput(StrictModel):
    name: str = Field(pattern=NAME_PATTERN)
    image_digest: str = Field(pattern=DIGEST_PATTERN)
    config: dict[str, Any] = Field(default_factory=dict)
    dependencies: list[str] = Field(default_factory=list)
    expected_version: str = Field(min_length=1, max_length=64)

    @field_validator("config")
    @classmethod
    def validate_config_keys(cls, value: dict[str, Any]) -> dict[str, Any]:
        for key in value:
            if not isinstance(key, str) or not key.strip():
                raise ValueError("config keys must be non-empty strings")
            if len(key) > 128:
                raise ValueError("config keys must be at most 128 characters")
        return value

    @field_validator("dependencies")
    @classmethod
    def validate_dependencies(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("dependencies must not contain duplicates")
        for dependency in value:
            if not dependency:
                raise ValueError("dependency names must be non-empty")
        return value


class EnvironmentInput(StrictModel):
    name: str = Field(pattern=NAME_PATTERN)
    version: int = Field(ge=1)
    version_lag_tolerance: int = Field(default=0, ge=0, le=1000)
    is_baseline: bool = False
    units: list[DeploymentUnitInput] = Field(min_length=1)

    @field_validator("units")
    @classmethod
    def validate_unique_units(
        cls, value: list[DeploymentUnitInput]
    ) -> list[DeploymentUnitInput]:
        names = [unit.name for unit in value]
        if len(names) != len(set(names)):
            raise ValueError("service names must be unique within an environment")
        return value


class FullSnapshotInput(StrictModel):
    commit_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")
    snapshot_version: int = Field(ge=1)
    environments: list[EnvironmentInput] = Field(min_length=1)

    @field_validator("environments")
    @classmethod
    def validate_environments(
        cls, value: list[EnvironmentInput]
    ) -> list[EnvironmentInput]:
        names = [environment.name for environment in value]
        if len(names) != len(set(names)):
            raise ValueError("environment names must be unique")
        if sum(1 for environment in value if environment.is_baseline) != 1:
            raise ValueError("exactly one environment must be marked as baseline")
        return value


class IncrementalSnapshotInput(StrictModel):
    commit_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")
    environment: str = Field(pattern=NAME_PATTERN)
    base_version: int = Field(ge=1)
    units: list[DeploymentUnitInput] = Field(min_length=1)

    @field_validator("units")
    @classmethod
    def validate_unique_units(
        cls, value: list[DeploymentUnitInput]
    ) -> list[DeploymentUnitInput]:
        names = [unit.name for unit in value]
        if len(names) != len(set(names)):
            raise ValueError("service names in an increment must be unique")
        return value
