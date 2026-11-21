"""Public request/response models for the configuration coordinator."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .codec import compute_snapshot_checksum, utc_now_iso
from .errors import DomainError, E_SNAPSHOT_INVALID, E_VALIDATION


def _require_text(value: str, field_name: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


class Snapshot(BaseModel):
    """A complete, checksummed configuration snapshot."""

    model_config = ConfigDict(extra="forbid")

    version: int = Field(ge=1)
    generated_at: str
    configs: dict[str, Any]
    checksum: str

    @field_validator("generated_at")
    @classmethod
    def validate_generated_at(cls, value: str) -> str:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("generated_at must be ISO-8601") from exc
        return value

    @field_validator("configs")
    @classmethod
    def validate_configs(cls, value: dict[str, Any]) -> dict[str, Any]:
        if not value:
            raise ValueError("configs must not be empty")
        for key in value:
            if not key.strip():
                raise ValueError("configuration keys must not be blank")
        return value

    def assert_checksum(self) -> None:
        expected = compute_snapshot_checksum(self.version, self.generated_at, self.configs)
        if self.checksum != expected:
            raise DomainError(
                E_SNAPSHOT_INVALID,
                "snapshot checksum does not match its canonical content",
                details={"expected_checksum": expected, "actual_checksum": self.checksum},
                http_status=422,
            )


class ComponentDeclaration(BaseModel):
    """A component that consumes configuration and can be dispatched to."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    component_id: str
    config_keys: list[str] = Field(default_factory=list)
    expected_config_version: int = Field(ge=1)
    endpoint_url: str
    cluster_id: str = "default"
    logical_component: str | None = None
    depends_on: list[str] = Field(default_factory=list)

    @field_validator("component_id", "cluster_id")
    @classmethod
    def validate_id(cls, value: str) -> str:
        return _require_text(value, "identifier")

    @field_validator("config_keys", "depends_on")
    @classmethod
    def validate_string_lists(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value]
        if any(not item for item in cleaned):
            raise ValueError("list entries must not be blank")
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("list entries must be unique")
        return cleaned

    @field_validator("endpoint_url")
    @classmethod
    def validate_endpoint(cls, value: str) -> str:
        value = value.strip()
        if not (value.startswith("http://") or value.startswith("https://")):
            raise ValueError("endpoint_url must be an absolute http(s) URL")
        return value

    @property
    def logical_id(self) -> str:
        return self.logical_component or self.component_id

    def stable_dict(self) -> dict[str, Any]:
        return {
            "component_id": self.component_id,
            "config_keys": sorted(set(self.config_keys)),
            "expected_config_version": self.expected_config_version,
            "endpoint_url": self.endpoint_url,
            "cluster_id": self.cluster_id,
            "logical_component": self.logical_id,
            "depends_on": sorted(set(self.depends_on)),
        }


def validate_components(
    components: list[ComponentDeclaration], configs: dict[str, Any]
) -> list[ComponentDeclaration]:
    ids = [component.component_id for component in components]
    if len(ids) != len(set(ids)):
        raise DomainError(
            E_VALIDATION,
            "component identifiers must be unique",
            details={"duplicate_ids": sorted({item for item in ids if ids.count(item) > 1})},
            http_status=422,
        )
    known = set(ids)
    for component in components:
        unknown = sorted(set(component.config_keys) - set(configs))
        if unknown:
            raise DomainError(
                E_UNKNOWN_CONFIG_KEY,
                "component references configuration keys that do not exist",
                details={"component_id": component.component_id, "unknown_keys": unknown},
                http_status=422,
            )
        unknown_deps = sorted(set(component.depends_on) - known)
        if unknown_deps:
            raise DomainError(
                E_VALIDATION,
                "component references unknown components",
                details={"component_id": component.component_id, "unknown_components": unknown_deps},
                http_status=422,
            )
    return sorted(components, key=lambda item: item.component_id)


class SnapshotSubmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    snapshot: Snapshot
    components: list[ComponentDeclaration] | None = None


class IncrementRequest(BaseModel):
    """An optimistic-concurrency patch in the shared version sequence."""

    model_config = ConfigDict(extra="forbid")

    base_version: int = Field(ge=0)
    generated_at: str | None = None
    config_set: dict[str, Any] = Field(default_factory=dict)
    config_delete: list[str] = Field(default_factory=list)
    components_upsert: list[ComponentDeclaration] = Field(default_factory=list)
    components_remove: list[str] = Field(default_factory=list)
    reason: str | None = None

    @field_validator("generated_at")
    @classmethod
    def validate_generated_at(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("generated_at must be ISO-8601") from exc
        return value

    @field_validator("config_set")
    @classmethod
    def validate_config_set(cls, value: dict[str, Any]) -> dict[str, Any]:
        for key in value:
            if not key.strip():
                raise ValueError("configuration keys must not be blank")
        return value

    @field_validator("config_delete", "components_remove")
    @classmethod
    def validate_lists(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value]
        if any(not item for item in cleaned):
            raise ValueError("list entries must not be blank")
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("list entries must be unique")
        return cleaned

    @model_validator(mode="after")
    def validate_non_empty_patch(self) -> "IncrementRequest":
        if not (self.config_set or self.config_delete or self.components_upsert or self.components_remove):
            raise ValueError("increment must change at least one configuration or declaration")
        return self

    def generated_timestamp(self) -> str:
        return self.generated_at or utc_now_iso()


class TransitionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    snapshot_version: int = Field(ge=1)
    to_status: str
    reason: str = Field(min_length=1, max_length=500)


class DispatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    snapshot_version: int = Field(ge=1)
    policy: Literal["CONTINUE", "ROLLBACK"] = "CONTINUE"
    timeout_seconds: float = Field(default=1.0, gt=0, le=30)

