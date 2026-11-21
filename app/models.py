from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


SHA256_PATTERN = r"^[0-9a-fA-F]{64}$"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class BackendSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    url: str
    capacity_bytes: int = Field(gt=0)
    max_concurrency: int = Field(gt=0)
    max_parts_per_session: int = Field(gt=0)
    estimated_latency_ms: int = Field(default=100, ge=1)
    enabled: bool = True


class GatewaySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    global_concurrency: int = Field(default=4, gt=0)
    queue_limit: int = Field(default=8, ge=0)
    default_time_budget_ms: int = Field(default=2000, gt=0)
    write_timeout_ms: int = Field(default=1000, gt=0)
    read_timeout_ms: int = Field(default=1000, gt=0)
    backends: list[BackendSpec]

    def backend(self, backend_id: str) -> BackendSpec:
        for backend in self.backends:
            if backend.id == backend_id:
                return backend
        raise KeyError(backend_id)


class CreateSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    object_key: str = Field(min_length=1, max_length=1024)
    backend_id: str
    max_parts: int = Field(gt=0, le=10000)
    total_parts: int | None = Field(default=None, gt=0, le=10000)
    priority: int = Field(default=100, ge=0)
    session_id: str | None = Field(default=None, min_length=1, max_length=128)
    request_id: str | None = Field(default=None, min_length=1, max_length=256)
    conflict_policy: Literal["reject_active"] = "reject_active"
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("object_key")
    @classmethod
    def object_key_must_not_be_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("object_key must not be blank")
        return value

    @model_validator(mode="after")
    def total_must_fit_max(self) -> "CreateSessionRequest":
        if self.total_parts is not None and self.total_parts > self.max_parts:
            raise ValueError("total_parts cannot exceed max_parts")
        return self


class SubmitPartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seq: int
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=SHA256_PATTERN)
    arrived_at: str | None = None
    request_id: str | None = Field(default=None, min_length=1, max_length=256)

    @field_validator("sha256")
    @classmethod
    def normalize_digest(cls, value: str) -> str:
        return value.lower()


class ScheduleRoundRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str | None = Field(default=None, min_length=1, max_length=256)
    time_budget_ms: int | None = Field(default=None, gt=0, le=600_000)
    queue_limit: int | None = Field(default=None, ge=0, le=10000)
    write_timeout_ms: int | None = Field(default=None, gt=0, le=600_000)
    read_timeout_ms: int | None = Field(default=None, gt=0, le=600_000)
    session_ids: list[str] | None = None


class FinalizeSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str | None = Field(default=None, min_length=1, max_length=256)
