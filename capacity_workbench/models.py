from __future__ import annotations

import ipaddress
import re
import uuid
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .errors import PlanInvalid


_PLAN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")


class StageSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    concurrency: int = Field(gt=0)
    duration_seconds: int = Field(gt=0)


class Thresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    p99_ms: float = Field(gt=0)
    error_rate: float = Field(ge=0, le=1)
    min_throughput_rps: float = Field(ge=0)


class DecisionRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    on_reject: Literal["stop", "fallback", "step_down_retry"] = "fallback"
    max_retries: int = Field(default=1, ge=0, le=10)
    step_down_factor: float = Field(default=0.5, gt=0, lt=1)


class SamplingSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requests_per_second_per_worker: int = Field(default=100, gt=0, le=1000)
    min_samples_for_percentiles: int = Field(default=100, ge=1)


class PlanCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    name: str = Field(min_length=1, max_length=200)
    target_url: str = Field(min_length=1)
    stages: list[StageSpec] = Field(min_length=1)
    thresholds: Thresholds
    decision_rule: DecisionRule = Field(default_factory=DecisionRule)
    seed: int = Field(ge=0, le=(1 << 63) - 1)
    profile: str = Field(default="stable", min_length=1, max_length=100)
    request_timeout_ms: int = Field(default=500, gt=0, le=600_000)
    sampling: SamplingSpec = Field(default_factory=SamplingSpec)

    @model_validator(mode="after")
    def validate_semantics(self) -> "PlanCreate":
        if self.id is not None and not _PLAN_ID_RE.fullmatch(self.id):
            raise ValueError("id contains unsupported characters")

        previous = 0
        for index, stage in enumerate(self.stages):
            if stage.concurrency <= previous:
                raise ValueError(
                    f"stages[{index}].concurrency must be strictly increasing"
                )
            previous = stage.concurrency

        parts = urlsplit(self.target_url)
        if parts.scheme not in {"http", "https"} or not parts.hostname:
            raise ValueError("target_url must be an absolute HTTP(S) loopback URL")
        if parts.username or parts.password:
            raise ValueError("target_url must not contain credentials")
        try:
            port = parts.port
        except ValueError as exc:
            raise ValueError("target_url has an invalid port") from exc
        if port is None:
            raise ValueError("target_url must include an explicit port")
        if not _is_loopback(parts.hostname):
            raise ValueError("target_url must point to a loopback host")
        return self

    def document(self) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        data["id"] = self.id or f"plan_{uuid.uuid4().hex}"
        return data


def validate_plan_payload(payload: Any) -> dict[str, Any]:
    try:
        return PlanCreate.model_validate(payload).document()
    except PlanInvalid:
        raise
    except Exception as exc:
        raise PlanInvalid("Invalid plan", details=str(exc)) from exc


def _is_loopback(hostname: str) -> bool:
    normalized = hostname.strip("[]").lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


@dataclass(frozen=True)
class PlanRecord:
    id: str
    name: str
    target_url: str
    seed: int
    profile: str
    request_timeout_ms: int
    stages: tuple[StageSpec, ...]
    thresholds: Thresholds
    decision_rule: DecisionRule
    sampling: SamplingSpec
    status: str
    conclusion: dict[str, Any] | None
    raw: dict[str, Any]

    @property
    def stage_count(self) -> int:
        return len(self.stages)


@dataclass(frozen=True)
class RoundRecord:
    id: str
    plan_id: str
    stage_index: int
    attempt: int
    round_key: str
    concurrency: int
    duration_seconds: int
    origin: str
    retry_of_round_id: str | None
    status: str
    failure_reason: str | None
    created_at: str
    started_at: str | None
    ended_at: str | None

