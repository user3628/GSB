from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


NonNegativeMs = Annotated[int, Field(ge=0, strict=True)]


class RouteRule(StrictModel):
    rule_id: str = Field(min_length=1)
    route_id: str = Field(min_length=1)
    path_prefix: str = Field(pattern=r"^/")
    headers: dict[str, str] = Field(default_factory=dict)
    priority: int = Field(strict=True)

    @field_validator("headers")
    @classmethod
    def normalize_headers(cls, value: dict[str, str]) -> dict[str, str]:
        normalized = {key.lower(): item for key, item in value.items()}
        if len(normalized) != len(value):
            raise ValueError("header names must be unique after case normalization")
        return normalized


class VirtualServicePolicy(StrictModel):
    service: str = Field(min_length=1)
    routes: list[RouteRule] = Field(min_length=1)
    default_route: str = Field(min_length=1)


class RateLimitRule(StrictModel):
    threshold: int = Field(ge=0, strict=True)
    window_ms: int = Field(gt=0, strict=True)


class CircuitBreakerRule(StrictModel):
    consecutive_failures: int = Field(gt=0, strict=True)
    window_ms: int = Field(gt=0, strict=True)


class TargetRule(StrictModel):
    rate_limit: RateLimitRule
    timeout_ms: int = Field(gt=0, strict=True)
    circuit_breaker: CircuitBreakerRule


class PolicySnapshot(StrictModel):
    version: int = Field(gt=0, strict=True)
    generated_at: str = Field(min_length=1)
    virtual_service: VirtualServicePolicy
    target_rules: dict[str, TargetRule]
    checksum: str = ""

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

    @model_validator(mode="after")
    def validate_policy_graph(self) -> "PolicySnapshot":
        rule_ids = [rule.rule_id for rule in self.virtual_service.routes]
        if len(rule_ids) != len(set(rule_ids)):
            raise ValueError("virtual_service route rule_id values must be unique")

        expected_routes = {rule.route_id for rule in self.virtual_service.routes}
        expected_routes.add(self.virtual_service.default_route)
        target_routes = set(self.target_rules)
        missing = sorted(expected_routes - target_routes)
        extra = sorted(target_routes - expected_routes)
        if missing or extra:
            raise ValueError(
                "target_rules must exactly cover route ids plus default_route; "
                f"missing={missing}, extra={extra}"
            )
        return self


class UpstreamResult(StrictModel):
    status_code: int = Field(ge=100, le=599, strict=True)
    latency_ms: NonNegativeMs
    connection_error: bool = False


class TraceRequest(StrictModel):
    request_id: str = Field(min_length=1)
    timestamp_ms: NonNegativeMs
    method: str = Field(min_length=1)
    path: str = Field(pattern=r"^/")
    headers: dict[str, str] = Field(default_factory=dict)
    upstream: UpstreamResult

    @field_validator("method")
    @classmethod
    def normalize_method(cls, value: str) -> str:
        return value.upper()

    @field_validator("headers")
    @classmethod
    def normalize_headers(cls, value: dict[str, str]) -> dict[str, str]:
        normalized = {key.lower(): item for key, item in value.items()}
        if len(normalized) != len(value):
            raise ValueError("header names must be unique after case normalization")
        return normalized


class TraceSample(StrictModel):
    trace_id: str = Field(min_length=1)
    description: str = ""
    requests: list[TraceRequest] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_request_ids(self) -> "TraceSample":
        request_ids = [request.request_id for request in self.requests]
        if len(request_ids) != len(set(request_ids)):
            raise ValueError("trace request_id values must be unique")
        return self


class SimulationSubmit(StrictModel):
    snapshot_version: int = Field(gt=0, strict=True)
    trace: TraceSample
    execution: Literal["commit", "prepare"] = "commit"


class RouteResolveRequest(StrictModel):
    snapshot_version: int = Field(gt=0, strict=True)
    path: str = Field(pattern=r"^/")
    headers: dict[str, str] = Field(default_factory=dict)
    strict: bool = False

    @field_validator("headers")
    @classmethod
    def normalize_headers(cls, value: dict[str, str]) -> dict[str, str]:
        normalized = {key.lower(): item for key, item in value.items()}
        if len(normalized) != len(value):
            raise ValueError("header names must be unique after case normalization")
        return normalized


class BatchStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    ABORTED = "ABORTED"


class TransitionRequest(StrictModel):
    status: Literal["PENDING", "RUNNING", "COMPLETED", "ABORTED"]
