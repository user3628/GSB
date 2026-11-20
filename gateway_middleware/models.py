from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

WindowName = Literal["short", "long"]
MAX_TIMESTAMP_MS = (1 << 63) - 1 - 86_400_000
MAX_STORED_INT = (1 << 63) - 1


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class EventInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    event_seq: int = Field(ge=0, le=(1 << 63) - 1)
    request_id: str = Field(min_length=1, max_length=256)
    timestamp_ms: int = Field(ge=0, le=MAX_TIMESTAMP_MS)
    route_key: str = Field(min_length=1, max_length=256)
    client_id: str = Field(min_length=1, max_length=256)
    token_id: str = Field(min_length=1, max_length=256)
    token_digest: str = Field(min_length=1, max_length=256)
    weight: int = Field(default=1, ge=1, le=1_000_000)

    def content_dict(self) -> dict[str, Any]:
        return {
            "timestamp_ms": self.timestamp_ms,
            "route_key": self.route_key,
            "client_id": self.client_id,
            "token_id": self.token_id,
            "token_digest": self.token_digest,
            "weight": self.weight,
        }

    def fingerprint(self) -> str:
        return sha256_text(canonical_json(self.content_dict()))


class BatchInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    batch_id: str = Field(min_length=1, max_length=256)
    events: list[EventInput] = Field(min_length=1)


@dataclass(slots=True)
class TokenState:
    token_id: str
    client_id: str
    quota_group: str
    expires_at_ms: int
    digest_hex: str
    active: bool
    issued_at_ms: int


@dataclass(slots=True)
class QuotaGroup:
    name: str
    short_limit_weight: int
    long_limit_weight: int
    target_weight_per_sec: int


@dataclass(slots=True)
class AuthRegistry:
    tokens: dict[str, TokenState]
    client_groups: dict[str, str]
    quota_groups: dict[str, QuotaGroup]
    token_version: int
    local_secret: str


@dataclass(slots=True, frozen=True)
class AuthResult:
    ok: bool
    code: str
    reason: str
    quota_group: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "code": self.code,
            "reason": self.reason,
            "quota_group": self.quota_group,
        }


@dataclass(slots=True, frozen=True)
class WindowEstimate:
    window: str
    event_count: int
    exact_weight: int
    estimated_weight: int
    confidence_low: int
    confidence_high: int
    sample_rate_num: int
    sample_rate_den: int
    method: str
    exact_limit: int
    max_window_events: int
    variance: int = 0

    @property
    def sample_rate(self) -> float:
        return self.sample_rate_num / self.sample_rate_den

    @property
    def is_exact(self) -> bool:
        return self.method == "exact" and self.sample_rate_num == self.sample_rate_den

    def to_dict(self) -> dict[str, Any]:
        return {
            "window": self.window,
            "event_count": self.event_count,
            "exact_weight": self.exact_weight,
            "estimated_weight": self.estimated_weight,
            "confidence_low": self.confidence_low,
            "confidence_high": self.confidence_high,
            "sample_rate": self.sample_rate,
            "sample_rate_num": self.sample_rate_num,
            "sample_rate_den": self.sample_rate_den,
            "method": self.method,
            "exact_limit": self.exact_limit,
            "max_window_events": self.max_window_events,
            "variance": self.variance,
        }


@dataclass(slots=True)
class Decision:
    request_id: str
    batch_id: str
    event_seq: int
    event_fingerprint: str
    timestamp_ms: int
    route_key: str
    client_id: str
    quota_group: str | None
    weight: int
    auth_ok: bool
    auth_code: str | None
    auth_reason: str
    allowed: bool
    decision_code: str
    decision_reasons: tuple[str, ...]
    rate_evaluated: bool
    trigger_window: str | None
    short: WindowEstimate | None
    long: WindowEstimate | None
    short_quota: int | None
    long_quota: int | None
    quota_scale_ppm: int | None
    read_version: int
    window_version: int
    token_version: int
    commit_version: int
    reused: bool = False
    submitted_batch_id: str | None = None
    submitted_event_seq: int | None = None
    created_at_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        error = None
        if not self.allowed:
            error = {
                "code": self.decision_code,
                "message": (
                    "authorization failed"
                    if self.decision_code in ("E_AUTH_DENIED", "E_TOKEN_EXPIRED")
                    else "rate limit exceeded"
                ),
                "details": {
                    "auth_code": self.auth_code,
                    "auth_reason": self.auth_reason,
                    "trigger_window": self.trigger_window,
                    "decision_reasons": list(self.decision_reasons),
                    "short_count": self.short.estimated_weight if self.short else None,
                    "long_count": self.long.estimated_weight if self.long else None,
                    "short_quota": self.short_quota,
                    "long_quota": self.long_quota,
                },
            }
        return {
            "ok": self.allowed,
            "request_id": self.request_id,
            "batch_id": self.batch_id,
            "event_seq": self.event_seq,
            "event_fingerprint": self.event_fingerprint,
            "timestamp_ms": self.timestamp_ms,
            "route_key": self.route_key,
            "client_id": self.client_id,
            "quota_group": self.quota_group,
            "weight": self.weight,
            "auth": {
                "ok": self.auth_ok,
                "code": self.auth_code,
                "reason": self.auth_reason,
            },
            "allowed": self.allowed,
            "decision_code": self.decision_code,
            "decision_reasons": list(self.decision_reasons),
            "error": error,
            "rate_evaluated": self.rate_evaluated,
            "trigger_window": self.trigger_window,
            "windows": {
                "short": self.short.to_dict() if self.short else None,
                "long": self.long.to_dict() if self.long else None,
            },
            "quotas": {
                "short": self.short_quota,
                "long": self.long_quota,
                "scale_ppm": self.quota_scale_ppm,
            },
            "versions": {
                "read_version": self.read_version,
                "window_version": self.window_version,
                "token_version": self.token_version,
                "commit_version": self.commit_version,
            },
            "reused": self.reused,
            "submitted_batch_id": self.submitted_batch_id,
            "submitted_event_seq": self.submitted_event_seq,
            "created_at_ms": self.created_at_ms,
        }


@dataclass(slots=True)
class BatchResult:
    batch_id: str
    batch_fingerprint: str
    status: str
    base_version: int
    commit_version: int | None
    replayed: bool
    event_count: int
    decided_event_count: int
    reused_event_count: int
    auth_denied_count: int
    token_expired_count: int
    rate_limited_count: int
    allowed_count: int
    decisions: list[Decision] = field(default_factory=list)
    writer_wait_ms: int = 0

    def summary_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "batch_fingerprint": self.batch_fingerprint,
            "status": self.status,
            "base_version": self.base_version,
            "commit_version": self.commit_version,
            "replayed": self.replayed,
            "event_count": self.event_count,
            "decided_event_count": self.decided_event_count,
            "reused_event_count": self.reused_event_count,
            "auth_denied_count": self.auth_denied_count,
            "token_expired_count": self.token_expired_count,
            "rate_limited_count": self.rate_limited_count,
            "allowed_count": self.allowed_count,
            "writer_queue_policy": "queue_as_next_batch",
            "writer_wait_ms": self.writer_wait_ms,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": True,
            **self.summary_dict(),
            "decisions": [decision.to_dict() for decision in self.decisions],
        }


@dataclass(slots=True, frozen=True)
class QuotaDecision:
    short_quota: int
    long_quota: int
    scale_ppm: int


@dataclass(slots=True, frozen=True)
class BucketDelta:
    route_key: str
    client_id: str
    window_name: str
    bucket_start_ms: int
    bucket_end_ms: int
    event_count: int
    weight_sum: int
    admitted_count: int
    admitted_weight_sum: int
    updated_at_ms: int
