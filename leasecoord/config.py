"""Runtime configuration with conservative local defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import Mapping


@dataclass(frozen=True, slots=True)
class CoordinatorConfig:
    exact_limit: int = 64
    priority_min: int = 0
    priority_max: int = 100
    heartbeat_timeout_ms: int = 5_000
    min_lease_ttl_ms: int = 100
    max_lease_ttl_ms: int = 86_400_000
    sample_fraction: float = 0.10
    sample_min_per_bucket: int = 1
    sqlite_busy_timeout_ms: int = 15_000
    recovery_remote_stale_after_ms: int = 120_000
    allow_time_override: bool = False

    def validate(self) -> None:
        if self.exact_limit < 1:
            raise ValueError("exact_limit must be >= 1")
        if self.priority_min > self.priority_max:
            raise ValueError("priority_min must be <= priority_max")
        if self.heartbeat_timeout_ms < 1:
            raise ValueError("heartbeat_timeout_ms must be >= 1")
        if self.min_lease_ttl_ms < 1 or self.max_lease_ttl_ms < self.min_lease_ttl_ms:
            raise ValueError("invalid lease TTL range")
        if not (0.0 < self.sample_fraction <= 1.0):
            raise ValueError("sample_fraction must be in (0, 1]")
        if self.sample_min_per_bucket < 0:
            raise ValueError("sample_min_per_bucket must be >= 0")
        if self.sqlite_busy_timeout_ms < 1:
            raise ValueError("sqlite_busy_timeout_ms must be >= 1")

    def to_public_dict(self) -> dict[str, object]:
        return {
            "exact_limit": self.exact_limit,
            "priority_range": [self.priority_min, self.priority_max],
            "heartbeat_timeout_ms": self.heartbeat_timeout_ms,
            "lease_ttl_range_ms": [self.min_lease_ttl_ms, self.max_lease_ttl_ms],
            "sample_fraction": self.sample_fraction,
            "sample_min_per_bucket": self.sample_min_per_bucket,
            "allow_time_override": self.allow_time_override,
        }

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
        **overrides: object,
    ) -> "CoordinatorConfig":
        env = os.environ if environ is None else environ
        defaults = cls()
        base = cls(
            exact_limit=int(env.get("LEASE_EXACT_LIMIT", defaults.exact_limit)),
            priority_min=int(env.get("LEASE_PRIORITY_MIN", defaults.priority_min)),
            priority_max=int(env.get("LEASE_PRIORITY_MAX", defaults.priority_max)),
            heartbeat_timeout_ms=int(
                env.get("LEASE_HEARTBEAT_TIMEOUT_MS", defaults.heartbeat_timeout_ms)
            ),
            min_lease_ttl_ms=int(
                env.get("LEASE_MIN_TTL_MS", defaults.min_lease_ttl_ms)
            ),
            max_lease_ttl_ms=int(
                env.get("LEASE_MAX_TTL_MS", defaults.max_lease_ttl_ms)
            ),
            sample_fraction=float(
                env.get("LEASE_SAMPLE_FRACTION", defaults.sample_fraction)
            ),
            sample_min_per_bucket=int(
                env.get(
                    "LEASE_SAMPLE_MIN_PER_BUCKET", defaults.sample_min_per_bucket
                )
            ),
            sqlite_busy_timeout_ms=int(
                env.get(
                    "LEASE_SQLITE_BUSY_TIMEOUT_MS", defaults.sqlite_busy_timeout_ms
                )
            ),
            recovery_remote_stale_after_ms=int(
                env.get(
                    "LEASE_RECOVERY_REMOTE_STALE_AFTER_MS",
                    defaults.recovery_remote_stale_after_ms,
                )
            ),
            allow_time_override=env.get("LEASE_ALLOW_TIME_OVERRIDE", "0")
            in {"1", "true", "TRUE", "yes", "YES"},
        )
        if overrides:
            base = replace(base, **overrides)
        base.validate()
        return base
