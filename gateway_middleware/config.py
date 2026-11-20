from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_LOCAL_SECRET = "local-deterministic-gateway-secret-v1"


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)


@dataclass(frozen=True, slots=True)
class Settings:
    """Runtime configuration shared by the API and experiment scripts."""

    db_path: Path = field(
        default_factory=lambda: Path(os.getenv("GW_DB_PATH", "data/gateway.sqlite3"))
    )
    local_secret: str = field(
        default_factory=lambda: os.getenv("GW_LOCAL_SECRET", DEFAULT_LOCAL_SECRET)
    )

    short_length_ms: int = 1_000
    short_step_ms: int = 100
    long_length_ms: int = 60_000
    long_step_ms: int = 1_000

    short_exact_limit: int = 512
    long_exact_limit: int = 2_048
    confidence_z_ppm: int = 2_576_000  # 99% normal interval

    quota_adjust_gain_ppm: int = 250_000
    quota_damping_ppm: int = 200_000
    quota_min_scale_ppm: int = 500_000
    quota_max_scale_ppm: int = 1_500_000
    fixed_point_scale: int = 1_000_000

    busy_timeout_ms: int = 30_000
    api_max_events: int = 1_500_000
    writer_queue_policy: str = "queue"

    def __post_init__(self) -> None:
        if self.short_length_ms <= 0 or self.long_length_ms <= 0:
            raise ValueError("window lengths must be positive")
        if self.short_step_ms <= 0 or self.long_step_ms <= 0:
            raise ValueError("window steps must be positive")
        if self.short_length_ms % self.short_step_ms:
            raise ValueError("short window length must be divisible by its step")
        if self.long_length_ms % self.long_step_ms:
            raise ValueError("long window length must be divisible by its step")
        if self.short_exact_limit <= 0 or self.long_exact_limit <= 0:
            raise ValueError("exact limits must be positive")
        if not (0 < self.quota_min_scale_ppm <= self.fixed_point_scale):
            raise ValueError("quota_min_scale_ppm is out of range")
        if self.quota_max_scale_ppm < self.fixed_point_scale:
            raise ValueError("quota_max_scale_ppm must be >= 1.0")
        if not (0 <= self.quota_damping_ppm <= self.fixed_point_scale):
            raise ValueError("quota damping must be in [0, 1]")
        if self.short_length_ms >= self.long_length_ms:
            raise ValueError("short window must be shorter than long window")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=Path(os.getenv("GW_DB_PATH", "data/gateway.sqlite3")),
            local_secret=os.getenv("GW_LOCAL_SECRET", DEFAULT_LOCAL_SECRET),
            short_length_ms=_env_int("GW_SHORT_LENGTH_MS", 1_000),
            short_step_ms=_env_int("GW_SHORT_STEP_MS", 100),
            long_length_ms=_env_int("GW_LONG_LENGTH_MS", 60_000),
            long_step_ms=_env_int("GW_LONG_STEP_MS", 1_000),
            short_exact_limit=_env_int("GW_SHORT_EXACT_LIMIT", 512),
            long_exact_limit=_env_int("GW_LONG_EXACT_LIMIT", 2_048),
            quota_adjust_gain_ppm=_env_int("GW_QUOTA_GAIN_PPM", 250_000),
            quota_damping_ppm=_env_int("GW_QUOTA_DAMPING_PPM", 200_000),
            quota_min_scale_ppm=_env_int("GW_QUOTA_MIN_SCALE_PPM", 500_000),
            quota_max_scale_ppm=_env_int("GW_QUOTA_MAX_SCALE_PPM", 1_500_000),
            api_max_events=_env_int("GW_API_MAX_EVENTS", 1_500_000),
        )

    def with_db(self, db_path: str | Path) -> "Settings":
        return Settings(
            db_path=Path(db_path),
            local_secret=self.local_secret,
            short_length_ms=self.short_length_ms,
            short_step_ms=self.short_step_ms,
            long_length_ms=self.long_length_ms,
            long_step_ms=self.long_step_ms,
            short_exact_limit=self.short_exact_limit,
            long_exact_limit=self.long_exact_limit,
            confidence_z_ppm=self.confidence_z_ppm,
            quota_adjust_gain_ppm=self.quota_adjust_gain_ppm,
            quota_damping_ppm=self.quota_damping_ppm,
            quota_min_scale_ppm=self.quota_min_scale_ppm,
            quota_max_scale_ppm=self.quota_max_scale_ppm,
            fixed_point_scale=self.fixed_point_scale,
            busy_timeout_ms=self.busy_timeout_ms,
            api_max_events=self.api_max_events,
            writer_queue_policy=self.writer_queue_policy,
        )
