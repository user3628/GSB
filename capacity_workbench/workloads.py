from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .randomness import SplitMix64


DEFAULT_WORKLOAD_PATH = Path(__file__).resolve().parent.parent / "config" / "workloads.json"


@dataclass(frozen=True)
class WorkloadProfile:
    name: str
    distribution: dict[str, Any]
    base_error_rate: float
    base_timeout_rate: float
    knee_concurrency: int
    latency_overload_slope: float
    error_slope_per_concurrency: float
    timeout_slope_per_concurrency: float
    timeout_sleep_ms: int


@dataclass(frozen=True)
class WorkloadRegistry:
    default_profile: str
    profiles: dict[str, WorkloadProfile]

    def get(self, name: str | None) -> WorkloadProfile:
        selected = name or self.default_profile
        try:
            return self.profiles[selected]
        except KeyError as exc:
            raise KeyError(f"unknown workload profile: {selected}") from exc

    @classmethod
    def load(cls, path: str | Path | None = None) -> "WorkloadRegistry":
        config_path = Path(path) if path else DEFAULT_WORKLOAD_PATH
        with config_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        profiles: dict[str, WorkloadProfile] = {}
        for name, raw in data.get("profiles", {}).items():
            profiles[name] = WorkloadProfile(
                name=name,
                distribution=dict(raw["distribution"]),
                base_error_rate=float(raw.get("base_error_rate", 0.0)),
                base_timeout_rate=float(raw.get("base_timeout_rate", 0.0)),
                knee_concurrency=int(raw.get("knee_concurrency", 1)),
                latency_overload_slope=float(raw.get("latency_overload_slope", 0.0)),
                error_slope_per_concurrency=float(
                    raw.get("error_slope_per_concurrency", 0.0)
                ),
                timeout_slope_per_concurrency=float(
                    raw.get("timeout_slope_per_concurrency", 0.0)
                ),
                timeout_sleep_ms=int(raw.get("timeout_sleep_ms", 500)),
            )
        default_profile = str(
            data.get("default_profile", next(iter(profiles), "stable"))
        )
        return cls(default_profile=default_profile, profiles=profiles)


@dataclass(frozen=True)
class GeneratedSample:
    latency_ms: float
    kind: str
    error: bool
    timeout: bool


def generate_sample(
    profile: WorkloadProfile, concurrency: int, nonce: int
) -> GeneratedSample:
    rng = SplitMix64(nonce)
    distribution = profile.distribution
    kind = str(distribution.get("kind", "lognormal"))
    if kind == "lognormal":
        median = float(distribution["median_ms"])
        sigma = float(distribution["sigma"])
        latency = median * math.exp(sigma * rng.gauss())
    elif kind == "normal":
        mean = float(distribution["mean_ms"])
        stddev = float(distribution["stddev_ms"])
        minimum = float(distribution.get("minimum_ms", 0.0))
        latency = max(minimum, mean + stddev * rng.gauss())
    elif kind == "uniform":
        minimum = float(distribution["minimum_ms"])
        maximum = float(distribution["maximum_ms"])
        latency = minimum + (maximum - minimum) * rng.random()
    else:
        raise ValueError(f"unsupported distribution kind: {kind}")

    excess = max(0, concurrency - profile.knee_concurrency)
    multiplier = 1.0 + profile.latency_overload_slope * (excess ** 1.5)
    latency = max(0.001, latency * multiplier)
    error_rate = min(
        0.95,
        profile.base_error_rate
        + profile.error_slope_per_concurrency * excess,
    )
    timeout_rate = min(
        0.95,
        profile.base_timeout_rate
        + profile.timeout_slope_per_concurrency * excess,
    )
    timeout = rng.random() < timeout_rate
    error = (not timeout) and (rng.random() < error_rate)
    return GeneratedSample(
        latency_ms=round(latency, 6), kind=kind, error=error, timeout=timeout
    )

