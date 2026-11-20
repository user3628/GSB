from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .errors import BusinessError


@dataclass(frozen=True)
class SimulatorConfig:
    mss_bytes: int = 1000
    initial_cwnd_bytes: int = 4000
    min_cwnd_bytes: int = 1000
    initial_ssthresh_bytes: int = 8000
    base_rtt_us: int = 40_000
    min_rto_us: int = 200_000
    max_rto_us: int = 60_000_000
    max_events_per_batch: int = 10_000
    latency_budget_ms: int = 5_000

    def __post_init__(self) -> None:
        fields = (
            "mss_bytes",
            "initial_cwnd_bytes",
            "min_cwnd_bytes",
            "initial_ssthresh_bytes",
            "base_rtt_us",
            "min_rto_us",
            "max_rto_us",
            "max_events_per_batch",
            "latency_budget_ms",
        )
        for name in fields:
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise BusinessError(
                    "E_CONFIG_INVALID",
                    context={"field": name, "value": value, "rule": "positive_integer"},
                )
        if self.min_cwnd_bytes > self.initial_cwnd_bytes:
            raise BusinessError(
                "E_CONFIG_INVALID",
                context={
                    "field": "min_cwnd_bytes",
                    "value": self.min_cwnd_bytes,
                    "rule": "must_not_exceed_initial_cwnd_bytes",
                },
            )
        if self.min_rto_us > self.max_rto_us:
            raise BusinessError(
                "E_CONFIG_INVALID",
                context={
                    "field": "min_rto_us",
                    "value": self.min_rto_us,
                    "rule": "must_not_exceed_max_rto_us",
                },
            )

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None) -> "SimulatorConfig":
        if value is None:
            return cls()
        if not isinstance(value, dict):
            raise BusinessError(
                "E_CONFIG_INVALID",
                context={"rule": "config_must_be_object", "actual_type": type(value).__name__},
            )
        allowed = set(cls.__dataclass_fields__)
        unknown = sorted(set(value) - allowed)
        if unknown:
            raise BusinessError(
                "E_CONFIG_INVALID",
                context={"rule": "unknown_fields", "fields": unknown},
            )
        return cls(**value)

    def to_dict(self) -> dict[str, int]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SimulatorConfig":
        return cls(**value)
