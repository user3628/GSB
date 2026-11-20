"""Configuration and explicit numerical/window contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any, Literal


Interpolation = Literal["nearest_rank", "linear"]


@dataclass(slots=True)
class GuardConfig:
    # All windows are anchored to Unix epoch milliseconds and use [start, end).
    window_length_ms: int = 8_000
    step_ms: int = 4_000
    watermark_lag_ms: int = 0

    # Staleness protection for a caller that pins a window version.
    stale_grace_ms: int = 16_000

    # Statistical output contract.
    primary_interpolation: Interpolation = "linear"
    latency_decimal_places: int = 6
    rate_decimal_places: int = 12
    factor_decimal_places: int = 9
    probability_decimal_places: int = 9
    rounding_mode: str = "ROUND_HALF_EVEN"
    confidence_level: Decimal = Decimal("0.95")
    support_max_latency_us: int = 5_000_000
    min_samples_p50: int = 2
    min_samples_p95: int = 20
    min_samples_p99: int = 100

    # Adaptive quota contract. All factors are represented as signed/unsigned
    # integer fixed point where 1_000_000 == 1.0.
    max_quota: int = 200
    min_quota: int = 0
    target_latency_us: int = 50_000
    target_concurrency: int = 64
    concurrency_factor_floor: Decimal = Decimal("0.10")
    quality_success_weight: Decimal = Decimal("0.45")
    quality_latency_weight: Decimal = Decimal("0.35")
    quality_budget_weight: Decimal = Decimal("0.20")
    damping_alpha_bp: int = 2_500
    damping_deadband: int = 1
    fixed_point_scale: int = 1_000_000
    breaker_weight_closed_bp: int = 10_000
    breaker_weight_half_open_bp: int = 2_500
    breaker_weight_open_bp: int = 0

    # Breaker contract. Failure evaluation is intentionally conservative.
    failure_rate_threshold: Decimal = Decimal("0.50")
    min_breaker_samples: int = 20
    consecutive_failure_threshold: int = 5
    open_duration_ms: int = 4_000
    half_open_success_threshold: int = 3
    propagation_weights: tuple[Decimal, ...] = field(
        default_factory=lambda: (Decimal("0.50"), Decimal("0.30"), Decimal("0.20"))
    )

    def as_dict(self) -> dict[str, Any]:
        values = asdict(self)
        for key, value in list(values.items()):
            if isinstance(value, Decimal):
                values[key] = str(value)
            elif isinstance(value, tuple):
                values[key] = [str(v) for v in value]
        return values

    def quantize_decimal(self, value: Decimal, places: int) -> Decimal:
        quantum = Decimal(1).scaleb(-places)
        return value.quantize(quantum, rounding=ROUND_HALF_EVEN)

    def validate(self) -> None:
        if self.window_length_ms <= 0:
            raise ValueError("window_length_ms must be positive")
        if self.step_ms <= 0:
            raise ValueError("step_ms must be positive")
        if self.min_quota < 0 or self.max_quota < self.min_quota:
            raise ValueError("invalid quota bounds")
        if self.fixed_point_scale <= 0:
            raise ValueError("fixed_point_scale must be positive")
        if self.damping_alpha_bp < 0 or self.damping_alpha_bp > 10_000:
            raise ValueError("damping_alpha_bp must be within 0..10000")
        if self.primary_interpolation not in ("nearest_rank", "linear"):
            raise ValueError("invalid primary_interpolation")
        total = (
            self.quality_success_weight
            + self.quality_latency_weight
            + self.quality_budget_weight
        )
        if total != Decimal("1"):
            raise ValueError("quota quality weights must sum to 1")

    def window_start_for(self, timestamp_ms: int) -> int:
        return (timestamp_ms // self.step_ms) * self.step_ms

    def containing_window_starts(self, timestamp_ms: int) -> list[int]:
        latest = self.window_start_for(timestamp_ms)
        max_back = (self.window_length_ms - 1) // self.step_ms
        candidates = [latest - i * self.step_ms for i in range(max_back + 1)]
        return [
            start
            for start in candidates
            if start <= timestamp_ms < start + self.window_length_ms
        ]

