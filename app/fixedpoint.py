from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from app.errors import InsufficientSamplesError
from app.units import round_half_up_div, us_to_ms_x100

LATENCY_OUTPUT_DECIMALS = 2
US_PER_OUTPUT_QUANTUM = 10
OUTPUT_UNIT = "0.01ms"
ROUNDING_MODE = "round_half_up"


@dataclass
class CompensatedIntSum:
    """Neumaier compensated summation over an explicit integer fixed-point domain."""

    total: int = 0
    compensation: int = 0
    terms: list[int] = field(default_factory=list)

    def add(self, term: int) -> None:
        self.terms.append(term)
        tentative = self.total + term
        if abs(self.total) >= abs(term):
            self.compensation += (self.total - tentative) + term
        else:
            self.compensation += (term - tentative) + self.total
        self.total = tentative

    @property
    def value(self) -> int:
        return self.total + self.compensation

    def as_dict(self) -> dict[str, int | str | list[int]]:
        return {
            "algorithm": "neumaier_integer_fixedpoint",
            "unit": "us",
            "terms": list(self.terms),
            "term_count": len(self.terms),
            "raw_total": self.total,
            "compensation": self.compensation,
            "total": self.value,
        }


def compensated_sum_us(values: Iterable[int]) -> CompensatedIntSum:
    result = CompensatedIntSum()
    for value in values:
        if not isinstance(value, int):
            raise TypeError("compensated sum accepts integer microseconds only")
        result.add(value)
    return result


@dataclass(frozen=True)
class QuantileValue:
    label: str
    percentile: int
    sample_count: int
    rank: int
    raw_us: int
    output_ms_x100: int
    rounded_us: int
    rounding_error_us: int

    def as_dict(self) -> dict[str, int | str | bool]:
        return {
            "label": self.label,
            "percentile": self.percentile,
            "method": "nearest_rank",
            "sample_count": self.sample_count,
            "rank": self.rank,
            "raw_us": self.raw_us,
            "output_unit": OUTPUT_UNIT,
            "output_ms_fixed": self.output_ms_x100,
            "rounded_us": self.rounded_us,
            "rounding_error_us": self.rounding_error_us,
            "rounding_bit": self.rounding_error_us != 0,
        }


def nearest_rank(values: list[int], percentile: int, label: str | None = None) -> QuantileValue:
    if percentile < 1 or percentile > 100:
        raise ValueError("percentile must be in [1, 100]")
    if not values:
        raise InsufficientSamplesError(
            f"{label or f'P{percentile}'} requires at least one latency observation",
            details={
                "requested_percentile": percentile,
                "sample_count": 0,
                "method": "nearest_rank",
            },
        )
    rank = max(1, (percentile * len(values) + 99) // 100)
    if rank > len(values):
        raise InsufficientSamplesError(
            f"{label or f'P{percentile}'} rank {rank} exceeds sample count {len(values)}",
            details={
                "requested_percentile": percentile,
                "rank": rank,
                "sample_count": len(values),
                "method": "nearest_rank",
            },
        )
    ordered = sorted(values)
    raw = ordered[rank - 1]
    output = us_to_ms_x100(raw)
    rounded_us = output * US_PER_OUTPUT_QUANTUM
    return QuantileValue(
        label=label or f"P{percentile}",
        percentile=percentile,
        sample_count=len(values),
        rank=rank,
        raw_us=raw,
        output_ms_x100=output,
        rounded_us=rounded_us,
        rounding_error_us=rounded_us - raw,
    )


def quantile_set(values: list[int]) -> list[QuantileValue]:
    return [
        nearest_rank(values, 50, "P50"),
        nearest_rank(values, 95, "P95"),
        nearest_rank(values, 99, "P99"),
    ]


def output_conversion_trace(raw_us: int) -> dict[str, int | str | bool]:
    output = us_to_ms_x100(raw_us)
    rounded_us = output * US_PER_OUTPUT_QUANTUM
    error = rounded_us - raw_us
    return {
        "source_unit": "us",
        "target_unit": OUTPUT_UNIT,
        "target_scale": 100,
        "rounding_mode": ROUNDING_MODE,
        "quantum_us": US_PER_OUTPUT_QUANTUM,
        "unrounded_us": raw_us,
        "output_ms_fixed": output,
        "rounded_us": rounded_us,
        "rounding_error_us": error,
        "rounding_bit": error != 0,
        "rounding_direction": "up" if error > 0 else ("down" if error < 0 else "none"),
        "discarded_us": raw_us % US_PER_OUTPUT_QUANTUM,
    }


def rounding_units(raw_us: int) -> tuple[int, int, int]:
    """Return (half-up quotient, remainder, signed corresponding error)."""

    quotient = round_half_up_div(raw_us, US_PER_OUTPUT_QUANTUM)
    return quotient, raw_us % US_PER_OUTPUT_QUANTUM, quotient * US_PER_OUTPUT_QUANTUM - raw_us
