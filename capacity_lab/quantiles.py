"""Deterministic percentile implementations using exact rational arithmetic."""

from __future__ import annotations

from fractions import Fraction
from typing import Any, Iterable

from .errors import InsufficientSamples
from .fixedpoint import ceil_div, ceil_percentile_min_samples, fixed_decimal, quantize


METHODS = {"nearest_rank", "linear"}


def _prepare(values: Iterable[int | float | Fraction], *, already_sorted: bool = False) -> list[Fraction]:
    prepared = [value if isinstance(value, Fraction) else Fraction(value) for value in values]
    if any(value < 0 for value in prepared):
        raise ValueError("latency samples must be non-negative")
    if not already_sorted:
        prepared.sort()
    return prepared


def nearest_rank_fraction(
    values: Iterable[int | float | Fraction], percentile: str | int | Fraction, *, already_sorted: bool = False
) -> Fraction:
    p = _parse_percentile(percentile)
    ordered = _prepare(values, already_sorted=already_sorted)
    if not ordered:
        raise InsufficientSamples("no samples available")
    if p == 0:
        return ordered[0]
    rank = ceil_div(p.numerator * len(ordered), p.denominator)
    rank = max(1, min(rank, len(ordered)))
    return ordered[rank - 1]


def linear_fraction(
    values: Iterable[int | float | Fraction], percentile: str | int | Fraction, *, already_sorted: bool = False
) -> Fraction:
    p = _parse_percentile(percentile)
    ordered = _prepare(values, already_sorted=already_sorted)
    if len(ordered) < 2:
        raise InsufficientSamples("linear interpolation requires at least 2 samples")
    position = Fraction(len(ordered) - 1) * p
    lower_index = position.numerator // position.denominator
    fraction = position - lower_index
    if lower_index >= len(ordered) - 1:
        return ordered[-1]
    lower = ordered[lower_index]
    upper = ordered[lower_index + 1]
    return lower + fraction * (upper - lower)


def percentile_fraction(
    values: Iterable[int | float | Fraction],
    percentile: str | int | Fraction,
    method: str,
    *,
    min_samples: int = 1,
    already_sorted: bool = False,
) -> Fraction:
    if method not in METHODS:
        raise ValueError(f"unsupported quantile method: {method}")
    ordered = _prepare(values, already_sorted=already_sorted)
    p = _parse_percentile(percentile)
    required = max(min_samples, ceil_percentile_min_samples(p))
    if len(ordered) < required:
        raise InsufficientSamples(
            "sample count is insufficient for the requested percentile",
            details={
                "sample_count": len(ordered),
                "required_samples": required,
                "percentile": str(float(p)),
                "quantile_method": method,
            },
        )
    if method == "nearest_rank":
        return nearest_rank_fraction(ordered, p, already_sorted=True)
    return linear_fraction(ordered, p, already_sorted=True)


def percentile_snapshot(
    values: Iterable[int | float | Fraction],
    percentile: str | int | Fraction,
    method: str,
    *,
    decimals: int,
    rounding: str,
    min_samples: int,
) -> dict[str, Any]:
    ordered = _prepare(values)
    exact = percentile_fraction(
        ordered,
        percentile,
        method,
        min_samples=min_samples,
        already_sorted=True,
    )
    scale = 10**decimals
    fixed = quantize(exact, scale, rounding)
    return {
        "percentile": _percentile_label(percentile),
        "method": method,
        "sample_count": len(ordered),
        "exact_fraction": f"{exact.numerator}/{exact.denominator}",
        "fixed": fixed,
        "decimal": fixed_decimal(fixed, decimals),
        "decimals": decimals,
        "scale": scale,
        "rounding": rounding,
    }


def _parse_percentile(percentile: str | int | Fraction) -> Fraction:
    if isinstance(percentile, Fraction):
        value = percentile
    elif isinstance(percentile, int) and not isinstance(percentile, bool):
        value = Fraction(percentile)
    elif isinstance(percentile, str):
        text = percentile.strip()
        if text.endswith("%"):
            value = Fraction(text[:-1]) / 100
        elif text in {"50", "95", "99"}:
            value = Fraction(text, 100)
        else:
            value = Fraction(text)
    else:
        raise ValueError("percentile must be a string, integer or Fraction")
    if value < 0 or value > 1:
        raise ValueError("percentile must be in [0,1]")
    return value


def _percentile_label(percentile: str | int | Fraction) -> str:
    p = _parse_percentile(percentile)
    if p == Fraction(1, 2):
        return "P50"
    if p == Fraction(19, 20):
        return "P95"
    if p == Fraction(99, 100):
        return "P99"
    return f"P{float(p) * 100:g}"

