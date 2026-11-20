from __future__ import annotations

import math
from collections.abc import Iterable

from .errors import InsufficientSamples


PERCENTILES: dict[str, float] = {"p50_ms": 0.50, "p95_ms": 0.95, "p99_ms": 0.99}


def required_sample_count(percentile: float) -> int:
    """Minimum sample count for a meaningful nearest-rank percentile.

    Using the inverse percentile denominator means P99 needs at least 100
    observations. This prevents a one-sample stage from being labelled P99.
    """

    if not 0.0 < percentile <= 1.0:
        raise ValueError("percentile must be in (0, 1]")
    if percentile == 1.0:
        return 1
    return math.ceil(1.0 / (1.0 - percentile))


def nearest_rank(values: Iterable[float], percentile: float) -> float:
    """Deterministic nearest-rank percentile using 1-based ceil(p * n)."""

    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise InsufficientSamples(0, 1, f"p{percentile * 100:g}")
    required = required_sample_count(percentile)
    if len(ordered) < required:
        raise InsufficientSamples(
            len(ordered), required, f"p{percentile * 100:g}"
        )
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def compute_percentiles(
    values: Iterable[float], *, minimum_samples: int = 0
) -> dict[str, float]:
    ordered = sorted(float(value) for value in values)
    required = max(minimum_samples, *(required_sample_count(p) for p in PERCENTILES.values()))
    if len(ordered) < required:
        raise InsufficientSamples(len(ordered), required, "P99")
    result: dict[str, float] = {}
    for name, percentile in PERCENTILES.items():
        rank = max(1, math.ceil(percentile * len(ordered)))
        result[name] = round(ordered[rank - 1], 6)
    return result

