"""Deterministic nearest-rank quantiles and window aggregation.

Nearest-rank rule (explicit contract, no interpolation):

    rank  = ceil(p * n)          (1-based, smallest rank whose cumulative
                                  coverage reaches the requested proportion)
    value = sorted_values[rank - 1]

The same sorted input always yields the same output, bit for bit, regardless
of platform float behavior, because selection is purely integer indexing.
"""
from __future__ import annotations

import math
from typing import Iterable, Sequence

from .config import QUANTILE_POINTS
from .errors import InsufficientSamples


def nearest_rank(sorted_values: Sequence[int], point: float) -> int:
    n = len(sorted_values)
    if n == 0:
        raise InsufficientSamples(
            "cannot compute quantile over an empty window",
            {"sample_count": 0, "quantile": point},
        )
    rank = math.ceil(point * n)
    rank = max(1, min(rank, n))
    return sorted_values[rank - 1]


def quantile_map(sorted_values: Sequence[int], points: Iterable[float] = QUANTILE_POINTS):
    return {f"p{int(round(p * 100)):02d}": nearest_rank(sorted_values, p) for p in points}


def good_rate_fixed(good_flags: Sequence[int]) -> int:
    """Good-event rate as a fixed-point value (scale 10**6)."""
    total = len(good_flags)
    if total == 0:
        raise InsufficientSamples("cannot compute good rate over an empty window")
    good = sum(1 for flag in good_flags if flag)
    # Exact: good * SCALE // total, half-to-even would only matter beyond the
    # 6th digit; integer truncation here is documented in docs/design.md.
    from .fixedpoint import SCALE

    numerator = good * SCALE
    quotient, remainder = divmod(numerator, total)
    if remainder * 2 >= total:
        quotient += 1
    return quotient
