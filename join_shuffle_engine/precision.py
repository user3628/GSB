from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable


UNIT_ROUNDOFF = 2.0 ** -53
MERGE_CHUNK_SIZE = 4096


@dataclass(frozen=True)
class CompensatedSum:
    total: float
    compensation: float


def kahan_sum(values: Iterable[float]) -> CompensatedSum:
    total = 0.0
    compensation = 0.0
    for value in values:
        y = float(value) - compensation
        t = total + y
        compensation = (t - total) - y
        total = t
    return CompensatedSum(total, compensation)


def naive_sum(values: Iterable[float]) -> float:
    total = 0.0
    for value in values:
        total += float(value)
    return total


def merge_compensated_chunks(chunks: Iterable[CompensatedSum]) -> CompensatedSum:
    total = 0.0
    compensation = 0.0
    for chunk in chunks:
        y = chunk.total - compensation
        t = total + y
        compensation = (t - total) - y
        total = t
    return CompensatedSum(total, compensation)


def high_precision_bound(row_count: int, sum_abs: float) -> float:
    """Conservative two-level Kahan bound: (4u + 16Nu^2) * sum_abs."""

    if row_count <= 0 or sum_abs == 0.0:
        return 0.0
    unit = UNIT_ROUNDOFF
    bound = (4.0 + 16.0 * row_count * unit) * unit * sum_abs
    return bound if math.isfinite(bound) else math.inf


def fast_precision_bound(row_count: int, sum_abs: float) -> float:
    """Classical gamma_N bound: N*u/(1-N*u) * sum_abs."""

    if row_count <= 0 or sum_abs == 0.0:
        return 0.0
    product = row_count * UNIT_ROUNDOFF
    if product >= 0.5:
        return math.inf
    bound = (product / (1.0 - product)) * sum_abs
    return bound if math.isfinite(bound) else math.inf
