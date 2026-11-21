"""Deterministic bucket assignment: equal-width and equal-frequency.

Boundary conflict ruling (deterministic, documented):
  * equal_width: buckets are half-open [lo, hi); a score exactly on a
    boundary goes to the higher-index bucket; score == 1.0 goes to the
    last bucket.
  * equal_freq: records are ranked by (score, sample_id); records sharing
    the same score are never split across buckets - the whole tied group
    goes to the bucket of the group's lowest rank.
"""
from __future__ import annotations

import numpy as np

from .config import BINNING_EQUAL_WIDTH
from .models import EvalRecord


def assign_equal_width(score: float, n_bins: int) -> int:
    clipped = min(1.0, max(0.0, float(score)))
    return min(int(clipped * n_bins), n_bins - 1)


def assign_equal_freq(scores: np.ndarray, sample_ids: list[str], n_bins: int) -> np.ndarray:
    """Equal-frequency buckets with deterministic tie handling."""
    n = len(scores)
    if n == 0:
        return np.zeros(0, dtype=int)
    order = sorted(range(n), key=lambda i: (scores[i], sample_ids[i]))
    buckets = np.zeros(n, dtype=int)
    idx = 0
    while idx < n:
        # group of tied scores starting at rank idx
        jdx = idx + 1
        while jdx < n and scores[order[jdx]] == scores[order[idx]]:
            jdx += 1
        bucket = min((idx * n_bins) // n, n_bins - 1)  # lowest-rank rule
        for k in range(idx, jdx):
            buckets[order[k]] = bucket
        idx = jdx
    return buckets


def assign_buckets(records: list[EvalRecord], n_bins: int, binning: str) -> list[int]:
    if binning == BINNING_EQUAL_WIDTH:
        return [assign_equal_width(r.score, n_bins) for r in records]
    scores = np.array([r.score for r in records], dtype=float)
    ids = [r.sample_id for r in records]
    return [int(b) for b in assign_equal_freq(scores, ids, n_bins)]
