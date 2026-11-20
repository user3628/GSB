"""Small deterministic time and statistics helpers."""

from __future__ import annotations

import math
from statistics import NormalDist


def wilson_interval(successes: int, trials: int, z: float) -> tuple[float, float]:
    """Wilson score interval for a binomial proportion."""

    if trials <= 0:
        return 0.0, 1.0
    p = successes / trials
    denominator = 1.0 + (z * z / trials)
    centre = (p + (z * z) / (2.0 * trials)) / denominator
    margin = (
        z
        * math.sqrt(
            (p * (1.0 - p) / trials) + (z * z) / (4.0 * trials * trials)
        )
        / denominator
    )
    return max(0.0, centre - margin), min(1.0, centre + margin)


def z_for_confidence(confidence: float) -> float:
    """Two-sided normal critical value for the configured confidence level."""

    return NormalDist().inv_cdf((1.0 + confidence) / 2.0)
