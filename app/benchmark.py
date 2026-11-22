from __future__ import annotations

import math
from fractions import Fraction
from typing import Any, Iterable

import numpy as np


# Fixed mixed-magnitude Q6 sample. The first value is far above the ULP needed
# to represent the trailing unit values in binary64.
SYNTHETIC_VALUES_Q6: tuple[int, ...] = (10**16,) + (1,) * 20 + (3,) * 10


def naive_sum(values: Iterable[int]) -> float:
    total = np.float64(0.0)
    for value in values:
        total = np.float64(total + np.float64(value))
    return float(total)


def kahan_sum(values: Iterable[int]) -> float:
    total = np.float64(0.0)
    compensation = np.float64(0.0)
    for value in values:
        adjusted = np.float64(np.float64(value) - compensation)
        updated = np.float64(total + adjusted)
        compensation = np.float64((updated - total) - adjusted)
        total = updated
    return float(total)


def _float_error_payload(observed: float, exact: int) -> dict[str, Any]:
    exact_float = float(exact)
    observed_minus_exact = observed - exact_float
    relative = (
        abs(observed_minus_exact) / abs(exact_float) if exact_float != 0.0 else 0.0
    )
    return {
        "observed": observed,
        "observed_as_integer_rounded": int(round(observed)),
        "error_q6": observed_minus_exact,
        "relative_error": relative,
    }


def compensation_benchmark(values: Iterable[int] = SYNTHETIC_VALUES_Q6) -> dict[str, Any]:
    items = tuple(values)
    exact = sum(items)
    naive = naive_sum(items)
    compensated = kahan_sum(items)
    count = len(items)
    epsilon = math.ulp(1.0)
    sum_abs = sum(abs(value) for value in items)
    # Conservative first-order bounds; Kahan's compensation removes the leading
    # n*epsilon term, leaving a quadratic-in-epsilon bound.
    denominator = Fraction(max(1.0 - count * epsilon, 1e-300))
    naive_bound = Fraction(count * epsilon) / denominator * sum_abs
    kahan_bound = Fraction(
        2 * epsilon + count * count * epsilon * epsilon
    ) / denominator * sum_abs
    observed_naive = abs(naive - float(exact))
    observed_kahan = abs(compensated - float(exact))
    return {
        "sample_name": "fixed_mixed_magnitude_q6",
        "input_count": count,
        "inputs_q6": list(items),
        "exact_integer_sum_q6": exact,
        "exact_display": f"{exact // 1_000_000}.{exact % 1_000_000:06d}",
        "naive_float64": _float_error_payload(naive, exact),
        "kahan_float64": _float_error_payload(compensated, exact),
        "error_bounds": {
            "naive_gamma_bound_q6": f"{naive_bound.numerator}/{naive_bound.denominator}",
            "kahan_bound_q6": f"{kahan_bound.numerator}/{kahan_bound.denominator}",
            "naive_observed_q6": observed_naive,
            "kahan_observed_q6": observed_kahan,
            "naive_within_bound": observed_naive <= float(naive_bound),
            "kahan_within_bound": observed_kahan <= float(kahan_bound),
        },
        "conclusion": (
            "Exact Q6 integer accumulation is canonical. Kahan is retained as a "
            "defensive float aggregation check; deterministic workbench totals never "
            "depend on float summation order."
        ),
    }
