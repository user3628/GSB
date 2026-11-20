from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np

from .kernel import SumStrategy, normalize_strategy, strategy_error_order


@dataclass(frozen=True)
class HighPrecisionSample:
    row: int
    col: int
    value: float
    sum_abs: float
    nonfinite: bool

    def to_dict(self) -> dict[str, float | int | bool]:
        return {
            "row": self.row,
            "col": self.col,
            "value": float(self.value),
            "sum_abs": float(self.sum_abs),
            "nonfinite": self.nonfinite,
        }


def _pow2_split(size: int) -> int:
    if size <= 1:
        return 1
    return 1 << (size.bit_length() - 1)


def _pow2_pairwise_k(
    a_rows: np.ndarray,
    b_columns: np.ndarray,
    k0: int,
    k1: int,
) -> np.ndarray:
    size = k1 - k0
    if size == 1:
        with np.errstate(invalid="ignore", over="ignore", under="ignore", divide="ignore"):
            return a_rows[:, k0, None] * b_columns[k0, None, :]
    if size <= 0:
        return np.zeros((a_rows.shape[0], b_columns.shape[1]), dtype=a_rows.dtype)
    split = _pow2_split(size)
    if split == size:
        split = size // 2
    left = _pow2_pairwise_k(a_rows, b_columns, k0, k0 + split)
    right = _pow2_pairwise_k(a_rows, b_columns, k0 + split, k1)
    with np.errstate(invalid="ignore", over="ignore", under="ignore", divide="ignore"):
        return left + right


def pow2_pairwise_rows(
    a: np.ndarray,
    b: np.ndarray,
    rows: Sequence[int],
    columns: Sequence[int] | None = None,
) -> np.ndarray:
    """Reference GEMM rows via a power-of-two decomposition over K.

    The recursion splits each K interval at the largest power of two and sums
    the two independently generated sub-results.  No tiled kernel accumulator
    is shared with the implementation under test.
    """

    row_array = np.asarray(rows, dtype=np.intp)
    if row_array.ndim != 1 or row_array.size == 0:
        raise ValueError("rows must be a non-empty 1-D sequence")
    if isinstance(columns, Sequence):
        column_array = np.asarray(columns, dtype=np.intp)
    elif columns is not None:
        column_array = np.asarray(columns, dtype=np.intp)
    else:
        column_array = np.arange(b.shape[1], dtype=np.intp)
    if column_array.ndim != 1 or column_array.size == 0:
        raise ValueError("columns must be a non-empty 1-D sequence")
    return _pow2_pairwise_k(a[row_array, :], b[:, column_array], 0, a.shape[1])


def high_precision_element(a: np.ndarray, b: np.ndarray, row: int, col: int) -> HighPrecisionSample:
    """Compute one dot product with ``math.fsum`` over float products.

    Non-finite terms are accumulated in IEEE order because compensated exact
    summation is undefined for infinities; this keeps NaN/Inf propagation
    visible instead of normalizing it away.
    """

    with np.errstate(invalid="ignore", over="ignore", under="ignore", divide="ignore"):
        products = a[row, :].astype(np.float64) * b[:, col].astype(np.float64)
    nonfinite = bool(np.any(~np.isfinite(products)))
    abs_terms = np.abs(products)
    sum_abs = math.fsum(float(value) for value in abs_terms)
    if nonfinite:
        total = 0.0
        with np.errstate(invalid="ignore", over="ignore", under="ignore", divide="ignore"):
            for value in products:
                total = float(np.float64(total) + np.float64(value))
        value = total
    else:
        value = math.fsum(float(item) for item in products)
    return HighPrecisionSample(
        row=int(row),
        col=int(col),
        value=float(value),
        sum_abs=float(sum_abs),
        nonfinite=nonfinite,
    )


def high_precision_elements(
    a: np.ndarray,
    b: np.ndarray,
    positions: Iterable[tuple[int, int]],
) -> list[HighPrecisionSample]:
    return [high_precision_element(a, b, int(row), int(col)) for row, col in positions]


def error_bound(
    *,
    dtype: np.dtype,
    strategy: SumStrategy | str,
    k: int,
    sum_abs: float,
) -> dict[str, float | int | str]:
    normalized = normalize_strategy(strategy)
    eps = float(np.finfo(dtype).eps)
    unit = eps / 2.0
    order_info = strategy_error_order(normalized, k)
    order = int(order_info["summation_order"])
    if normalized is SumStrategy.COMPENSATED:
        bound = 2.0 * unit * sum_abs + 2.0 * unit * unit * max(1, k) * sum_abs
    else:
        denominator = 1.0 - order * unit
        gamma = float("inf") if denominator <= 0.0 else order * unit / denominator
        bound = gamma * sum_abs
    return {
        "strategy": normalized.value,
        "summation_order": order,
        "epsilon": eps,
        "sum_abs": float(sum_abs),
        "absolute_bound": float(bound),
        "description": str(order_info["description"]),
    }


def comparison_metrics(actual: np.ndarray, reference: np.ndarray, *, atol: float, rtol: float) -> dict[str, object]:
    actual = np.asarray(actual)
    reference = np.asarray(reference)
    if actual.shape != reference.shape:
        raise ValueError("actual and reference shapes differ")
    actual_nan = np.isnan(actual)
    reference_nan = np.isnan(reference)
    actual_inf = np.isinf(actual)
    reference_inf = np.isinf(reference)
    both_finite = np.isfinite(actual) & np.isfinite(reference)
    nonfinite_pattern_mismatch = ~(
        (actual_nan == reference_nan)
        & (actual_inf == reference_inf)
        & (
            (~actual_inf)
            | (np.signbit(actual) == np.signbit(reference))
        )
    )
    absolute = np.zeros(reference.shape, dtype=np.float64)
    relative = np.zeros(reference.shape, dtype=np.float64)
    absolute[both_finite] = np.abs(actual[both_finite] - reference[both_finite]).astype(np.float64)
    denominator = np.maximum(np.abs(reference[both_finite]).astype(np.float64), np.finfo(np.float64).tiny)
    relative[both_finite] = absolute[both_finite] / denominator
    threshold = atol + rtol * np.abs(reference.astype(np.float64))
    mismatch = nonfinite_pattern_mismatch | (both_finite & (absolute > threshold))
    mismatch_count = int(np.count_nonzero(mismatch))
    max_abs = float(np.max(absolute[both_finite])) if np.any(both_finite) else 0.0
    max_rel = float(np.max(relative[both_finite])) if np.any(both_finite) else 0.0
    return {
        "shape": list(actual.shape),
        "finite_compared": int(np.count_nonzero(both_finite)),
        "nonfinite_pattern_mismatch": int(np.count_nonzero(nonfinite_pattern_mismatch)),
        "mismatch_count": mismatch_count,
        "max_abs_error": max_abs,
        "max_rel_error": max_rel,
        "mismatch_mask": mismatch,
        "absolute": absolute,
        "relative": relative,
    }

