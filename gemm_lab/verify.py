from __future__ import annotations

import math
from typing import Any, Sequence

import numpy as np

from .errors import BusinessCode, GemmError, fail
from .kernel import SumStrategy
from .reference import comparison_metrics, error_bound, high_precision_elements, pow2_pairwise_rows


def nonfinite_summary(array: np.ndarray, *, label: str, block_size: int = 64) -> dict[str, Any]:
    array = np.asarray(array)
    nan_mask = np.isnan(array)
    pos_inf = np.isposinf(array)
    neg_inf = np.isneginf(array)
    nan_count = int(np.count_nonzero(nan_mask))
    pos_count = int(np.count_nonzero(pos_inf))
    neg_count = int(np.count_nonzero(neg_inf))
    result: dict[str, Any] = {
        "label": label,
        "shape": list(array.shape),
        "nan_count": nan_count,
        "pos_inf_count": pos_count,
        "neg_inf_count": neg_count,
        "total_nonfinite": nan_count + pos_count + neg_count,
    }
    if array.ndim != 2:
        result["positions_sample"] = []
        result["row_block_counts"] = {}
        result["col_block_counts"] = {}
        return result
    mask = nan_mask | pos_inf | neg_inf
    flat = np.flatnonzero(mask.reshape(-1))
    if flat.size:
        stride = max(1, flat.size // 16)
        indices = flat[::stride][:16]
        positions = []
        for flat_index in indices:
            row, col = np.unravel_index(int(flat_index), array.shape)
            value = array[row, col]
            kind = "nan" if np.isnan(value) else "pos_inf" if np.isposinf(value) else "neg_inf"
            positions.append({"row": int(row), "col": int(col), "kind": kind})
        result["positions_sample"] = positions
        rows = np.unique(flat // array.shape[1])
        cols = np.unique(flat % array.shape[1])
        result["row_block_counts"] = {
            str(int(row // block_size)): int(np.count_nonzero((flat // array.shape[1]) == row))
            for row in rows[:64]
        }
        result["col_block_counts"] = {
            str(int(col // block_size)): int(np.count_nonzero((flat % array.shape[1]) == col))
            for col in cols[:64]
        }
    else:
        result["positions_sample"] = []
        result["row_block_counts"] = {}
        result["col_block_counts"] = {}
    return result


def _row_blocks(m: int, seed: int, maximum: int = 3, block_rows: int = 8) -> list[tuple[int, int]]:
    block_rows = min(block_rows, m)
    rng = np.random.default_rng(seed)
    starts = {0, max(0, m // 2), max(0, m - block_rows)}
    possible_starts = max(1, m - block_rows + 1)
    target = min(maximum, possible_starts)
    while len(starts) < target:
        starts.add(int(rng.integers(0, possible_starts)))
    ordered = sorted(starts)[:maximum]
    return [(start, min(start + block_rows, m)) for start in ordered]


def _position_sample(
    rows: Sequence[int],
    n: int,
    seed: int,
    maximum: int = 12,
) -> list[tuple[int, int]]:
    rng = np.random.default_rng(seed)
    positions: list[tuple[int, int]] = []
    for row in rows:
        count = min(maximum, n)
        columns = np.unique(rng.integers(0, n, size=count))
        positions.extend((int(row), int(col)) for col in columns)
    return positions


def verify_single_matrix(
    output: np.ndarray,
    a_logical: np.ndarray,
    b_logical: np.ndarray,
    *,
    strategy: SumStrategy | str,
    dtype: np.dtype,
    atol: float,
    rtol: float,
    seed: int,
    maximum_row_blocks: int = 3,
) -> dict[str, Any]:
    m, k = a_logical.shape
    n = b_logical.shape[1]
    blocks = _row_blocks(m, seed, maximum=maximum_row_blocks)
    block_reports: list[dict[str, Any]] = []
    overall_mismatch = False
    high_precision_reports: list[dict[str, Any]] = []
    bound_reports: list[dict[str, Any]] = []
    max_abs_all = 0.0
    max_rel_all = 0.0
    for block_index, (row_start, row_stop) in enumerate(blocks):
        rows = list(range(row_start, row_stop))
        reference = pow2_pairwise_rows(a_logical, b_logical, rows)
        actual = output[row_start:row_stop, :]
        metrics = comparison_metrics(actual, reference, atol=atol, rtol=rtol)
        mismatch_mask = np.asarray(metrics.pop("mismatch_mask"))
        metrics.pop("absolute")
        metrics.pop("relative")
        offending = np.argwhere(mismatch_mask)
        if offending.size:
            min_col = int(np.min(offending[:, 1]))
            max_col = int(np.max(offending[:, 1]))
            max_deviation = float(metrics["max_abs_error"])
            overall_mismatch = True
        else:
            min_col = 0
            max_col = max(0, n - 1)
            max_deviation = 0.0
        max_abs_all = max(max_abs_all, float(metrics["max_abs_error"]))
        max_rel_all = max(max_rel_all, float(metrics["max_rel_error"]))
        block_reports.append(
            {
                "row_interval": [row_start, row_stop],
                "col_interval": [min_col, max_col],
                "max_abs_error_vs_pow2": metrics["max_abs_error"],
                "max_rel_error_vs_pow2": metrics["max_rel_error"],
                "finite_compared": metrics["finite_compared"],
                "nonfinite_pattern_mismatch": metrics["nonfinite_pattern_mismatch"],
                "mismatch_count": metrics["mismatch_count"],
                "max_deviation": max_deviation,
            }
        )
        positions = _position_sample(rows, n, seed + 1009 * (block_index + 1), maximum=12)
        samples = high_precision_elements(a_logical, b_logical, positions)
        for sample in samples:
            actual_value = float(output[sample.row, sample.col])
            if sample.nonfinite or not math.isfinite(actual_value):
                both_nan = math.isnan(sample.value) and math.isnan(actual_value)
                both_same_inf = (
                    math.isinf(sample.value)
                    and math.isinf(actual_value)
                    and (sample.value > 0) == (actual_value > 0)
                )
                nonfinite_match = bool(both_nan or both_same_inf)
                high_precision_reports.append(
                    {
                        "row": sample.row,
                        "col": sample.col,
                        "actual": actual_value,
                        "high_precision_fsum": sample.value,
                        "abs_error": None,
                        "rel_error": None,
                        "nonfinite_match": nonfinite_match,
                        "theoretical_abs_bound": None,
                        "within_theoretical_bound": nonfinite_match,
                    }
                )
                if not nonfinite_match:
                    overall_mismatch = True
                continue
            abs_error = abs(actual_value - sample.value)
            denominator = max(abs(sample.value), np.finfo(np.float64).tiny)
            relative_error = abs_error / denominator
            bound = error_bound(dtype=dtype, strategy=strategy, k=k, sum_abs=sample.sum_abs)
            bound_reports.append(bound)
            high_precision_reports.append(
                {
                    "row": sample.row,
                    "col": sample.col,
                    "actual": actual_value,
                    "high_precision_fsum": sample.value,
                    "abs_error": abs_error,
                    "rel_error": relative_error,
                    "theoretical_abs_bound": bound["absolute_bound"],
                    "within_theoretical_bound": bool(
                        not math.isfinite(bound["absolute_bound"])
                        or abs_error <= float(bound["absolute_bound"]) * (1.0 + 1e-12)
                    ),
                    "nonfinite_match": True,
                }
            )
    finite_bounds = [float(item["absolute_bound"]) for item in bound_reports if math.isfinite(float(item["absolute_bound"]))]
    theoretical_bound_max = max(finite_bounds) if finite_bounds else float("inf")
    bound_violations = sum(1 for item in high_precision_reports if not item["within_theoretical_bound"])
    return {
        "passed": not overall_mismatch,
        "sampled_row_blocks": [[start, stop] for start, stop in blocks],
        "block_comparisons": block_reports,
        "max_abs_error_vs_pow2": max_abs_all,
        "max_rel_error_vs_pow2": max_rel_all,
        "high_precision_reference": "math.fsum over rounded float products",
        "pow2_reference": "recursive power-of-two K decomposition",
        "comparison": "elementwise absolute and relative deviation",
        "high_precision_samples": high_precision_reports,
        "max_abs_error_vs_high_precision": max(
            (float(item["abs_error"]) for item in high_precision_reports if item["abs_error"] is not None),
            default=0.0,
        ),
        "max_rel_error_vs_high_precision": max(
            (float(item["rel_error"]) for item in high_precision_reports if item["rel_error"] is not None),
            default=0.0,
        ),
        "theoretical_abs_bound_max": theoretical_bound_max,
        "theoretical_bound_samples": len(bound_reports),
        "theoretical_bound_violations": bound_violations,
        "tolerance": {"atol": atol, "rtol": rtol},
    }


def verify_result(
    output: np.ndarray,
    a_logical: np.ndarray,
    b_logical: np.ndarray,
    *,
    strategy: SumStrategy | str,
    dtype: np.dtype,
    atol: float,
    rtol: float,
    seed: int,
) -> dict[str, Any]:
    if output.ndim == 2:
        matrix_reports = [
            verify_single_matrix(
                output,
                a_logical,
                b_logical,
                strategy=strategy,
                dtype=dtype,
                atol=atol,
                rtol=rtol,
                seed=seed,
            )
        ]
    else:
        matrix_reports = []
        for batch_index in range(output.shape[0]):
            matrix_reports.append(
                verify_single_matrix(
                    output[batch_index],
                    a_logical[batch_index],
                    b_logical[batch_index],
                    strategy=strategy,
                    dtype=dtype,
                    atol=atol,
                    rtol=rtol,
                    seed=seed + 7919 * (batch_index + 1),
                )
            )
    passed = all(bool(item["passed"]) for item in matrix_reports)
    report = {
        "passed": passed,
        "reference_sources": {
            "high_precision": "math.fsum over rounded float products",
            "pow2": "recursive power-of-two K decomposition",
        },
        "comparison": "per-element absolute |actual-reference| and relative |actual-reference|/|reference|",
        "tolerance": {"atol": atol, "rtol": rtol},
        "matrices": matrix_reports,
        "max_abs_error_vs_pow2": max(
            (float(item["max_abs_error_vs_pow2"]) for item in matrix_reports), default=0.0
        ),
        "max_rel_error_vs_pow2": max(
            (float(item["max_rel_error_vs_pow2"]) for item in matrix_reports), default=0.0
        ),
        "max_abs_error_vs_high_precision": max(
            (float(item["max_abs_error_vs_high_precision"]) for item in matrix_reports), default=0.0
        ),
        "max_rel_error_vs_high_precision": max(
            (float(item["max_rel_error_vs_high_precision"]) for item in matrix_reports), default=0.0
        ),
        "theoretical_abs_bound_max": max(
            (float(item["theoretical_abs_bound_max"]) for item in matrix_reports), default=0.0
        ),
        "theoretical_bound_violations": sum(
            int(item["theoretical_bound_violations"]) for item in matrix_reports
        ),
    }
    if not passed:
        raise GemmError(
            BusinessCode.E_NUMERIC_MISMATCH,
            "sampled output blocks exceed configured tolerance",
            details={
                "report": report,
                "message": "output buffer was not returned; caller-visible partial output is prohibited",
            },
        )
    return report

