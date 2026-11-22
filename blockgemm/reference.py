"""Local high-precision reference and sampled numeric self-checks."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .errors import numeric_mismatch


DEFAULT_ATOL = 1e-12
DEFAULT_RTOL = 1e-10


def power_of_two_sum_tile(
    a: np.ndarray,
    b: np.ndarray,
    *,
    leaf_k: int = 8,
) -> np.ndarray:
    """Reference tile product using long-double and a power-of-two sum tree.

    Products are first accumulated in fixed-size leaf groups (default 8 K
    positions), then leaf results are reduced by repeatedly pairing adjacent
    nodes. Thus every final sum is a binary tree of non-overlapping ranges,
    making the reference tolerant to ordinary GEMM K-order cancellation.
    """

    a_high = np.asarray(a, dtype=np.longdouble)
    b_high = np.asarray(b, dtype=np.longdouble)
    if a_high.ndim != 2 or b_high.ndim != 2:
        raise ValueError("reference tile operands must be 2-D")
    if a_high.shape[1] != b_high.shape[0]:
        raise ValueError("reference tile K dimensions do not match")
    if leaf_k <= 0:
        raise ValueError("leaf_k must be positive")

    width = b_high.shape[1]
    terms: list[np.ndarray] = []
    for k_start in range(0, a_high.shape[1], leaf_k):
        k_end = min(a_high.shape[1], k_start + leaf_k)
        leaf = np.zeros((a_high.shape[0], width), dtype=np.longdouble)
        with np.errstate(invalid="ignore", over="ignore"):
            for k_index in range(k_start, k_end):
                leaf += np.multiply.outer(a_high[:, k_index], b_high[k_index, :])
        terms.append(leaf)

    if not terms:
        return np.zeros((a_high.shape[0], width), dtype=np.longdouble)
    while len(terms) > 1:
        next_level: list[np.ndarray] = []
        for index in range(0, len(terms), 2):
            if index + 1 == len(terms):
                next_level.append(terms[index])
            else:
                next_level.append(terms[index] + terms[index + 1])
        terms = next_level
    return terms[0]


def compare_tile(
    actual: np.ndarray,
    reference: np.ndarray,
    *,
    shard_id: str,
    block_coord: list[int],
    row_range: list[int],
    col_range: list[int],
    sequence: int | None = None,
    atol: float = DEFAULT_ATOL,
    rtol: float = DEFAULT_RTOL,
    skip_nonfinite: bool = True,
) -> dict[str, Any]:
    """Check a computed tile and return a JSON-safe per-block error report.

    When a block inherits NaN/Inf inputs, numeric ordering checks are skipped
    but the report explicitly retains the non-finite counts.
    """

    actual64 = np.asarray(actual, dtype=np.float64)
    reference64 = np.asarray(reference, dtype=np.float64)
    if actual64.shape != reference64.shape:
        raise ValueError("actual and reference tiles must have the same shape")
    actual_nonfinite = int(np.count_nonzero(~np.isfinite(actual64)))
    reference_nonfinite = int(np.count_nonzero(~np.isfinite(reference64)))
    if skip_nonfinite and (actual_nonfinite or reference_nonfinite):
        return {
            "sequence": sequence,
            "shard_id": shard_id,
            "block_coord": list(block_coord),
            "row_range": list(row_range),
            "col_range": list(col_range),
            "status": "skipped_nonfinite",
            "actual_nonfinite_count": actual_nonfinite,
            "reference_nonfinite_count": reference_nonfinite,
            "max_abs_error": None,
            "max_rel_error": None,
        }

    difference = np.abs(actual64 - reference64)
    finite = np.isfinite(difference)
    if not np.all(finite):
        max_abs = float("inf")
        max_rel = float("inf")
        violations = int(np.count_nonzero(~finite))
    else:
        max_abs = float(np.max(difference)) if difference.size else 0.0
        denominator = np.maximum(np.abs(reference64), np.finfo(np.float64).tiny)
        relative = difference / denominator
        max_rel = float(np.max(relative)) if relative.size else 0.0
        violations = int(
            np.count_nonzero(difference > (float(atol) + float(rtol) * np.abs(reference64)))
        )

    report = {
        "sequence": sequence,
        "shard_id": shard_id,
        "block_coord": list(block_coord),
        "row_range": list(row_range),
        "col_range": list(col_range),
        "status": "ok" if violations == 0 else "mismatch",
        "actual_nonfinite_count": actual_nonfinite,
        "reference_nonfinite_count": reference_nonfinite,
        "max_abs_error": max_abs,
        "max_rel_error": max_rel,
        "violating_elements": violations,
        "tolerances": {"atol": float(atol), "rtol": float(rtol)},
    }
    if violations:
        raise numeric_mismatch(
            "sampled block exceeds the configured numeric tolerance",
            shard_id=shard_id,
            block_coord=list(block_coord),
            row_range=list(row_range),
            col_range=list(col_range),
            max_abs_error=max_abs,
            max_rel_error=max_rel,
            tolerances={"atol": float(atol), "rtol": float(rtol), "violating_elements": violations},
            sequence=sequence,
        )
    return report


def sample_sequences(total_blocks: int, requested: int) -> list[int]:
    if requested <= 0 or total_blocks <= 0:
        return []
    if requested >= total_blocks:
        return list(range(total_blocks))
    if requested == 1:
        return [0]
    indices = {
        int(round(index * (total_blocks - 1) / (requested - 1)))
        for index in range(requested)
    }
    return sorted(indices)


def verify_block_against_reference(
    problem: Any,
    descriptor: Any,
    actual: np.ndarray,
    *,
    atol: float = DEFAULT_ATOL,
    rtol: float = DEFAULT_RTOL,
) -> dict[str, Any]:
    a_matrix = problem.a_matrix(descriptor.batch_index)
    b_matrix = problem.b_matrix(descriptor.batch_index)
    reference = power_of_two_sum_tile(
        a_matrix[descriptor.row_start : descriptor.row_end, :],
        b_matrix[:, descriptor.col_start : descriptor.col_end],
    )
    reference64 = np.asarray(reference, dtype=np.float64)
    return compare_tile(
        actual,
        reference64,
        shard_id=descriptor.shard_id,
        block_coord=descriptor.block_coord,
        row_range=descriptor.row_range,
        col_range=descriptor.col_range,
        sequence=descriptor.sequence,
        atol=atol,
        rtol=rtol,
        skip_nonfinite=descriptor.nonfinite_affected,
    )


def json_safe_float(value: float | None) -> float | None:
    if value is None:
        return None
    return value if math.isfinite(value) else None
