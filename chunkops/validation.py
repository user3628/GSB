"""High-precision reference validation and per-block error reports."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .errors import ChunkOpsError, E_INVALID_SHAPE, E_NUMERIC_MISMATCH
from .fingerprint import digest_object, sha256_array, sha256_text
from .ops import block_error_estimate, find_zero_rows, scan_nonfinite
from .strategies import ChunkPlan


def _ranges(length: int, step: int) -> list[tuple[int, int]]:
    step = max(1, int(step))
    return [(start, min(length, start + step)) for start in range(0, length, step)]


def _json_number(value: float | int | None) -> float | int | str | None:
    if value is None:
        return None
    number = float(value)
    if math.isnan(number):
        return "NaN"
    if math.isinf(number):
        return "Infinity" if number > 0 else "-Infinity"
    return number


def _ieee_fsum(values: list[float] | tuple[float, ...]) -> float:
    """math.fsum with deterministic IEEE handling of NaNs and mixed infinities."""

    sequence = list(values)
    if any(math.isnan(value) for value in sequence):
        return float("nan")
    pos_inf = any(value == float("inf") for value in sequence)
    neg_inf = any(value == float("-inf") for value in sequence)
    if pos_inf and neg_inf:
        return float("nan")
    if pos_inf:
        return float("inf")
    if neg_inf:
        return float("-inf")
    try:
        return math.fsum(sequence)
    except (OverflowError, ValueError):
        return float("nan")


def naive_softmax_vector(values: np.ndarray) -> np.ndarray:
    """Definition-level float64 reference using math.fsum, one vector at a time."""

    vector = np.asarray(values, dtype=np.float64)
    if vector.size == 0:
        return vector.copy()
    maximum = float(np.max(vector))
    exponentials = []
    for value in vector:
        with np.errstate(all="ignore"):
            try:
                item = math.exp(float(value) - maximum)
            except (OverflowError, ValueError):
                item = float("nan")
        exponentials.append(item)
    denominator = _ieee_fsum(exponentials)
    result = np.empty(vector.shape, dtype=np.float64)
    for index, item in enumerate(exponentials):
        with np.errstate(all="ignore"):
            try:
                result[index] = item / denominator
            except ZeroDivisionError:
                result[index] = float("nan")
    return result


def naive_layernorm_vector(
    values: np.ndarray,
    gamma: np.ndarray | None = None,
    beta: np.ndarray | None = None,
    eps: float = 1e-5,
) -> np.ndarray:
    """Definition-level Welford-free LayerNorm reference using math.fsum."""

    vector = np.asarray(values, dtype=np.float64)
    count = vector.size
    if count == 0:
        return vector.copy()
    mean = _ieee_fsum([float(value) for value in vector]) / count
    centered = [float(value) - mean for value in vector]
    variance = _ieee_fsum([value * value for value in centered]) / count
    with np.errstate(all="ignore"):
        inverse_std = 1.0 / math.sqrt(variance + eps)
    result = np.empty(vector.shape, dtype=np.float64)
    for index, value in enumerate(centered):
        result[index] = value * inverse_std
        if gamma is not None:
            result[index] *= float(gamma[index])
        if beta is not None:
            result[index] += float(beta[index])
    return result


def _reference_block(
    x: np.ndarray,
    plan: ChunkPlan,
    row_range: tuple[int, int],
    col_range: tuple[int, int],
    *,
    gamma: np.ndarray | None,
    beta: np.ndarray | None,
    eps: float,
) -> np.ndarray:
    rows = np.arange(row_range[0], row_range[1])
    cols = np.arange(col_range[0], col_range[1])
    if plan.axis == 1:
        block = np.empty((rows.size, cols.size), dtype=np.float64)
        for local, row in enumerate(rows):
            vector = x[row, :]
            if plan.op == "softmax":
                block[local, :] = naive_softmax_vector(vector)
            else:
                block[local, :] = naive_layernorm_vector(
                    vector, gamma=gamma, beta=beta, eps=eps
                )
        return block
    block = np.empty((rows.size, cols.size), dtype=np.float64)
    for local, col in enumerate(cols):
        vector = x[:, col]
        if plan.op == "softmax":
            block[:, local] = naive_softmax_vector(vector)
        else:
            block[:, local] = naive_layernorm_vector(
                vector, gamma=gamma, beta=beta, eps=eps
            )
    return block


def _compare_blocks(actual: np.ndarray, reference: np.ndarray, atol: float, rtol: float) -> dict[str, Any]:
    actual64 = np.asarray(actual, dtype=np.float64)
    reference64 = np.asarray(reference, dtype=np.float64)
    if actual64.shape != reference64.shape:
        raise ChunkOpsError(
            E_INVALID_SHAPE,
            "output and reference block shapes differ",
            {"actual": list(actual64.shape), "reference": list(reference64.shape)},
        )
    actual_nan = np.isnan(actual64)
    reference_nan = np.isnan(reference64)
    nan_match = actual_nan & reference_nan
    actual_finite = np.isfinite(actual64)
    reference_finite = np.isfinite(reference64)
    finite_pair = actual_finite & reference_finite
    identical_inf = np.isinf(actual64) & np.isinf(reference64) & (
        np.signbit(actual64) == np.signbit(reference64)
    )

    difference = np.zeros_like(actual64)
    difference[finite_pair] = np.abs(actual64[finite_pair] - reference64[finite_pair])
    absolute_tolerance = atol + rtol * np.abs(reference64)
    violations = np.zeros(actual64.shape, dtype=bool)
    violations[finite_pair] = difference[finite_pair] > absolute_tolerance[finite_pair]
    nonfinite_mismatch = ~(nan_match | identical_inf | finite_pair)
    violations |= nonfinite_mismatch

    finite_differences = difference[finite_pair]
    max_abs = float(np.max(finite_differences)) if finite_differences.size else 0.0
    if np.any(nonfinite_mismatch):
        max_abs = float("inf")
    denominator = np.maximum(np.abs(reference64), np.finfo(np.float64).tiny)
    relative = np.zeros_like(difference)
    relative[finite_pair] = difference[finite_pair] / denominator[finite_pair]
    max_rel = float(np.max(relative[finite_pair])) if finite_pair.any() else 0.0
    if np.any(nonfinite_mismatch):
        max_rel = float("inf")

    coords: list[list[int]] = []
    if np.any(violations):
        for flat_index in np.flatnonzero(violations).reshape(-1)[:10]:
            row, col = np.unravel_index(int(flat_index), actual64.shape)
            coords.append([int(row), int(col)])
    return {
        "within_tolerance": bool(not np.any(violations)),
        "violation_count": int(np.count_nonzero(violations)),
        "mismatch_coordinates_local": coords,
        "max_abs": _json_number(max_abs),
        "max_rel": _json_number(max_rel),
        "max_abs_finite": _json_number(
            float(np.max(finite_differences)) if finite_differences.size else 0.0
        ),
        "nan_match_count": int(np.count_nonzero(nan_match)),
        "nonfinite_mismatch_count": int(np.count_nonzero(nonfinite_mismatch)),
    }


def build_error_estimates(
    x: np.ndarray,
    plan: ChunkPlan,
    *,
    output_itemsize: int,
    seed: int,
    effective_row_block: int | None = None,
    effective_col_block: int | None = None,
) -> list[dict[str, Any]]:
    """Per-output-block analytic envelopes with explicit sampled positions."""

    rng = np.random.default_rng(seed)
    reduction_length = plan.reduction_length
    row_block = int(effective_row_block or plan.row_block)
    col_block = int(effective_col_block or plan.col_block)
    reduction_ranges = (
        _ranges(plan.cols, col_block)
        if plan.axis == 1
        else _ranges(plan.rows, row_block)
    )
    output_ranges = (
        _ranges(plan.rows, row_block)
        if plan.axis == 1
        else _ranges(plan.cols, col_block)
    )
    estimates: list[dict[str, Any]] = []
    for block_index, (output_start, output_end) in enumerate(output_ranges):
        count = min(4, max(1, output_end - output_start))
        sampled_units = rng.choice(
            np.arange(output_start, output_end), size=count, replace=False
        )
        sample_positions: list[list[int]] = []
        sampled_values: list[float] = []
        sampled_max_weight = 0.0
        sampled_abs_sum = 0.0
        sample_count = min(8, reduction_length)
        for unit in sampled_units:
            reduction_indices = rng.choice(
                np.arange(reduction_length), size=sample_count, replace=False
            )
            if plan.op == "softmax":
                row_value = x[int(unit), :] if plan.axis == 1 else x[:, int(unit)]
                with np.errstate(all="ignore"):
                    maximum = float(np.max(row_value))
                    for red_index in np.sort(reduction_indices):
                        value = float(row_value[int(red_index)])
                        weight = math.exp(value - maximum) if math.isfinite(value - maximum) else float("nan")
                        sampled_values.append(weight)
                        sampled_max_weight = max(sampled_max_weight, weight)
                        if plan.axis == 1:
                            sample_positions.append([int(unit), int(red_index)])
                        else:
                            sample_positions.append([int(red_index), int(unit)])
            else:
                for red_index in np.sort(reduction_indices):
                    value = float(
                        x[int(unit), int(red_index)]
                        if plan.axis == 1
                        else x[int(red_index), int(unit)]
                    )
                    sampled_values.append(value)
                    sampled_abs_sum += abs(value)
                    if plan.axis == 1:
                        sample_positions.append([int(unit), int(red_index)])
                    else:
                        sample_positions.append([int(red_index), int(unit)])
        if plan.op == "softmax":
            finite_weights = [value for value in sampled_values if math.isfinite(value)]
            sampled_max_weight = max(finite_weights, default=0.0)
            sampled_abs_scaled = max(0.0, float(sampled_max_weight))
        else:
            sampled_count = max(1, len(sampled_values))
            sampled_abs_scaled = sampled_abs_sum * reduction_length / sampled_count
        bound = block_error_estimate(
            op=plan.op,
            reduction_length=reduction_length,
            strategy=plan.sum_strategy,
            sampled_abs_sum_scaled=sampled_abs_scaled,
            sampled_max_weight=sampled_max_weight,
            output_itemsize=output_itemsize,
        )
        if any(not math.isfinite(value) for value in sampled_values):
            bound = float("inf")
        if plan.axis == 1:
            row_range = [int(output_start), int(output_end)]
            col_range = [0, int(plan.cols)]
        else:
            row_range = [0, int(plan.rows)]
            col_range = [int(output_start), int(output_end)]
        estimates.append(
            {
                "block_index": block_index,
                "row_range": row_range,
                "col_range": col_range,
                "reduction_order": [[int(a), int(b)] for a, b in reduction_ranges],
                "sum_strategy": plan.sum_strategy,
                "sample_seed": seed,
                "sample_positions": sample_positions,
                "sampled_values": [_json_number(value) for value in sampled_values],
                "sampled_abs_sum_scaled": _json_number(sampled_abs_scaled),
                "sampled_max_weight": _json_number(sampled_max_weight),
                "estimated_error_bound": _json_number(bound),
                "method": (
                    "gamma(order) * sampled magnitude + output quantization; "
                    "inf when a sampled value is non-finite"
                ),
            }
        )
    return estimates


def _select_sample_ranges(
    rows: int,
    cols: int,
    plan: ChunkPlan,
    *,
    seed: int,
    max_blocks: int,
    forced_blocks: list[int],
    effective_row_block: int | None = None,
    effective_col_block: int | None = None,
) -> list[tuple[int, int, int, int]]:
    rng = np.random.default_rng(seed)
    if plan.axis == 1:
        full = rows
        block = max(1, int(effective_row_block or plan.row_block))
        forced = forced_blocks
    else:
        full = cols
        block = max(1, int(effective_col_block or plan.col_block))
        forced = forced_blocks
    starts = {0, max(0, full - block)}
    starts.update(min(max(0, start), max(0, full - block)) for start in forced)
    possible = max(1, (full + block - 1) // block)
    for _ in range(min(max_blocks, possible * 2)):
        starts.add(int(rng.integers(0, possible)) * block)
        if len(starts) >= max_blocks:
            break
    ranges = []
    for start in sorted(starts):
        end = min(full, start + block)
        if start >= end:
            continue
        if plan.axis == 1:
            ranges.append((start, end, 0, cols))
        else:
            ranges.append((0, rows, start, end))
        if len(ranges) >= max_blocks:
            break
    return ranges


def validate_output(
    x: np.ndarray,
    output: np.ndarray,
    plan: ChunkPlan,
    *,
    gamma: np.ndarray | None = None,
    beta: np.ndarray | None = None,
    eps: float = 1e-5,
    atol: float = 1e-4,
    rtol: float = 1e-4,
    max_sample_blocks: int = 10,
    seed: int | None = None,
    fragment_id: str | None = None,
    raise_on_mismatch: bool = True,
    effective_row_block: int | None = None,
    effective_col_block: int | None = None,
) -> dict[str, Any]:
    """Compare deterministic sampled output blocks with a naive reference."""

    if x.shape != output.shape:
        raise ChunkOpsError(
            E_INVALID_SHAPE,
            "output shape differs from input shape",
            {
                "fragment_id": fragment_id,
                "input_shape": [int(v) for v in x.shape],
                "output_shape": [int(v) for v in output.shape],
                "position": "output",
            },
        )
    plan_digest = digest_object(
        {
            "op": plan.op,
            "axis": plan.axis,
            "rows": plan.rows,
            "cols": plan.cols,
            "row_block": plan.row_block,
            "col_block": plan.col_block,
            "sum_strategy": plan.sum_strategy,
        }
    )
    if seed is None:
        seed = int(sha256_text(plan_digest)[:16], 16)
    nonfinite = scan_nonfinite(x, fragment_id=fragment_id)
    zero_rows = find_zero_rows(x)
    forced: list[int] = list(zero_rows[:4])
    for key in ("first_nan", "first_pos_inf", "first_neg_inf"):
        position = nonfinite.get(key)
        if position:
            forced.append(position["row"] if plan.axis == 1 else position["col"])
    sample_ranges = _select_sample_ranges(
        plan.rows,
        plan.cols,
        plan,
        seed=seed,
        max_blocks=max_sample_blocks,
        forced_blocks=forced,
        effective_row_block=effective_row_block,
        effective_col_block=effective_col_block,
    )
    error_estimates = build_error_estimates(
        x,
        plan,
        output_itemsize=output.dtype.itemsize,
        seed=seed,
        effective_row_block=effective_row_block,
        effective_col_block=effective_col_block,
    )
    block_reports: list[dict[str, Any]] = []
    failed_blocks: list[dict[str, Any]] = []
    for index, (row_start, row_end, col_start, col_end) in enumerate(sample_ranges):
        actual = output[row_start:row_end, col_start:col_end]
        reference = _reference_block(
            x,
            plan,
            (row_start, row_end),
            (col_start, col_end),
            gamma=gamma,
            beta=beta,
            eps=eps,
        )
        comparison = _compare_blocks(actual, reference, atol=atol, rtol=rtol)
        overlapping_estimates = []
        for candidate_index, estimate in enumerate(error_estimates):
            e_row_start, e_row_end = estimate["row_range"]
            e_col_start, e_col_end = estimate["col_range"]
            if (
                e_row_start < row_end
                and e_row_end > row_start
                and e_col_start < col_end
                and e_col_end > col_start
            ):
                overlapping_estimates.append((candidate_index, estimate))
        estimated_bound: float | str | None = None
        if overlapping_estimates:
            bounds = [
                estimate["estimated_error_bound"]
                for _, estimate in overlapping_estimates
            ]
            if any(bound in ("Infinity", "NaN") for bound in bounds):
                estimated_bound = "Infinity"
            else:
                numeric_bounds = [float(bound) for bound in bounds if bound is not None]
                if numeric_bounds:
                    estimated_bound = max(numeric_bounds)
        observed = comparison["max_abs"]
        if isinstance(observed, (int, float)) and isinstance(estimated_bound, (int, float)):
            covers = float(observed) <= float(estimated_bound)
        elif estimated_bound == "Infinity":
            covers = True
        elif observed in ("Infinity", "-Infinity", "NaN") and estimated_bound in (
            "Infinity",
            "-Infinity",
            "NaN",
        ):
            covers = True
        elif observed == 0:
            covers = True
        else:
            covers = None
        item = {
            "block_index": index,
            "row_range": [row_start, row_end],
            "col_range": [col_start, col_end],
            "error_estimate_block_indices": [
                index for index, _ in overlapping_estimates
            ],
            "estimated_error_bound": estimated_bound,
            "bound_covers_observed": covers,
            **comparison,
        }
        block_reports.append(item)
        if not comparison["within_tolerance"]:
            failed_blocks.append(item)
    report = {
        "schema_version": 1,
        "status": "ok" if not failed_blocks else "mismatch",
        "fragment_id": fragment_id,
        "op": plan.op,
        "axis": plan.axis,
        "shape": [int(v) for v in x.shape],
        "plan_digest": plan_digest,
        "sum_strategy": plan.sum_strategy,
        "row_block": plan.row_block,
        "col_block": plan.col_block,
        "effective_row_block": int(effective_row_block or plan.row_block),
        "effective_col_block": int(effective_col_block or plan.col_block),
        "tolerance": {"atol": float(atol), "rtol": float(rtol)},
        "sampling_seed": seed,
        "sampled_block_count": len(block_reports),
        "sample_blocks": block_reports,
        "failed_blocks": failed_blocks,
        "reference_method": (
            "per-vector float64 naive definition with math.fsum, no blocked "
            "accumulator and no probability product"
        ),
        "error_estimates": error_estimates,
        "input_nonfinite": nonfinite,
        "all_zero_rows": zero_rows,
        "output_digest": sha256_array(output),
    }
    if failed_blocks and raise_on_mismatch:
        raise ChunkOpsError(
            E_NUMERIC_MISMATCH,
            "sampled output blocks exceed configured tolerance",
            {
                "fragment_id": fragment_id,
                "failed_blocks": failed_blocks,
                "tolerance": report["tolerance"],
                "report": report,
            },
        )
    return report
