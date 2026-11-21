"""Unified deterministic block activation kernels."""

from __future__ import annotations

import math
from typing import Any, Iterator

import numpy as np

from .errors import E_NUMERIC
from .util import json_float, json_values

ACTIVATION_BOUNDS = {
    "gelu": (-0.171, math.inf),
    "silu": (-0.279, math.inf),
    "sigmoid": (0.0, 1.0),
}


def matrix_layout(shape: tuple[int, ...]) -> tuple[int, int]:
    return int(np.prod(shape[:-1], dtype=np.int64)), int(shape[-1])


def block_count(extent: int, block: int) -> int:
    return (extent + block - 1) // block


def activation_schedule(shape: tuple[int, ...], row_block: int, col_block: int) -> list[dict[str, Any]]:
    rows, cols = matrix_layout(shape)
    row_tiles = block_count(rows, row_block)
    col_tiles = block_count(cols, col_block)
    schedule: list[dict[str, Any]] = []
    for row_tile in range(row_tiles):
        row_start = row_tile * row_block
        row_end = min(rows, row_start + row_block)
        for col_tile in range(col_tiles):
            col_start = col_tile * col_block
            col_end = min(cols, col_start + col_block)
            index = len(schedule)
            schedule.append(
                {
                    "index": index,
                    "key": f"a-r{row_tile:04d}-c{col_tile:04d}",
                    "coords": {
                        "row_block": row_tile,
                        "col_block": col_tile,
                        "row_start": row_start,
                        "row_end": row_end,
                        "col_start": col_start,
                        "col_end": col_end,
                    },
                    "row_range": [row_start, row_end],
                    "col_range": [col_start, col_end],
                    "block_shape": [row_end - row_start, col_end - col_start],
                }
            )
    return schedule


def _erf_approximation(x: np.ndarray) -> np.ndarray:
    """Abramowitz & Stegun 7.1.26 erf approximation, vectorized and overflow-safe."""

    sign = np.sign(x)
    absolute = np.abs(x)
    t = np.divide(1.0, 1.0 + 0.3275911 * absolute)
    poly = 1.061405429
    poly = poly * t - 1.453152027
    poly = poly * t + 1.421413741
    poly = poly * t - 0.284496736
    poly = poly * t + 0.254829592
    poly = poly * t
    erf = 1.0 - np.exp(-np.square(absolute)) * poly
    return sign * erf


def _sigmoid_stable(x: np.ndarray) -> np.ndarray:
    exp_negative_abs = np.exp(-np.abs(x))
    positive = np.divide(1.0, 1.0 + exp_negative_abs)
    negative = np.divide(exp_negative_abs, 1.0 + exp_negative_abs)
    return np.where(x >= 0.0, positive, negative)


def evaluate_activation(name: str, x: np.ndarray) -> np.ndarray:
    if name == "sigmoid":
        return _sigmoid_stable(x)
    if name == "silu":
        return x * _sigmoid_stable(x)
    if name == "gelu":
        cdf = 0.5 * (1.0 + _erf_approximation(np.divide(x, math.sqrt(2.0))))
        return x * cdf
    raise ValueError(f"unknown activation: {name}")


def _matrix_view(flat: np.ndarray, rows: int, cols: int) -> np.ndarray:
    return flat.reshape(rows, cols)


def _nonfinite_diagnostics(matrix: np.ndarray, limit: int = 12) -> dict[str, Any] | None:
    mask = ~np.isfinite(matrix)
    count = int(np.count_nonzero(mask))
    if count == 0:
        return None
    positions = np.argwhere(mask)[:limit]
    values = np.asarray([matrix[tuple(position)] for position in positions], dtype=np.float64)
    return {
        "count": count,
        "positions": [[int(row), int(col)] for row, col in positions],
        "values": json_values(values),
    }


def _zero_row_diagnostics(matrix: np.ndarray, limit: int = 24) -> dict[str, Any] | None:
    rows = np.flatnonzero(np.all(matrix == 0.0, axis=1))
    if rows.size == 0:
        return None
    return {"count": int(rows.size), "rows": [int(row) for row in rows[:limit]]}


def compute_activation_block(
    array: np.ndarray,
    schedule_item: dict[str, Any],
    activation: str,
    *,
    block_context: dict[str, Any],
) -> tuple[np.ndarray, dict[str, Any]]:
    rows, cols = matrix_layout(tuple(array.shape))
    row_start, row_end = schedule_item["row_range"]
    col_start, col_end = schedule_item["col_range"]
    source_matrix = _matrix_view(array, rows, cols)
    block = np.ascontiguousarray(
        source_matrix[row_start:row_end, col_start:col_end], dtype=np.float64
    )

    nonfinite = _nonfinite_diagnostics(block)
    zero_rows = _zero_row_diagnostics(block)
    finite_values = block[np.isfinite(block)]
    if finite_values.size:
        reference = float(finite_values[int(np.argmax(np.abs(finite_values)))])
    else:
        reference = 0.0

    with np.errstate(over="ignore", invalid="ignore", under="ignore", divide="ignore"):
        shifted = block - reference
        restored = shifted + reference
        output = evaluate_activation(activation, restored)
    # Never coerce an anomalous input to zero: preserve its NaN/Inf sentinel in the product.
    output = np.ascontiguousarray(output, dtype=np.float64)
    input_nonfinite = ~np.isfinite(block)
    output[input_nonfinite] = block[input_nonfinite]

    finite_output = output[np.isfinite(output)]
    lower, upper = ACTIVATION_BOUNDS[activation]
    range_violation = None
    if finite_output.size:
        below = finite_output < lower
        above = finite_output > upper
        if np.any(below) or np.any(above):
            bad_values = finite_output[np.logical_or(below, above)]
            range_violation = {
                "count": int(bad_values.size),
                "values": json_values(bad_values),
                "legal_range": [json_float(lower), json_float(upper)],
            }

    diagnostics: dict[str, Any] = {
        "code": E_NUMERIC,
        "stage": "activation",
        **block_context,
        "row_range": [int(row_start), int(row_end)],
        "col_range": [int(col_start), int(col_end)],
        "reference_extreme": reference,
        "finite_input_count": int(finite_values.size),
        "nonfinite": nonfinite,
        "zero_rows": zero_rows,
        "range_violation": range_violation,
    }
    has_alert = any((nonfinite, zero_rows, range_violation))
    metadata = {
        "block_key": schedule_item["key"],
        "block_index": int(schedule_item["index"]),
        "shape": [int(dim) for dim in output.shape],
        "activation": activation,
        "reference_extreme": reference,
        "status": "error" if has_alert else "ok",
        "diagnostics": diagnostics if has_alert else None,
    }
    return output, metadata


def iter_activation_blocks(
    array: np.ndarray,
    config: Any,
) -> Iterator[tuple[dict[str, Any], np.ndarray, dict[str, Any]]]:
    schedule = activation_schedule(tuple(array.shape), config.row_block, config.col_block)
    context = {"shard_id": getattr(config, "_shard_id", "unknown")}
    for item in schedule:
        block, metadata = compute_activation_block(
            array, item, config.activation, block_context=context
        )
        yield item, block, metadata

