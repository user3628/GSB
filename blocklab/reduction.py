"""Overflow-safe block and final-stage reductions."""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from .errors import E_NUMERIC
from .util import json_values


def stable_aggregate(values: np.ndarray, strategy: str) -> dict[str, Any]:
    flat = np.ascontiguousarray(values, dtype=np.float64).reshape(-1)
    finite_mask = np.isfinite(flat)
    finite = flat[finite_mask]
    nonfinite_count = int(flat.size - finite.size)
    finite_values = [float(item) for item in finite]
    if strategy == "l2":
        norm = 0.0
        for item in finite_values:
            norm = math.hypot(norm, item)
        aggregate = norm
    else:
        total = math.fsum(finite_values)
        aggregate = total if strategy == "sum" else (total / len(finite_values) if finite_values else 0.0)
    return {
        "strategy": strategy,
        "value": float(aggregate),
        "finite_count": int(finite.size),
        "nonfinite_count": nonfinite_count,
    }


def finite_slice_detail(values: np.ndarray, limit: int = 12) -> list[dict[str, Any]]:
    flat = values.reshape(-1)
    bad = np.flatnonzero(~np.isfinite(flat))[:limit]
    return [
        {"flat_index": int(index), "value": json_values([float(flat[index])])[0]}
        for index in bad
    ]


def reduction_diagnostics(
    block: np.ndarray,
    *,
    row_range: list[int],
    col_range: list[int],
    block_context: dict[str, Any],
    tolerance: dict[str, float],
) -> dict[str, Any] | None:
    matrix = np.ascontiguousarray(block, dtype=np.float64).reshape(-1, block.shape[-1])
    nonfinite = ~np.isfinite(matrix)
    nonfinite_count = int(np.count_nonzero(nonfinite))
    zero_rows = np.flatnonzero(np.all(matrix == 0.0, axis=1))
    if nonfinite_count == 0 and zero_rows.size == 0:
        return None
    positions = np.argwhere(nonfinite)[:12]
    return {
        "code": E_NUMERIC,
        "stage": "reduce",
        **block_context,
        "row_range": [int(row_range[0]), int(row_range[1])],
        "col_range": [int(col_range[0]), int(col_range[1])],
        "nonfinite": {
            "count": nonfinite_count,
            "positions": [[int(row), int(col)] for row, col in positions],
            "values": json_values(
                [matrix[int(row), int(col)] for row, col in positions]
            ),
        }
        if nonfinite_count
        else None,
        "zero_rows": {
            "count": int(zero_rows.size),
            "rows": [int(row) for row in zero_rows[:24]],
        }
        if zero_rows.size
        else None,
        "tolerance": tolerance,
    }

