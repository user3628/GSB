"""Peak-memory planning and deterministic execution splitting."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any

import numpy as np

from .blocks import BlockScheme, iter_intervals
from .csr import CSRMatrix
from .errors import fail


@dataclass(frozen=True)
class ExecutionSchedule:
    row_block: int
    col_block: int
    row_chunks: tuple[tuple[int, int], ...]
    col_chunks: tuple[tuple[int, int], ...]
    memory_limit_bytes: int
    estimated_peak_bytes: int
    output_bytes: int
    dense_slice_bytes: int
    accumulator_bytes: int
    row_workspace_bytes: int
    logical_tile_count: int
    executed_tile_count: int
    split_count: int
    budget_detail: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "execution_row_block": self.row_block,
            "execution_col_block": self.col_block,
            "row_chunk_count": len(self.row_chunks),
            "col_chunk_count": len(self.col_chunks),
            "row_chunks": [[int(a), int(b)] for a, b in self.row_chunks],
            "col_chunks": [[int(a), int(b)] for a, b in self.col_chunks],
            "memory_limit_bytes": self.memory_limit_bytes,
            "estimated_peak_bytes": self.estimated_peak_bytes,
            "output_bytes": self.output_bytes,
            "dense_slice_bytes": self.dense_slice_bytes,
            "accumulator_bytes": self.accumulator_bytes,
            "row_workspace_bytes": self.row_workspace_bytes,
            "logical_tile_count": self.logical_tile_count,
            "executed_tile_count": self.executed_tile_count,
            "split_count": self.split_count,
            "budget_detail": self.budget_detail,
        }


def _split_intervals(
    base_intervals: list[tuple[int, int]], chunk_size: int
) -> tuple[tuple[int, int], ...]:
    chunks: list[tuple[int, int]] = []
    for start, end in base_intervals:
        current = start
        while current < end:
            next_end = min(current + chunk_size, end)
            chunks.append((current, next_end))
            current = next_end
    return tuple(chunks)


def _split_size_candidates(limit: int) -> list[int]:
    values = {1, int(limit)}
    value = int(limit)
    while value > 1:
        value = max(1, value // 2)
        values.add(value)
    for divisor in (1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 24, 32, 48, 64):
        if int(limit) % divisor == 0:
            values.add(int(limit) // divisor)
    return sorted(values)


def _peak_components(
    matrix: CSRMatrix,
    dense_cols: int,
    row_block: int,
    col_block: int,
    sum_strategy: str,
) -> dict[str, int]:
    itemsize = matrix.itemsize
    compensation_copies = 2 if sum_strategy == "compensated" else 1
    output_bytes = matrix.rows * dense_cols * itemsize
    dense_slice_bytes = col_block * dense_cols * itemsize
    accumulator_bytes = min(row_block, matrix.rows) * dense_cols * itemsize * compensation_copies
    counts = matrix.row_counts().astype(np.int64)
    window = min(row_block, matrix.rows)
    if window >= matrix.rows:
        max_block_nnz = int(counts.sum())
    else:
        prefix = np.concatenate(([0], np.cumsum(counts, dtype=np.int64)))
        windows = prefix[window:] - prefix[:-window]
        max_block_nnz = int(windows.max()) if windows.size else 0
    # Stable ordering can transiently hold key, order, and source arrays. The
    # 48-byte multiplier is deliberately conservative; list/array overhead is
    # accounted per row, and scalar/vector temporaries are included separately.
    row_workspace_bytes = (
        max_block_nnz * 48
        + min(row_block, matrix.rows) * 256
        + dense_cols * itemsize * 8
    )
    metadata_bytes = 4096
    peak = output_bytes + dense_slice_bytes + accumulator_bytes + row_workspace_bytes + metadata_bytes
    return {
        "output_bytes": int(output_bytes),
        "dense_slice_bytes": int(dense_slice_bytes),
        "accumulator_bytes": int(accumulator_bytes),
        "row_workspace_bytes": int(row_workspace_bytes),
        "metadata_bytes": int(metadata_bytes),
        "estimated_peak_bytes": int(peak),
    }


def plan_execution_schedule(
    matrix: CSRMatrix,
    dense_cols: int,
    scheme: BlockScheme,
    *,
    sum_strategy: str,
    memory_limit_bytes: int,
) -> ExecutionSchedule:
    """Fit the logical plan into a managed-memory envelope.

    ``memory_limit_bytes`` covers allocations managed by this operation: the
    output array, temporary dense B slices, accumulator/compensation tiles, and
    row-position workspace. The caller-owned input B matrix is resident and is
    reported separately by the integration layer, not double counted here.
    """

    if int(memory_limit_bytes) <= 0:
        fail(
            "E_USAGE",
            "memory_limit_bytes must be positive",
            details={"memory_limit_bytes": int(memory_limit_bytes)},
        )
    if sum_strategy not in {"sequential", "compensated"}:
        fail("E_USAGE", "unknown sum strategy", details={"sum_strategy": sum_strategy})

    limit = int(memory_limit_bytes)
    base_rows = list(iter_intervals(matrix.rows, min(scheme.row_block, matrix.rows)))
    base_cols = list(iter_intervals(matrix.cols, min(scheme.col_block, matrix.cols)))
    logical_tiles = len(base_rows) * len(base_cols)
    base_peak = _peak_components(matrix, dense_cols, scheme.row_block, scheme.col_block, sum_strategy)

    if base_peak["estimated_peak_bytes"] <= limit:
        return ExecutionSchedule(
            row_block=int(scheme.row_block),
            col_block=int(scheme.col_block),
            row_chunks=tuple(base_rows),
            col_chunks=tuple(base_cols),
            memory_limit_bytes=limit,
            estimated_peak_bytes=int(base_peak["estimated_peak_bytes"]),
            output_bytes=int(base_peak["output_bytes"]),
            dense_slice_bytes=int(base_peak["dense_slice_bytes"]),
            accumulator_bytes=int(base_peak["accumulator_bytes"]),
            row_workspace_bytes=int(base_peak["row_workspace_bytes"]),
            logical_tile_count=logical_tiles,
            executed_tile_count=logical_tiles,
            split_count=0,
            budget_detail={"fits_without_split": True, **base_peak},
        )

    candidates: list[dict[str, Any]] = []
    for row_block in _split_size_candidates(min(scheme.row_block, matrix.rows)):
        row_chunks = _split_intervals(base_rows, row_block)
        for col_block in _split_size_candidates(min(scheme.col_block, matrix.cols)):
            peak = _peak_components(matrix, dense_cols, row_block, col_block, sum_strategy)
            if peak["estimated_peak_bytes"] > limit:
                continue
            col_chunks = _split_intervals(base_cols, col_block)
            executed_tiles = len(row_chunks) * len(col_chunks)
            candidates.append(
                {
                    "row_block": row_block,
                    "col_block": col_block,
                    "row_chunks": row_chunks,
                    "col_chunks": col_chunks,
                    "peak": peak,
                    "executed_tiles": executed_tiles,
                }
            )

    if not candidates:
        minimum = _peak_components(matrix, dense_cols, 1, 1, sum_strategy)
        fail(
            "E_MEMORY_BUDGET",
            "managed peak memory cannot be reduced below the output buffer requirement",
            details={
                "memory_limit_bytes": limit,
                "estimated_peak_bytes": int(minimum["estimated_peak_bytes"]),
                "shortfall_bytes": int(minimum["estimated_peak_bytes"] - limit),
                "breakdown": minimum,
                "formula": (
                    "output + B_column_slice + accumulator(+compensation) + "
                    "row_position_workspace + 4096 metadata"
                ),
                "suggested_minimum_limit_bytes": int(minimum["estimated_peak_bytes"]),
            },
            location={"kind": "execution_memory_budget"},
        )

    candidates.sort(
        key=lambda item: (
            item["executed_tiles"],
            -item["row_block"] * item["col_block"],
            item["row_block"],
            item["col_block"],
        )
    )
    selected = candidates[0]
    executed_tiles = int(selected["executed_tiles"])
    peak = selected["peak"]
    return ExecutionSchedule(
        row_block=int(selected["row_block"]),
        col_block=int(selected["col_block"]),
        row_chunks=tuple(selected["row_chunks"]),
        col_chunks=tuple(selected["col_chunks"]),
        memory_limit_bytes=limit,
        estimated_peak_bytes=int(peak["estimated_peak_bytes"]),
        output_bytes=int(peak["output_bytes"]),
        dense_slice_bytes=int(peak["dense_slice_bytes"]),
        accumulator_bytes=int(peak["accumulator_bytes"]),
        row_workspace_bytes=int(peak["row_workspace_bytes"]),
        logical_tile_count=logical_tiles,
        executed_tile_count=executed_tiles,
        split_count=executed_tiles - logical_tiles,
        budget_detail={
            "fits_without_split": False,
            "base_peak_bytes": int(base_peak["estimated_peak_bytes"]),
            "selected_peak_bytes": int(peak["estimated_peak_bytes"]),
            "candidate_count": len(candidates),
            **peak,
        },
    )

