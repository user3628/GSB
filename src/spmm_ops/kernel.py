"""Sparse-only CSR x dense kernel with deterministic accumulation order."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from typing import Any, Literal

import numpy as np

from .csr import CSRMatrix
from .errors import fail
from .memory import ExecutionSchedule


SumStrategy = Literal["sequential", "compensated"]
AccumulationOrder = Literal["ascending", "block"]
VALID_SUM_STRATEGIES = frozenset({"sequential", "compensated"})
VALID_ORDERS = frozenset({"ascending", "block"})


@dataclass
class KernelResult:
    output: np.ndarray
    executed_terms: int
    executed_tiles: int
    nonfinite_sources: list[dict[str, Any]]
    nonfinite_source_count: int
    nonfinite_sources_truncated: bool
    nonfinite_rows: list[int]

    def diagnostics(self) -> dict[str, Any]:
        return {
            "executed_terms": self.executed_terms,
            "executed_tiles": self.executed_tiles,
            "nonfinite_row_count": len(self.nonfinite_rows),
            "nonfinite_rows": self.nonfinite_rows,
            "nonfinite_source_count": self.nonfinite_source_count,
            "nonfinite_sources_truncated": self.nonfinite_sources_truncated,
            "nonfinite_sources": self.nonfinite_sources,
        }


def _ordered_keys_for_row(
    matrix: CSRMatrix,
    row: int,
    order: AccumulationOrder,
    col_chunk_starts: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    start, end = int(matrix.indptr[row]), int(matrix.indptr[row + 1])
    positions = np.arange(start, end, dtype=np.int64)
    if start == end:
        return positions, np.empty(0, dtype=np.int64)
    columns = matrix.indices[start:end]
    if order == "ascending":
        keys = columns.astype(np.int64, copy=False)
    else:
        keys = np.searchsorted(col_chunk_starts, columns, side="right") - 1
    stable_order = np.argsort(keys, kind="stable")
    return positions[stable_order], keys[stable_order]


def execute_spmm(
    matrix: CSRMatrix,
    dense: np.ndarray,
    schedule: ExecutionSchedule,
    *,
    sum_strategy: SumStrategy = "sequential",
    order: AccumulationOrder = "ascending",
    max_nonfinite_sources: int = 10_000,
) -> KernelResult:
    """Execute CSR x dense SpMM.

    No sparse package or matrix-multiplication primitive is used. The kernel
    visits each stored CSR entry (including explicit zeros and duplicates) once
    and multiplies it by one dense row with elementwise NumPy operations.
    """

    if sum_strategy not in VALID_SUM_STRATEGIES:
        fail("E_USAGE", "unknown accumulation strategy", details={"sum_strategy": sum_strategy})
    if order not in VALID_ORDERS:
        fail("E_USAGE", "unknown accumulation order", details={"order": order})
    if dense.ndim != 2 or dense.shape[0] != matrix.cols:
        fail(
            "E_SHAPE_INVALID",
            "dense matrix must have shape (CSR cols, output cols)",
            details={"dense_shape": list(dense.shape), "csr_shape": list(matrix.shape)},
        )
    if dense.dtype != matrix.dtype:
        fail(
            "E_DTYPE_MISMATCH",
            "CSR data and dense matrix must use the same dtype",
            details={"csr_dtype": str(matrix.dtype), "dense_dtype": str(dense.dtype)},
        )

    dense = np.ascontiguousarray(dense)
    output = np.zeros((matrix.rows, dense.shape[1]), dtype=matrix.dtype)
    col_chunk_starts = np.asarray([start for start, _ in schedule.col_chunks], dtype=np.int64)
    executed_terms = 0
    executed_tiles = 0

    for row_start, row_end in schedule.row_chunks:
        row_count = row_end - row_start
        positions: list[np.ndarray] = []
        keys: list[np.ndarray] = []
        for row in range(row_start, row_end):
            row_positions, row_keys = _ordered_keys_for_row(matrix, row, order, col_chunk_starts)
            positions.append(row_positions)
            keys.append(row_keys)

        accumulator = np.zeros((row_count, dense.shape[1]), dtype=matrix.dtype)
        compensation = (
            np.zeros_like(accumulator) if sum_strategy == "compensated" else None
        )

        for col_index, (col_start, col_end) in enumerate(schedule.col_chunks):
            executed_tiles += 1
            dense_slice = np.ascontiguousarray(dense[col_start:col_end, :], dtype=matrix.dtype)
            for local_row, row in enumerate(range(row_start, row_end)):
                row_keys = keys[local_row]
                row_positions = positions[local_row]
                if order == "ascending":
                    lo = int(np.searchsorted(row_keys, col_start, side="left"))
                    hi = int(np.searchsorted(row_keys, col_end, side="left"))
                else:
                    lo = int(np.searchsorted(row_keys, col_index, side="left"))
                    hi = int(np.searchsorted(row_keys, col_index, side="right"))
                if lo == hi:
                    continue
                selected = row_positions[lo:hi]
                for nnz_position in selected:
                    nnz_position_int = int(nnz_position)
                    column_index = int(matrix.indices[nnz_position_int])
                    coefficient = matrix.data[nnz_position_int]
                    product = coefficient * dense_slice[column_index - col_start]
                    executed_terms += 1
                    if sum_strategy == "sequential":
                        accumulator[local_row] += product
                    else:
                        current = accumulator[local_row]
                        with np.errstate(invalid="ignore", over="ignore"):
                            corrected = current + product
                        finite_domain = np.isfinite(current) & np.isfinite(product)
                        nonfinite_domain = ~finite_domain
                        if np.any(nonfinite_domain):
                            accumulator[local_row][nonfinite_domain] = corrected[nonfinite_domain]
                            compensation[local_row][nonfinite_domain] = 0  # type: ignore[index]
                        abs_current = np.abs(current)
                        abs_product = np.abs(product)
                        larger = finite_domain & (abs_current >= abs_product)
                        smaller = finite_domain & ~larger
                        compensation_row = compensation[local_row]  # type: ignore[index]
                        if np.any(larger):
                            compensation_row[larger] += (
                                (current[larger] - corrected[larger]) + product[larger]
                            )
                        if np.any(smaller):
                            compensation_row[smaller] += (
                                (product[smaller] - corrected[smaller]) + current[smaller]
                            )
                        accumulator[local_row][finite_domain] = corrected[finite_domain]

        if compensation is not None:
            accumulator += compensation
        output[row_start:row_end, :] = accumulator

    finite_output = np.isfinite(output)
    nonfinite_rows = [int(row) for row in np.flatnonzero(~np.all(finite_output, axis=1))]
    sources, total_sources, truncated = _collect_nonfinite_sources(
        matrix,
        dense,
        nonfinite_rows,
        max_sources=max_nonfinite_sources,
    )
    return KernelResult(
        output=output,
        executed_terms=executed_terms,
        executed_tiles=executed_tiles,
        nonfinite_sources=sources,
        nonfinite_source_count=total_sources,
        nonfinite_sources_truncated=truncated,
        nonfinite_rows=nonfinite_rows,
    )


def _collect_nonfinite_sources(
    matrix: CSRMatrix,
    dense: np.ndarray,
    nonfinite_rows: list[int],
    *,
    max_sources: int,
) -> tuple[list[dict[str, Any]], int, bool]:
    if max_sources < 0:
        fail("E_USAGE", "max_nonfinite_sources must be nonnegative", details={"value": max_sources})
    rows_set = set(nonfinite_rows)
    if not rows_set:
        return [], 0, False
    records: list[dict[str, Any]] = []
    total = 0

    def append(record: dict[str, Any]) -> None:
        nonlocal total
        total += 1
        if len(records) < max_sources:
            records.append(record)

    for row in range(matrix.rows):
        if row not in rows_set:
            continue
        start, end = int(matrix.indptr[row]), int(matrix.indptr[row + 1])
        if start == end:
            append(
                {
                    "row": row,
                    "nnz_position": None,
                    "column_index": None,
                    "output_column": None,
                    "reasons": ["EMPTY_ROW_CANNOT_BE_NONFINITE"],
                }
            )
            continue
        found = False
        for nnz_position in range(start, end):
            coefficient = matrix.data[nnz_position]
            column_index = int(matrix.indices[nnz_position])
            dense_row = dense[column_index]
            product = coefficient * dense_row
            reasons: list[str] = []
            if not np.isfinite(coefficient):
                reasons.append("A_NONFINITE")
            if not np.all(np.isfinite(dense_row)):
                reasons.append("B_NONFINITE")
            if not np.all(np.isfinite(product)) and not reasons:
                reasons.append("PRODUCT_OVERFLOW")
            if reasons:
                nonfinite_output_columns = np.flatnonzero(~np.isfinite(product))
                output_column = (
                    int(nonfinite_output_columns[0]) if nonfinite_output_columns.size else None
                )
                append(
                    {
                        "row": row,
                        "nnz_position": int(nnz_position),
                        "column_index": column_index,
                        "output_column": output_column,
                        "reasons": reasons,
                    }
                )
                found = True
        if not found:
            append(
                {
                    "row": row,
                    "nnz_position": int(start),
                    "column_index": int(matrix.indices[start]),
                    "output_column": None,
                    "reasons": ["ACCUMULATION_OVERFLOW"],
                }
            )
    return records, total, total > len(records)


def naive_spmm(
    matrix: CSRMatrix,
    dense: np.ndarray,
    *,
    sum_strategy: SumStrategy = "sequential",
    order: AccumulationOrder = "ascending",
    col_block: int | None = None,
    col_chunks: Sequence[tuple[int, int]] | None = None,
) -> np.ndarray:
    """Straightforward non-vectorized reference used by result self-checks."""

    if sum_strategy not in VALID_SUM_STRATEGIES:
        fail("E_USAGE", "unknown accumulation strategy", details={"sum_strategy": sum_strategy})
    if order not in VALID_ORDERS:
        fail("E_USAGE", "unknown accumulation order", details={"order": order})
    if dense.shape != (matrix.cols, dense.shape[1]):
        fail("E_SHAPE_INVALID", "dense shape does not match CSR columns", details={"dense_shape": list(dense.shape)})
    if dense.dtype != matrix.dtype:
        fail("E_DTYPE_MISMATCH", "naive reference requires equal dtypes")

    block = int(col_block or matrix.cols)
    chunk_starts = (
        np.asarray([start for start, _ in col_chunks], dtype=np.int64)
        if col_chunks is not None
        else None
    )
    output = np.zeros((matrix.rows, dense.shape[1]), dtype=matrix.dtype)
    for row in range(matrix.rows):
        start, end = int(matrix.indptr[row]), int(matrix.indptr[row + 1])
        positions = list(range(start, end))
        if order == "ascending":
            positions.sort(key=lambda position: (int(matrix.indices[position]), position))
        else:
            if chunk_starts is not None:
                def chunk_key(position: int) -> int:
                    return int(
                        np.searchsorted(chunk_starts, int(matrix.indices[position]), side="right") - 1
                    )

                positions.sort(key=lambda position: (chunk_key(position), position))
            else:
                positions.sort(key=lambda position: (int(matrix.indices[position]) // block, position))
        accumulator = np.zeros(dense.shape[1], dtype=matrix.dtype)
        compensation = (
            np.zeros(dense.shape[1], dtype=matrix.dtype)
            if sum_strategy == "compensated"
            else None
        )
        for position in positions:
            coefficient = matrix.data[position]
            product = coefficient * dense[int(matrix.indices[position]), :]
            if sum_strategy == "sequential":
                accumulator = accumulator + product
            else:
                current = accumulator
                with np.errstate(invalid="ignore", over="ignore"):
                    corrected = current + product
                finite_domain = np.isfinite(current) & np.isfinite(product)
                nonfinite_domain = ~finite_domain
                if np.any(nonfinite_domain):
                    accumulator[nonfinite_domain] = corrected[nonfinite_domain]
                    compensation[nonfinite_domain] = 0  # type: ignore[index]
                abs_current = np.abs(current)
                abs_product = np.abs(product)
                larger = finite_domain & (abs_current >= abs_product)
                smaller = finite_domain & ~larger
                if np.any(larger):
                    compensation[larger] += (  # type: ignore[index]
                        (current[larger] - corrected[larger]) + product[larger]
                    )
                if np.any(smaller):
                    compensation[smaller] += (  # type: ignore[index]
                        (product[smaller] - corrected[smaller]) + current[smaller]
                    )
                accumulator[finite_domain] = corrected[finite_domain]
        if compensation is not None:
            accumulator = accumulator + compensation
        output[row, :] = accumulator
    return output

