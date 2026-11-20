"""High-precision reference loading, error bounds, and result self-checks."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import numpy as np

from .blocks import BlockScheme, iter_intervals
from .csr import CSRMatrix
from .errors import SpMMError, fail
from .kernel import AccumulationOrder, SumStrategy, naive_spmm
from .memory import ExecutionSchedule
from .util import array_digest


@dataclass
class ReferenceMatrix:
    values: np.ndarray
    strings: np.ndarray
    digest: str
    precision_digits: int
    path: str

    @property
    def shape(self) -> tuple[int, int]:
        return (int(self.values.shape[0]), int(self.values.shape[1]))


def _parse_reference_token(token: str) -> float:
    normalized = token.strip().upper()
    if normalized in {"NAN", "+NAN", "-NAN"}:
        return float("nan")
    if normalized in {"INF", "+INF", "INFINITY", "+INFINITY"}:
        return float("inf")
    if normalized in {"-INF", "-INFINITY"}:
        return float("-inf")
    try:
        return float(token)
    except ValueError as exc:
        raise SpMMError(
            "E_REFERENCE_INVALID",
            "reference contains a token that is neither a decimal nor an IEEE special value",
            {"token": token},
        ) from exc


def load_high_precision_reference(
    path: str | Path,
    *,
    expected_shape: tuple[int, int],
) -> ReferenceMatrix:
    file_path = Path(path)
    if not file_path.exists():
        fail(
            "E_REFERENCE_MISSING",
            "an explicit local high-precision reference is required for auditing",
            details={"reference_path": str(file_path), "expected_shape": list(expected_shape)},
        )
    try:
        with np.load(file_path, allow_pickle=False) as archive:
            if "values" not in archive.files:
                fail(
                    "E_REFERENCE_INVALID",
                    "reference NPZ must contain a 'values' array",
                    details={"path": str(file_path), "keys": list(archive.files)},
                )
            strings = np.asarray(archive["values"])
            precision = int(np.asarray(archive["precision"]).reshape(-1)[0]) if "precision" in archive.files else 40
    except SpMMError:
        raise
    except (OSError, ValueError) as exc:
        raise SpMMError(
            "E_REFERENCE_INVALID",
            f"cannot load reference NPZ: {file_path}",
            {"path": str(file_path), "cause": str(exc)},
        ) from exc
    if strings.ndim != 2:
        fail(
            "E_REFERENCE_INVALID",
            "reference values must be a two-dimensional string array",
            details={"shape": list(strings.shape)},
        )
    if tuple(strings.shape) != tuple(expected_shape):
        fail(
            "E_REFERENCE_INVALID",
            "reference shape does not match the SpMM output shape",
            details={"reference_shape": list(strings.shape), "output_shape": list(expected_shape)},
        )
    strings = strings.astype(str, copy=True)
    values = np.empty(strings.shape, dtype=np.float64)
    for index, token in np.ndenumerate(strings):
        values[index] = _parse_reference_token(str(token))
    digest = array_digest(strings)
    return ReferenceMatrix(
        values=values,
        strings=strings,
        digest=digest,
        precision_digits=precision,
        path=str(file_path),
    )


def _semantic_blocks(
    actual: np.ndarray, reference: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return comparable finite mask, nonfinite class mask, and finite diff."""

    actual_finite = np.isfinite(actual)
    reference_finite = np.isfinite(reference)
    both_finite = actual_finite & reference_finite
    both_nan = np.isnan(actual) & np.isnan(reference)
    both_positive_inf = np.isposinf(actual) & np.isposinf(reference)
    both_negative_inf = np.isneginf(actual) & np.isneginf(reference)
    nonfinite_match = both_nan | both_positive_inf | both_negative_inf
    nonfinite_mismatch = (~both_finite) & (~nonfinite_match)
    difference = np.zeros_like(actual, dtype=np.float64)
    if np.any(both_finite):
        difference[both_finite] = np.abs(
            actual[both_finite].astype(np.float64) - reference[both_finite]
        )
    return both_finite, nonfinite_mismatch, difference


def _theoretical_row_bounds(
    matrix: CSRMatrix,
    dense: np.ndarray,
    sum_strategy: SumStrategy,
) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """Return per-row bound and per-row sum(abs(products)) envelope.

    Sequential summation uses ``gamma_(n+1) * sum(abs(term))`` to account for
    both product rounding and all additions. Neumaier compensation uses the
    conservative envelope ``(4u + (n+2)u^2 + 16u^3) * sum(abs(term))``.
    ``n`` includes duplicate and explicitly stored zero positions.
    """

    dtype = matrix.dtype
    u = float(np.finfo(dtype).eps) / 2.0
    bounds = np.zeros(matrix.rows, dtype=np.float64)
    sum_abs_rows = np.zeros(matrix.rows, dtype=np.float64)
    capped_rows = 0
    for row in range(matrix.rows):
        start, end = int(matrix.indptr[row]), int(matrix.indptr[row + 1])
        n_terms = end - start
        if n_terms == 0:
            continue
        absolute_sums = np.zeros(dense.shape[1], dtype=np.float64)
        nonfinite = False
        for position in range(start, end):
            coefficient = matrix.data[position]
            dense_row = dense[int(matrix.indices[position])]
            if not np.isfinite(coefficient) or not np.all(np.isfinite(dense_row)):
                nonfinite = True
                break
            absolute_sums += np.abs(np.float64(coefficient)) * np.abs(dense_row.astype(np.float64))
        if nonfinite:
            bounds[row] = np.inf
            sum_abs_rows[row] = np.inf
            capped_rows += 1
            continue
        gamma_n = (n_terms * u) / (1.0 - n_terms * u) if n_terms * u < 1.0 else np.inf
        inflated_sum_abs = float(np.max(absolute_sums)) * (1.0 + gamma_n + 16.0 * u * u)
        if not np.isfinite(inflated_sum_abs):
            bounds[row] = np.inf
            sum_abs_rows[row] = np.inf
            capped_rows += 1
            continue
        if sum_strategy == "sequential":
            product = (n_terms + 1) * u
            factor = product / (1.0 - product) if product < 1.0 else np.inf
        else:
            factor = 4.0 * u + (n_terms + 2) * u * u + 16.0 * u * u * u
        bounds[row] = float(factor * inflated_sum_abs)
        sum_abs_rows[row] = inflated_sum_abs
    model = {
        "unit_roundoff_u": u,
        "sequential_factor": "gamma_(n+1), n = stored CSR entries in row",
        "compensated_factor": "4u + (n+2)u^2 + 16u^3",
        "sum_abs_inflation": "1 + gamma_n + 16u^2",
        "n_semantics": "includes duplicates and explicitly stored zeros",
        "rows_with_nonfinite_bound": int(capped_rows),
    }
    return bounds, sum_abs_rows, model


def _safe_float(value: float | np.floating[Any]) -> float | None:
    result = float(value)
    return result if np.isfinite(result) else None


def _decimal_error_string(output_value: float, reference_token: str) -> str | None:
    try:
        reference = Decimal(reference_token)
    except InvalidOperation:
        return None
    actual = Decimal.from_float(float(output_value))
    return format(abs(actual - reference), ".12E")


def _self_check_blocks(
    matrix: CSRMatrix,
    dense: np.ndarray,
    output: np.ndarray,
    scheme: BlockScheme,
    *,
    sum_strategy: SumStrategy,
    order: AccumulationOrder,
    sample_blocks: int,
    rng: np.random.Generator,
    absolute_tolerance: float,
    relative_tolerance: float,
    execution_schedule: ExecutionSchedule | None = None,
) -> dict[str, Any]:
    row_blocks = list(iter_intervals(matrix.rows, scheme.row_block))
    if sample_blocks < 0:
        fail("E_USAGE", "self_check_blocks must be nonnegative", details={"value": sample_blocks})
    count = min(int(sample_blocks), len(row_blocks))
    if count == 0:
        return {
            "sampled_row_blocks": [],
            "absolute_tolerance": absolute_tolerance,
            "relative_tolerance": relative_tolerance,
            "passed": True,
            "reference_implementation": "naive per-nonzero Python accumulation",
        }
    chosen_indices = sorted(int(x) for x in rng.choice(len(row_blocks), size=count, replace=False))
    naive = naive_spmm(
        matrix,
        dense,
        sum_strategy=sum_strategy,
        order=order,
        col_block=scheme.col_block,
        col_chunks=None if execution_schedule is None else execution_schedule.col_chunks,
    )
    checks: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for block_index in chosen_indices:
        row_start, row_end = row_blocks[block_index]
        actual_block = output[row_start:row_end]
        expected_block = naive[row_start:row_end]
        both_finite, nonfinite_mismatch, difference = _semantic_blocks(actual_block, expected_block)
        tolerance = absolute_tolerance + relative_tolerance * np.abs(expected_block.astype(np.float64))
        finite_failures = both_finite & (difference > tolerance)
        failure_mask = finite_failures | nonfinite_mismatch
        check: dict[str, Any] = {
            "logical_block_index": block_index,
            "row_start": row_start,
            "row_end": row_end,
            "col_start": 0,
            "col_end": dense.shape[1],
            "passed": not bool(np.any(failure_mask)),
            "max_absolute_deviation": _safe_float(np.max(difference)) if difference.size else 0.0,
            "max_relative_deviation": None,
            "nonfinite_mismatch_count": int(np.count_nonzero(nonfinite_mismatch)),
        }
        if both_finite.any():
            denominator = np.maximum(
                np.maximum(np.abs(actual_block.astype(np.float64)), np.abs(expected_block.astype(np.float64))),
                np.finfo(np.float64).tiny,
            )
            relative = np.zeros_like(difference)
            relative[both_finite] = difference[both_finite] / denominator[both_finite]
            check["max_relative_deviation"] = _safe_float(np.max(relative))
        if np.any(failure_mask):
            coordinates = np.argwhere(failure_mask)
            first_row, first_col = (int(coordinates[0][0]), int(coordinates[0][1]))
            check["first_failure"] = {
                "row": row_start + first_row,
                "column": first_col,
                "actual": float(actual_block[first_row, first_col]),
                "naive_reference": float(expected_block[first_row, first_col]),
                "absolute_deviation": _safe_float(difference[first_row, first_col]),
            }
            failures.append(check)
        checks.append(check)
    result = {
        "sampled_row_blocks": checks,
        "absolute_tolerance": float(absolute_tolerance),
        "relative_tolerance": float(relative_tolerance),
        "passed": not failures,
        "reference_implementation": "naive per-nonzero Python accumulation",
    }
    if failures:
        fail(
            "E_NUMERIC_MISMATCH",
            "sampled output row blocks exceed the configured self-check tolerance",
            details={"kind": "self_check_tolerance", "failures": failures, "checks": checks},
            location={
                "row_start": failures[0]["row_start"],
                "row_end": failures[0]["row_end"],
                "col_start": 0,
                "col_end": dense.shape[1],
            },
        )
    return result


def audit_result(
    matrix: CSRMatrix,
    dense: np.ndarray,
    output: np.ndarray,
    scheme: BlockScheme,
    *,
    sum_strategy: SumStrategy,
    order: AccumulationOrder,
    reference: ReferenceMatrix,
    sampling_seed: int = 20261121,
    sampling_count: int = 64,
    self_check_blocks: int = 3,
    absolute_tolerance: float = 1e-10,
    relative_tolerance: float = 1e-10,
    nonfinite_sources: list[dict[str, Any]] | None = None,
    nonfinite_source_count: int = 0,
    nonfinite_sources_truncated: bool = False,
    execution_schedule: ExecutionSchedule | None = None,
) -> dict[str, Any]:
    if reference.shape != output.shape:
        fail(
            "E_REFERENCE_INVALID",
            "reference and output shapes differ",
            details={"reference_shape": list(reference.shape), "output_shape": list(output.shape)},
        )
    if sampling_count < 0:
        fail("E_USAGE", "sampling_count must be nonnegative", details={"value": sampling_count})
    if absolute_tolerance < 0 or relative_tolerance < 0:
        fail(
            "E_USAGE",
            "tolerances must be nonnegative",
            details={"atol": absolute_tolerance, "rtol": relative_tolerance},
        )

    rng = np.random.default_rng(int(sampling_seed))
    self_check = _self_check_blocks(
        matrix,
        dense,
        output,
        scheme,
        sum_strategy=sum_strategy,
        order=order,
        sample_blocks=self_check_blocks,
        rng=rng,
        absolute_tolerance=absolute_tolerance,
        relative_tolerance=relative_tolerance,
        execution_schedule=execution_schedule,
    )

    row_bounds, row_sum_abs, bound_model = _theoretical_row_bounds(matrix, dense, sum_strategy)
    both_finite, nonfinite_mismatch, difference = _semantic_blocks(output, reference.values)
    if np.any(nonfinite_mismatch):
        coordinates = np.argwhere(nonfinite_mismatch)
        row, column = int(coordinates[0][0]), int(coordinates[0][1])
        fail(
            "E_NUMERIC_MISMATCH",
            "IEEE non-finite classes differ from the high-precision reference",
            details={
                "kind": "nonfinite_reference_mismatch",
                "mismatch_count": int(np.count_nonzero(nonfinite_mismatch)),
                "actual": float(output[row, column]),
                "reference": str(reference.strings[row, column]),
            },
            location={"row": row, "column": column},
        )

    ref_roundoff = np.zeros(reference.values.shape, dtype=np.float64)
    finite_ref = np.isfinite(reference.values)
    if finite_ref.any():
        ref_roundoff[finite_ref] = np.maximum(
            np.spacing(np.abs(reference.values[finite_ref])), 1e-35
        )

    blocks: list[dict[str, Any]] = []
    bound_failures: list[dict[str, Any]] = []
    for row_start, row_end in iter_intervals(matrix.rows, scheme.row_block):
        row_block_bounds = row_bounds[row_start:row_end]
        finite_row_bounds = row_block_bounds[np.isfinite(row_block_bounds)]
        nonfinite_bound_rows = [
            row_start + int(index) for index in np.flatnonzero(~np.isfinite(row_block_bounds))
        ]
        max_bound = float(finite_row_bounds.max()) if finite_row_bounds.size else None
        finite_slice = both_finite[row_start:row_end]
        diff_slice = difference[row_start:row_end]
        ref_round_slice = ref_roundoff[row_start:row_end]
        max_abs_error = float(diff_slice[finite_slice].max()) if np.any(finite_slice) else 0.0
        max_abs_position = None
        if finite_slice.any():
            masked = np.where(finite_slice, diff_slice, -1.0)
            flat = int(np.argmax(masked))
            local_row, column = np.unravel_index(flat, masked.shape)
            max_abs_position = [row_start + int(local_row), int(column)]
        max_relative = 0.0
        if finite_slice.any():
            denom = np.maximum(
                np.maximum(
                    np.abs(output[row_start:row_end].astype(np.float64)),
                    np.abs(reference.values[row_start:row_end]),
                ),
                np.finfo(np.float64).tiny,
            )
            rel = np.zeros_like(diff_slice)
            rel[finite_slice] = diff_slice[finite_slice] / denom[finite_slice]
            max_relative = float(rel.max())
        allowable = None
        bound_holds = True
        if max_bound is not None:
            reference_allowance = float(ref_round_slice[finite_slice].max()) if np.any(finite_slice) else 0.0
            allowable = max_bound + reference_allowance
            bound_holds = max_abs_error <= allowable + 8.0 * np.finfo(np.float64).eps * max(1.0, allowable)
        block_report = {
            "row_start": row_start,
            "row_end": row_end,
            "col_start": 0,
            "col_end": dense.shape[1],
            "term_count_max": int(np.max(np.diff(matrix.indptr)[row_start:row_end])),
            "theoretical_abs_error_bound": max_bound,
            "reference_rounding_allowance": _safe_float(
                float(ref_round_slice[finite_slice].max()) if np.any(finite_slice) else 0.0
            ),
            "allowable_error": _safe_float(allowable) if allowable is not None else None,
            "observed_max_abs_error": _safe_float(max_abs_error),
            "observed_max_rel_error": _safe_float(max_relative),
            "max_abs_position": max_abs_position,
            "bound_holds": bool(bound_holds),
            "bound_kind": (
                "mixed_finite_and_nonfinite"
                if max_bound is not None and nonfinite_bound_rows
                else ("finite" if max_bound is not None else "nonfinite_ieee_only")
            ),
            "nonfinite_rows_in_block": nonfinite_bound_rows,
        }
        blocks.append(block_report)
        if not bound_holds:
            bound_failures.append(block_report)

    total_positions = matrix.rows * dense.shape[1]
    if sampling_count == 0 or total_positions == 0:
        sampled_coordinates: list[tuple[int, int]] = []
    else:
        chosen_count = min(int(sampling_count), total_positions)
        flat_indices = rng.choice(total_positions, size=chosen_count, replace=False)
        sampled_coordinates = [
            (int(index // dense.shape[1]), int(index % dense.shape[1])) for index in flat_indices
        ]
    sampled_positions: list[dict[str, Any]] = []
    for row, column in sampled_coordinates:
        token = str(reference.strings[row, column])
        actual = float(output[row, column])
        reference_float = float(reference.values[row, column])
        if np.isfinite(actual) and np.isfinite(reference_float):
            decimal_error = _decimal_error_string(actual, token)
            abs_error = abs(actual - reference_float)
            rel_error = abs_error / max(abs(reference_float), abs(actual), np.finfo(np.float64).tiny)
        else:
            decimal_error = None
            abs_error = 0.0 if (
                (np.isnan(actual) and np.isnan(reference_float))
                or (np.isposinf(actual) and np.isposinf(reference_float))
                or (np.isneginf(actual) and np.isneginf(reference_float))
            ) else None
            rel_error = abs_error
        sampled_positions.append(
            {
                "row": row,
                "column": column,
                "output": actual,
                "high_precision_reference": token,
                "abs_error": _safe_float(abs_error) if abs_error is not None else None,
                "abs_error_from_decimal_reference": decimal_error,
                "relative_error": _safe_float(rel_error) if rel_error is not None else None,
                "theoretical_row_bound": _safe_float(row_bounds[row]),
            }
        )

    report = {
        "passed": not bound_failures,
        "reference": {
            "path": reference.path,
            "format": "npz/values UTF-8 decimal strings",
            "precision_digits": reference.precision_digits,
            "digest": reference.digest,
            "explicit_local_file": True,
        },
        "bound_model": bound_model,
        "blocks": blocks,
        "sampling": {
            "seed": int(sampling_seed),
            "requested_count": int(sampling_count),
            "actual_count": len(sampled_positions),
            "positions": sampled_positions,
        },
        "self_check": self_check,
        "nonfinite": {
            "source_count": int(nonfinite_source_count),
            "sources_truncated": bool(nonfinite_sources_truncated),
            "sources": nonfinite_sources or [],
        },
    }
    if bound_failures:
        fail(
            "E_NUMERIC_MISMATCH",
            "one or more output blocks exceed the verifiable error upper bound",
            details={"kind": "error_bound_exceeded", "failures": bound_failures, "report": report},
            location={
                "row_start": bound_failures[0]["row_start"],
                "row_end": bound_failures[0]["row_end"],
                "col_start": 0,
                "col_end": dense.shape[1],
            },
        )
    return report

