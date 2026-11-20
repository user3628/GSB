"""Deterministic row/column block planning for CSR SpMM."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Iterable

import numpy as np

from .csr import CSRMatrix
from .errors import fail
from .util import sha256_hex, canonical_json


@dataclass(frozen=True)
class BlockScheme:
    """A logical row-block and sparse-column-block plan."""

    row_block: int
    col_block: int
    source: str = "auto"

    def __post_init__(self) -> None:
        if int(self.row_block) <= 0 or int(self.col_block) <= 0:
            fail(
                "E_USAGE",
                "block sizes must be positive",
                details={"row_block": self.row_block, "col_block": self.col_block},
            )

    @property
    def scheme_id(self) -> str:
        return sha256_hex(
            canonical_json({"row_block": int(self.row_block), "col_block": int(self.col_block)})
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_block": int(self.row_block),
            "col_block": int(self.col_block),
            "source": self.source,
            "scheme_id": self.scheme_id,
        }


@dataclass(frozen=True)
class CandidateScore:
    row_block: int
    col_block: int
    row_blocks: int
    col_blocks: int
    work_set_bytes: int
    predicted_traffic_bytes: int
    row_load_imbalance: float
    col_load_imbalance: float
    empty_row_block_ratio: float
    spin_ratio: float
    pressure: float
    score: float
    fits_cache_budget: bool
    components: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_block": self.row_block,
            "col_block": self.col_block,
            "row_blocks": self.row_blocks,
            "col_blocks": self.col_blocks,
            "work_set_bytes": self.work_set_bytes,
            "predicted_traffic_bytes": self.predicted_traffic_bytes,
            "row_load_imbalance": round(self.row_load_imbalance, 12),
            "col_load_imbalance": round(self.col_load_imbalance, 12),
            "empty_row_block_ratio": round(self.empty_row_block_ratio, 12),
            "spin_ratio": round(self.spin_ratio, 12),
            "pressure": round(self.pressure, 12),
            "score": round(self.score, 12),
            "fits_cache_budget": self.fits_cache_budget,
            "components": {key: round(value, 12) for key, value in self.components.items()},
        }


@dataclass(frozen=True)
class BlockPlan:
    scheme: BlockScheme
    cache_budget_bytes: int
    features: dict[str, Any]
    chosen: CandidateScore
    candidates: tuple[CandidateScore, ...]
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "scheme": self.scheme.to_dict(),
            "cache_budget_bytes": self.cache_budget_bytes,
            "features": self.features,
            "chosen": self.chosen.to_dict(),
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "reason": self.reason,
        }


def iter_intervals(size: int, block: int) -> Iterable[tuple[int, int]]:
    if size <= 0 or block <= 0:
        fail("E_USAGE", "size and block must be positive", details={"size": size, "block": block})
    start = 0
    while start < size:
        end = min(start + block, size)
        yield start, end
        start = end


def _candidate_sizes(limit: int) -> list[int]:
    values = {1, int(limit)}
    value = 1
    while value < limit:
        value *= 2
        values.add(min(value, limit))
    for divisor in (2, 3, 4, 5, 6, 8):
        if limit % divisor == 0:
            values.add(limit // divisor)
    return sorted(value for value in values if 1 <= value <= limit)


def _load_imbalance(block_sums: np.ndarray) -> float:
    if block_sums.size == 0:
        return 0.0
    mean = float(block_sums.mean())
    if mean <= 0.0:
        return 0.0
    return max(0.0, float(block_sums.max()) / mean - 1.0)


def _row_block_stats(counts: np.ndarray, row_block: int) -> tuple[np.ndarray, float, int, int]:
    sums = []
    empty_blocks = 0
    for start, end in iter_intervals(int(counts.size), row_block):
        block_sum = float(counts[start:end].sum())
        sums.append(block_sum)
        if block_sum == 0.0:
            empty_blocks += 1
    array = np.asarray(sums, dtype=np.float64)
    return array, _load_imbalance(array), empty_blocks, int(array.size)


def _column_block_stats(column_counts: np.ndarray, col_block: int) -> tuple[float, int]:
    sums = []
    for start, end in iter_intervals(int(column_counts.size), col_block):
        sums.append(float(column_counts[start:end].sum()))
    array = np.asarray(sums, dtype=np.float64)
    return _load_imbalance(array), int(array.size)


def select_block_scheme(
    matrix: CSRMatrix,
    dense_cols: int,
    *,
    cache_budget_bytes: int,
    sum_strategy: str = "sequential",
    candidate_rows: Iterable[int] | None = None,
    candidate_cols: Iterable[int] | None = None,
) -> BlockPlan:
    """Choose a plan by predicted traffic and load balance.

    The plan is architecture independent and deterministic. A row block's
    working set mirrors the kernel implementation: dense column-slice copy,
    output accumulator tile, the optional compensation tile, and row-position
    ordering metadata.
    """

    if int(cache_budget_bytes) <= 0:
        fail(
            "E_USAGE",
            "cache_budget_bytes must be positive",
            details={"cache_budget_bytes": int(cache_budget_bytes)},
        )
    if int(dense_cols) <= 0:
        fail(
            "E_SHAPE_INVALID",
            "dense right-hand matrix must have at least one column",
            details={"dense_cols": int(dense_cols)},
        )
    if sum_strategy not in {"sequential", "compensated"}:
        fail("E_USAGE", "unknown sum strategy", details={"sum_strategy": sum_strategy})

    budget = int(cache_budget_bytes)
    p = int(dense_cols)
    counts = matrix.row_counts()
    nnz = matrix.nnz
    itemsize = matrix.itemsize
    avg_nnz = float(nnz / matrix.rows)
    column_counts = np.bincount(matrix.indices, minlength=matrix.cols).astype(np.float64)
    row_sizes = sorted(set(int(x) for x in candidate_rows)) if candidate_rows is not None else _candidate_sizes(matrix.rows)
    col_sizes = sorted(set(int(x) for x in candidate_cols)) if candidate_cols is not None else _candidate_sizes(matrix.cols)
    if not row_sizes or any(x <= 0 or x > matrix.rows for x in row_sizes):
        fail(
            "E_USAGE",
            "candidate row block sizes must be in [1, rows]",
            details={"rows": matrix.rows, "candidate_rows": row_sizes},
        )
    if not col_sizes or any(x <= 0 or x > matrix.cols for x in col_sizes):
        fail(
            "E_USAGE",
            "candidate column block sizes must be in [1, cols]",
            details={"cols": matrix.cols, "candidate_cols": col_sizes},
        )

    raw: list[dict[str, Any]] = []
    for row_block in row_sizes:
        row_sums, row_imbalance, empty_blocks, row_blocks = _row_block_stats(counts, row_block)
        for col_block in col_sizes:
            col_imbalance, col_blocks = _column_block_stats(column_counts, col_block)
            compensation_count = 2 if sum_strategy == "compensated" else 1
            b_slice_bytes = col_block * p * itemsize
            accumulator_bytes = min(row_block, matrix.rows) * p * itemsize * compensation_count
            max_row_block_nnz = float(row_sums.max()) if row_sums.size else 0.0
            row_order_bytes = (
                int(math.ceil(max_row_block_nnz)) * 48
                + min(row_block, matrix.rows) * 256
                + p * itemsize * 8
            )
            metadata_bytes = 4096
            work_set = b_slice_bytes + accumulator_bytes + row_order_bytes + metadata_bytes

            repeated_b_reads = row_blocks * matrix.cols * p * itemsize
            sparse_reads = nnz * (itemsize + 8)
            dense_term_reads = nnz * p * itemsize
            output_writes = matrix.rows * p * itemsize
            tile_spin = row_blocks * col_blocks * (64 + int(math.ceil(avg_nnz)) * 8)
            traffic = repeated_b_reads + sparse_reads + dense_term_reads + output_writes + tile_spin
            empty_ratio = float(empty_blocks / row_blocks)
            spin_ratio = float(row_blocks * col_blocks / max(1.0, float(nnz)))
            row_imbalance_component = max(row_imbalance, col_imbalance)
            raw.append(
                {
                    "row_block": row_block,
                    "col_block": col_block,
                    "row_blocks": row_blocks,
                    "col_blocks": col_blocks,
                    "row_sums": row_sums,
                    "work_set_bytes": int(work_set),
                    "predicted_traffic_bytes": int(traffic),
                    "row_load_imbalance": float(row_imbalance_component),
                    "col_load_imbalance": float(col_imbalance),
                    "empty_row_block_ratio": empty_ratio,
                    "spin_ratio": spin_ratio,
                    "pressure": float(work_set / budget),
                    "fits_cache_budget": bool(work_set <= budget),
                }
            )

    feasible = [candidate for candidate in raw if candidate["fits_cache_budget"]]
    if not feasible:
        minimum = min(raw, key=lambda item: (item["work_set_bytes"], item["row_block"], item["col_block"]))
        fail(
            "E_MEMORY_BUDGET",
            "no automatic block candidate fits the configured cache budget",
            details={
                "cache_budget_bytes": budget,
                "minimum_candidate_work_set_bytes": minimum["work_set_bytes"],
                "minimum_candidate": {
                    "row_block": minimum["row_block"],
                    "col_block": minimum["col_block"],
                },
                "shortfall_bytes": int(minimum["work_set_bytes"] - budget),
                "work_set_formula": "B_slice + accumulator(+compensation) + row_order_metadata + 4096",
            },
            location={"kind": "cache_budget"},
        )

    min_traffic = min(candidate["predicted_traffic_bytes"] for candidate in feasible)
    min_spin = min(candidate["spin_ratio"] for candidate in feasible)
    scored: list[CandidateScore] = []
    for candidate in feasible:
        traffic_norm = candidate["predicted_traffic_bytes"] / max(1.0, min_traffic)
        spin_norm = candidate["spin_ratio"] / max(1e-30, min_spin)
        components = {
            "traffic_excess": max(0.0, traffic_norm - 1.0),
            "load_imbalance": candidate["row_load_imbalance"],
            "empty_block_ratio": candidate["empty_row_block_ratio"],
            "spin_excess": max(0.0, spin_norm - 1.0),
            "cache_pressure": candidate["pressure"],
        }
        score = (
            0.45 * components["traffic_excess"]
            + 0.25 * components["load_imbalance"]
            + 0.20 * components["empty_block_ratio"]
            + 0.08 * components["spin_excess"]
            + 0.02 * components["cache_pressure"]
        )
        scored.append(
            CandidateScore(
                row_block=int(candidate["row_block"]),
                col_block=int(candidate["col_block"]),
                row_blocks=int(candidate["row_blocks"]),
                col_blocks=int(candidate["col_blocks"]),
                work_set_bytes=int(candidate["work_set_bytes"]),
                predicted_traffic_bytes=int(candidate["predicted_traffic_bytes"]),
                row_load_imbalance=float(candidate["row_load_imbalance"]),
                col_load_imbalance=float(candidate["col_load_imbalance"]),
                empty_row_block_ratio=float(candidate["empty_row_block_ratio"]),
                spin_ratio=float(candidate["spin_ratio"]),
                pressure=float(candidate["pressure"]),
                score=float(score),
                fits_cache_budget=True,
                components=components,
            )
        )

    scored.sort(key=lambda item: (item.score, item.row_block, item.col_block))
    chosen = scored[0]
    scheme = BlockScheme(chosen.row_block, chosen.col_block, source="auto")
    reason = (
        "selected deterministic minimum score under the cache budget: "
        f"predicted_traffic={chosen.predicted_traffic_bytes} bytes, "
        f"work_set={chosen.work_set_bytes}/{budget} bytes, "
        f"row_load_imbalance={chosen.row_load_imbalance:.6g}, "
        f"empty_row_block_ratio={chosen.empty_row_block_ratio:.6g}, "
        f"spin_ratio={chosen.spin_ratio:.6g}"
    )
    features = {
        "rows": matrix.rows,
        "cols": matrix.cols,
        "nnz": matrix.nnz,
        "avg_nnz_per_row": avg_nnz,
        "empty_rows": int(np.count_nonzero(counts == 0)),
        "empty_row_ratio": float(np.count_nonzero(counts == 0) / matrix.rows),
        "dense_cols": p,
        "dtype": str(matrix.dtype),
        "sum_strategy": sum_strategy,
    }
    return BlockPlan(
        scheme=scheme,
        cache_budget_bytes=budget,
        features=features,
        chosen=chosen,
        candidates=tuple(scored),
        reason=reason,
    )


def make_user_scheme(matrix: CSRMatrix, row_block: int, col_block: int) -> BlockScheme:
    row_block = int(row_block)
    col_block = int(col_block)
    if not (1 <= row_block <= matrix.rows):
        fail(
            "E_USAGE",
            "user row_block is outside the matrix",
            details={"row_block": row_block, "rows": matrix.rows},
        )
    if not (1 <= col_block <= matrix.cols):
        fail(
            "E_USAGE",
            "user col_block is outside the matrix",
            details={"col_block": col_block, "cols": matrix.cols},
        )
    return BlockScheme(row_block, col_block, source="user")

