"""Three-level blocked GEMM/BMM kernel and deterministic block layout."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np

from .errors import invalid_shape
from .fingerprint import (
    array_payload_digest,
    block_input_digest,
    combine_digests,
    shape_fingerprint,
    sha256_json,
)
from .tiling import PlanSelection, TilePlan


ACCUMULATION_STRATEGY = "f64-k-ascending-mr-nr-cache-v1"
ACCUMULATION_STRATEGY_DESCENDING = "f64-k-descending-cache-blocks-mr-nr-v1"
ACCUMULATION_STRATEGY_ALIASES = {
    "k-ascending": ACCUMULATION_STRATEGY,
    ACCUMULATION_STRATEGY: ACCUMULATION_STRATEGY,
    "k-descending": ACCUMULATION_STRATEGY_DESCENDING,
    ACCUMULATION_STRATEGY_DESCENDING: ACCUMULATION_STRATEGY_DESCENDING,
}
KERNEL_VERSION = "blockgemm-kernel-1.0.0"
SUPPORTED_DTYPES = (np.dtype("float32"), np.dtype("float64"))


@dataclass(frozen=True)
class ShapeExpectation:
    m: int
    n: int
    k: int
    batch: int = 1
    dtype: str | None = None
    kind: str | None = None
    a_transpose: bool | None = None
    b_transpose: bool | None = None
    case_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "M": self.m,
            "N": self.n,
            "K": self.k,
            "batch": self.batch,
            "dtype": self.dtype,
            "kind": self.kind,
            "a_transpose": self.a_transpose,
            "b_transpose": self.b_transpose,
            "case_id": self.case_id,
        }


@dataclass
class Problem:
    kind: str
    a_raw: np.ndarray
    b_raw: np.ndarray
    a: np.ndarray
    b: np.ndarray
    a_transpose: bool
    b_transpose: bool
    m: int
    n: int
    k: int
    batch: int
    dtype: np.dtype
    shape_fingerprint: str

    @property
    def output_shape(self) -> tuple[int, ...]:
        if self.kind == "gemm":
            return (self.m, self.n)
        return (self.batch, self.m, self.n)

    @property
    def batch_count(self) -> int:
        return self.batch if self.kind == "bmm" else 1

    def a_matrix(self, batch_index: int | None) -> np.ndarray:
        if self.kind == "gemm":
            return self.a
        return self.a[int(batch_index)]

    def b_matrix(self, batch_index: int | None) -> np.ndarray:
        if self.kind == "gemm":
            return self.b
        return self.b[int(batch_index)]


@dataclass(frozen=True)
class BlockDescriptor:
    sequence: int
    shard_id: str
    batch_index: int | None
    tile_row: int
    tile_col: int
    row_start: int
    row_end: int
    col_start: int
    col_end: int
    m: int
    n: int
    k: int
    batch: int
    plan: TilePlan
    plan_fingerprint: str
    shape_fingerprint: str
    static_version: str
    version_fingerprint: str
    accumulation_strategy: str
    a_stage_digest: str
    b_stage_digest: str
    block_input_digest: str
    tail_m: bool
    tail_n: bool
    tail_k: bool
    a_nonfinite_count: int
    b_nonfinite_count: int
    nonfinite_affected: bool

    @property
    def block_coord(self) -> list[int]:
        return [self.tile_row, self.tile_col]

    @property
    def row_range(self) -> list[int]:
        return [self.row_start, self.row_end]

    @property
    def col_range(self) -> list[int]:
        return [self.col_start, self.col_end]

    @property
    def tail_flags(self) -> dict[str, bool]:
        return {"m": self.tail_m, "n": self.tail_n, "k": self.tail_k}

    def block_fingerprint(self) -> str:
        return sha256_json(
            {
                "schema": "blockgemm.block.v1",
                "sequence": self.sequence,
                "shard_id": self.shard_id,
                "batch_index": self.batch_index,
                "block_coord": self.block_coord,
                "row_range": self.row_range,
                "col_range": self.col_range,
                "K": self.k,
                "plan_fingerprint": self.plan_fingerprint,
                "shape_fingerprint": self.shape_fingerprint,
                "static_version": self.static_version,
                "block_input_digest": self.block_input_digest,
            }
        )


def _shard_id(kind: str, batch_index: int | None) -> str:
    if kind == "gemm":
        return "gemm"
    assert batch_index is not None
    return f"batch-{batch_index:04d}"


def normalize_problem(
    a: np.ndarray,
    b: np.ndarray,
    *,
    a_transpose: bool = False,
    b_transpose: bool = False,
    expected: ShapeExpectation | None = None,
) -> Problem:
    a = np.asarray(a)
    b = np.asarray(b)
    if a.ndim not in (2, 3) or b.ndim not in (2, 3):
        raise invalid_shape(
            "A and B must be 2-D GEMM arrays or 3-D BMM arrays",
            shard_id="gemm" if min(a.ndim, b.ndim) == 2 else "bmm",
            reason="unsupported_ndim",
            details={"a_ndim": a.ndim, "b_ndim": b.ndim},
        )
    kind = "gemm" if a.ndim == 2 and b.ndim == 2 else "bmm"
    if a.ndim != b.ndim:
        raise invalid_shape(
            "A and B must have the same batch rank",
            shard_id=kind,
            reason="rank_mismatch",
            details={"a_shape": list(a.shape), "b_shape": list(b.shape)},
        )
    if a.dtype not in SUPPORTED_DTYPES or b.dtype not in SUPPORTED_DTYPES:
        raise invalid_shape(
            "only float32 and float64 inputs are supported",
            shard_id=kind,
            reason="unsupported_dtype",
            details={"a_dtype": str(a.dtype), "b_dtype": str(b.dtype)},
        )
    if a.dtype != b.dtype:
        raise invalid_shape(
            "A and B must have the same dtype",
            shard_id=kind,
            reason="dtype_mismatch",
            details={"a_dtype": str(a.dtype), "b_dtype": str(b.dtype)},
        )

    if kind == "gemm":
        batch = 1
        a_eff = a.T if a_transpose else a
        b_eff = b.T if b_transpose else b
        if a_eff.ndim != 2 or b_eff.ndim != 2:
            raise invalid_shape(
                "effective GEMM operands must be 2-D",
                shard_id="gemm",
                reason="invalid_transpose_shape",
                details={"a_shape": list(a.shape), "b_shape": list(b.shape)},
            )
        m, k_a = a_eff.shape
        k_b, n = b_eff.shape
    else:
        if a.shape[0] != b.shape[0]:
            raise invalid_shape(
                "A and B batch dimensions must match",
                shard_id="batch-0000",
                reason="batch_mismatch",
                details={"a_shape": list(a.shape), "b_shape": list(b.shape)},
            )
        batch = int(a.shape[0])
        a_eff = np.swapaxes(a, -1, -2) if a_transpose else a
        b_eff = np.swapaxes(b, -1, -2) if b_transpose else b
        m, k_a = a_eff.shape[-2:]
        k_b, n = b_eff.shape[-2:]

    if k_a != k_b:
        raise invalid_shape(
            "A and B contract dimensions do not match",
            shard_id=kind,
            reason="contract_dimension_mismatch",
            details={"A_shape": list(a_eff.shape), "B_shape": list(b_eff.shape)},
        )
    if min(m, n, k_a, batch) <= 0:
        raise invalid_shape(
            "all effective dimensions must be positive",
            shard_id=kind,
            reason="non_positive_dimension",
            details={"M": m, "N": n, "K": k_a, "batch": batch},
        )

    if expected is not None:
        failures: list[str] = []
        if int(expected.m) != int(m):
            failures.append(f"M expected {expected.m}, got {m}")
        if int(expected.n) != int(n):
            failures.append(f"N expected {expected.n}, got {n}")
        if int(expected.k) != int(k_a):
            failures.append(f"K expected {expected.k}, got {k_a}")
        if int(expected.batch) != int(batch):
            failures.append(f"batch expected {expected.batch}, got {batch}")
        if expected.dtype is not None and np.dtype(expected.dtype) != a.dtype:
            failures.append(f"dtype expected {expected.dtype}, got {a.dtype}")
        if expected.kind is not None and expected.kind != kind:
            failures.append(f"kind expected {expected.kind}, got {kind}")
        if expected.a_transpose is not None and bool(expected.a_transpose) != bool(a_transpose):
            failures.append("A transpose flag differs from shape manifest")
        if expected.b_transpose is not None and bool(expected.b_transpose) != bool(b_transpose):
            failures.append("B transpose flag differs from shape manifest")
        if failures:
            raise invalid_shape(
                "input arrays do not match the shape manifest",
                shard_id=kind,
                reason="shape_manifest_mismatch",
                details={
                    "case_id": expected.case_id,
                    "expected": expected.to_dict(),
                    "actual": {
                        "M": m,
                        "N": n,
                        "K": k_a,
                        "batch": batch,
                        "dtype": str(a.dtype),
                        "kind": kind,
                        "a_transpose": bool(a_transpose),
                        "b_transpose": bool(b_transpose),
                        "a_stored_shape": list(a.shape),
                        "b_stored_shape": list(b.shape),
                    },
                    "failures": failures,
                },
            )

    shape_fp = shape_fingerprint(
        kind=kind,
        m=int(m),
        n=int(n),
        k=int(k_a),
        batch=int(batch),
        dtype=a.dtype.str,
        a_transpose=bool(a_transpose),
        b_transpose=bool(b_transpose),
    )
    return Problem(
        kind=kind,
        a_raw=a,
        b_raw=b,
        a=a_eff,
        b=b_eff,
        a_transpose=bool(a_transpose),
        b_transpose=bool(b_transpose),
        m=int(m),
        n=int(n),
        k=int(k_a),
        batch=int(batch),
        dtype=a.dtype,
        shape_fingerprint=shape_fp,
    )


def content_digests(problem: Problem) -> tuple[str, str]:
    """Digest effective operand values; transpose flags are in static metadata."""

    a_digest = combine_digests(
        "effective-A",
        array_payload_digest(np.ascontiguousarray(problem.a)),
    )
    b_digest = combine_digests(
        "effective-B",
        array_payload_digest(np.ascontiguousarray(problem.b)),
    )
    return a_digest, b_digest


def _stage_digest(array: np.ndarray) -> str:
    return array_payload_digest(np.ascontiguousarray(array))


def _nonfinite_count(array: np.ndarray) -> int:
    return int(np.count_nonzero(~np.isfinite(array)))


def build_block_descriptors(
    problem: Problem,
    plan_selection: PlanSelection,
    *,
    static_version: str,
    version_fp: str,
    accumulation_strategy: str = ACCUMULATION_STRATEGY,
) -> list[BlockDescriptor]:
    plan = plan_selection.plan
    tiles_m = math.ceil(problem.m / plan.mc)
    tiles_n = math.ceil(problem.n / plan.nc)
    plan_fp = plan.fingerprint()
    descriptors: list[BlockDescriptor] = []

    a_digest_cache: dict[tuple[int, int], str] = {}
    b_digest_cache: dict[tuple[int, int], str] = {}
    a_nonfinite_cache: dict[tuple[int, int], int] = {}
    b_nonfinite_cache: dict[tuple[int, int], int] = {}
    sequence = 0

    for batch_index in range(problem.batch_count):
        public_batch_index = None if problem.kind == "gemm" else batch_index
        shard_id = _shard_id(problem.kind, public_batch_index)
        a_matrix = problem.a_matrix(public_batch_index)
        b_matrix = problem.b_matrix(public_batch_index)
        for tile_row in range(tiles_m):
            row_start = tile_row * plan.mc
            row_end = min(problem.m, row_start + plan.mc)
            a_key = (batch_index, tile_row)
            if a_key not in a_digest_cache:
                a_stage = a_matrix[row_start:row_end, :]
                a_digest_cache[a_key] = _stage_digest(a_stage)
                a_nonfinite_cache[a_key] = _nonfinite_count(a_stage)
            for tile_col in range(tiles_n):
                col_start = tile_col * plan.nc
                col_end = min(problem.n, col_start + plan.nc)
                b_key = (batch_index, tile_col)
                if b_key not in b_digest_cache:
                    b_stage = b_matrix[:, col_start:col_end]
                    b_digest_cache[b_key] = _stage_digest(b_stage)
                    b_nonfinite_cache[b_key] = _nonfinite_count(b_stage)

                a_stage_digest = a_digest_cache[a_key]
                b_stage_digest = b_digest_cache[b_key]
                block_digest = block_input_digest(
                    a_stage_digest=a_stage_digest,
                    b_stage_digest=b_stage_digest,
                    m_range=(row_start, row_end),
                    n_range=(col_start, col_end),
                    k=problem.k,
                    batch_index=public_batch_index,
                )
                a_nonfinite = a_nonfinite_cache[a_key]
                b_nonfinite = b_nonfinite_cache[b_key]
                descriptors.append(
                    BlockDescriptor(
                        sequence=sequence,
                        shard_id=shard_id,
                        batch_index=public_batch_index,
                        tile_row=tile_row,
                        tile_col=tile_col,
                        row_start=row_start,
                        row_end=row_end,
                        col_start=col_start,
                        col_end=col_end,
                        m=problem.m,
                        n=problem.n,
                        k=problem.k,
                        batch=problem.batch,
                        plan=plan,
                        plan_fingerprint=plan_fp,
                        shape_fingerprint=problem.shape_fingerprint,
                        static_version=static_version,
                        version_fingerprint=version_fp,
                        accumulation_strategy=accumulation_strategy,
                        a_stage_digest=a_stage_digest,
                        b_stage_digest=b_stage_digest,
                        block_input_digest=block_digest,
                        tail_m=(row_end - row_start) < plan.mc,
                        tail_n=(col_end - col_start) < plan.nc,
                        tail_k=(problem.k % plan.kc != 0),
                        a_nonfinite_count=a_nonfinite,
                        b_nonfinite_count=b_nonfinite,
                        nonfinite_affected=bool(a_nonfinite or b_nonfinite),
                    )
                )
                sequence += 1
    return descriptors


def compute_block(problem: Problem, descriptor: BlockDescriptor) -> np.ndarray:
    """Compute one indivisible MCxNC output block with MRxNR register tiles.

    The outer K loop is the cache-block accumulation stage. Inside each KC
    slice, each MRxNR register tile accumulates its dot products before being
    added to the output cache block. The returned tile is the eventual
    checkpoint payload; no partial accumulator is ever persisted.
    """

    plan = descriptor.plan
    a_matrix = problem.a_matrix(descriptor.batch_index)
    b_matrix = problem.b_matrix(descriptor.batch_index)
    tile_h = descriptor.row_end - descriptor.row_start
    tile_w = descriptor.col_end - descriptor.col_start
    accumulator = np.zeros((tile_h, tile_w), dtype=np.float64)

    k_starts = list(range(0, problem.k, plan.kc))
    if descriptor.accumulation_strategy == ACCUMULATION_STRATEGY_DESCENDING:
        k_starts.reverse()
    for k_start in k_starts:
        k_end = min(problem.k, k_start + plan.kc)
        a_cache = np.asarray(
            a_matrix[descriptor.row_start : descriptor.row_end, k_start:k_end],
            dtype=np.float64,
        )
        b_cache = np.asarray(
            b_matrix[k_start:k_end, descriptor.col_start : descriptor.col_end],
            dtype=np.float64,
        )
        for row in range(0, tile_h, plan.mr):
            register_rows = min(plan.mr, tile_h - row)
            for col in range(0, tile_w, plan.nr):
                register_cols = min(plan.nr, tile_w - col)
                with np.errstate(invalid="ignore", over="ignore"):
                    register = np.dot(
                        a_cache[row : row + register_rows, :],
                        b_cache[:, col : col + register_cols],
                    )
                accumulator[
                    row : row + register_rows, col : col + register_cols
                ] += register

    if problem.dtype == np.dtype("float32"):
        return accumulator.astype(np.float32, copy=False)
    return accumulator


def descriptor_record_fields(descriptor: BlockDescriptor) -> dict[str, Any]:
    return {
        "sequence": descriptor.sequence,
        "shard_id": descriptor.shard_id,
        "batch_index": descriptor.batch_index,
        "block_coord": descriptor.block_coord,
        "row_range": descriptor.row_range,
        "col_range": descriptor.col_range,
        "input_shape": {
            "M": descriptor.m,
            "N": descriptor.n,
            "K": descriptor.k,
            "batch": descriptor.batch,
        },
        "tail_flags": descriptor.tail_flags,
    }
