"""End-to-end SpMM orchestration for library consumers."""

from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
import os
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from .blocks import BlockPlan, BlockScheme, make_user_scheme, select_block_scheme
from .csr import CSRMatrix, load_csr_npz, load_dense_npy
from .errors import SpMMError, fail
from .kernel import AccumulationOrder, KernelResult, SumStrategy, execute_spmm
from .memory import ExecutionSchedule, plan_execution_schedule
from .numeric import audit_result, load_high_precision_reference
from .util import array_digest, atomic_write_bytes, json_digest, output_digest
from .version_store import CacheLookup, VersionStore


@dataclass
class RunOptions:
    sum_strategy: SumStrategy = "sequential"
    order: AccumulationOrder = "ascending"
    block_scheme: BlockScheme | tuple[int, int] | None = None
    cache_budget_bytes: int = 512 * 1024
    memory_limit_bytes: int = 4 * 1024 * 1024
    duplicate_tolerance: int = 1
    audit: bool = True
    reference_path: str | os.PathLike[str] | None = None
    sampling_seed: int = 20261121
    sampling_count: int = 64
    self_check_blocks: int = 3
    absolute_tolerance: float = 1e-10
    relative_tolerance: float = 1e-10
    version_store: str | os.PathLike[str] | None = None
    use_cache: bool = True
    expected_version: int | None = None
    expected_digest: str | None = None
    lock_timeout_seconds: float = 30.0
    max_nonfinite_sources: int = 10_000
    output_path: str | os.PathLike[str] | None = None

    def __post_init__(self) -> None:
        if self.sum_strategy not in {"sequential", "compensated"}:
            fail("E_USAGE", "unknown sum strategy", details={"sum_strategy": self.sum_strategy})
        if self.order not in {"ascending", "block"}:
            fail("E_USAGE", "unknown accumulation order", details={"order": self.order})
        if self.duplicate_tolerance < 0:
            fail("E_USAGE", "duplicate_tolerance must be nonnegative")
        if self.cache_budget_bytes <= 0 or self.memory_limit_bytes <= 0:
            fail(
                "E_USAGE",
                "cache and memory budgets must be positive",
                details={
                    "cache_budget_bytes": self.cache_budget_bytes,
                    "memory_limit_bytes": self.memory_limit_bytes,
                },
            )


@dataclass
class SpMMResult:
    output: np.ndarray
    scheme: BlockScheme
    block_plan: dict[str, Any]
    schedule: ExecutionSchedule
    audit_report: dict[str, Any] | None
    cache: dict[str, Any]
    version: dict[str, Any] | None
    kernel_diagnostics: dict[str, Any]
    features: dict[str, Any]
    output_digest: str

    def to_summary(self, *, include_plan_candidates: bool = False) -> dict[str, Any]:
        plan = dict(self.block_plan)
        if not include_plan_candidates:
            candidates = plan.pop("candidates", [])
            plan["candidate_count"] = len(candidates)
            plan["top_candidates"] = candidates[:3]
        audit = self.audit_report
        if audit is not None:
            audit_summary = {
                "passed": audit["passed"],
                "block_count": len(audit["blocks"]),
                "bound_model": audit["bound_model"],
                "sampling": {
                    "seed": audit["sampling"]["seed"],
                    "actual_count": audit["sampling"]["actual_count"],
                },
                "self_check": audit["self_check"],
                "nonfinite": audit["nonfinite"],
                "known_paths": True,
            }
        else:
            audit_summary = None
        return {
            "ok": True,
            "scheme": self.scheme.to_dict(),
            "block_plan": plan,
            "execution": self.schedule.to_dict(),
            "cache": self.cache,
            "version": self.version,
            "kernel": self.kernel_diagnostics,
            "input_features": self.features,
            "output_digest": self.output_digest,
            "audit": audit_summary,
        }


def _load_matrix_inputs(
    sparse: CSRMatrix | str | os.PathLike[str],
    dense: np.ndarray | str | os.PathLike[str],
    *,
    shape: tuple[int, int] | None,
) -> tuple[CSRMatrix, np.ndarray, str | None, str | None]:
    sparse_path: str | None = None
    dense_path: str | None = None
    if isinstance(sparse, CSRMatrix):
        matrix = sparse
    else:
        sparse_path = str(sparse)
        matrix = load_csr_npz(sparse, shape=shape)
    if isinstance(dense, np.ndarray):
        dense_array = np.asarray(dense)
    else:
        dense_path = str(dense)
        dense_array = load_dense_npy(dense)
    if dense_array.ndim != 2:
        fail(
            "E_SHAPE_INVALID",
            "dense matrix must be two-dimensional",
            details={"ndim": int(dense_array.ndim)},
            location={"array": "dense", "ndim": int(dense_array.ndim)},
        )
    if tuple(dense_array.shape) != (matrix.cols, int(dense_array.shape[1])):
        fail(
            "E_SHAPE_INVALID",
            "dense matrix leading dimension must match CSR columns",
            details={"csr_shape": list(matrix.shape), "dense_shape": list(dense_array.shape)},
            location={"kind": "dense_leading_dimension", "csr_shape": list(matrix.shape), "dense_shape": list(dense_array.shape)},
        )
    if dense_array.shape[1] <= 0:
        fail(
            "E_SHAPE_INVALID",
            "dense matrix must have positive output width",
            details={"dense_shape": list(dense_array.shape)},
            location={"kind": "dense_output_width", "dense_shape": list(dense_array.shape)},
        )
    if dense_array.dtype != matrix.dtype:
        fail(
            "E_DTYPE_MISMATCH",
            "CSR data and dense matrix must share one floating precision domain",
            details={"csr_dtype": str(matrix.dtype), "dense_dtype": str(dense_array.dtype)},
        )
    return matrix, np.ascontiguousarray(dense_array), sparse_path, dense_path


def _dense_fingerprint(dense: np.ndarray) -> str:
    return array_digest(dense)


def make_cache_key(
    matrix: CSRMatrix,
    dense: np.ndarray,
    scheme: BlockScheme,
    *,
    sum_strategy: str,
    order: str,
    memory_limit_bytes: int | None = None,
    schedule: ExecutionSchedule | None = None,
) -> str:
    return json_digest(
        {
            "cache_format": 1,
            "shape_fingerprint": matrix.shape_fingerprint(),
            "structure_fingerprint": matrix.structure_fingerprint(),
            "sparse_value_fingerprint": matrix.value_fingerprint(),
            "dense_fingerprint": _dense_fingerprint(dense),
            "dense_shape": [int(x) for x in dense.shape],
            "dtype": str(matrix.dtype),
            "block_scheme": {
                "row_block": int(scheme.row_block),
                "col_block": int(scheme.col_block),
                "scheme_id": scheme.scheme_id,
            },
            "sum_strategy": sum_strategy,
            "accumulation_order": order,
            "memory_limit_bytes": None if memory_limit_bytes is None else int(memory_limit_bytes),
            "execution_schedule": None
            if schedule is None
            else {
                "row_block": int(schedule.row_block),
                "col_block": int(schedule.col_block),
                "row_chunks": [[int(a), int(b)] for a, b in schedule.row_chunks],
                "col_chunks": [[int(a), int(b)] for a, b in schedule.col_chunks],
            },
        }
    )


def _replace_output_file(path: str | os.PathLike[str], output: np.ndarray) -> None:
    buffer = BytesIO()
    np.save(buffer, output, allow_pickle=False)
    atomic_write_bytes(path, buffer.getvalue())


def run_spmm(
    sparse: CSRMatrix | str | os.PathLike[str],
    dense: np.ndarray | str | os.PathLike[str],
    *,
    shape: tuple[int, int] | None = None,
    options: RunOptions | None = None,
) -> SpMMResult:
    """Run deterministic CSR x dense SpMM with optional audit, cache, and versions."""

    opts = options or RunOptions()
    matrix, dense_array, sparse_path, dense_path = _load_matrix_inputs(sparse, dense, shape=shape)
    matrix.validate_duplicates(opts.duplicate_tolerance)

    auto_plan: BlockPlan | None = None
    if opts.block_scheme is None:
        auto_plan = select_block_scheme(
            matrix,
            dense_array.shape[1],
            cache_budget_bytes=opts.cache_budget_bytes,
            sum_strategy=opts.sum_strategy,
        )
        scheme = auto_plan.scheme
        block_plan = auto_plan.to_dict()
    else:
        if isinstance(opts.block_scheme, BlockScheme):
            scheme = opts.block_scheme
            if not (1 <= scheme.row_block <= matrix.rows and 1 <= scheme.col_block <= matrix.cols):
                fail(
                    "E_USAGE",
                    "user block scheme lies outside the matrix",
                    details={"scheme": scheme.to_dict(), "csr_shape": list(matrix.shape)},
                )
            scheme = BlockScheme(scheme.row_block, scheme.col_block, source="user")
        else:
            scheme = make_user_scheme(matrix, int(opts.block_scheme[0]), int(opts.block_scheme[1]))
        block_plan = {
            "scheme": scheme.to_dict(),
            "cache_budget_bytes": int(opts.cache_budget_bytes),
            "features": {
                "rows": matrix.rows,
                "cols": matrix.cols,
                "nnz": matrix.nnz,
                "dense_cols": int(dense_array.shape[1]),
                "sum_strategy": opts.sum_strategy,
            },
            "chosen": None,
            "candidates": [],
            "reason": "user-specified block scheme; automatic scoring bypassed",
        }

    schedule = plan_execution_schedule(
        matrix,
        dense_array.shape[1],
        scheme,
        sum_strategy=opts.sum_strategy,
        memory_limit_bytes=opts.memory_limit_bytes,
    )
    cache_key = make_cache_key(
        matrix,
        dense_array,
        scheme,
        sum_strategy=opts.sum_strategy,
        order=opts.order,
        memory_limit_bytes=opts.memory_limit_bytes,
        schedule=schedule,
    )
    features = matrix.features()
    features["dense_shape"] = [int(x) for x in dense_array.shape]
    features["dense_fingerprint"] = _dense_fingerprint(dense_array)
    features["sum_strategy"] = opts.sum_strategy
    features["accumulation_order"] = opts.order

    store: VersionStore | None = None
    version_info: dict[str, Any] | None = None
    scheme_sequence: int | None = None
    scheme_digest: str | None = None
    cache_lookup = CacheLookup(False, None, "cache_disabled")
    if opts.version_store is not None:
        store = VersionStore(opts.version_store, lock_timeout=opts.lock_timeout_seconds)
        scheme_payload = {
            "scheme": scheme.to_dict(),
            "cache_budget_bytes": int(opts.cache_budget_bytes),
            "sum_strategy": opts.sum_strategy,
            "accumulation_order": opts.order,
            "fingerprints": matrix.fingerprints(),
            "dense_fingerprint": _dense_fingerprint(dense_array),
            "dense_shape": [int(x) for x in dense_array.shape],
            "dtype": str(matrix.dtype),
            "selection": block_plan,
        }
        version_info = store.ensure_scheme(
            scheme_payload,
            expected_sequence=opts.expected_version,
            expected_digest=opts.expected_digest,
        )
        scheme_sequence = int(version_info["sequence"])
        scheme_digest = str(version_info["digest"])
        if opts.use_cache:
            cache_lookup = store.lookup_cache(
                cache_key,
                scheme_sequence=scheme_sequence,
                scheme_digest=scheme_digest,
            )

    kernel_diagnostics: dict[str, Any]
    if cache_lookup.hit and cache_lookup.output is not None:
        output = cache_lookup.output
        cached_metadata = cache_lookup.metadata or {}
        cached_nonfinite = cached_metadata.get("nonfinite", {})
        kernel_diagnostics = {
            "cache_hit": True,
            "executed_terms": None,
            "executed_tiles": None,
            "nonfinite_row_count": len(cached_nonfinite.get("nonfinite_rows", [])),
            "nonfinite_rows": cached_nonfinite.get("nonfinite_rows", []),
            "nonfinite_source_count": cached_nonfinite.get("source_count", 0),
            "nonfinite_sources_truncated": cached_nonfinite.get("sources_truncated", False),
            "nonfinite_sources": cached_nonfinite.get("sources", []),
        }
    else:
        kernel_result: KernelResult = execute_spmm(
            matrix,
            dense_array,
            schedule,
            sum_strategy=opts.sum_strategy,
            order=opts.order,
            max_nonfinite_sources=opts.max_nonfinite_sources,
        )
        output = kernel_result.output
        kernel_diagnostics = {"cache_hit": False, **kernel_result.diagnostics()}

    audit_report: dict[str, Any] | None = None
    if opts.audit:
        reference_path = opts.reference_path
        if reference_path is None:
            if sparse_path is None:
                fail(
                    "E_REFERENCE_MISSING",
                    "an explicit reference_path is required when auditing an in-memory CSR",
                    details={"expected_shape": list(output.shape)},
                )
            reference_path = str(Path(sparse_path).with_suffix(".ref.npz"))
        reference = load_high_precision_reference(reference_path, expected_shape=output.shape)
        audit_report = audit_result(
            matrix,
            dense_array,
            output,
            scheme,
            sum_strategy=opts.sum_strategy,
            order=opts.order,
            reference=reference,
            sampling_seed=opts.sampling_seed,
            sampling_count=opts.sampling_count,
            self_check_blocks=opts.self_check_blocks,
            absolute_tolerance=opts.absolute_tolerance,
            relative_tolerance=opts.relative_tolerance,
            nonfinite_sources=kernel_diagnostics.get("nonfinite_sources", []),
            nonfinite_source_count=int(kernel_diagnostics.get("nonfinite_source_count", 0)),
            nonfinite_sources_truncated=bool(kernel_diagnostics.get("nonfinite_sources_truncated", False)),
            execution_schedule=schedule,
        )

    cache_info: dict[str, Any] = {
        "enabled": store is not None and opts.use_cache,
        "hit": bool(cache_lookup.hit),
        "lookup_reason": cache_lookup.reason,
        "cache_key": cache_key,
        "cache_record_sequence": cache_lookup.record_sequence,
        "written": False,
    }
    if store is not None and opts.use_cache and not cache_lookup.hit:
        cache_payload = {
            "scheme_sequence": int(scheme_sequence or 0),
            "scheme_digest": str(scheme_digest or ""),
            "sum_strategy": opts.sum_strategy,
            "accumulation_order": opts.order,
            "nonfinite": {
                "nonfinite_rows": kernel_diagnostics.get("nonfinite_rows", []),
                "source_count": int(kernel_diagnostics.get("nonfinite_source_count", 0)),
                "sources_truncated": bool(kernel_diagnostics.get("nonfinite_sources_truncated", False)),
                "sources": kernel_diagnostics.get("nonfinite_sources", []),
            },
        }
        published = store.publish_cache(cache_key, output, cache_payload)
        cache_info["written"] = True
        cache_info["cache_record_sequence"] = int(published["sequence"])
        cache_info["cache_record_digest"] = published["digest"]

    if opts.output_path is not None:
        _replace_output_file(opts.output_path, output)

    return SpMMResult(
        output=output,
        scheme=scheme,
        block_plan=block_plan,
        schedule=schedule,
        audit_report=audit_report,
        cache=cache_info,
        version=version_info,
        kernel_diagnostics=kernel_diagnostics,
        features=features,
        output_digest=output_digest(output),
    )


def spmm(
    sparse: CSRMatrix | str | os.PathLike[str],
    dense: np.ndarray | str | os.PathLike[str],
    **kwargs: Any,
) -> SpMMResult:
    """Convenience wrapper accepted by training/inference integration code."""

    shape = kwargs.pop("shape", None)
    options = kwargs.pop("options", None)
    if kwargs:
        option_fields = set(RunOptions.__dataclass_fields__)
        unknown = sorted(set(kwargs) - option_fields)
        if unknown:
            fail("E_USAGE", "unknown SpMM option", details={"unknown": unknown, "allowed": sorted(option_fields)})
        options = RunOptions(**kwargs)
    return run_spmm(sparse, dense, shape=shape, options=options)

