"""Deterministic, checkpointed GEMM/BMM operators for local CPU execution."""

from __future__ import annotations

import os as _os

# Single-thread BLAS keeps the per-microtile K-order reproducible across runs.
# Respect explicit user settings when present.
for _thread_variable in (
    "OPENBLAS_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    _os.environ.setdefault(_thread_variable, "1")

from .errors import (  # noqa: E402
    BlockGemmError,
    E_INVALID_CONFIG,
    E_INVALID_SHAPE,
    E_NUMERIC_MISMATCH,
    E_STALE_CHECKPOINT,
    E_WORKSPACE,
)
from .engine import (  # noqa: E402
    ExecutionContext,
    ExecutionResult,
    ResumeDecision,
    analyze_resume,
    bmm,
    clear_stop,
    describe_execution,
    gemm,
    query_diagnostics,
    query_invalidation,
    query_plan,
    query_report,
    query_status,
    request_stop,
    run_bmm_job,
    run_gemm_job,
)
from .checkpoint import CheckpointStore, ScanResult, WorkspacePaths  # noqa: E402
from .fingerprint import (  # noqa: E402
    array_payload_digest,
    sha256_json,
    shape_fingerprint,
    version_fingerprint,
)
from .kernel import (  # noqa: E402
    ACCUMULATION_STRATEGY,
    ACCUMULATION_STRATEGY_ALIASES,
    ACCUMULATION_STRATEGY_DESCENDING,
    KERNEL_VERSION,
    ShapeExpectation,
    build_block_descriptors,
    compute_block,
    normalize_problem,
)
from .manifest import ensure_samples, generate_samples, load_case, load_manifest  # noqa: E402
from .reference import compare_tile, power_of_two_sum_tile, sample_sequences  # noqa: E402
from .tiling import TilePlan, select_tiling  # noqa: E402

__version__ = "0.1.0"

__all__ = [
    "ACCUMULATION_STRATEGY",
    "ACCUMULATION_STRATEGY_ALIASES",
    "ACCUMULATION_STRATEGY_DESCENDING",
    "BlockGemmError",
    "CheckpointStore",
    "E_INVALID_CONFIG",
    "E_INVALID_SHAPE",
    "E_NUMERIC_MISMATCH",
    "E_STALE_CHECKPOINT",
    "E_WORKSPACE",
    "ExecutionResult",
    "ExecutionContext",
    "KERNEL_VERSION",
    "ShapeExpectation",
    "ResumeDecision",
    "ScanResult",
    "TilePlan",
    "array_payload_digest",
    "analyze_resume",
    "bmm",
    "build_block_descriptors",
    "clear_stop",
    "compare_tile",
    "compute_block",
    "describe_execution",
    "ensure_samples",
    "gemm",
    "generate_samples",
    "load_case",
    "load_manifest",
    "normalize_problem",
    "power_of_two_sum_tile",
    "query_plan",
    "query_report",
    "query_diagnostics",
    "query_invalidation",
    "query_status",
    "request_stop",
    "run_bmm_job",
    "run_gemm_job",
    "sample_sequences",
    "select_tiling",
    "sha256_json",
    "shape_fingerprint",
    "version_fingerprint",
    "WorkspacePaths",
]
