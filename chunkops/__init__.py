"""chunkops: deterministic blocked Softmax/LayerNorm operators and pipeline."""

from .errors import (
    ChunkOpsError,
    E_INVALID_AXIS,
    E_INVALID_CONFIG,
    E_INVALID_SHAPE,
    E_INVALID_STATE,
    E_MEMORY_BUDGET,
    E_MISSING_FRAGMENT,
    E_NONFINITE,
    E_NUMERIC_MISMATCH,
    E_STALE_PUBLISH,
    E_STALE_VERSION,
    E_ZERO_ROW,
)
from .ops import (
    ExecutionSchedule,
    OpResult,
    execute_blocked_op,
    layernorm,
    reference_layernorm,
    reference_softmax,
    reduce_sum,
    reject_nonfinite_or_zero_rows,
    scan_nonfinite,
    softmax,
)
from .pipeline import Pipeline, import_fragment, load_output, request_stop
from .strategies import (
    COMPENSATED,
    PAIRWISE,
    SEQ,
    ChunkPlan,
    build_auto_plan,
    build_requested_plan,
    gamma_for_strategy,
)
from .validation import validate_output

__version__ = "1.0.0"

__all__ = [
    "ChunkOpsError",
    "ChunkPlan",
    "COMPENSATED",
    "E_INVALID_AXIS",
    "E_INVALID_CONFIG",
    "E_INVALID_SHAPE",
    "E_INVALID_STATE",
    "E_MEMORY_BUDGET",
    "E_MISSING_FRAGMENT",
    "E_NONFINITE",
    "E_NUMERIC_MISMATCH",
    "E_STALE_PUBLISH",
    "E_STALE_VERSION",
    "E_ZERO_ROW",
    "ExecutionSchedule",
    "OpResult",
    "PAIRWISE",
    "Pipeline",
    "SEQ",
    "build_auto_plan",
    "build_requested_plan",
    "execute_blocked_op",
    "gamma_for_strategy",
    "import_fragment",
    "layernorm",
    "load_output",
    "reduce_sum",
    "reject_nonfinite_or_zero_rows",
    "reference_layernorm",
    "reference_softmax",
    "request_stop",
    "scan_nonfinite",
    "softmax",
    "validate_output",
]
