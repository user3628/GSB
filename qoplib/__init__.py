"""qoplib: deterministic low-precision quantization operator library."""

from __future__ import annotations

from .errors import (
    ALL_CODES,
    E_COMPUTE_BUDGET,
    E_INVALID_BITWIDTH,
    E_INVALID_STAGE,
    E_MEMORY_BUDGET,
    E_NUMERIC_INPUT,
    E_NUMERIC_MISMATCH,
    E_STALE_CHECKPOINT,
    E_WORKER_FAILED,
    E_WORKER_QUOTA,
    NumericInputError,
    NumericMismatchError,
    QopError,
    StructuredError,
)
from .manifest import Budget, Manifest, ShardSpec, load_manifest
from .ops import LayerSpec, run_linear_chain
from .pipeline import ShardRunResult, run_shard_pipeline
from .quantization import (
    QuantConfig,
    QuantResult,
    dequantize,
    quantize,
    quantized_linear,
    reference_linear,
)
from .runner import build_summary, request_stop, run
from .scheduler import ScheduleDecision, plan_schedule

__version__ = "1.0.0"

__all__ = [
    "ALL_CODES",
    "Budget",
    "LayerSpec",
    "Manifest",
    "NumericInputError",
    "NumericMismatchError",
    "QopError",
    "QuantConfig",
    "QuantResult",
    "ScheduleDecision",
    "ShardRunResult",
    "ShardSpec",
    "StructuredError",
    "E_COMPUTE_BUDGET",
    "E_INVALID_BITWIDTH",
    "E_INVALID_STAGE",
    "E_MEMORY_BUDGET",
    "E_NUMERIC_INPUT",
    "E_NUMERIC_MISMATCH",
    "E_STALE_CHECKPOINT",
    "E_WORKER_FAILED",
    "E_WORKER_QUOTA",
    "build_summary",
    "dequantize",
    "load_manifest",
    "plan_schedule",
    "quantize",
    "quantized_linear",
    "reference_linear",
    "request_stop",
    "run",
    "run_linear_chain",
    "run_shard_pipeline",
]
