"""Deterministic, auditable sparse matrix multiplication components."""

from .blocks import BlockPlan, BlockScheme, CandidateScore, make_user_scheme, select_block_scheme
from .csr import CSRMatrix, load_csr_npz, load_dense_npy
from .engine import RunOptions, SpMMResult, make_cache_key, run_spmm, spmm
from .errors import BUSINESS_CODES, SpMMError
from .kernel import execute_spmm, naive_spmm
from .memory import ExecutionSchedule, plan_execution_schedule
from .numeric import audit_result, load_high_precision_reference
from .samples import SPECS, generate_samples, load_manifest
from .version_store import VersionStore

__all__ = [
    "BUSINESS_CODES",
    "BlockPlan",
    "BlockScheme",
    "CSRMatrix",
    "CandidateScore",
    "ExecutionSchedule",
    "RunOptions",
    "SPECS",
    "SpMMError",
    "SpMMResult",
    "VersionStore",
    "audit_result",
    "execute_spmm",
    "generate_samples",
    "load_csr_npz",
    "load_dense_npy",
    "load_high_precision_reference",
    "load_manifest",
    "make_cache_key",
    "make_user_scheme",
    "naive_spmm",
    "plan_execution_schedule",
    "run_spmm",
    "select_block_scheme",
    "spmm",
]

