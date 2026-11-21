"""Deterministic factual claim merge and evidence-chain toolkit."""

from .api import (
    demotion_reasons,
    migrate_v1,
    query_claim,
    rebuild_evidence,
    request_stop,
    resume_batch,
    run_batch,
    validate_evidence,
    verify_entity,
    version_status,
)
from .config import load_config, stage_fingerprints
from .errors import FactMergeError
from .pipeline import FactMergeEngine

__all__ = [
    "FactMergeEngine",
    "FactMergeError",
    "demotion_reasons",
    "load_config",
    "migrate_v1",
    "query_claim",
    "rebuild_evidence",
    "request_stop",
    "resume_batch",
    "run_batch",
    "stage_fingerprints",
    "validate_evidence",
    "verify_entity",
    "version_status",
]

__version__ = "1.0.0"
