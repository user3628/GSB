"""Cross-language legal citation conflict-resolution component."""

from .config import config_fingerprint, default_config
from .errors import (ALL_ERROR_CODES, E_CITATION_UNPARSED, E_ELEMENT_INVALID,
                     E_EVIDENCE_BROKEN, E_INCREMENTAL_MISMATCH,
                     E_INVALID_STAGE, E_STALE_CHECKPOINT,
                     E_VERSION_SHAPE_MISMATCH, LegalCiteError)
from .evidence import import_manifest, verify_manifest
from .pipeline import StopController, run_pipeline, run_pipeline_resumable
from .serde import read_jsonl

__all__ = [
    "default_config", "config_fingerprint", "LegalCiteError",
    "ALL_ERROR_CODES", "E_CITATION_UNPARSED", "E_ELEMENT_INVALID",
    "E_EVIDENCE_BROKEN", "E_VERSION_SHAPE_MISMATCH", "E_STALE_CHECKPOINT",
    "E_INVALID_STAGE", "E_INCREMENTAL_MISMATCH", "run_pipeline",
    "run_pipeline_resumable", "StopController", "read_jsonl",
    "verify_manifest", "import_manifest",
]
