"""Local contract-scan clause-region detection and citation-chain conflict resolver."""
from .checkpoints import CheckpointStore, FileLock
from .codec import canonical_bytes, canonical_text, digest, read_json, write_json
from .config import PipelineConfig
from .detect import run_detection
from .errors import (
    ChainError,
    EvidenceBrokenError,
    IncrementalMismatchError,
    InvalidStageError,
    RefParseError,
    StaleCheckpointError,
    StopRequested,
    UnitUnresolvedError,
)
from .evidence import build_evidence, export_manifest, import_manifest, reconstruct_decisions
from .normalize import run_normalize
from .parse import normalize_text, run_parse
from .pipeline import Pipeline
from .resolve import run_resolve
from .samplegen import build_evidence_template, build_sample, write_workspace_bundle

__all__ = [
    "Pipeline",
    "PipelineConfig",
    "CheckpointStore",
    "FileLock",
    "build_sample",
    "build_evidence_template",
    "write_workspace_bundle",
    "run_detection",
    "run_parse",
    "run_normalize",
    "run_resolve",
    "build_evidence",
    "export_manifest",
    "import_manifest",
    "reconstruct_decisions",
    "normalize_text",
    "canonical_bytes",
    "canonical_text",
    "digest",
    "read_json",
    "write_json",
    "ChainError",
    "RefParseError",
    "UnitUnresolvedError",
    "EvidenceBrokenError",
    "IncrementalMismatchError",
    "StaleCheckpointError",
    "InvalidStageError",
    "StopRequested",
]
