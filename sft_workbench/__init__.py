"""sft_workbench: local instruction-tuning data construction and format
interoperability workbench (no external services, models or GPUs)."""
from .checkpoint import STAGES, CheckpointRecord, CheckpointStore
from .codec import V1ImportRules, decode_sample, encode_sample, encode_v1, encode_v2
from .dedup import DedupConfig, dedup_samples
from .errors import (
    E_DECODE_SAMPLE,
    E_EMPTY_SAMPLE,
    E_INVALID_INPUT,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    E_UNSUPPORTED_VERSION,
    WorkbenchError,
)
from .generator import generate_shards
from .models import InternalSample, QualityAnnotation
from .pipeline import GracefulStop, Pipeline, PipelineConfig
from .quality import ScoringConfig, score_samples
from .sampling import SamplingConfig, stratified_mixture

__all__ = [
    "STAGES",
    "CheckpointRecord",
    "CheckpointStore",
    "V1ImportRules",
    "decode_sample",
    "encode_sample",
    "encode_v1",
    "encode_v2",
    "DedupConfig",
    "dedup_samples",
    "E_DECODE_SAMPLE",
    "E_EMPTY_SAMPLE",
    "E_INVALID_INPUT",
    "E_INVALID_STAGE",
    "E_STALE_CHECKPOINT",
    "E_UNSUPPORTED_VERSION",
    "WorkbenchError",
    "generate_shards",
    "InternalSample",
    "QualityAnnotation",
    "GracefulStop",
    "Pipeline",
    "PipelineConfig",
    "ScoringConfig",
    "score_samples",
    "SamplingConfig",
    "stratified_mixture",
]
