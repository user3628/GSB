"""Public library surface for vadkit."""

from .audio import ChunkData, Dataset, load_dataset, pcm_fingerprint, read_wav
from .checkpoint import CheckpointStore, version_fingerprint
from .config import VADConfig, config_from_dict
from .errors import VadError
from .features import FeatureVector, inspect_features, iter_frames, score_frame
from .injection import execute_injection
from .metrics import EndpointMetrics, aggregate_metrics, match_endpoints
from .pipeline import EvaluationOutcome, evaluate_dataset, workspace_status
from .samplegen import generate_dataset
from .stream import (
    DetectionResult,
    Endpoint,
    FrameDecision,
    StreamingVAD,
    detect_samples,
    stream_frames,
)

__all__ = [
    "ChunkData",
    "CheckpointStore",
    "Dataset",
    "DetectionResult",
    "Endpoint",
    "EndpointMetrics",
    "EvaluationOutcome",
    "FeatureVector",
    "FrameDecision",
    "StreamingVAD",
    "VADConfig",
    "VadError",
    "config_from_dict",
    "aggregate_metrics",
    "detect_samples",
    "evaluate_dataset",
    "execute_injection",
    "generate_dataset",
    "inspect_features",
    "iter_frames",
    "load_dataset",
    "match_endpoints",
    "pcm_fingerprint",
    "read_wav",
    "score_frame",
    "stream_frames",
    "version_fingerprint",
    "workspace_status",
]
