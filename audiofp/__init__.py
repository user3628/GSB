"""Local audio-fingerprint compression and quantization-error audit toolkit."""

from .api import (
    get_report,
    import_dataset,
    load_stage_artifact,
    query_status,
)
from .audit import audit_shard, build_summary
from .checkpoints import STAGES, CheckpointStore
from .config import Manifest, ReferencePair, ShardConfig, load_manifest, load_reference_pairs
from .dsp import Peak, PeakResult, detect_peaks
from .errors import (
    E_BUCKET_COLLISION,
    E_INVALID_CONFIG,
    E_INVALID_STAGE,
    E_INVALID_WAV,
    E_NO_PEAK,
    E_STALE_CHECKPOINT,
    FingerprintError,
)
from .pairing import FingerprintPair, PairResult, generate_fingerprint_pairs
from .pipeline import (
    RunResult,
    clear_stop,
    request_stop,
    resume_pipeline,
    run_pipeline,
    stop_requested,
)
from .quantize import QuantizationResult, QuantizedPair, quantize_pairs
from .samples import generate_samples
from .wavio import WavData, read_wav


__all__ = [
    "CheckpointStore",
    "E_BUCKET_COLLISION",
    "E_INVALID_CONFIG",
    "E_INVALID_STAGE",
    "E_INVALID_WAV",
    "E_NO_PEAK",
    "E_STALE_CHECKPOINT",
    "FingerprintError",
    "FingerprintPair",
    "Manifest",
    "PairResult",
    "Peak",
    "PeakResult",
    "QuantizationResult",
    "QuantizedPair",
    "ReferencePair",
    "RunResult",
    "STAGES",
    "ShardConfig",
    "WavData",
    "audit_shard",
    "build_summary",
    "clear_stop",
    "detect_peaks",
    "generate_fingerprint_pairs",
    "generate_samples",
    "get_report",
    "import_dataset",
    "load_manifest",
    "load_reference_pairs",
    "load_stage_artifact",
    "quantize_pairs",
    "query_status",
    "read_wav",
    "request_stop",
    "resume_pipeline",
    "run_pipeline",
    "stop_requested",
]

