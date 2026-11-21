"""FlashAttention-style blockwise attention with incremental checkpoints."""

from .checkpoint import CheckpointStore
from .config import AttentionConfig
from .core import CoreAttentionResult, full_attention
from .engine import AttentionEngine, AttentionResult, PrefixUpdateRequest
from .errors import FlashBlockError
from .reference import ReferenceResult, reference_attention
from .samples import generate_samples

__all__ = [
    "AttentionConfig",
    "AttentionEngine",
    "AttentionResult",
    "CheckpointStore",
    "CoreAttentionResult",
    "FlashBlockError",
    "ReferenceResult",
    "PrefixUpdateRequest",
    "full_attention",
    "generate_samples",
    "reference_attention",
]

__version__ = "0.1.0"

