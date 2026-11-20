"""Deterministic local LLM sampling and decoding engine.

The package intentionally has no model, GPU, network, or service dependency.
NumPy is used for the numerical kernels; the HTTP layer is the Python standard
library only.
"""

from .config import EngineConfig
from .engine import SamplingEngine
from .errors import EngineError
from .manifest import ManifestCodec
from .models import Manifest, SamplingRequest

__all__ = [
    "EngineConfig",
    "EngineError",
    "Manifest",
    "ManifestCodec",
    "SamplingEngine",
    "SamplingRequest",
]

__version__ = "1.0.0"
