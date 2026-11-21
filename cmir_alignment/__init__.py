"""Deterministic cross-modal alignment and retrieval reference component."""

from .errors import CMIRError
from .engine import RetrievalEngine

__all__ = ["CMIRError", "RetrievalEngine"]
__version__ = "0.1.0"
