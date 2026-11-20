"""Deterministic local storyboard timeline compositor."""

from .errors import PipelineError
from .pipeline import PipelineRunner

__all__ = ["PipelineError", "PipelineRunner"]
__version__ = "1.0.0"
