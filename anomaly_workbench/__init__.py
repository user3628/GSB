"""Deterministic local anomaly-detection and incremental diagnostics workbench."""

from .errors import WorkbenchError
from .pipeline import Pipeline

__all__ = ["Pipeline", "WorkbenchError"]
__version__ = "1.0.0"
