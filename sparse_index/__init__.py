"""Deterministic Chinese sparse inverted-index pipeline.

The public API is intentionally small.  Command-line entry points live in the
``scripts`` directory and in :mod:`sparse_index.cli`.
"""

from .errors import PipelineError
from .config import IndexConfig
from .pipeline import Pipeline
from .query import QueryEngine

__all__ = ["IndexConfig", "Pipeline", "PipelineError", "QueryEngine"]
__version__ = "1.0.0"
