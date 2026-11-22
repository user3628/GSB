"""Deterministic table feature encoding and selection workbench."""

from .errors import WorkbenchError
from .engine import Workbench

__all__ = ["Workbench", "WorkbenchError"]
__version__ = "0.1.0"

