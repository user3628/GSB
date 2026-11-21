"""Deterministic local KDE drift and distribution comparison workbench."""

from .config import DEFAULT_CONFIG, normalize_config
from .errors import WorkbenchError

__all__ = ["DEFAULT_CONFIG", "WorkbenchError", "normalize_config"]
__version__ = "1.0.0"
