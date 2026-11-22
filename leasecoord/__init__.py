"""Lease election coordinator for deterministic local multi-process tests."""

from .config import CoordinatorConfig
from .coordinator import Coordinator
from .errors import BusinessError

__all__ = ["BusinessError", "Coordinator", "CoordinatorConfig"]
__version__ = "0.1.0"
