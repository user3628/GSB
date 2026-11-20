"""Local deterministic convolution and interpolation workbench."""

from .engine import Workbench
from .errors import WorkbenchError

__all__ = ["Workbench", "WorkbenchError"]
__version__ = "0.1.0"

