"""Local, deterministic transcript editing workbench."""

from .config import WorkbenchConfig
from .errors import WorkbenchError
from .models import ChunkInfo, Word
from .workspace import Workspace

__all__ = [
    "ChunkInfo",
    "Word",
    "WorkbenchConfig",
    "WorkbenchError",
    "Workspace",
]

__version__ = "1.0.0"
