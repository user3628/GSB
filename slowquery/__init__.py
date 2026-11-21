"""Local SQLite slow-query diagnosis and rewrite workbench."""

from .api import create_app
from .config import WorkbenchConfig

__all__ = ["WorkbenchConfig", "create_app"]
__version__ = "1.0.0"
