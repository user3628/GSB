"""Local SLI/SLO error-budget calculator."""

from .config import Settings
from .engine import SLIEngine

__all__ = ["Settings", "SLIEngine"]
__version__ = "1.0.0"
