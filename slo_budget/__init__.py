"""Deterministic SLI/SLO error-budget toolkit.

The package intentionally has no dependency on a live monitoring system.  All
clock values can be injected so snapshots, reports and tests remain repeatable.
"""

from .errors import SloError
from .models import SnapshotManifest, WindowRecord
from .policy import BoundaryPolicy, CalculationPolicy

__all__ = [
    "BoundaryPolicy",
    "CalculationPolicy",
    "SloError",
    "SnapshotManifest",
    "WindowRecord",
]

__version__ = "1.0.0"
