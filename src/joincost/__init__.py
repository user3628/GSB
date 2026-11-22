"""Deterministic join-order optimization and cost diagnostics."""

from .errors import BusinessError
from .model import Predicate, PlanOptions, RelationStats, StatsSnapshot
from .optimizer import build_plan

__all__ = [
    "BusinessError",
    "Predicate",
    "PlanOptions",
    "RelationStats",
    "StatsSnapshot",
    "build_plan",
]

__version__ = "1.0.0"

