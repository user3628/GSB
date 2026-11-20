"""A deterministic local SQL logical-plan engine."""

from .business import EngineError
from .parser import parse_sql
from .planner import build_logical_plan
from .rewriter import normalize_plan
from .service import PlanService

__all__ = [
    "EngineError",
    "PlanService",
    "build_logical_plan",
    "normalize_plan",
    "parse_sql",
]

__version__ = "1.0.0"
