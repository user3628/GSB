"""Deterministic local news-summary and rewriting engine."""

from __future__ import annotations

from typing import Any

from .errors import EngineError

__all__ = ["SummaryEngine", "EngineError"]
__version__ = "0.1.0"


def __getattr__(name: str) -> Any:
    if name == "SummaryEngine":
        from .engine import SummaryEngine

        return SummaryEngine
    raise AttributeError(name)
