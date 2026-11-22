"""Structured pipeline error codes.

Every failure surfaced by the pipeline, the query engine or the HTTP/CLI
front-ends uses one of the codes below so that clients can react
programmatically.  Error payloads always carry the machine readable ``code``
plus a ``details`` object whose fields depend on the failing stage.
"""
from __future__ import annotations

from typing import Any


E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_DIM_INVALID = "E_DIM_INVALID"
E_LATENCY_BUDGET = "E_LATENCY_BUDGET"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_NUMERIC = "E_NUMERIC"
E_EMPTY_FEATURE = "E_EMPTY_FEATURE"
E_NOT_FOUND = "E_NOT_FOUND"
E_STOPPED = "E_STOPPED"


class PipelineError(Exception):
    """Structured error raised by every pipeline component."""

    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": _jsonable(self.details),
            }
        }


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)
