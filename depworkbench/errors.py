"""Business error codes and structured error objects.

All failures inside the workbench are expressed with a stable error code so
that CLI and HTTP surfaces can surface the same, verifiable diagnostics.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_BUDGET_GLOBAL = "E_BUDGET_GLOBAL"
E_BUDGET_WORKER = "E_BUDGET_WORKER"
E_BUDGET_SENTENCE = "E_BUDGET_SENTENCE"
E_TREE_INVALID = "E_TREE_INVALID"
E_NUMERIC = "E_NUMERIC"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_CORRUPT_CHECKPOINT = "E_CORRUPT_CHECKPOINT"
E_NOT_FOUND = "E_NOT_FOUND"
E_SHARD_MISMATCH = "E_SHARD_MISMATCH"
E_PARSE_FAILED = "E_PARSE_FAILED"


class WorkbenchError(Exception):
    """Exception carrying a structured, serialisable error object."""

    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


@dataclass
class StructuredError:
    """Plain data container used inside result envelopes."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_exception(cls, exc: WorkbenchError) -> "StructuredError":
        return cls(code=exc.code, message=exc.message, details=dict(exc.details))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
