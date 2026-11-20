"""Structured business error codes used across the pipeline.

Every failure that crosses a thread or process boundary is represented by
:class:`PipeError` so callers always receive a machine-readable object instead
# of a free-form traceback string.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any


E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_VERSION_MISMATCH = "E_VERSION_MISMATCH"
E_BUDGET_EXHAUSTED = "E_BUDGET_EXHAUSTED"
E_WORKER_QUOTA = "E_WORKER_QUOTA"
E_WORKER_FAILED = "E_WORKER_FAILED"


@dataclass
class PipeError(Exception):
    """Structured error object carrying a code and contextual fields."""

    code: str
    message: str
    shard_index: int | None = None
    field: str | None = None
    details: dict[str, Any] | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        loc = []
        if self.shard_index is not None:
            loc.append(f"shard={self.shard_index}")
        if self.field is not None:
            loc.append(f"field={self.field}")
        suffix = f" ({', '.join(loc)})" if loc else ""
        return f"[{self.code}] {self.message}{suffix}"

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        if data["details"] is None:
            del data["details"]
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipeError":
        return cls(**data)
