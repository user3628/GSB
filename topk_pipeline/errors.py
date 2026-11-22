"""Structured business errors used by every pipeline stage."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_INVALID_K = "E_INVALID_K"
E_INVALID_TEMP = "E_INVALID_TEMP"
E_NUMERIC = "E_NUMERIC"
E_BUDGET_EXCEEDED = "E_BUDGET_EXCEEDED"
E_MEMORY_BUDGET = "E_MEMORY_BUDGET"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_INVALID_MANIFEST = "E_INVALID_MANIFEST"
E_INVALID_NUCLEUS = "E_INVALID_NUCLEUS"
E_INTERNAL = "E_INTERNAL"


@dataclass
class PipelineError(Exception):
    """A serializable, stage-aware error with a stable business code."""

    code: str
    message: str
    stage: str | None = None
    shard_id: str | None = None
    row: int | None = None
    k: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "stage": self.stage,
            "shard_id": self.shard_id,
        }
        if self.row is not None:
            result["row"] = self.row
        if self.k is not None:
            result["k"] = self.k
        if self.details:
            result["details"] = self.details
        return result


def internal_error(exc: BaseException, *, stage: str, shard_id: str | None = None) -> PipelineError:
    """Convert an unexpected worker exception into a structured error."""

    return PipelineError(
        code=E_INTERNAL,
        message=f"{type(exc).__name__}: {exc}",
        stage=stage,
        shard_id=shard_id,
        details={"exception_type": type(exc).__name__},
    )
