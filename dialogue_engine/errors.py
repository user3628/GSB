"""Stable business errors used by the dialogue engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_INPUT_LENGTH = "E_INPUT_LENGTH"
E_INVALID_TURN = "E_INVALID_TURN"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_NUMERIC = "E_NUMERIC"
E_EMPTY_TURN = "E_EMPTY_TURN"
E_FEATURE_DIM = "E_FEATURE_DIM"

ERROR_CODES = (
    E_INPUT_LENGTH,
    E_INVALID_TURN,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    E_NUMERIC,
    E_EMPTY_TURN,
    E_FEATURE_DIM,
)


@dataclass
class EngineError(RuntimeError):
    """An error with a stable code and enough context for callers to react."""

    code: str
    message: str
    shard_id: str | None = None
    turn_index: int | None = None
    stage: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "shard_id": self.shard_id,
            "turn_index": self.turn_index,
            "stage": self.stage,
            "details": self.details,
        }

    def __str__(self) -> str:
        context = []
        if self.shard_id is not None:
            context.append(f"shard={self.shard_id}")
        if self.turn_index is not None:
            context.append(f"turn={self.turn_index}")
        if self.stage is not None:
            context.append(f"stage={self.stage}")
        suffix = f" ({', '.join(context)})" if context else ""
        return f"{self.code}: {self.message}{suffix}"


def as_error(
    code: str,
    message: str,
    *,
    shard_id: str | None = None,
    turn_index: int | None = None,
    stage: str | None = None,
    **details: Any,
) -> EngineError:
    return EngineError(
        code=code,
        message=message,
        shard_id=shard_id,
        turn_index=turn_index,
        stage=stage,
        details=details,
    )

