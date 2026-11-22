from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from .errors import AppError, ErrorCode


class BatchState(StrEnum):
    INJECTING = "INJECTING"
    RETRYING = "RETRYING"
    DEGRADED = "DEGRADED"
    VERIFIED = "VERIFIED"
    ABORTED = "ABORTED"


LEGAL_TRANSITIONS: dict[BatchState, frozenset[BatchState]] = {
    BatchState.INJECTING: frozenset(
        {BatchState.RETRYING, BatchState.DEGRADED, BatchState.VERIFIED, BatchState.ABORTED}
    ),
    BatchState.RETRYING: frozenset(
        {BatchState.RETRYING, BatchState.DEGRADED, BatchState.VERIFIED, BatchState.ABORTED}
    ),
    BatchState.DEGRADED: frozenset({BatchState.VERIFIED, BatchState.ABORTED}),
    BatchState.VERIFIED: frozenset(),
    BatchState.ABORTED: frozenset(),
}


@dataclass(slots=True)
class StateTransition:
    sequence: int
    from_state: BatchState
    to_state: BatchState
    reason: str
    code: ErrorCode
    at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "from_state": self.from_state.value,
            "to_state": self.to_state.value,
            "reason": self.reason,
            "code": self.code.value,
            "at": self.at,
        }


@dataclass(slots=True)
class StateTracker:
    current: BatchState = BatchState.INJECTING
    history: list[StateTransition] = field(default_factory=list)

    def transition(
        self,
        to_state: BatchState | str,
        *,
        reason: str,
        code: ErrorCode = ErrorCode.OK,
    ) -> StateTransition:
        target = BatchState(to_state)
        if target not in LEGAL_TRANSITIONS[self.current]:
            raise AppError(
                ErrorCode.INVALID_TRANSITION,
                f"Illegal state transition {self.current.value} -> {target.value}",
                {
                    "from_state": self.current.value,
                    "to_state": target.value,
                    "allowed": sorted(state.value for state in LEGAL_TRANSITIONS[self.current]),
                },
            )
        transition = StateTransition(
            sequence=len(self.history) + 1,
            from_state=self.current,
            to_state=target,
            reason=reason,
            code=code,
        )
        self.history.append(transition)
        self.current = target
        return transition

    def snapshot(self) -> dict[str, Any]:
        return {
            "current": self.current.value,
            "history": [transition.to_dict() for transition in self.history],
        }

