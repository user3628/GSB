"""Pure domain models and state-transition validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .constants import MESSAGE_STATES, VALID_TRANSITIONS
from .errors import E_INVALID_TRANSITION, MWError


@dataclass(frozen=True)
class Message:
    queue_name: str
    message_id: str
    seq: int
    body_digest: str
    body: Any = None
    visibility_timeout_ms: int = 30_000
    max_retries: int = 3
    status: str = "PENDING"
    attempt_count: int = 0
    redelivery_count: int = 0
    available_at_ms: int = 0
    last_error: str | None = None


@dataclass(frozen=True)
class Consumer:
    consumer_id: str
    group_name: str
    endpoint: str
    queues: tuple[str, ...] = field(default_factory=tuple)
    enabled: bool = True


def validate_transition(current: str, target: str) -> None:
    if current not in MESSAGE_STATES:
        raise MWError(
            E_INVALID_TRANSITION,
            f"unknown persisted state {current!r}",
            details={"from": current, "to": target, "valid_targets": []},
        )
    allowed = sorted(VALID_TRANSITIONS[current])
    if target not in allowed:
        raise MWError(
            E_INVALID_TRANSITION,
            f"transition {current} -> {target} is not allowed",
            details={"from": current, "to": target, "valid_targets": allowed},
        )


def can_transition(current: str, target: str) -> bool:
    return target in VALID_TRANSITIONS.get(current, frozenset())
