from __future__ import annotations

from enum import Enum


class RoundState(str, Enum):
    COLLECTING = "COLLECTING"
    READY = "READY"
    AGGREGATED = "AGGREGATED"
    ABORTED = "ABORTED"


class FinalizationPolicy(str, Enum):
    STRICT = "STRICT"
    DEGRADED = "DEGRADED"


LEGAL_TRANSITIONS: dict[RoundState, frozenset[RoundState]] = {
    RoundState.COLLECTING: frozenset({RoundState.READY, RoundState.ABORTED}),
    RoundState.READY: frozenset({RoundState.AGGREGATED, RoundState.ABORTED}),
    RoundState.AGGREGATED: frozenset(),
    RoundState.ABORTED: frozenset(),
}


def assert_legal_transition(current: RoundState, target: RoundState) -> None:
    if target not in LEGAL_TRANSITIONS[current]:
        raise ValueError(f"illegal round transition {current.value} -> {target.value}")

