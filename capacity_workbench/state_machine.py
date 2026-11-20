from __future__ import annotations

from enum import StrEnum

from .errors import InvalidTransition


class RoundStatus(StrEnum):
    PENDING = "PENDING"
    SAMPLING = "SAMPLING"
    CONVERGED = "CONVERGED"
    ABORTED = "ABORTED"


TERMINAL_STATUSES = frozenset({RoundStatus.CONVERGED, RoundStatus.ABORTED})

# A conclusion-bearing round is terminal. SAMPLING may only be reached from
# PENDING, and both terminal states are sinks.
ALLOWED_TRANSITIONS: dict[RoundStatus, frozenset[RoundStatus]] = {
    RoundStatus.PENDING: frozenset({RoundStatus.SAMPLING, RoundStatus.ABORTED}),
    RoundStatus.SAMPLING: frozenset({RoundStatus.CONVERGED, RoundStatus.ABORTED}),
    RoundStatus.CONVERGED: frozenset(),
    RoundStatus.ABORTED: frozenset(),
}


def validate_transition(current: str | RoundStatus, target: str | RoundStatus) -> None:
    try:
        current_status = RoundStatus(current)
        target_status = RoundStatus(target)
    except ValueError as exc:
        raise InvalidTransition(str(current), str(target)) from exc
    if target_status not in ALLOWED_TRANSITIONS[current_status]:
        raise InvalidTransition(current_status.value, target_status.value)

