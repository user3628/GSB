"""Request state machine and batch lifecycle constants."""

from __future__ import annotations

from .errors import E_INVALID_TRANSITION, SchedulerError


PENDING = "PENDING"
ADMITTED = "ADMITTED"
BATCHED = "BATCHED"
RUNNING = "RUNNING"
COMPLETED = "COMPLETED"
REJECTED = "REJECTED"
PREEMPTED = "PREEMPTED"

REQUEST_STATES = (
    PENDING,
    ADMITTED,
    BATCHED,
    RUNNING,
    COMPLETED,
    REJECTED,
    PREEMPTED,
)

ACTIVE_RESERVATION_STATES = frozenset({ADMITTED, BATCHED, RUNNING})
TERMINAL_STATES = frozenset({COMPLETED, REJECTED})

VALID_TRANSITIONS: dict[str, frozenset[str]] = {
    PENDING: frozenset({ADMITTED, REJECTED}),
    ADMITTED: frozenset({BATCHED, REJECTED, PREEMPTED}),
    BATCHED: frozenset({RUNNING, PREEMPTED}),
    RUNNING: frozenset({COMPLETED, PREEMPTED}),
    PREEMPTED: frozenset({PENDING}),
    COMPLETED: frozenset(),
    REJECTED: frozenset(),
}


def ensure_transition(from_state: str, to_state: str) -> None:
    if to_state not in REQUEST_STATES:
        raise SchedulerError(
            E_INVALID_TRANSITION,
            f"unknown target state {to_state!r}",
            details={"from_state": from_state, "to_state": to_state},
        )
    if to_state not in VALID_TRANSITIONS.get(from_state, frozenset()):
        raise SchedulerError(
            E_INVALID_TRANSITION,
            f"illegal request transition {from_state} -> {to_state}",
            details={
                "from_state": from_state,
                "to_state": to_state,
                "allowed": sorted(VALID_TRANSITIONS.get(from_state, frozenset())),
            },
        )


BATCH_PLANNED = "PLANNED"
BATCH_QUEUED = "QUEUED"
BATCH_RUNNING = "RUNNING"
BATCH_COMPLETED = "COMPLETED"
BATCH_PREEMPTED = "PREEMPTED"
BATCH_ABORTED = "ABORTED"

BATCH_STATES = (
    BATCH_PLANNED,
    BATCH_QUEUED,
    BATCH_RUNNING,
    BATCH_COMPLETED,
    BATCH_PREEMPTED,
    BATCH_ABORTED,
)

BATCH_TERMINAL_STATES = frozenset(
    {BATCH_COMPLETED, BATCH_PREEMPTED, BATCH_ABORTED}
)

