"""Explicit component state machine."""

from __future__ import annotations

from .errors import DomainError, E_INVALID_TRANSITION

PENDING = "PENDING"
DISPATCHED = "DISPATCHED"
APPLIED = "APPLIED"
FAILED = "FAILED"
ROLLED_BACK = "ROLLED_BACK"

ALL_STATUSES = (PENDING, DISPATCHED, APPLIED, FAILED, ROLLED_BACK)

LEGAL_TRANSITIONS: dict[str, frozenset[str]] = {
    PENDING: frozenset({DISPATCHED, FAILED}),
    DISPATCHED: frozenset({APPLIED, FAILED, ROLLED_BACK}),
    APPLIED: frozenset({ROLLED_BACK, PENDING}),
    FAILED: frozenset({ROLLED_BACK, PENDING}),
    ROLLED_BACK: frozenset({PENDING}),
}


def require_transition(from_status: str, to_status: str, *, reason: str = "") -> None:
    if to_status not in ALL_STATUSES:
        raise DomainError(
            E_INVALID_TRANSITION,
            "unknown target component status",
            details={"from_status": from_status, "to_status": to_status, "reason": reason},
        )
    if to_status not in LEGAL_TRANSITIONS.get(from_status, frozenset()):
        raise DomainError(
            E_INVALID_TRANSITION,
            "illegal component state transition",
            details={
                "from_status": from_status,
                "to_status": to_status,
                "reason": reason,
                "allowed": sorted(LEGAL_TRANSITIONS.get(from_status, frozenset())),
            },
        )

