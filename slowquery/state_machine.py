from __future__ import annotations

from enum import StrEnum

from .errors import invalid_transition


class SessionStatus(StrEnum):
    CREATED = "CREATED"
    ANALYZING = "ANALYZING"
    SUGGESTED = "SUGGESTED"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"


LEGAL_TRANSITIONS: dict[SessionStatus, frozenset[SessionStatus]] = {
    SessionStatus.CREATED: frozenset({SessionStatus.ANALYZING}),
    SessionStatus.ANALYZING: frozenset(
        {
            SessionStatus.SUGGESTED,
            SessionStatus.VERIFIED,
            SessionStatus.FAILED,
            # Recovery discards uncommitted analysis and makes the session retryable.
            SessionStatus.CREATED,
        }
    ),
    SessionStatus.SUGGESTED: frozenset({SessionStatus.ANALYZING, SessionStatus.FAILED}),
    SessionStatus.VERIFIED: frozenset(),
    SessionStatus.FAILED: frozenset(),
}


def can_transition(current: str | SessionStatus, target: str | SessionStatus) -> bool:
    try:
        current_status = SessionStatus(current)
        target_status = SessionStatus(target)
    except ValueError:
        return False
    return target_status in LEGAL_TRANSITIONS[current_status]


def require_transition(
    current: str | SessionStatus,
    target: str | SessionStatus,
    *,
    session_id: str | None = None,
) -> None:
    if can_transition(current, target):
        return
    raise invalid_transition(
        f"Illegal diagnosis-session transition: {current} -> {target}.",
        session_id=session_id,
        current_status=str(current),
        target_status=str(target),
    )


def is_terminal(status: str | SessionStatus) -> bool:
    try:
        return not LEGAL_TRANSITIONS[SessionStatus(status)]
    except ValueError:
        return False
