"""Injection plan state machine."""

from __future__ import annotations

from .errors import BizError, E_INVALID_TRANSITION
from .models import (
    STATUS_ABORTED,
    STATUS_COMPLETED,
    STATUS_PAUSED,
    STATUS_PLANNED,
    STATUS_RUNNING,
)

ALLOWED_TRANSITIONS = {
    STATUS_PLANNED: {STATUS_RUNNING, STATUS_ABORTED},
    STATUS_RUNNING: {STATUS_PAUSED, STATUS_COMPLETED, STATUS_ABORTED},
    STATUS_PAUSED: {STATUS_RUNNING, STATUS_ABORTED},
    STATUS_COMPLETED: set(),
    STATUS_ABORTED: set(),
}


def check_transition(current, target):
    if target not in ALLOWED_TRANSITIONS.get(current, set()):
        raise BizError(
            E_INVALID_TRANSITION,
            f"illegal plan state transition {current} -> {target}",
            {"current": current, "target": target,
             "allowed": sorted(ALLOWED_TRANSITIONS.get(current, set()))},
        )
    return target
