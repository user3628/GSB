"""Service state machine.

States: HEALTHY, DEGRADED, ISOLATED, FAILED_OVER, FAILED.
Illegal transitions raise E_INVALID_TRANSITION and never touch storage.
"""
from __future__ import annotations

from .errors import BizError, E_INVALID_TRANSITION

HEALTHY = "HEALTHY"
DEGRADED = "DEGRADED"
ISOLATED = "ISOLATED"
FAILED_OVER = "FAILED_OVER"
FAILED = "FAILED"

STATES = {HEALTHY, DEGRADED, ISOLATED, FAILED_OVER, FAILED}

TRANSITIONS: dict[str, set[str]] = {
    HEALTHY: {DEGRADED, ISOLATED, FAILED_OVER, FAILED},
    DEGRADED: {HEALTHY, ISOLATED, FAILED_OVER, FAILED},
    ISOLATED: {HEALTHY, DEGRADED, FAILED_OVER, FAILED},
    FAILED_OVER: {HEALTHY, DEGRADED, FAILED},
    FAILED: {HEALTHY},          # only an explicit restore can revive
}


def check_transition(current: str, target: str) -> None:
    if current not in STATES or target not in STATES:
        raise BizError(E_INVALID_TRANSITION, "unknown state",
                       {"current": current, "target": target})
    if target == current:
        return  # no-op transitions are always legal
    if target not in TRANSITIONS[current]:
        raise BizError(E_INVALID_TRANSITION,
                       f"illegal transition {current} -> {target}",
                       {"current": current, "target": target,
                        "allowed": sorted(TRANSITIONS[current])})
