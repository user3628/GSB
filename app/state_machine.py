from __future__ import annotations

from enum import Enum

from .errors import invalid_transition


class ServiceState(str, Enum):
    STABLE = "STABLE"
    SCALING_UP = "SCALING_UP"
    SCALING_DOWN = "SCALING_DOWN"
    DEGRADED = "DEGRADED"
    BLOCKED = "BLOCKED"


# Self transitions are deliberately omitted: callers must omit a write when the
# state is unchanged, while operator-requested self transitions are illegal.
VALID_TRANSITIONS: dict[ServiceState, frozenset[ServiceState]] = {
    ServiceState.STABLE: frozenset(
        {
            ServiceState.SCALING_UP,
            ServiceState.SCALING_DOWN,
            ServiceState.DEGRADED,
            ServiceState.BLOCKED,
        }
    ),
    ServiceState.SCALING_UP: frozenset(
        {
            ServiceState.STABLE,
            ServiceState.SCALING_DOWN,
            ServiceState.DEGRADED,
            ServiceState.BLOCKED,
        }
    ),
    ServiceState.SCALING_DOWN: frozenset(
        {
            ServiceState.STABLE,
            ServiceState.SCALING_UP,
            ServiceState.DEGRADED,
            ServiceState.BLOCKED,
        }
    ),
    ServiceState.DEGRADED: frozenset(
        {
            ServiceState.STABLE,
            ServiceState.SCALING_UP,
            ServiceState.SCALING_DOWN,
            ServiceState.BLOCKED,
        }
    ),
    ServiceState.BLOCKED: frozenset(
        {
            ServiceState.STABLE,
            ServiceState.SCALING_UP,
            ServiceState.SCALING_DOWN,
            ServiceState.DEGRADED,
        }
    ),
}


def transition(current: ServiceState, target: ServiceState) -> ServiceState:
    """Validate a strict state transition used by explicit operator actions."""

    if target not in VALID_TRANSITIONS[current]:
        raise invalid_transition(
            f"illegal service state transition {current.value} -> {target.value}",
            current=current.value,
            target=target.value,
            allowed=sorted(state.value for state in VALID_TRANSITIONS[current]),
        )
    return target


def plan_transition(current: ServiceState, target: ServiceState) -> ServiceState:
    """Validate an automatic transition, treating an unchanged state as a no-op."""

    if current == target:
        return current
    return transition(current, target)
