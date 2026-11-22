from __future__ import annotations

from typing import Final

from .errors import fail


FRESH: Final = "FRESH"
STALE: Final = "STALE"
REFRESHING: Final = "REFRESHING"
PARTIAL: Final = "PARTIAL"
INVALID: Final = "INVALID"

STATES: Final = frozenset({FRESH, STALE, REFRESHING, PARTIAL, INVALID})

# Self transitions are not state transitions. Invalidation treats an already
# STALE/PARTIAL view as a no-op while still recording a propagation event.
ALLOWED_TRANSITIONS: Final[dict[str, frozenset[str]]] = {
    FRESH: frozenset({STALE, REFRESHING, INVALID}),
    STALE: frozenset({REFRESHING, INVALID}),
    REFRESHING: frozenset({FRESH, PARTIAL, STALE, INVALID}),
    PARTIAL: frozenset({STALE, REFRESHING, INVALID}),
    INVALID: frozenset({STALE, REFRESHING}),
}


def assert_transition(current: str, target: str, view: str) -> None:
    if current == target:
        return
    if current not in STATES or target not in STATES:
        fail(
            "E_INVALID_TRANSITION",
            "Unknown view state.",
            context={"view": view, "from": current, "to": target, "allowed": []},
            status_code=409,
        )
    if target not in ALLOWED_TRANSITIONS[current]:
        fail(
            "E_INVALID_TRANSITION",
            "Illegal materialized-view state transition.",
            context={
                "view": view,
                "from": current,
                "to": target,
                "allowed": sorted(ALLOWED_TRANSITIONS[current]),
            },
            status_code=409,
        )


def healthy(state: str) -> bool:
    return state == FRESH

