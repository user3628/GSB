from __future__ import annotations

from .errors import BusinessError
from .models import AppState


# A target state is legal only when it is explicitly listed here. In particular,
# ABORTED is terminal and a fresh desired version moves CONVERGED -> PENDING.
LEGAL_TRANSITIONS: dict[AppState, frozenset[AppState]] = {
    AppState.PENDING: frozenset(
        {AppState.ROLLING_BACK, AppState.CONVERGED, AppState.FAILED, AppState.ABORTED}
    ),
    AppState.ROLLING_BACK: frozenset(
        {AppState.PENDING, AppState.CONVERGED, AppState.FAILED, AppState.ABORTED}
    ),
    AppState.CONVERGED: frozenset({AppState.PENDING, AppState.ROLLING_BACK}),
    AppState.FAILED: frozenset(
        {AppState.PENDING, AppState.ROLLING_BACK, AppState.CONVERGED, AppState.ABORTED}
    ),
    AppState.ABORTED: frozenset(),
}


def initial_state(desired_version: str, effective_version: str) -> AppState:
    return (
        AppState.CONVERGED
        if desired_version == effective_version
        else AppState.PENDING
    )


def ensure_transition(current: AppState, target: AppState) -> None:
    if target not in LEGAL_TRANSITIONS[current]:
        raise BusinessError(
            "E_INVALID_TRANSITION",
            f"application state cannot transition from {current.value} to {target.value}",
            {
                "from": current.value,
                "to": target.value,
                "allowed": sorted(item.value for item in LEGAL_TRANSITIONS[current]),
            },
        )


def transitions_as_dict() -> dict[str, list[str]]:
    return {
        state.value: sorted(target.value for target in targets)
        for state, targets in LEGAL_TRANSITIONS.items()
    }
