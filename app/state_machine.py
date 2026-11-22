from __future__ import annotations

from .errors import BusinessError, E_INVALID_TRANSITION

VALID_STATES = {"CREATING", "READY", "BUSY", "DRAINING", "TERMINATED"}

# CREATING may become BUSY directly when the invocation that caused the cold start
# is already holding a slot on the not-yet-ready instance.
TRANSITIONS: dict[str, set[str]] = {
    "CREATING": {"READY", "BUSY", "TERMINATED"},
    "READY": {"BUSY", "DRAINING", "TERMINATED"},
    "BUSY": {"READY", "DRAINING"},
    "DRAINING": {"TERMINATED"},
    "TERMINATED": set(),
}


def assert_transition(current: str, target: str, occupied_slots: int) -> None:
    if current not in VALID_STATES or target not in VALID_STATES:
        raise BusinessError(
            E_INVALID_TRANSITION,
            "unknown instance state in transition",
            detail={"from": current, "to": target},
        )
    if target not in TRANSITIONS[current]:
        raise BusinessError(
            E_INVALID_TRANSITION,
            "illegal instance state transition",
            detail={
                "from": current,
                "to": target,
                "allowed": sorted(TRANSITIONS[current]),
            },
        )
    if target == "READY" and occupied_slots != 0:
        raise BusinessError(
            E_INVALID_TRANSITION,
            "an instance with occupied slots cannot become READY",
            detail={"from": current, "to": target, "occupied_slots": occupied_slots},
        )
    if target == "BUSY" and occupied_slots <= 0:
        raise BusinessError(
            E_INVALID_TRANSITION,
            "an instance with no occupied slots cannot become BUSY",
            detail={"from": current, "to": target, "occupied_slots": occupied_slots},
        )
    if target == "TERMINATED" and occupied_slots != 0:
        raise BusinessError(
            E_INVALID_TRANSITION,
            "an instance cannot terminate while it has in-flight calls",
            detail={"from": current, "to": target, "occupied_slots": occupied_slots},
        )

