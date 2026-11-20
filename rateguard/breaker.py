"""Circuit-breaker state machine.

Legal transition graph:
    CLOSED    -> OPEN
    OPEN      -> HALF_OPEN
    HALF_OPEN -> CLOSED
    HALF_OPEN -> OPEN

Same-state observations are recorded as new breaker versions but are not state
transitions. Any other state pair is rejected without modifying persisted state.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from .config import GuardConfig
from .errors import BusinessError


STATES = ("CLOSED", "OPEN", "HALF_OPEN")
LEGAL_TRANSITIONS = {
    ("CLOSED", "OPEN"),
    ("OPEN", "HALF_OPEN"),
    ("HALF_OPEN", "CLOSED"),
    ("HALF_OPEN", "OPEN"),
}


@dataclass(slots=True)
class BreakerDecision:
    state: str
    changed: bool
    reason: str
    opened_at_ms: int | None
    next_probe_at_ms: int | None
    half_open_since_ms: int | None
    details: dict[str, Any]


@dataclass(slots=True)
class BreakerState:
    state: str = "CLOSED"
    version: int = 0
    opened_at_ms: int | None = None
    next_probe_at_ms: int | None = None
    half_open_since_ms: int | None = None
    last_reason: str = "INITIALIZED"


def _failure_rate(stats: Mapping[str, Any]) -> Decimal:
    return Decimal(stats["success_rate"]["value"])


def evaluate_automatic(
    current: BreakerState,
    stats: Mapping[str, Any],
    evaluated_at_ms: int,
    config: GuardConfig,
) -> BreakerDecision:
    """Evaluate a closed window at its end boundary."""

    samples = stats["samples"]
    sample_count = int(samples["sample_count"])
    consecutive_failures = int(samples["consecutive_failures"])
    consecutive_successes = int(samples["consecutive_successes"])
    success_rate = Decimal(int(samples["success_count"])) / Decimal(sample_count)
    failure_rate = Decimal(int(samples["failure_count"])) / Decimal(sample_count)
    last_event_ts = int(samples["last_event_ts_ms"])
    last_failure_ts = samples.get("last_failure_ts_ms")

    state = current.state
    opened_at = current.opened_at_ms
    next_probe = current.next_probe_at_ms
    half_open_since = current.half_open_since_ms
    reason = "EVALUATED"
    changed = False

    if state == "CLOSED":
        by_consecutive = consecutive_failures >= config.consecutive_failure_threshold
        by_rate = (
            sample_count >= config.min_breaker_samples
            and failure_rate >= config.failure_rate_threshold
        )
        if by_consecutive or by_rate:
            state = "OPEN"
            changed = True
            reason = "CONSECUTIVE_FAILURES" if by_consecutive else "FAILURE_RATE"
            opened_at = int(last_failure_ts) if last_failure_ts is not None else evaluated_at_ms
            next_probe = opened_at + config.open_duration_ms
            half_open_since = None
        else:
            reason = "EVALUATED_CLOSED"

    elif state == "OPEN":
        if next_probe is not None and evaluated_at_ms >= next_probe:
            state = "HALF_OPEN"
            changed = True
            reason = "COOLDOWN_ELAPSED"
            half_open_since = evaluated_at_ms
        else:
            reason = "COOLDOWN_PENDING"

    elif state == "HALF_OPEN":
        probe_floor = half_open_since if half_open_since is not None else opened_at
        saw_probe_event = probe_floor is None or last_event_ts >= probe_floor
        if not saw_probe_event:
            reason = "WAITING_FOR_PROBE"
        elif consecutive_failures > 0:
            state = "OPEN"
            changed = True
            reason = "FAILURE_WHILE_HALF_OPEN"
            opened_at = int(last_failure_ts) if last_failure_ts is not None else evaluated_at_ms
            next_probe = opened_at + config.open_duration_ms
            half_open_since = None
        elif consecutive_successes >= config.half_open_success_threshold:
            state = "CLOSED"
            changed = True
            reason = "HALF_OPEN_PROBE_SUCCEEDED"
            opened_at = None
            next_probe = None
            half_open_since = None
        else:
            reason = "PROBE_INCONCLUSIVE"
    else:
        raise ValueError(f"unknown breaker state: {state}")

    details = {
        "from_state": current.state,
        "to_state": state,
        "changed": changed,
        "reason": reason,
        "evaluated_at_ms": evaluated_at_ms,
        "sample_count": sample_count,
        "success_rate": str(success_rate),
        "failure_rate": str(failure_rate),
        "consecutive_failures": consecutive_failures,
        "consecutive_successes": consecutive_successes,
        "opened_at_ms": opened_at,
        "next_probe_at_ms": next_probe,
        "half_open_since_ms": half_open_since,
        "rules": {
            "failure_rate_threshold": str(config.failure_rate_threshold),
            "min_breaker_samples": config.min_breaker_samples,
            "consecutive_failure_threshold": config.consecutive_failure_threshold,
            "open_duration_ms": config.open_duration_ms,
            "half_open_success_threshold": config.half_open_success_threshold,
        },
    }
    return BreakerDecision(
        state=state,
        changed=changed,
        reason=reason,
        opened_at_ms=opened_at,
        next_probe_at_ms=next_probe,
        half_open_since_ms=half_open_since,
        details=details,
    )


def manual_transition(
    current: BreakerState,
    to_state: str,
    occurred_at_ms: int,
    reason: str,
    config: GuardConfig,
) -> BreakerDecision:
    if to_state not in STATES:
        raise BusinessError(
            "E_INVALID_TRANSITION",
            "unknown target breaker state",
            details={"from": current.state, "to": to_state, "legal_targets": sorted(STATES)},
        )
    if to_state == current.state:
        details = {
            "from_state": current.state,
            "to_state": to_state,
            "changed": False,
            "reason": "IDEMPOTENT_SAME_STATE",
            "occurred_at_ms": occurred_at_ms,
            "opened_at_ms": current.opened_at_ms,
            "next_probe_at_ms": current.next_probe_at_ms,
            "half_open_since_ms": current.half_open_since_ms,
        }
        return BreakerDecision(
            state=to_state,
            changed=False,
            reason="IDEMPOTENT_SAME_STATE",
            opened_at_ms=current.opened_at_ms,
            next_probe_at_ms=current.next_probe_at_ms,
            half_open_since_ms=current.half_open_since_ms,
            details=details,
        )
    if (current.state, to_state) not in LEGAL_TRANSITIONS:
        raise BusinessError(
            "E_INVALID_TRANSITION",
            f"transition {current.state} -> {to_state} is not legal",
            409,
            {
                "from": current.state,
                "to": to_state,
                "legal_transitions": [list(pair) for pair in sorted(LEGAL_TRANSITIONS)],
            },
        )
    if current.state == "OPEN" and to_state == "HALF_OPEN":
        if current.next_probe_at_ms is not None and occurred_at_ms < current.next_probe_at_ms:
            raise BusinessError(
                "E_INVALID_TRANSITION",
                "OPEN -> HALF_OPEN is legal only after the breaker cooldown",
                409,
                {
                    "from": current.state,
                    "to": to_state,
                    "occurred_at_ms": occurred_at_ms,
                    "next_probe_at_ms": current.next_probe_at_ms,
                },
            )

    opened_at = current.opened_at_ms
    next_probe = current.next_probe_at_ms
    half_open_since = current.half_open_since_ms
    if to_state == "OPEN":
        opened_at = occurred_at_ms
        next_probe = occurred_at_ms + config.open_duration_ms
        half_open_since = None
    elif to_state == "HALF_OPEN":
        half_open_since = occurred_at_ms
    elif to_state == "CLOSED":
        opened_at = None
        next_probe = None
        half_open_since = None

    details = {
        "from_state": current.state,
        "to_state": to_state,
        "changed": True,
        "reason": reason,
        "occurred_at_ms": occurred_at_ms,
        "opened_at_ms": opened_at,
        "next_probe_at_ms": next_probe,
        "half_open_since_ms": half_open_since,
        "source": "MANUAL",
    }
    return BreakerDecision(
        state=to_state,
        changed=True,
        reason=reason,
        opened_at_ms=opened_at,
        next_probe_at_ms=next_probe,
        half_open_since_ms=half_open_since,
        details=details,
    )

