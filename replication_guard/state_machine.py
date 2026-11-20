from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from .errors import AppError
from .models import Observation, Policy
from .versions import parse_version, relation


HEALTHY = "HEALTHY"
DELAYED = "DELAYED"
DEGRADED = "DEGRADED"
RESTORED = "RESTORED"
STATES = (HEALTHY, DELAYED, DEGRADED, RESTORED)

LEGAL_TRANSITIONS: dict[str, frozenset[str]] = {
    HEALTHY: frozenset({DELAYED, DEGRADED}),
    DELAYED: frozenset({DEGRADED, RESTORED}),
    DEGRADED: frozenset({RESTORED}),
    RESTORED: frozenset({HEALTHY, DELAYED, DEGRADED}),
}


def transition_allowed(current: str, target: str) -> bool:
    return target in LEGAL_TRANSITIONS.get(current, frozenset())


def ensure_transition(current: str, target: str) -> None:
    if target not in STATES:
        raise AppError(
            "E_INVALID_TRANSITION",
            f"未知状态 {target}",
            status_code=409,
            details={"currentState": current, "targetState": target, "allowedStates": sorted(STATES)},
        )
    if not transition_allowed(current, target):
        raise AppError(
            "E_INVALID_TRANSITION",
            f"非法状态迁移 {current} -> {target}",
            status_code=409,
            details={
                "currentState": current,
                "targetState": target,
                "allowedTargets": sorted(LEGAL_TRANSITIONS.get(current, frozenset())),
                "persistedStateUnchanged": True,
            },
        )


def priority_band(priority: int) -> str:
    if priority <= 33:
        return "HIGH"
    if priority <= 66:
        return "MEDIUM"
    return "LOW"


def effective_action(state: str, policy: Policy) -> tuple[str, str]:
    if state == HEALTHY:
        return "NONE", "HEALTHY 不执行降级动作"
    if state == RESTORED:
        return "RESTORE_SERVICE", "已通过健康窗口，执行恢复动作"
    if state == DEGRADED:
        action = policy.degradation_action if policy.degradation_action != "NONE" else "READ_ONLY"
        return action, "DEGRADED 应用策略声明的降级动作；声明的 NONE 在降级状态下提升为 READ_ONLY"
    if priority_band(policy.priority) == "HIGH":
        action = policy.degradation_action if policy.degradation_action != "NONE" else "MONITOR"
        return action, "DELAYED 且高优先级（0-33）应用声明的降级动作"
    return "MONITOR", "DELAYED 且非高优先级仅监控，避免过早降级"


def deduplicate_observations(observations: Sequence[Observation]) -> list[Observation]:
    selected: dict[tuple[str, int], Observation] = {}
    for observation in observations:
        key = (observation.slave_id, observation.sampled_at_ms)
        previous = selected.get(key)
        if previous is None or observation.deterministic_key() > previous.deterministic_key():
            selected[key] = observation
    return sorted(selected.values(), key=lambda item: (item.slave_id, item.sampled_at_ms))


@dataclass(frozen=True)
class StateDecision:
    slave_id: str
    state_before: str
    state_after: str
    version_relation: str
    reported_version: str
    expected_version: str
    requires_manual_review: bool
    business_code: str | None
    effective_action: str
    action_reason: str
    trigger: dict[str, Any]
    window: list[dict[str, Any]]
    selected_observation: dict[str, Any] | None
    transition: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "slaveId": self.slave_id,
            "stateBefore": self.state_before,
            "stateAfter": self.state_after,
            "transition": self.transition,
            "versionRelation": self.version_relation,
            "reportedVersion": self.reported_version,
            "reportedVersionSegments": list(parse_version(self.reported_version).segments)
            if self.reported_version
            else [],
            "expectedVersion": self.expected_version,
            "expectedVersionSegments": list(parse_version(self.expected_version).segments),
            "requiresManualReview": self.requires_manual_review,
            "businessCode": self.business_code,
            "effectiveAction": self.effective_action,
            "actionReason": self.action_reason,
            "trigger": self.trigger,
            "window": self.window,
            "selectedObservation": self.selected_observation,
        }


def decide_state(
    policy: Policy,
    observations: Sequence[Observation],
    *,
    state_before: str,
    window_size: int,
) -> StateDecision:
    ordered = sorted(observations, key=lambda item: item.sampled_at_ms)
    window = ordered[-window_size:] if window_size > 0 else []
    if not window:
        action, action_reason = effective_action(state_before, policy)
        return StateDecision(
            slave_id=policy.slave_id,
            state_before=state_before,
            state_after=state_before,
            version_relation="UNKNOWN",
            reported_version="",
            expected_version=policy.expected_version,
            requires_manual_review=False,
            business_code=None,
            effective_action=action,
            action_reason=action_reason,
            trigger={"code": "NO_OBSERVATION"},
            window=[],
            selected_observation=None,
            transition="NONE",
        )

    latest = window[-1]
    expected = parse_version(policy.expected_version)
    reported = parse_version(latest.reported_version)
    version_relation = relation(reported, expected)
    manual_review = version_relation == "ABOVE" and not policy.forward_compatible
    business_code = "E_VERSION_AHEAD" if manual_review else None

    consecutive_breach = 0
    for item in reversed(window):
        if item.delay_ms > policy.delay_threshold_ms:
            consecutive_breach += 1
        else:
            break
    consecutive_over_tolerable = 0
    for item in reversed(window):
        if item.delay_ms > policy.tolerable_delay_ms:
            consecutive_over_tolerable += 1
        else:
            break
    consecutive_healthy = 0
    for item in reversed(window):
        if item.delay_ms <= policy.delay_threshold_ms:
            consecutive_healthy += 1
        else:
            break

    state_after = state_before
    trigger: dict[str, Any]
    if manual_review:
        trigger = {
            "code": "VERSION_AHEAD_MANUAL_REVIEW",
            "reportedVersion": reported.raw,
            "expectedVersion": expected.raw,
            "forwardCompatible": policy.forward_compatible,
            "reason": "从库上报版本高于预期且策略未声明前向兼容，冻结自动迁移并请求人工复核",
        }
    elif latest.delay_ms > policy.tolerable_delay_ms:
        if consecutive_over_tolerable >= window_size:
            state_after = DEGRADED if state_before != DEGRADED else DEGRADED
            trigger = {
                "code": "TOLERANCE_BREACH_CONFIRMED",
                "delayMs": latest.delay_ms,
                "tolerableDelayMs": policy.tolerable_delay_ms,
                "consecutiveCount": consecutive_over_tolerable,
                "windowSize": window_size,
            }
        elif state_before != DEGRADED and consecutive_breach >= window_size:
            state_after = DELAYED
            trigger = {
                "code": "LAG_BREACH_CONFIRMED",
                "delayMs": latest.delay_ms,
                "delayThresholdMs": policy.delay_threshold_ms,
                "tolerableDelayMs": policy.tolerable_delay_ms,
                "consecutiveCount": consecutive_breach,
                "windowSize": window_size,
                "note": "超过可容忍值的样本尚未填满窗口，但判定阈值连续突破窗口已满足",
            }
        else:
            trigger = {
                "code": "WINDOW_NOT_MET",
                "delayMs": latest.delay_ms,
                "tolerableDelayMs": policy.tolerable_delay_ms,
                "consecutiveCount": consecutive_over_tolerable,
                "windowSize": window_size,
            }
    elif latest.delay_ms > policy.delay_threshold_ms:
        if consecutive_breach >= window_size:
            state_after = state_before if state_before == DEGRADED else DELAYED
            trigger = {
                "code": "LAG_BREACH_CONFIRMED",
                "delayMs": latest.delay_ms,
                "delayThresholdMs": policy.delay_threshold_ms,
                "consecutiveCount": consecutive_breach,
                "windowSize": window_size,
            }
        else:
            trigger = {
                "code": "WINDOW_NOT_MET",
                "delayMs": latest.delay_ms,
                "delayThresholdMs": policy.delay_threshold_ms,
                "consecutiveCount": consecutive_breach,
                "windowSize": window_size,
            }
    else:
        if consecutive_healthy >= window_size:
            if state_before in {DELAYED, DEGRADED}:
                state_after = RESTORED
                trigger = {
                    "code": "RESTORED_AFTER_HEALTHY_WINDOW",
                    "delayMs": latest.delay_ms,
                    "delayThresholdMs": policy.delay_threshold_ms,
                    "consecutiveCount": consecutive_healthy,
                    "windowSize": window_size,
                }
            elif state_before == RESTORED:
                state_after = HEALTHY
                trigger = {
                    "code": "HEALTHY_CONFIRMED",
                    "consecutiveCount": consecutive_healthy,
                    "windowSize": window_size,
                }
            else:
                trigger = {
                    "code": "HEALTHY_CONFIRMED",
                    "consecutiveCount": consecutive_healthy,
                    "windowSize": window_size,
                }
        else:
            trigger = {
                "code": "HEALTHY_WINDOW_NOT_MET",
                "consecutiveCount": consecutive_healthy,
                "windowSize": window_size,
            }

    # Automatic planning must always be legal. TOLERANCE_BREACH_CONFIRMED from RESTORED
    # is legal; DELAYED stays DELAYED; DEGRADED only leaves on a healthy window.
    if state_after != state_before:
        ensure_transition(state_before, state_after)
        transition = f"{state_before}->{state_after}"
    else:
        transition = "NONE"

    action, action_reason = effective_action(state_after, policy)
    if manual_review:
        action = "MANUAL_REVIEW"
        action_reason = "版本前向兼容未声明，禁止自动降级裁决"

    return StateDecision(
        slave_id=policy.slave_id,
        state_before=state_before,
        state_after=state_after,
        version_relation=version_relation,
        reported_version=latest.reported_version,
        expected_version=policy.expected_version,
        requires_manual_review=manual_review,
        business_code=business_code,
        effective_action=action,
        action_reason=action_reason,
        trigger=trigger,
        window=[item.to_dict() for item in window],
        selected_observation=latest.to_dict(),
        transition=transition,
    )


def replay_decisions(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    window_size = int(snapshot.get("window_size", 2))
    policies = [Policy(**item) for item in snapshot.get("policies", [])]
    observations = [Observation(**item) for item in snapshot.get("observations", [])]
    states_before = {key: str(value) for key, value in snapshot.get("states_before", {}).items()}
    deduped = deduplicate_observations(observations)
    by_slave: dict[str, list[Observation]] = {}
    for observation in deduped:
        by_slave.setdefault(observation.slave_id, []).append(observation)
    results = []
    for policy in sorted(policies, key=lambda item: item.slave_id):
        if policy.slave_id not in by_slave:
            continue
        decision = decide_state(
            policy,
            by_slave[policy.slave_id],
            state_before=states_before.get(policy.slave_id, HEALTHY),
            window_size=window_size,
        )
        results.append(decision.as_dict())
    return results

