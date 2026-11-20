"""组件级状态机：定义五个状态及全部合法迁移路径。"""

from __future__ import annotations

from .errors import InvalidTransitionError

PENDING = "PENDING"
DISPATCHED = "DISPATCHED"
APPLIED = "APPLIED"
FAILED = "FAILED"
ROLLED_BACK = "ROLLED_BACK"

ALL_STATES = frozenset({PENDING, DISPATCHED, APPLIED, FAILED, ROLLED_BACK})

# 合法迁移表。非法迁移抛出 E_INVALID_TRANSITION 且由仓储层保证不落库。
LEGAL_TRANSITIONS: dict[str, frozenset[str]] = {
    PENDING: frozenset({DISPATCHED, ROLLED_BACK}),
    # DISPATCHED -> PENDING 仅用于恢复流程把在途请求重新排队
    DISPATCHED: frozenset({APPLIED, FAILED, ROLLED_BACK, PENDING}),
    APPLIED: frozenset({ROLLED_BACK, FAILED}),
    FAILED: frozenset({PENDING, ROLLED_BACK}),
    ROLLED_BACK: frozenset(),
}


def ensure_transition(current: str, target: str) -> None:
    if current not in ALL_STATES:
        raise InvalidTransitionError(
            f"未知状态: {current}", details={"current": current, "target": target}
        )
    if target not in ALL_STATES:
        raise InvalidTransitionError(
            f"未知目标状态: {target}", details={"current": current, "target": target}
        )
    if target not in LEGAL_TRANSITIONS[current]:
        raise InvalidTransitionError(
            f"非法状态迁移 {current} -> {target}",
            details={"current": current, "target": target},
        )


def is_legal(current: str, target: str) -> bool:
    try:
        ensure_transition(current, target)
    except InvalidTransitionError:
        return False
    return True
