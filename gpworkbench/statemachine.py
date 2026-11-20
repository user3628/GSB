"""请求状态机：合法迁移路径与迁移校验。"""

from __future__ import annotations

from .errors import E_INVALID_TRANSITION, BusinessError

PENDING = "PENDING"
ADMITTED = "ADMITTED"
BATCHED = "BATCHED"
RUNNING = "RUNNING"
COMPLETED = "COMPLETED"
REJECTED = "REJECTED"
PREEMPTED = "PREEMPTED"

ALL_STATES = (PENDING, ADMITTED, BATCHED, RUNNING, COMPLETED, REJECTED, PREEMPTED)

# 合法迁移路径。终态 COMPLETED / REJECTED 无出边。
ALLOWED_TRANSITIONS = {
    PENDING: (ADMITTED, REJECTED),
    ADMITTED: (BATCHED, REJECTED, PREEMPTED),
    BATCHED: (RUNNING, ADMITTED, PREEMPTED),
    RUNNING: (COMPLETED,),
    PREEMPTED: (ADMITTED, REJECTED),
    COMPLETED: (),
    REJECTED: (),
}

# 标准触发原因
REASON_SUBMITTED = "submitted"
REASON_ADMITTED = "quota_admitted"
REASON_QUOTA_REJECTED = "quota_rejected"
REASON_SPEC_REJECTED = "spec_rejected"
REASON_BATCHED = "batch_formed"
REASON_BATCH_STARTED = "batch_started"
REASON_BATCH_REORGANIZED = "batch_reorganized"
REASON_COMPLETED = "batch_completed"
REASON_PREEMPTED = "preempted_by_higher_priority"
REASON_READMITTED = "readmitted_after_preemption"
REASON_RECOVERY_ABORT = "recovery_aborted"


def ensure_transition(from_state: str, to_state: str) -> None:
    if from_state == to_state:
        raise BusinessError(
            E_INVALID_TRANSITION,
            f"请求已处于 {from_state}，无需重复迁移",
            {"from_state": from_state, "to_state": to_state},
        )
    if to_state not in ALLOWED_STATES:
        raise BusinessError(
            E_INVALID_TRANSITION,
            f"未知目标状态 {to_state}",
            {"from_state": from_state, "to_state": to_state},
        )
    if to_state not in ALLOWED_TRANSITIONS.get(from_state, ()):
        raise BusinessError(
            E_INVALID_TRANSITION,
            f"非法状态迁移 {from_state} -> {to_state}",
            {"from_state": from_state, "to_state": to_state},
        )


ALLOWED_STATES = frozenset(ALL_STATES)
