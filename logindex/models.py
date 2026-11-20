"""批次状态机与核心数据模型。

状态机合法迁移路径（正常路径）::

    (新建)        -> PENDING
    PENDING       -> INDEXING
    INDEXING      -> COMMITTED
    PENDING       -> ABORTED
    INDEXING      -> ABORTED
    COMMITTED     -> INVALIDATED
    INDEXING      -> PENDING   # 仅恢复器可走的回滚路径

非法迁移抛出 :class:`E_INVALID_TRANSITION`，且不触碰任何持久化状态。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .errors import InvalidTransitionError


class BatchState(str, Enum):
    PENDING = "PENDING"
    INDEXING = "INDEXING"
    COMMITTED = "COMMITTED"
    INVALIDATED = "INVALIDATED"
    ABORTED = "ABORTED"


# 正常业务路径允许的迁移；恢复路径单独列出，只允许 recovery 模块调用。
LEGAL_TRANSITIONS = {
    BatchState.PENDING: {BatchState.INDEXING, BatchState.ABORTED},
    BatchState.INDEXING: {
        BatchState.COMMITTED,
        BatchState.ABORTED,
        BatchState.PENDING,
    },
    BatchState.COMMITTED: {BatchState.INVALIDATED},
    BatchState.INVALIDATED: set(),
    BatchState.ABORTED: set(),
}

# 仅恢复器可使用的回滚迁移。
RECOVERY_TRANSITIONS = {(BatchState.INDEXING, BatchState.PENDING)}


def assert_transition(
    current: BatchState, target: BatchState, *, recovery: bool = False
) -> None:
    """校验状态迁移，非法时抛 :class:`InvalidTransitionError`。"""
    allowed = LEGAL_TRANSITIONS.get(current, set())
    if target in allowed:
        if (current, target) in RECOVERY_TRANSITIONS and not recovery:
            raise InvalidTransitionError(
                f"非法状态迁移: {current.value} -> {target.value}（仅恢复器可回滚）",
                {"from": current.value, "to": target.value},
            )
        return
    raise InvalidTransitionError(
        f"非法状态迁移: {current.value} -> {target.value}",
        {"from": current.value, "to": target.value},
    )


@dataclass(frozen=True)
class LogRecord:
    """一条解析后的日志记录。"""

    timestamp_ms: int
    service: str
    level: str
    message: str
    line_no: int

    def content_tuple(self):
        """幂等比对使用的内容指纹（不含行号以外的批次信息）。"""
        return (self.timestamp_ms, self.service, self.level, self.message)


@dataclass
class BatchResult:
    """一次批次提交（或重打/失效）的结构化结果。"""

    batch_id: str
    state: str
    version: Optional[int]
    path_mode: str
    rebuilt_partitions: list = field(default_factory=list)
    rebuild_reason: Optional[str] = None
    affected_services: list = field(default_factory=list)
    record_count: int = 0
    reused: bool = False


@dataclass
class RecoveryReport:
    """仅基于已持久化事实生成的恢复报告。"""

    recovered: bool
    previous_write_lock_owner: Optional[str]
    open_intents: list
    actions: list
    last_complete_version: Optional[int]
    last_complete_batch: Optional[str]
    notes: list = field(default_factory=list)
