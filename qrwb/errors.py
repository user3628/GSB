"""稳定业务错误码与结构化错误对象。

所有阶段产生的错误都使用 :class:`BusinessError`，保证：
* 错误码字符串稳定（CLI / HTTP / 检查点共用同一套词汇表）；
* 可被 ``canonical_dumps`` 序列化，不包含线程 id、墙钟时间等非确定字段；
* 保留诊断上下文（query_id、阶段、原因、相关引用）。
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional


# ---- 稳定错误码词汇表 ----------------------------------------------------
E_EMPTY_QUERY = "E_EMPTY_QUERY"            # 规范化后为空 / 仅标点
E_BUDGET_EXHAUSTED = "E_BUDGET_EXHAUSTED"  # 全局评估预算不足
E_CHANNEL_QUOTA = "E_CHANNEL_QUOTA"        # 单通道配额不足
E_LATENCY_BUDGET = "E_LATENCY_BUDGET"      # 超过每请求延迟预算
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"  # 序号落后/重复/错位
E_INVALID_STAGE = "E_INVALID_STAGE"        # 前置阶段缺失或非法跳转
E_UNKNOWN_TOKEN = "E_UNKNOWN_TOKEN"        # 桥接词典缺失词项（回退标记，非致命）
E_WORKER_EXCEPTION = "E_WORKER_EXCEPTION"  # 工作线程内异常（结构化记录）
E_FINGERPRINT_MISMATCH = "E_FINGERPRINT_MISMATCH"  # 旧指纹产物不可复用

ALL_ERROR_CODES = (
    E_EMPTY_QUERY,
    E_BUDGET_EXHAUSTED,
    E_CHANNEL_QUOTA,
    E_LATENCY_BUDGET,
    E_STALE_CHECKPOINT,
    E_INVALID_STAGE,
    E_UNKNOWN_TOKEN,
    E_WORKER_EXCEPTION,
    E_FINGERPRINT_MISMATCH,
)


@dataclass(frozen=True)
class BusinessError(Exception):
    """结构化业务错误（frozen，便于安全跨线程传递）。"""

    code: str
    message: str
    query_id: Optional[str] = None
    stage: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:  # pragma: no cover - 仅用于日志展示
        head = self.code
        if self.query_id:
            head = f"{head}@{self.query_id}"
        return f"{head}: {self.message}"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def make_error(code: str, message: str, query_id: Optional[str] = None,
               stage: Optional[str] = None, **context: Any) -> BusinessError:
    return BusinessError(code=code, message=message, query_id=query_id,
                         stage=stage, context=dict(context))
