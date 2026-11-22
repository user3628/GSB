"""业务错误码定义。

所有业务失败都抛出 :class:`BenchError`，由 HTTP 层统一翻译成 JSON 响应，
服务层与存储层不耦合 Web 框架，便于直接以库方式测试。
"""

from __future__ import annotations

from typing import Any


class BenchError(Exception):
    """携带稳定业务码的领域异常。

    ``code`` 是对外契约的一部分，调用方按业务码而不是 HTTP 状态码判断失败类型。
    ``details`` 中只放可复现核对所需的结构化事实（如环路径、冲突字段）。
    """

    code: str = "E_INTERNAL"
    http_status: int = 400

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {"ok": False, "error": {"code": self.code, "message": self.message, "details": self.details}}


class SpecInvalid(BenchError):
    """资源规格非法：标识重复、父引用缺失、权重越界、字段类型错误等。"""

    code = "E_SPEC_INVALID"
    http_status = 422


class SnapshotInvalid(BenchError):
    """快照级校验失败：版本不单调、摘要不匹配。"""

    code = "E_SNAPSHOT_INVALID"
    http_status = 409


class StaleBase(BenchError):
    """增量片段的基础版本不是当前版本，或同基础版本并发竞争失败。"""

    code = "E_STALE_BASE"
    http_status = 409


class DependencyCycle(BenchError):
    """依赖图存在环；仍携带未被污染资源的部分结果。"""

    code = "E_DEPENDENCY_CYCLE"
    http_status = 200


class ConflictingRequest(BenchError):
    """同一提交标识重复提交但请求体内容不一致。"""

    code = "E_CONFLICTING_REQUEST"
    http_status = 409


class IntentNotFound(BenchError):
    """查询的提交标识不存在。"""

    code = "E_INTENT_NOT_FOUND"
    http_status = 404


class NoSnapshot(BenchError):
    """尚无任何已提交快照，诊断查询无法执行。"""

    code = "E_NO_SNAPSHOT"
    http_status = 404
