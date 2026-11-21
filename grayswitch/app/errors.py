"""统一业务错误码与异常类型。

所有失败响应都通过 :class:`BusinessError` 抛出，由 HTTP 层翻译为统一 JSON：
``{"error": {"code": ..., "message": ..., "context": {...}}}``。
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class BusinessError(Exception):
    http_status = 422

    def __init__(
        self,
        code: str,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        http_status: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context: Dict[str, Any] = context or {}
        if http_status is not None:
            self.http_status = http_status

    def to_payload(self) -> Dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            }
        }


# 快照非法（版本号不单调、摘要不匹配、字段缺失等），已提交状态保持不变。
E_SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
# 事件批次非法（时间戳/权重解析失败等），整批不入库。
E_EVENT_INVALID = "E_EVENT_INVALID"
# 权重非法（总和为零、负权重、比例越界等），不允许静默回落默认版本。
E_WEIGHT_INVALID = "E_WEIGHT_INVALID"
# 灰度使用比例越过灰度上限（相对容差之外）。
E_GRAY_LIMIT_EXCEEDED = "E_GRAY_LIMIT_EXCEEDED"
# 同一请求标识重复提交但内容不一致。
E_CONFLICTING_REQUEST = "E_CONFLICTING_REQUEST"
# 资源不存在。
E_NOT_FOUND = "E_NOT_FOUND"
# 去重/状态冲突等其它冲突。
E_STATE_CONFLICT = "E_STATE_CONFLICT"
