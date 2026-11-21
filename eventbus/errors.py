from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


ERROR_CATALOG: dict[str, dict[str, Any]] = {
    "E_EVENT_INVALID": {
        "http_status": 400,
        "description": "事件或批次无法解析；整批不入库。",
    },
    "E_CONFLICTING_EVENT": {
        "http_status": 409,
        "description": "同一事件标识被以不同内容重复发布；首次内容保持不变。",
    },
    "E_INVALID_TRANSITION": {
        "http_status": 409,
        "description": "事件状态迁移不在合法迁移表中；不修改持久化状态。",
    },
    "E_LEASE_HELD": {
        "http_status": 409,
        "description": "同一消费者组、分区已有未过期的投递租约。",
    },
    "E_OFFSET_EXPIRED": {
        "http_status": 410,
        "description": "回拨目标早于分区最早保留生产序号。",
    },
    "E_OFFSET_NOT_FOUND": {
        "http_status": 404,
        "description": "回拨目标不是已提交的生产序号，或不是一次回拨。",
    },
    "E_NOT_DISPATCHED": {
        "http_status": 409,
        "description": "确认目标没有当前有效投递。",
    },
    "E_TOPIC_NOT_FOUND": {
        "http_status": 404,
        "description": "主题不存在。",
    },
    "E_CONFLICTING_TOPIC": {
        "http_status": 409,
        "description": "同名主题以不同声明重复创建。",
    },
    "E_GROUP_MISMATCH": {
        "http_status": 409,
        "description": "消费者组与主题声明不一致。",
    },
    "E_PARTITION_NOT_FOUND": {
        "http_status": 404,
        "description": "分区不存在。",
    },
    "E_EVENT_NOT_FOUND": {
        "http_status": 404,
        "description": "事件不存在。",
    },
    "E_REQUEST_INVALID": {
        "http_status": 400,
        "description": "请求结构或参数不合法。",
    },
    "E_TEST_HOOK_DISABLED": {
        "http_status": 403,
        "description": "测试用强制终止钩子未启用。",
    },
    "E_ROUTE_NOT_FOUND": {
        "http_status": 404,
        "description": "HTTP 路由或资源不存在。",
    },
    "E_INTERNAL": {
        "http_status": 500,
        "description": "未预期的内部错误。",
    },
}


@dataclass(slots=True)
class BusinessError(Exception):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None

    def __post_init__(self) -> None:
        if self.code not in ERROR_CATALOG:
            raise ValueError(f"unknown business error code: {self.code}")
        if self.http_status is None:
            self.http_status = int(ERROR_CATALOG[self.code]["http_status"])
        super().__init__(self.message)

    def as_response(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            },
        }
