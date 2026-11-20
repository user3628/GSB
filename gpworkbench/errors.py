"""业务错误码与异常定义。"""

# 业务码集中声明，API 层与 CLI 层共用
E_SPEC_INVALID = "E_SPEC_INVALID"
E_QUOTA_EXCEEDED = "E_QUOTA_EXCEEDED"
E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_BATCH_TOO_LARGE = "E_BATCH_TOO_LARGE"
E_NODE_FRAGMENTED = "E_NODE_FRAGMENTED"
E_NODE_UNHEALTHY = "E_NODE_UNHEALTHY"
E_NOT_FOUND = "E_NOT_FOUND"
E_PREEMPTION_NOT_POSSIBLE = "E_PREEMPTION_NOT_POSSIBLE"
E_INTENT_ABORTED = "E_INTENT_ABORTED"
E_DUPLICATE_REQUEST = "E_DUPLICATE_REQUEST"


class BusinessError(Exception):
    """携带稳定业务码与结构化诊断详情的异常。"""

    def __init__(self, code: str, message: str = "", details: dict | None = None):
        self.code = code
        self.details = details or {}
        if message:
            super().__init__(message)
        else:
            super().__init__(code)

    def to_dict(self) -> dict:
        return {"code": self.code, "message": str(self), "details": self.details}


class SpecValidationError(BusinessError):
    def __init__(self, message: str, field: str = "", value=None):
        details = {"field": field, "value": None if value is None else str(value)}
        super().__init__(E_SPEC_INVALID, message, details)


class CrashSimulated(Exception):
    """测试钩子：意图已落盘但应用事务尚未提交时模拟进程被 kill -9。"""

    def __init__(self, intent_id: str, intent_type: str):
        self.intent_id = intent_id
        self.intent_type = intent_type
        super().__init__(f"simulated crash after intent {intent_id} ({intent_type})")
