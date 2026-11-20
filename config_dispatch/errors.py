"""业务错误码定义。每种失败情形对应互不相同的稳定业务码。"""


class DispatchError(Exception):
    """所有可预期业务错误的基类。"""

    code = "E_INTERNAL"

    def __init__(self, message: str = "", details=None):
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details = details if details is not None else {}

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "details": self.details}


class StaleSnapshotError(DispatchError):
    code = "E_STALE_SNAPSHOT"


class InvalidSnapshotError(DispatchError):
    code = "E_INVALID_SNAPSHOT"


class DependencyCycleError(DispatchError):
    code = "E_DEPENDENCY_CYCLE"


class InvalidTransitionError(DispatchError):
    code = "E_INVALID_TRANSITION"


class EndpointRejectedError(DispatchError):
    """端点可达但返回业务拒绝（HTTP 4xx/5xx 业务响应）。"""

    code = "E_PUSH_REJECTED"


class EndpointConnectionError(DispatchError):
    """端点不可达（连接被拒绝/重置/DNS 失败等）。"""

    code = "E_PUSH_CONNECTION_FAILED"


class EndpointTimeoutError(DispatchError):
    """请求在超时窗口内未完成，连接已被真实关闭。"""

    code = "E_PUSH_TIMEOUT"


class DispatchNotFoundError(DispatchError):
    code = "E_DISPATCH_NOT_FOUND"


class SnapshotNotFoundError(DispatchError):
    code = "E_SNAPSHOT_NOT_FOUND"


class DispatchAbortedError(DispatchError):
    """进程在执行中被强制终止，运行被恢复流程判定为中止。"""

    code = "E_DISPATCH_ABORTED"
