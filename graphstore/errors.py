"""统一业务错误码。

所有可预期的失败都抛出 :class:`GraphStoreError` 的子类，HTTP 层会把它们
序列化为统一结构的 JSON：``{"ok": false, "error": {"code": ..., "message": ...,
"context": ...}}``。
"""


class GraphStoreError(Exception):
    code = "E_INTERNAL"

    def __init__(self, message, context=None):
        super().__init__(message)
        self.message = message
        self.context = context or {}

    def to_dict(self):
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


class SnapshotInvalidError(GraphStoreError):
    code = "E_SNAPSHOT_INVALID"


class WeightInvalidError(GraphStoreError):
    code = "E_WEIGHT_INVALID"


class GraphInconsistentError(GraphStoreError):
    code = "E_GRAPH_INCONSISTENT"


class VersionRetiredError(GraphStoreError):
    code = "E_VERSION_RETIRED"


class ConflictingRequestError(GraphStoreError):
    code = "E_CONFLICTING_REQUEST"
