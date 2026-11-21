"""业务错误码与异常链路。

每个业务错误都携带结构化 ``payload``，HTTP/CLI 层统一渲染为
``{"error": {"code", "message", ...}}``，便于程序化消费与验证。
"""

from __future__ import annotations


class ReconError(Exception):
    """全部业务异常的基类。"""

    code = "E_INTERNAL"
    http_status = 400

    def __init__(self, message: str, **payload):
        super().__init__(message)
        self.message = message
        self.payload = payload

    def to_dict(self) -> dict:
        body = {"code": self.code, "message": self.message}
        body.update(self.payload)
        return body


class EmptyFeatureError(ReconError):
    code = "E_EMPTY_FEATURE"
    http_status = 422


class InvalidPoseError(ReconError):
    code = "E_INVALID_POSE"
    http_status = 422


class GeometryDegenerateError(ReconError):
    code = "E_GEOMETRY_DEGENERATE"
    http_status = 422


class LatencyBudgetError(ReconError):
    code = "E_LATENCY_BUDGET"
    http_status = 503


class IncrementalMismatchError(ReconError):
    code = "E_INCREMENTAL_MISMATCH"
    http_status = 409


class StaleCheckpointError(ReconError):
    code = "E_STALE_CHECKPOINT"
    http_status = 409


class InvalidStageError(ReconError):
    code = "E_INVALID_STAGE"
    http_status = 409


class ManifestError(ReconError):
    code = "E_MANIFEST"
    http_status = 422


class NotFoundError(ReconError):
    code = "E_NOT_FOUND"
    http_status = 404


class StoppedError(ReconError):
    code = "E_STOPPED"
    http_status = 409
