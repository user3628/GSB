"""稳定业务错误码与管线异常类型。

所有错误均携带错误码、阶段名、样本标识（如适用）与人类可读消息，
便于在 CLI / HTTP / 验证工具中形成可追溯的错误链路。
"""

E_MISSING_INPUT = "E_MISSING_INPUT"
E_SHAPE_MISMATCH = "E_SHAPE_MISMATCH"
E_NUMERIC = "E_NUMERIC"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_CORRUPT_CHECKPOINT = "E_CORRUPT_CHECKPOINT"
E_STOP_REQUESTED = "E_STOP_REQUESTED"


class PipeError(Exception):
    def __init__(self, code: str, message: str, stage: str | None = None,
                 sample_id: str | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.stage = stage
        self.sample_id = sample_id

    def to_dict(self) -> dict:
        out = {"code": self.code, "message": self.message}
        if self.stage is not None:
            out["stage"] = self.stage
        if self.sample_id is not None:
            out["sample_id"] = self.sample_id
        return out


class StageFailure(PipeError):
    """阶段内样本级错误的聚合容器（阶段整体失败、不发布产物）。"""

    def __init__(self, stage: str, errors: list[dict]):
        super().__init__(errors[0]["code"] if errors else E_INVALID_STAGE,
                         f"stage {stage} failed with {len(errors)} sample error(s)",
                         stage=stage)
        self.errors = errors

    def to_dict(self) -> dict:
        out = super().to_dict()
        out["errors"] = self.errors
        return out
