"""统一错误码与异常类型。"""


class PipelineError(Exception):
    """管线业务错误。code 为稳定错误码，details 携带结构化上下文。"""

    def __init__(self, code, message, details=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self):
        return {"error": self.code, "message": self.message, "details": self.details}


E_EMPTY_SAMPLE = "E_EMPTY_SAMPLE"
E_INVALID_TRAIN = "E_INVALID_TRAIN"
E_NUMERIC = "E_NUMERIC"
E_MODEL_VERSION_MISMATCH = "E_MODEL_VERSION_MISMATCH"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_CORRUPT_CHECKPOINT = "E_CORRUPT_CHECKPOINT"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_BUDGET_EXCEEDED = "E_BUDGET_EXCEEDED"
E_INVALID_CONFIG = "E_INVALID_CONFIG"


def raise_invalid_stage(requested, missing):
    raise PipelineError(
        E_INVALID_STAGE,
        f"阶段 {requested} 的前置阶段产物缺失: {missing}",
        {"requested_stage": requested, "missing_stage": missing},
    )
