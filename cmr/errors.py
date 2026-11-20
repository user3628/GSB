"""Stable business error codes for the workbench."""

E_DIM_MISMATCH = "E_DIM_MISMATCH"
E_PROJECTION_INVALID = "E_PROJECTION_INVALID"
E_MISSING_FRAGMENT = "E_MISSING_FRAGMENT"
E_QUERY_INVALID = "E_QUERY_INVALID"
E_NUMERIC = "E_NUMERIC"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"


class CmrError(Exception):
    """Business error carrying a stable code, the pipeline stage and optional position."""

    def __init__(self, code, message, stage=None, position=None, detail=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.stage = stage
        self.position = position
        self.detail = detail or {}

    def to_dict(self):
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "stage": self.stage,
                "position": self.position,
                "detail": self.detail,
            }
        }


class GracefulStop(Exception):
    """Raised between indivisible stage units when a stop request is observed."""

    def __init__(self, stage, shard):
        super().__init__(f"stop requested before stage={stage} shard={shard}")
        self.stage = stage
        self.shard = shard
