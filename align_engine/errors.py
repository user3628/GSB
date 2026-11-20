"""Stable business error codes for the alignment engine."""

E_DIM_MISMATCH = "E_DIM_MISMATCH"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_FORMAT = "E_FORMAT"
E_NOT_FOUND = "E_NOT_FOUND"
E_STATE = "E_STATE"


class EngineError(Exception):
    """Business error carrying a stable machine-readable code."""

    def __init__(self, code, message, details=None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self):
        return {"code": self.code, "message": self.message, "details": self.details}
