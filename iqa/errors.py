"""Stable business error codes and exception type for the IQA pipeline."""

E_DIM_MISMATCH = "E_DIM_MISMATCH"
E_MISSING_REFERENCE = "E_MISSING_REFERENCE"
E_NUMERIC = "E_NUMERIC"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"


class BusinessError(Exception):
    """An error carrying a stable code plus machine-readable context."""

    def __init__(self, code, message, context=None):
        super().__init__(message)
        self.code = code
        self.context = dict(context or {})

    def to_dict(self):
        return {"code": self.code, "message": str(self), "context": self.context}
