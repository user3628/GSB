"""Stable business error codes used across all pipeline stages."""


class PrefAlignError(Exception):
    """Base error carrying a stable business code and structured details."""

    code = "E_INTERNAL"

    def __init__(self, message, code=None, details=None):
        super().__init__(message)
        if code is not None:
            self.code = code
        self.details = details or {}

    def to_dict(self):
        return {"code": self.code, "message": str(self), "details": self.details}


class InvalidSampleError(PrefAlignError):
    code = "E_INVALID_SAMPLE"


class NumericError(PrefAlignError):
    code = "E_NUMERIC"


class FeatureInvalidError(PrefAlignError):
    code = "E_FEATURE_INVALID"


class AllocationInvalidError(PrefAlignError):
    code = "E_ALLOCATION_INVALID"


class IncrementalMismatchError(PrefAlignError):
    code = "E_INCREMENTAL_MISMATCH"


class StaleCheckpointError(PrefAlignError):
    code = "E_STALE_CHECKPOINT"


class InvalidStageError(PrefAlignError):
    code = "E_INVALID_STAGE"


class ConfigError(PrefAlignError):
    code = "E_CONFIG_INVALID"
