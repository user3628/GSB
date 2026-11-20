"""Stable business error codes (E_xxx) surfaced in JSON / stderr output."""


class WorkbenchError(Exception):
    code = "E_INTERNAL"

    def __init__(self, message, details=None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self):
        return {"code": self.code, "message": self.message, "details": self.details}


class LatencyBudgetExceeded(WorkbenchError):
    code = "E_LATENCY_BUDGET"


class InconsistentError(WorkbenchError):
    code = "E_INCONSISTENT"


class StaleCheckpointError(WorkbenchError):
    code = "E_STALE_CHECKPOINT"


class InvalidStageError(WorkbenchError):
    code = "E_INVALID_STAGE"


class ConfigError(WorkbenchError):
    code = "E_CONFIG"


class SampleError(WorkbenchError):
    code = "E_SAMPLE"


class StateError(WorkbenchError):
    code = "E_STATE"
