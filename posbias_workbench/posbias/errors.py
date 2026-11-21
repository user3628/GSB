"""Stable business error codes shared by CLI and HTTP layers."""


class WorkbenchError(Exception):
    code = "E_UNKNOWN"

    def __init__(self, message, *, code=None, stage=None, scheme=None,
                 bin_id=None, context=None):
        super().__init__(message)
        if code is not None:
            self.code = code
        self.stage = stage
        self.scheme = scheme
        self.bin_id = bin_id
        self.context = dict(context or {})

    def to_dict(self):
        return {
            "code": self.code,
            "message": str(self),
            "stage": self.stage,
            "scheme": self.scheme,
            "bin_id": self.bin_id,
            "context": self.context,
        }


class NumericError(WorkbenchError):
    code = "E_NUMERIC"


class SeqTooLongError(WorkbenchError):
    code = "E_SEQ_TOO_LONG"


class InvalidParamError(WorkbenchError):
    code = "E_INVALID_PARAM"


class InvalidConfigError(WorkbenchError):
    code = "E_INVALID_CONFIG"


class StaleCheckpointError(WorkbenchError):
    code = "E_STALE_CHECKPOINT"


class InvalidStageError(WorkbenchError):
    code = "E_INVALID_STAGE"


class StoppedError(WorkbenchError):
    code = "E_STOPPED"


ERROR_CODES = (
    "E_NUMERIC",
    "E_SEQ_TOO_LONG",
    "E_INVALID_PARAM",
    "E_INVALID_CONFIG",
    "E_STALE_CHECKPOINT",
    "E_INVALID_STAGE",
    "E_STOPPED",
    "E_UNKNOWN",
)
