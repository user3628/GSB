"""Stable business error codes shared by every pipeline stage."""


class PipelineError(Exception):
    """Base class with a stable business code and structured context."""

    code = "E_PIPELINE"

    def __init__(self, message, context=None):
        super().__init__(message)
        self.message = message
        self.context = dict(context or {})

    def to_dict(self):
        return {"code": self.code, "message": self.message, "context": self.context}


class ManifestError(PipelineError):
    code = "E_MANIFEST"


class InputLengthError(PipelineError):
    code = "E_INPUT_LENGTH"


class SampleRateError(PipelineError):
    code = "E_SAMPLE_RATE"


class SilentAudioError(PipelineError):
    code = "E_SILENT_AUDIO"


class ClippedAudioError(PipelineError):
    code = "E_CLIPPED_AUDIO"


class InvalidFrameError(PipelineError):
    code = "E_INVALID_FRAME"


class StreamMismatchError(PipelineError):
    code = "E_STREAM_MISMATCH"


class EndpointMismatchError(PipelineError):
    code = "E_ENDPOINT_MISMATCH"


class BudgetExhaustedError(PipelineError):
    code = "E_BUDGET_EXHAUSTED"


class InvalidStageError(PipelineError):
    code = "E_INVALID_STAGE"


class StaleCheckpointError(PipelineError):
    code = "E_STALE_CHECKPOINT"


class CorruptCheckpointError(PipelineError):
    code = "E_CORRUPT_RECORD"
