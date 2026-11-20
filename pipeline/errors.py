"""Stable business error codes surfaced by the pipeline and the CLI."""

E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STOP_REQUESTED = "E_STOP_REQUESTED"
E_INPUT = "E_INPUT"
E_CONFIG = "E_CONFIG"


class PipelineError(Exception):
    """Business error carrying a stable machine-readable code."""

    def __init__(self, code, message):
        super().__init__("%s: %s" % (code, message))
        self.code = code
        self.message = message
