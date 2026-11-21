"""Business error codes for the checkpoint / WAL replay component."""


class WckptError(Exception):
    """Base class carrying a stable business code."""

    code = "E_INTERNAL"

    def __init__(self, message="", details=None):
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details = details or {}

    def to_dict(self):
        return {"code": self.code, "message": self.message, "details": self.details}


class WalCorruptError(WckptError):
    code = "E_WAL_CORRUPT"


class CheckpointInvalidError(WckptError):
    code = "E_CHECKPOINT_INVALID"


class ReplayMismatchError(WckptError):
    code = "E_REPLAY_MISMATCH"


class TruncateUnsafeError(WckptError):
    code = "E_TRUNCATE_UNSAFE"


class IntentConflictError(WckptError):
    code = "E_INTENT_CONFLICT"


class DuplicateRequestError(WckptError):
    code = "E_DUPLICATE_REQUEST"
