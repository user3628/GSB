"""Business error codes for the incseg engine."""

from __future__ import annotations


class EngineError(Exception):
    """Base class for all engine errors. Carries a stable machine-readable code."""

    code = "E_INTERNAL"
    http_status = 400

    def __init__(self, message: str = "", details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "details": self.details}


class InvalidConfigError(EngineError):
    code = "E_INVALID_CONFIG"


class InvalidStageError(EngineError):
    code = "E_INVALID_STAGE"


class StaleCheckpointError(EngineError):
    code = "E_STALE_CHECKPOINT"
    http_status = 409


class IncrementalMismatchError(EngineError):
    code = "E_INCREMENTAL_MISMATCH"
    http_status = 409


class WorkspaceError(EngineError):
    code = "E_WORKSPACE"


class StopRequested(Exception):
    """Internal control-flow signal: a graceful stop was requested."""
