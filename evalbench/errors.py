"""Stable business error codes shared by CLI and HTTP API."""
from __future__ import annotations


class EvalBenchError(Exception):
    """Base error carrying a stable business code and structured details."""

    code = "E_INTERNAL"

    def __init__(self, message: str, **details):
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


class VersionMismatchError(EvalBenchError):
    """Declared structure version does not match record content shape."""

    code = "E_VERSION_MISMATCH"


class InvalidStageError(EvalBenchError):
    """Illegal stage jump or missing prerequisite stage artifact."""

    code = "E_INVALID_STAGE"


class StaleCheckpointError(EvalBenchError):
    """Out-of-order, duplicated or cross-shard checkpoint publish."""

    code = "E_STALE_CHECKPOINT"


class FitFailedError(EvalBenchError):
    """Calibrator fitting failed (non-convergence, degenerate data)."""

    code = "E_FIT_FAILED"


class ConfigInvalidError(EvalBenchError):
    """Configuration is out of range or otherwise invalid."""

    code = "E_CONFIG_INVALID"


class RoundtripViolationError(EvalBenchError):
    """Format round-trip chain did not preserve record semantics."""

    code = "E_ROUNDTRIP_VIOLATION"


class ShardNotFoundError(EvalBenchError):
    code = "E_SHARD_NOT_FOUND"
