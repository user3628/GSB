"""Stable business error codes for the voxel reconstruction engine."""

from __future__ import annotations

E_VOXEL_INVALID = "E_VOXEL_INVALID"
E_SPARSE_EMPTY = "E_SPARSE_EMPTY"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_LOCK_TIMEOUT = "E_LOCK_TIMEOUT"
E_MANIFEST_INVALID = "E_MANIFEST_INVALID"
E_NOT_FOUND = "E_NOT_FOUND"
E_STOPPED = "E_STOPPED"
E_CHECKPOINT_CORRUPT = "E_CHECKPOINT_CORRUPT"


class EngineError(Exception):
    """Structured business error with a stable machine-readable code."""

    def __init__(self, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }
