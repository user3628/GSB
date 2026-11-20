from __future__ import annotations

from typing import Any, Dict, Optional


class KVError(Exception):
    """Base class for errors that cross the HTTP boundary."""

    code = "E_INTERNAL"
    http_status = 500

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def as_dict(self) -> Dict[str, Any]:
        error: Dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            error["details"] = self.details
        return {"ok": False, "error": error}


class InvalidArgument(KVError):
    code = "E_INVALID_ARGUMENT"
    http_status = 400


class FormatInvalid(KVError):
    code = "E_FORMAT_INVALID"
    http_status = 400


class KeyNotFound(KVError):
    code = "E_KEY_NOT_FOUND"
    http_status = 404


class VersionConflict(KVError):
    code = "E_VERSION_CONFLICT"
    http_status = 409


class ConcurrentWrite(KVError):
    code = "E_CONCURRENT_WRITE"
    http_status = 409


class SnapshotInvalid(KVError):
    code = "E_SNAPSHOT_INVALID"
    http_status = 409


class ReadTimeoutBudget(KVError):
    code = "E_READ_TIMEOUT"
    http_status = 408


class StorageError(KVError):
    code = "E_STORAGE"
    http_status = 500
