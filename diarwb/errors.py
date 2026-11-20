"""Structured business error codes for the diarization workbench.

Every failure raised by the workbench is a :class:`WorkbenchError` carrying a
stable ``code`` string.  The same codes are emitted by the CLI and the HTTP
layer so that callers can branch on a stable contract instead of message text.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


class WorkbenchError(Exception):
    """Base class for all structured workbench errors."""

    code = "E_INTERNAL"
    http_status = 400

    def __init__(self, message: str = "", details: Optional[Dict[str, Any]] = None):
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details: Dict[str, Any] = dict(details or {})

    def to_dict(self) -> Dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


class EImportValidation(WorkbenchError):
    """Manifest/WAV consistency checks failed at import time."""

    code = "E_IMPORT_VALIDATION"
    http_status = 422


class EInvalidConfig(WorkbenchError):
    """Configuration values are outside their allowed domain."""

    code = "E_INVALID_CONFIG"
    http_status = 422


class ELatencyBudget(WorkbenchError):
    """A query could not finish inside the configured latency budget."""

    code = "E_LATENCY_BUDGET"
    http_status = 503

    def __init__(self, message: str = "", details: Optional[Dict[str, Any]] = None):
        details = dict(details or {})
        details.setdefault("elapsed_ms", 0)
        details.setdefault("budget_ms", 0)
        details.setdefault("query_id", "")
        super().__init__(message or self.code, details)


class EIncrementalMismatch(WorkbenchError):
    """Incremental recomputation differs from a from-scratch build."""

    code = "E_INCREMENTAL_MISMATCH"
    http_status = 500


class EStaleSnapshot(WorkbenchError):
    """Snapshot storage was rolled back or tampered with."""

    code = "E_STALE_SNAPSHOT"
    http_status = 409

    def __init__(self, kind: str = "tamper", message: str = "",
                 details: Optional[Dict[str, Any]] = None):
        details = dict(details or {})
        details.setdefault("kind", kind)  # rollback | tamper | advance
        super().__init__(message or self.code, details)


class EStaleVersion(WorkbenchError):
    """An edit publish raced and lost the monotonic version comparison."""

    code = "E_STALE_VERSION"
    http_status = 409


class EInvalidStage(WorkbenchError):
    """A stage was requested without prerequisites or jumped illegally."""

    code = "E_INVALID_STAGE"
    http_status = 409


class EStopped(WorkbenchError):
    """A stop request cancelled a run after the current atomic unit."""

    code = "E_STOPPED"
    http_status = 499


class ENotFound(WorkbenchError):
    """Referenced workspace resource (shard, segment, speaker) is unknown."""

    code = "E_NOT_FOUND"
    http_status = 404
