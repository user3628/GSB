"""Stable business error codes and structured error objects.

All failures that cross a module boundary are expressed with one of the
``E_*`` codes defined here so that CLI and HTTP surfaces stay identical.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Stable error code registry
# ---------------------------------------------------------------------------
E_MISSING_RATIO = "E_MISSING_RATIO"
E_SERIES_TOO_SHORT = "E_SERIES_TOO_SHORT"
E_BUDGET_EXCEEDED = "E_BUDGET_EXCEEDED"
E_WORKER_QUOTA = "E_WORKER_QUOTA"
E_TIME_BUDGET = "E_TIME_BUDGET"
E_WORKER_FAILED = "E_WORKER_FAILED"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_CONFIG_INVALID = "E_CONFIG_INVALID"
E_MANIFEST_MISMATCH = "E_MANIFEST_MISMATCH"
E_DATA_INVALID = "E_DATA_INVALID"
E_NOT_FOUND = "E_NOT_FOUND"


class WorkbenchError(Exception):
    """Base exception carrying a stable code and structured context."""

    code = "E_WORKBENCH_ERROR"

    def __init__(self, message: str, code: Optional[str] = None,
                 context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        self.context = dict(context or {})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


class ConfigError(WorkbenchError):
    code = E_CONFIG_INVALID


class StageError(WorkbenchError):
    code = E_INVALID_STAGE


class StaleCheckpointError(WorkbenchError):
    code = E_STALE_CHECKPOINT


class ManifestError(WorkbenchError):
    code = E_MANIFEST_MISMATCH


@dataclass
class FoldError:
    """Structured error attached to a fold/series instead of an exception."""

    code: str
    message: str
    series_id: Optional[str] = None
    fold_index: Optional[int] = None
    context: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
