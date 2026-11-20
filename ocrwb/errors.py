"""Stable business error codes for the OCR diagnostic workbench.

Every error raised across the pipeline carries one of the stable string codes
defined here so that the CLI and HTTP layers can report deterministic results.
"""

from __future__ import annotations


E_DETECT_INVALID = "E_DETECT_INVALID"
E_RECOGNIZE_INVALID = "E_RECOGNIZE_INVALID"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_BUDGET_EXCEEDED = "E_BUDGET_EXCEEDED"


class WorkbenchError(Exception):
    """Base class carrying a stable ``code`` and structured ``details``."""

    code = "E_WORKBENCH"

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = dict(details or {})

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "details": self.details}


class DetectInvalid(WorkbenchError):
    code = E_DETECT_INVALID


class RecognizeInvalid(WorkbenchError):
    code = E_RECOGNIZE_INVALID


class StaleCheckpoint(WorkbenchError):
    code = E_STALE_CHECKPOINT


class InvalidStage(WorkbenchError):
    code = E_INVALID_STAGE


class IncrementalMismatch(WorkbenchError):
    code = E_INCREMENTAL_MISMATCH


class BudgetExceeded(WorkbenchError):
    code = E_BUDGET_EXCEEDED
