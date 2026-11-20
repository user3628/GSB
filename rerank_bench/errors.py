"""Stable business error codes and structured error objects."""

from __future__ import annotations

E_MISSING_FEATURE = "E_MISSING_FEATURE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_LATENCY_BUDGET = "E_LATENCY_BUDGET"
E_BUDGET_EXCEEDED = "E_BUDGET_EXCEEDED"
E_QUOTA_EXCEEDED = "E_QUOTA_EXCEEDED"
E_STOP_REQUESTED = "E_STOP_REQUESTED"
E_WORKER_FAILURE = "E_WORKER_FAILURE"


class PipelineError(Exception):
    """Structured pipeline error carrying a stable business code."""

    def __init__(self, code: str, message: str, **details):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


def error_dict(code: str, message: str, **details) -> dict:
    return {"code": code, "message": message, "details": details}
