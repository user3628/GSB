"""Typed workbench errors shared by the CLI, HTTP API and engine."""

from __future__ import annotations

from typing import Any


class WorkbenchError(Exception):
    """Base class for errors that are safe to expose through the API."""

    code = "E_WORKBENCH"
    http_status = 500

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class InvalidHyperparameter(WorkbenchError):
    code = "E_INVALID_HYPERPARAM"
    http_status = 422


class InvalidCV(WorkbenchError):
    code = "E_INVALID_CV"
    http_status = 422


class NumericError(WorkbenchError):
    code = "E_NUMERIC"
    http_status = 422


class StaleCheckpoint(WorkbenchError):
    code = "E_STALE_CHECKPOINT"
    http_status = 409


class InvalidStage(WorkbenchError):
    code = "E_INVALID_STAGE"
    http_status = 409


class InvalidData(WorkbenchError):
    code = "E_INVALID_DATA"
    http_status = 422


class Busy(WorkbenchError):
    code = "E_BUSY"
    http_status = 409


class NotFound(WorkbenchError):
    code = "E_NOT_FOUND"
    http_status = 404


class IOErrorWorkbench(WorkbenchError):
    code = "E_IO"
    http_status = 500


class StopRequested(Exception):
    """Internal control-flow signal; a graceful stop is not a failure."""


# E_EMPTY_FEATURE is intentionally a non-fatal per-column diagnostic.  It is
# represented as a warning dictionary and never raised for an otherwise usable
# matrix.  Keeping the code here documents the stable public error vocabulary.
EMPTY_FEATURE_CODE = "E_EMPTY_FEATURE"
