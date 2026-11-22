"""Stable business error types used by the CLI and HTTP API."""

from __future__ import annotations

from typing import Any


class PipelineError(Exception):
    """An expected, user-visible pipeline failure."""

    http_status = 400

    def __init__(
        self,
        code: str,
        message: str,
        details: Any | None = None,
        *,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details
        if http_status is not None:
            self.http_status = http_status

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "code": self.code,
            "message": self.message,
        }
        if self.details is not None:
            payload["details"] = self.details
        return payload


class StopRequested(Exception):
    """Internal cooperative cancellation signal."""


E_EMPTY_TEXT = "E_EMPTY_TEXT"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_INVALID_INPUT = "E_INVALID_INPUT"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_STOPPED = "E_STOPPED"
E_STOP_REQUESTED = "E_STOP_REQUESTED"
E_VALIDATION_FAILED = "E_VALIDATION_FAILED"
E_LOCK_TIMEOUT = "E_LOCK_TIMEOUT"
E_NOT_FOUND = "E_NOT_FOUND"
E_IO = "E_IO"


def invalid_stage(message: str, details: Any | None = None) -> PipelineError:
    return PipelineError(E_INVALID_STAGE, message, details, http_status=409)


def stale_checkpoint(message: str, details: Any | None = None) -> PipelineError:
    return PipelineError(E_STALE_CHECKPOINT, message, details, http_status=409)


def invalid_input(message: str, details: Any | None = None) -> PipelineError:
    return PipelineError(E_INVALID_INPUT, message, details, http_status=400)


def invalid_config(message: str, details: Any | None = None) -> PipelineError:
    return PipelineError(E_INVALID_CONFIG, message, details, http_status=400)
