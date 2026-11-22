"""Stable business errors used by the CLI, HTTP server, and stage code."""

from __future__ import annotations

from typing import Any, Mapping


E_INVALID_CHAIN = "E_INVALID_CHAIN"
E_INVALID_PARAM = "E_INVALID_PARAM"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_INPUT = "E_INVALID_INPUT"
E_INVALID_TASKS = "E_INVALID_TASKS"
E_TASK_NOT_FOUND = "E_TASK_NOT_FOUND"
E_RESULT_UNAVAILABLE = "E_RESULT_UNAVAILABLE"
E_RUN_IN_PROGRESS = "E_RUN_IN_PROGRESS"
E_LOCK_TIMEOUT = "E_LOCK_TIMEOUT"
E_ARTIFACT_CORRUPT = "E_ARTIFACT_CORRUPT"
E_IO = "E_IO"
E_INTERNAL = "E_INTERNAL"


HTTP_STATUS = {
    E_INVALID_CHAIN: 422,
    E_INVALID_PARAM: 422,
    E_INVALID_STAGE: 409,
    E_STALE_CHECKPOINT: 409,
    E_INVALID_INPUT: 422,
    E_INVALID_TASKS: 422,
    E_TASK_NOT_FOUND: 404,
    E_RESULT_UNAVAILABLE: 409,
    E_RUN_IN_PROGRESS: 409,
    E_LOCK_TIMEOUT: 503,
    E_ARTIFACT_CORRUPT: 500,
    E_IO: 500,
    E_INTERNAL: 500,
}


class PipelineError(Exception):
    """A structured, stable failure.

    ``to_dict`` is deliberately JSON-only and contains no Python object
    representations so that the same payload can be returned by CLI code and
    the HTTP API.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        task_id: str | None = None,
        operator: str | None = None,
        stage: str | None = None,
        details: Mapping[str, Any] | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.task_id = task_id
        self.operator = operator
        self.stage = stage
        self.details = dict(details or {})
        self.context = dict(context or {})

    @property
    def http_status(self) -> int:
        return HTTP_STATUS.get(self.code, 500)

    def to_dict(self) -> dict[str, Any]:
        error: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.task_id is not None:
            error["task_id"] = self.task_id
        if self.operator is not None:
            error["operator"] = self.operator
        if self.stage is not None:
            error["stage"] = self.stage
        if self.details:
            error["details"] = self.details
        if self.context:
            error["context"] = self.context
        return error

    def as_response(self) -> dict[str, Any]:
        return {"ok": False, "error": self.to_dict()}


def invalid_chain(
    task_id: str,
    operator: str | None,
    reason: str,
    message: str,
    **details: Any,
) -> PipelineError:
    return PipelineError(
        E_INVALID_CHAIN,
        message,
        task_id=task_id,
        operator=operator,
        stage="parse",
        details={"reason": reason, **details},
    )


def invalid_param(
    operator: str,
    message: str,
    *,
    task_id: str | None = None,
    **details: Any,
) -> PipelineError:
    return PipelineError(
        E_INVALID_PARAM,
        message,
        task_id=task_id,
        operator=operator,
        stage="execute",
        details=details,
    )


def invalid_stage(
    stage: str | None,
    reason: str,
    message: str,
    *,
    task_id: str | None = None,
    **details: Any,
) -> PipelineError:
    return PipelineError(
        E_INVALID_STAGE,
        message,
        task_id=task_id,
        stage=stage,
        details={"reason": reason, **details},
    )


def stale_checkpoint(
    task_id: str | None,
    stage: str | None,
    reason: str,
    message: str,
    **details: Any,
) -> PipelineError:
    return PipelineError(
        E_STALE_CHECKPOINT,
        message,
        task_id=task_id,
        stage=stage,
        details={"reason": reason, **details},
    )


def internal_error(exc: BaseException, **context: Any) -> PipelineError:
    return PipelineError(
        E_INTERNAL,
        f"Unexpected internal error: {type(exc).__name__}: {exc}",
        context=context,
    )
