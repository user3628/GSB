"""Business errors with stable machine-readable codes."""

from __future__ import annotations

from typing import Any


class PipelineError(Exception):
    """A pipeline failure that is safe to expose through CLI and HTTP APIs."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        stage: str | None = None,
        shard_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.stage = stage
        self.shard_id = shard_id
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "code": self.code,
            "message": self.message,
        }
        if self.stage is not None:
            payload["stage"] = self.stage
        if self.shard_id is not None:
            payload["shard_id"] = self.shard_id
        if self.details:
            payload["details"] = self.details
        return payload


def invalid_param(
    message: str,
    *,
    stage: str,
    shard_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> PipelineError:
    from .constants import ERROR_INVALID_PARAM

    return PipelineError(
        ERROR_INVALID_PARAM,
        message,
        stage=stage,
        shard_id=shard_id,
        details=details,
    )


def pose_invalid(
    message: str,
    *,
    shard_id: str,
    details: dict[str, Any] | None = None,
) -> PipelineError:
    from .constants import ASSEMBLY, ERROR_POSE_INVALID

    return PipelineError(
        ERROR_POSE_INVALID,
        message,
        stage=ASSEMBLY,
        shard_id=shard_id,
        details=details,
    )


def stale_checkpoint(
    message: str,
    *,
    shard_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> PipelineError:
    from .constants import ERROR_STALE_CHECKPOINT

    return PipelineError(
        ERROR_STALE_CHECKPOINT,
        message,
        shard_id=shard_id,
        details=details,
    )


def invalid_stage(
    message: str,
    *,
    shard_id: str | None = None,
    current_stage: str | None = None,
    details: dict[str, Any] | None = None,
) -> PipelineError:
    from .constants import ERROR_INVALID_STAGE

    merged = dict(details or {})
    merged["current_stage"] = current_stage
    return PipelineError(
        ERROR_INVALID_STAGE,
        message,
        shard_id=shard_id,
        details=merged,
    )
