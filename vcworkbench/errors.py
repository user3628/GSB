from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class WorkbenchError(Exception):
    """Stable business error surfaced by every CLI and HTTP entry point."""

    code: str
    message: str
    shard_id: str | None = None
    stage: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "shard_id": self.shard_id,
                "stage": self.stage,
                "details": self.details,
            },
        }


class OperationStopped(Exception):
    """Raised at a frame-block boundary after a graceful stop request."""

    def __init__(self, shard_id: str, stage: str, blocks_completed: int) -> None:
        self.shard_id = shard_id
        self.stage = stage
        self.blocks_completed = blocks_completed
        super().__init__(
            f"stop requested after {blocks_completed} completed block(s) in {stage}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "status": "stopped",
            "shard_id": self.shard_id,
            "stage": self.stage,
            "blocks_completed": self.blocks_completed,
            "message": "graceful stop completed at a frame-block boundary",
        }


ERROR_HTTP_STATUS = {
    "E_CHANNEL_UNSUPPORTED": 422,
    "E_SAMPLE_RATE_MISMATCH": 422,
    "E_NUMERIC": 422,
    "E_MANIFEST_MISMATCH": 422,
    "E_INVALID_PARAM": 422,
    "E_PITCH_OUT_OF_RANGE": 422,
    "E_STREAM_MISMATCH": 409,
    "E_STALE_CHECKPOINT": 409,
    "E_INVALID_STAGE": 409,
    "E_REFERENCE_MISSING": 422,
    "E_LATENCY_BUDGET": 504,
    "E_WORKSPACE_NOT_READY": 409,
    "E_JOB_NOT_FOUND": 404,
    "E_PORT_UNAVAILABLE": 503,
    "E_NOT_FOUND": 404,
    "E_METHOD_NOT_ALLOWED": 405,
    "E_BAD_REQUEST": 400,
}


def fail(
    code: str,
    message: str,
    *,
    shard_id: str | None = None,
    stage: str | None = None,
    **details: Any,
) -> None:
    raise WorkbenchError(code, message, shard_id=shard_id, stage=stage, details=details)
