"""Stable error objects used by the CLI, HTTP workbench and worker boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class ErrorCode:
    E_INVALID_PARAM = "E_INVALID_PARAM"
    E_MANIFEST_MISMATCH = "E_MANIFEST_MISMATCH"
    E_LABEL_INVALID = "E_LABEL_INVALID"
    E_FUNNEL_VIOLATION = "E_FUNNEL_VIOLATION"
    E_DATA_INVALID = "E_DATA_INVALID"
    E_WORKER_FAILED = "E_WORKER_FAILED"
    E_CHECKPOINT_CORRUPT = "E_CHECKPOINT_CORRUPT"
    E_SCHEDULE_EMPTY = "E_SCHEDULE_EMPTY"
    E_JOB_ACTIVE = "E_JOB_ACTIVE"
    E_NOT_FOUND = "E_NOT_FOUND"
    E_IO = "E_IO"


@dataclass
class PipelineError(Exception):
    """A serializable business error with stable code and location context."""

    code: str
    message: str
    stage: str | None = None
    shard_id: str | None = None
    row_number: int | None = None
    impression_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "stage": self.stage,
            "shard_id": self.shard_id,
            "row_number": self.row_number,
            "impression_id": self.impression_id,
            "details": self.details,
        }


class RunStopped(Exception):
    """Internal cooperative-stop signal carrying the persisted outcome."""

    def __init__(self, outcome: dict[str, Any]):
        super().__init__("run stopped cooperatively")
        self.outcome = outcome
