"""Stable business errors used by the CLI, HTTP API and workers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_CHAIN_INVALID = "E_CHAIN_INVALID"
E_TASK_INVALID = "E_TASK_INVALID"
E_INPUT_MISSING = "E_INPUT_MISSING"
E_CONFIG_INVALID = "E_CONFIG_INVALID"
E_BUDGET_GLOBAL_EXCEEDED = "E_BUDGET_GLOBAL_EXCEEDED"
E_WORKER_QUOTA_EXCEEDED = "E_WORKER_QUOTA_EXCEEDED"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_ARTIFACT_INVALID = "E_ARTIFACT_INVALID"
E_SIZE_MISMATCH = "E_SIZE_MISMATCH"
E_DIGEST_MISMATCH = "E_DIGEST_MISMATCH"
E_EXPORT_VERIFY = "E_EXPORT_VERIFY"
E_TASK_TIMEOUT = "E_TASK_TIMEOUT"
E_WORKER_PROCESS_EXITED = "E_WORKER_PROCESS_EXITED"
E_WORKER_TASK_FAILED = "E_WORKER_TASK_FAILED"
E_INCOMPLETE = "E_INCOMPLETE"
E_STOPPED = "E_STOPPED"


@dataclass
class WorkbenchError(Exception):
    code: str
    message: str
    task_id: str | None = None
    stage: str | None = None
    position: int | None = None
    reason: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "task_id": self.task_id,
            "stage": self.stage,
            "position": self.position,
            "reason": self.reason,
            "details": self.details,
        }


def as_error_dict(exc: BaseException) -> dict[str, Any]:
    if isinstance(exc, WorkbenchError):
        return exc.to_dict()
    return {
        "code": E_WORKER_TASK_FAILED,
        "message": f"{type(exc).__name__}: {exc}",
        "task_id": None,
        "stage": None,
        "position": None,
        "reason": "UNHANDLED_EXCEPTION",
        "details": {},
    }
