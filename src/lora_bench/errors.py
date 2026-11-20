"""Structured business error codes for the LoRA workbench.

Every error carries a stable code, a human readable message plus locating
fields (at minimum ``task_id`` and, where meaningful, ``step`` / ``field``).
"""
from __future__ import annotations

from dataclasses import dataclass, field as dc_field, asdict
from typing import Any


E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_TASK = "E_INVALID_TASK"
E_BUDGET_EXCEEDED = "E_BUDGET_EXCEEDED"
E_WEIGHT_MISMATCH = "E_WEIGHT_MISMATCH"
E_INVALID_PHASE = "E_INVALID_PHASE"
E_CORRUPT_CHECKPOINT = "E_CORRUPT_CHECKPOINT"
E_NOT_FOUND = "E_NOT_FOUND"


class BenchError(Exception):
    """Base class carrying a structured payload."""

    code: str = "E_INTERNAL"

    def __init__(
        self,
        message: str,
        *,
        task_id: str | None = None,
        step: int | None = None,
        field: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.task_id = task_id
        self.step = step
        self.field = field
        self.detail = detail or {}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "task_id": self.task_id,
            "step": self.step,
            "field": self.field,
        }
        payload.update(self.detail)
        return payload


class StaleCheckpointError(BenchError):
    code = E_STALE_CHECKPOINT


class InvalidTaskError(BenchError):
    code = E_INVALID_TASK


class BudgetExceededError(BenchError):
    code = E_BUDGET_EXCEEDED


class WeightMismatchError(BenchError):
    code = E_WEIGHT_MISMATCH


class InvalidPhaseError(BenchError):
    code = E_INVALID_PHASE


class CorruptCheckpointError(BenchError):
    code = E_CORRUPT_CHECKPOINT


class NotFoundError(BenchError):
    code = E_NOT_FOUND


@dataclass
class TaskError:
    """Structured error produced when a worker step fails."""

    code: str
    message: str
    task_id: str
    step: int | None = None
    field: str | None = None
    detail: dict[str, Any] = dc_field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
