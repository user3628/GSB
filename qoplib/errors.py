"""Stable business error codes and structured error objects.

All error codes are part of the public API and must remain stable across
releases.  Every error carries a machine readable ``code`` plus a human
readable message and a ``details`` mapping that callers can serialise.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional

E_NUMERIC_MISMATCH = "E_NUMERIC_MISMATCH"
E_NUMERIC_INPUT = "E_NUMERIC_INPUT"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_INVALID_BITWIDTH = "E_INVALID_BITWIDTH"
E_MEMORY_BUDGET = "E_MEMORY_BUDGET"
E_COMPUTE_BUDGET = "E_COMPUTE_BUDGET"
E_WORKER_QUOTA = "E_WORKER_QUOTA"
E_WORKER_FAILED = "E_WORKER_FAILED"

ALL_CODES = (
    E_NUMERIC_MISMATCH,
    E_NUMERIC_INPUT,
    E_STALE_CHECKPOINT,
    E_INVALID_STAGE,
    E_INVALID_BITWIDTH,
    E_MEMORY_BUDGET,
    E_COMPUTE_BUDGET,
    E_WORKER_QUOTA,
    E_WORKER_FAILED,
)


class QopError(Exception):
    """Base class for every business error raised by :mod:`qoplib`."""

    code: str = "E_QOPLIB"

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details: Dict[str, Any] = dict(details or {})

    def to_dict(self) -> Dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


class NumericMismatchError(QopError):
    code = E_NUMERIC_MISMATCH

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)


class NumericInputError(QopError):
    code = E_NUMERIC_INPUT

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)


class StaleCheckpointError(QopError):
    code = E_STALE_CHECKPOINT

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)


class InvalidStageError(QopError):
    code = E_INVALID_STAGE

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)


class InvalidBitwidthError(QopError):
    code = E_INVALID_BITWIDTH

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)


class MemoryBudgetError(QopError):
    code = E_MEMORY_BUDGET

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)


class ComputeBudgetError(QopError):
    code = E_COMPUTE_BUDGET

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)


class WorkerQuotaError(QopError):
    code = E_WORKER_QUOTA

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)


class WorkerFailedError(QopError):
    code = E_WORKER_FAILED

    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, details)


@dataclass
class StructuredError:
    """Serializable error record used for worker / shard failures."""

    code: str
    message: str
    shard_id: Optional[str] = None
    stage: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_exception(
        cls,
        exc: BaseException,
        shard_id: Optional[str] = None,
        stage: Optional[str] = None,
    ) -> "StructuredError":
        if isinstance(exc, QopError):
            payload = exc.to_dict()
            details = dict(payload["details"])
            code = payload["code"]
            message = payload["message"]
        else:
            code = E_WORKER_FAILED
            message = f"{type(exc).__name__}: {exc}"
            details = {"exception_type": type(exc).__name__}
        details.setdefault("shard_id", shard_id)
        details.setdefault("stage", stage)
        return cls(code=code, message=message, shard_id=shard_id,
                   stage=stage, details=details)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
