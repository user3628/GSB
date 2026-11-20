"""Stable business errors used by the CLI, HTTP service, and tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


E_INVALID_MANIFEST = "E_INVALID_MANIFEST"
E_INVALID_PARAM = "E_INVALID_PARAM"
E_NUMERIC = "E_NUMERIC"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_SNAPSHOT_CONFLICT = "E_SNAPSHOT_CONFLICT"
E_NOT_FOUND = "E_NOT_FOUND"
E_CONFLICT = "E_CONFLICT"


@dataclass(slots=True)
class EngineError(Exception):
    """An error with a stable code and machine-readable diagnostic context."""

    code: str
    message: str
    context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)
        self.context = dict(self.context)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }

    @property
    def http_status(self) -> int:
        if self.code == E_NOT_FOUND:
            return 404
        if self.code == E_CONFLICT:
            return 409
        return 400


def invalid_manifest(message: str, **context: Any) -> EngineError:
    return EngineError(E_INVALID_MANIFEST, message, context)


def invalid_param(message: str, **context: Any) -> EngineError:
    return EngineError(E_INVALID_PARAM, message, context)


def numeric_error(message: str, **context: Any) -> EngineError:
    return EngineError(E_NUMERIC, message, context)


def invalid_stage(message: str, **context: Any) -> EngineError:
    return EngineError(E_INVALID_STAGE, message, context)


def stale_checkpoint(message: str, **context: Any) -> EngineError:
    return EngineError(E_STALE_CHECKPOINT, message, context)


def snapshot_conflict(message: str, **context: Any) -> EngineError:
    return EngineError(E_SNAPSHOT_CONFLICT, message, context)
