"""Structured business errors used by the pipeline and HTTP API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass
class PipelineError(Exception):
    """A business error with a stable code and JSON-safe details."""

    code: str
    message: str
    details: Mapping[str, Any] = field(default_factory=dict)
    exit_code: int = 2

    def __post_init__(self) -> None:
        super().__init__(self.message)
        self.details = dict(self.details)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": dict(self.details),
            },
        }


class GracefulStop(Exception):
    """Raised at a safe unit boundary after a cooperative stop request."""


def error(code: str, message: str, **details: Any) -> PipelineError:
    return PipelineError(code=code, message=message, details=details)


def as_error_payload(exc: BaseException) -> dict[str, Any]:
    if isinstance(exc, PipelineError):
        return exc.to_dict()
    return {
        "ok": False,
        "error": {
            "code": "E_INTERNAL",
            "message": str(exc) or exc.__class__.__name__,
            "details": {"exception_type": exc.__class__.__name__},
        },
    }
