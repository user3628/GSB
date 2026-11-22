"""Business error codes and JSON error envelope for the RCA pipeline.

Every failure surfaced through the HTTP API (and most internal call sites)
uses :class:`PipelineError`, which carries a stable business code plus
machine-readable context.  The codes here are the contract used by tests and
by the failure-injection interface.
"""
from __future__ import annotations

from typing import Any


class PipelineError(Exception):
    """An error whose ``code`` is part of the public business contract."""

    http_status = 400

    def __init__(self, code: str, message: str, context: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {"code": self.code, "message": self.message, "context": self.context},
        }


class SampleInvalid(PipelineError):
    http_status = 422

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        super().__init__("E_SAMPLE_INVALID", message, context)


class StaleBase(PipelineError):
    http_status = 409

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        super().__init__("E_STALE_BASE", message, context)


class InsufficientSamples(PipelineError):
    http_status = 422

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        super().__init__("E_INSUFFICIENT_SAMPLES", message, context)


class InvalidTransition(PipelineError):
    http_status = 409

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        super().__init__("E_INVALID_TRANSITION", message, context)


class ConflictingBatch(PipelineError):
    http_status = 409

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        super().__init__("E_CONFLICTING_BATCH", message, context)


class TopologyInvalid(PipelineError):
    http_status = 422

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        super().__init__("E_TOPOLOGY_INVALID", message, context)


class ChecksumMismatch(PipelineError):
    http_status = 422

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        super().__init__("E_CHECKSUM_MISMATCH", message, context)


class RebuildFailed(PipelineError):
    http_status = 500

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        super().__init__("E_REBUILD_FAILED", message, context)


class NotFound(PipelineError):
    http_status = 404

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        super().__init__("E_NOT_FOUND", message, context)
