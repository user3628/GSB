"""Stable business errors used by the CLI, HTTP service, and tests."""

from __future__ import annotations

from typing import Any


E_INVALID_FEATURE = "E_INVALID_FEATURE"
E_HASH_DIM_ZERO = "E_HASH_DIM_ZERO"
E_CROSS_LIMIT = "E_CROSS_LIMIT"
E_VECTOR_LENGTH_MISMATCH = "E_VECTOR_LENGTH_MISMATCH"
E_BUDGET_EXCEEDED = "E_BUDGET_EXCEEDED"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_NOT_FOUND = "E_NOT_FOUND"
E_INVALID_REQUEST = "E_INVALID_REQUEST"

ERROR_MEANINGS = {
    E_INVALID_FEATURE: "A sample feature region/value or a numeric bucket definition is invalid.",
    E_HASH_DIM_ZERO: "The configured category or cross hash dimension is zero.",
    E_CROSS_LIMIT: "Requested cross features exceed the declared/global crossing limit.",
    E_VECTOR_LENGTH_MISMATCH: "The computed fixed vector length differs from the declaration.",
    E_BUDGET_EXCEEDED: "The selected plan's modeled compute consumption exceeds the declared budget.",
    E_INVALID_STAGE: "A stage transition, prerequisite, declaration, or stage payload is invalid.",
    E_STALE_CHECKPOINT: "Checkpoint publication is stale, duplicated, non-contiguous, or cross-scale.",
    E_NOT_FOUND: "The requested scale, artifact, or completed stage does not exist.",
    E_INVALID_REQUEST: "The CLI/HTTP request is syntactically invalid.",
}


class PipelineError(Exception):
    """A business error with a stable code and machine-readable context."""

    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


def error_payload(exc: PipelineError) -> dict[str, Any]:
    return {"ok": False, "error": exc.to_dict()}


def http_status_for(code: str) -> int:
    if code == E_STALE_CHECKPOINT:
        return 409
    if code == E_NOT_FOUND:
        return 404
    if code == E_INVALID_STAGE:
        return 409
    if code == E_INVALID_REQUEST:
        return 400
    return 400
