"""Stable business errors used by the engine, CLI, and HTTP API."""

from __future__ import annotations

from typing import Any


E_QUERY_INVALID = "E_QUERY_INVALID"
E_NUMERIC = "E_NUMERIC"
E_LATENCY_BUDGET = "E_LATENCY_BUDGET"
E_EVIDENCE_EMPTY = "E_EVIDENCE_EMPTY"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_BUDGET_EXHAUSTED = "E_BUDGET_EXHAUSTED"
E_WORKER = "E_WORKER"
E_CONFIG_INVALID = "E_CONFIG_INVALID"
E_DOCUMENT_INVALID = "E_DOCUMENT_INVALID"
E_REQUEST_NOT_FOUND = "E_REQUEST_NOT_FOUND"
E_INTERNAL = "E_INTERNAL"


class EngineError(Exception):
    """A structured, serializable failure.

    ``partial`` is deliberately kept on the exception so worker failures can
    return completed subquery results without advancing the stage checkpoint.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        query_id: str | None = None,
        subquery_index: int | None = None,
        doc_id: str | None = None,
        details: dict[str, Any] | None = None,
        partial: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.query_id = query_id
        self.subquery_index = subquery_index
        self.doc_id = doc_id
        self.details = details or {}
        self.partial = partial or {}

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.query_id is not None:
            value["query_id"] = self.query_id
        if self.subquery_index is not None:
            value["subquery_index"] = self.subquery_index
        if self.doc_id is not None:
            value["doc_id"] = self.doc_id
        if self.details:
            value["details"] = self.details
        return value


def error_result(
    error: EngineError,
    *,
    diagnostics: dict[str, Any] | None = None,
    version: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "query_id": error.query_id,
        "status": "error",
        "error": error.to_dict(),
        "partial": error.partial,
        "diagnostics": diagnostics or {},
    }
    if version is not None:
        result["version"] = version
    return result
