"""Business errors and stable JSON envelopes for the local plan engine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


HTTP_STATUS = {
    "OK": 200,
    "E_BAD_REQUEST": 400,
    "E_PARSE": 400,
    "E_NOT_FOUND": 404,
    "E_REQUEST_CONFLICT": 409,
    "E_REWRITE_ABORTED": 409,
    "E_PLAN_TOO_LARGE": 413,
    "E_INVALID_AGGREGATE": 422,
    "E_SEMANTIC": 422,
    "E_SAMPLE_NOT_READY": 503,
    "E_INTERNAL": 500,
}


@dataclass
class EngineError(Exception):
    """An expected engine failure that is safe to expose as JSON."""

    business_code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    @property
    def http_status(self) -> int:
        return HTTP_STATUS.get(self.business_code, 500)

    def to_dict(self, request_id: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "business_code": self.business_code,
            "message": self.message,
            "context": self.context,
        }
        if request_id is not None:
            payload["request_id"] = request_id
        return payload


class ParseError(EngineError):
    def __init__(self, message: str, *, line: int, column: int, offset: int, sql: str) -> None:
        excerpt_start = max(0, offset - 24)
        excerpt_end = min(len(sql), offset + 40)
        excerpt = sql[excerpt_start:excerpt_end]
        super().__init__(
            "E_PARSE",
            f"{message} (line {line}, column {column})",
            {
                "line": line,
                "column": column,
                "offset": offset,
                "sql_excerpt": excerpt,
            },
        )


class SemanticError(EngineError):
    def __init__(self, message: str, **context: Any) -> None:
        super().__init__("E_SEMANTIC", message, context)


class InvalidAggregateError(EngineError):
    def __init__(self, message: str, **context: Any) -> None:
        super().__init__("E_INVALID_AGGREGATE", message, context)


class PlanTooLargeError(EngineError):
    def __init__(self, actual: int, limit: int, *, stage: str, injected: bool = False) -> None:
        super().__init__(
            "E_PLAN_TOO_LARGE",
            f"logical plan has {actual} nodes, exceeding the limit of {limit}",
            {
                "actual_node_count": actual,
                "max_nodes": limit,
                "stage": stage,
                "injected": injected,
            },
        )


class RewriteAbortedError(EngineError):
    def __init__(self, message: str, **context: Any) -> None:
        super().__init__("E_REWRITE_ABORTED", message, context)


class RequestConflictError(EngineError):
    def __init__(self, request_id: str) -> None:
        super().__init__(
            "E_REQUEST_CONFLICT",
            "request_id was already committed with different SQL",
            {"request_id": request_id},
        )


class NotFoundError(EngineError):
    def __init__(self, resource: str, identifier: str) -> None:
        super().__init__(
            "E_NOT_FOUND",
            f"{resource} was not found",
            {"resource": resource, "identifier": identifier},
        )


class BadRequestError(EngineError):
    def __init__(self, message: str, **context: Any) -> None:
        super().__init__("E_BAD_REQUEST", message, context)


def ok_envelope(**fields: Any) -> dict[str, Any]:
    payload = {"ok": True, "business_code": "OK"}
    payload.update(fields)
    return payload

