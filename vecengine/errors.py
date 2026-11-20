from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """A classified error that is safe to expose through the local API."""

    code = "E_INTERNAL"
    http_status = 500

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


class PlanError(BusinessError):
    code = "E_PLAN_INVALID"
    http_status = 400


class MemoryBudgetError(BusinessError):
    code = "E_MEMORY_BUDGET"
    http_status = 400


class SessionNotFoundError(BusinessError):
    code = "E_SESSION_NOT_FOUND"
    http_status = 404


class DuplicateRequestError(BusinessError):
    code = "E_DUPLICATE_REQUEST"
    http_status = 409


class SessionNotReadyError(BusinessError):
    code = "E_SESSION_NOT_READY"
    http_status = 409


class SessionAbortedError(BusinessError):
    code = "E_SESSION_ABORTED"
    http_status = 409


class ResultNotFoundError(BusinessError):
    code = "E_RESULT_NOT_FOUND"
    http_status = 410


class ExecutionError(BusinessError):
    code = "E_EXECUTION_FAILED"
    http_status = 500


class RouteNotFoundError(BusinessError):
    code = "E_ROUTE_NOT_FOUND"
    http_status = 404


class MethodNotAllowedError(BusinessError):
    code = "E_METHOD_NOT_ALLOWED"
    http_status = 405

