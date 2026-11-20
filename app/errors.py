from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """An error with a stable, machine-readable business code."""

    code = "E_INTERNAL"
    http_status = 500

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


class SpecInvalidError(BusinessError):
    code = "E_SPEC_INVALID"
    http_status = 422


class TraceInvalidError(BusinessError):
    code = "E_TRACE_INVALID"
    http_status = 422


class InsufficientSamplesError(BusinessError):
    code = "E_INSUFFICIENT_SAMPLES"
    http_status = 422


class PrecisionDriftError(BusinessError):
    code = "E_PRECISION_DRIFT"
    http_status = 409


class BatchNotFoundError(BusinessError):
    code = "E_BATCH_NOT_FOUND"
    http_status = 404


class RunNotFoundError(BusinessError):
    code = "E_RUN_NOT_FOUND"
    http_status = 404


class TraceNotFoundError(BusinessError):
    code = "E_TRACE_NOT_FOUND"
    http_status = 404


class ConflictError(BusinessError):
    code = "E_CONFLICT"
    http_status = 409
