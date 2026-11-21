from __future__ import annotations

from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse


ERROR_STATUS = {
    "E_RECORD_INVALID": 422,
    "E_STALE_BASE": 409,
    "E_CONFLICTING_RECORD": 409,
    "E_SNAPSHOT_INVALID": 422,
    "E_REPLAY_TIMEOUT": 504,
    "E_BATCH_BUDGET_EXCEEDED": 422,
    "E_VERSION_NOT_FOUND": 404,
    "E_REPLAY_NOT_FOUND": 404,
    "E_REPLAY_FAILED": 502,
    "E_INTERNAL": 500,
}


class BusinessError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.status_code = status_code or ERROR_STATUS.get(code, 400)

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "details": self.details}


async def business_error_handler(_: Request, exc: BusinessError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.as_dict())


async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={
            "code": "E_SNAPSHOT_INVALID",
            "message": "Request payload validation failed",
            "details": {"errors": exc.errors()},
        },
    )


async def unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(
        status_code=500,
        content={
            "code": "E_INTERNAL",
            "message": "Unexpected internal error",
            "details": {"type": type(exc).__name__, "message": str(exc)},
        },
    )
