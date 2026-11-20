"""Business error codes and the single HTTP error envelope used by every API."""
from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse


class BusinessError(Exception):
    def __init__(self, code: str, message: str, http_status: int = 409, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details or {}


# Registry keeps HTTP mapping in one place.
ERROR_HTTP_STATUS = {
    "E_SESSION_CONFLICT": 409,
    "E_SESSION_NOT_FOUND": 404,
    "E_SESSION_ABORTED": 409,
    "E_SESSION_COMPLETED": 409,
    "E_SESSION_CANCELLED": 409,
    "E_PART_INVALID": 400,
    "E_PART_MISMATCH": 409,
    "E_PART_MISSING": 409,
    "E_SIZE_MISMATCH": 409,
    "E_DIGEST_MISMATCH": 409,
    "E_OBJECT_NOT_FOUND": 404,
}


def business_error_response(exc: BusinessError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.http_status,
        content={
            "ok": False,
            "error": {
                "code": exc.code,
                "message": exc.message,
                "details": exc.details,
            },
        },
    )


async def business_error_handler(_request: Request, exc: BusinessError) -> JSONResponse:
    return business_error_response(exc)


async def validation_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    # Normalise pydantic/request-validation failures into the same envelope.
    return JSONResponse(
        status_code=422,
        content={
            "ok": False,
            "error": {
                "code": "E_REQUEST_INVALID",
                "message": "Request payload failed schema validation.",
                "details": {"validation": getattr(exc, "errors", lambda: [])()},
            },
        },
    )
