from __future__ import annotations

import logging
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .errors import AppError, ErrorCode


LOGGER = logging.getLogger(__name__)


def install_error_handlers(
    app: FastAPI,
    *,
    request_validation_code: ErrorCode = ErrorCode.REQUEST_VALIDATION,
) -> None:
    @app.exception_handler(AppError)
    async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status or 500,
            content=jsonable_encoder(exc.to_envelope()),
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        error = AppError(
            request_validation_code,
            "Request validation failed",
            {"errors": exc.errors()},
        )
        return JSONResponse(
            status_code=error.http_status or 422,
            content=jsonable_encoder(error.to_envelope()),
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = ErrorCode.NOT_FOUND if exc.status_code == 404 else ErrorCode.REQUEST_VALIDATION
        if exc.status_code >= 500:
            code = ErrorCode.INTERNAL
        error = AppError(
            code,
            str(exc.detail),
            {"status_code": exc.status_code, "path": request.url.path},
            http_status=exc.status_code,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=jsonable_encoder(error.to_envelope()),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        LOGGER.exception("Unhandled exception for %s", request.url.path, exc_info=exc)
        error = AppError(
            ErrorCode.INTERNAL,
            "Internal service error",
            {"path": request.url.path},
        )
        return JSONResponse(
            status_code=500,
            content=jsonable_encoder(error.to_envelope()),
        )

