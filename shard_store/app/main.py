"""FastAPI application entry point with startup crash recovery."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

from app.api.routes import router
from app.config import Settings
from app.db.connection import connect
from app.db.schema import SCHEMA_SQL
from app.errors import (
    BusinessError,
    business_error_handler,
    validation_error_handler,
)
from app.services.recovery_service import RecoveryService


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings.from_env()
    # Ensure schema exists even if init script was skipped.
    conn = connect(settings)
    try:
        conn.executescript(SCHEMA_SQL)
    finally:
        conn.close()
    app.state.settings = settings
    # Startup scan: any pending intent left by a killed predecessor is resolved.
    app.state.last_recovery = RecoveryService(settings).recover()
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="Local Shard Upload Consistency Component",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.add_exception_handler(BusinessError, business_error_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.include_router(router)
    return app


app = create_app()
