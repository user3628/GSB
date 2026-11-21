from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import BusinessError
from .service import EngineService, VISIBILITY_RULES

LOGGER = logging.getLogger("pdb_engine")


class LoadSampleRequest(BaseModel):
    sample_path: str | None = Field(
        default=None,
        description="Local JSON path. Relative paths resolve from CWD, then the project root.",
    )


class EvictionDecisionRequest(BaseModel):
    pod: str = Field(min_length=1)
    request_id: str | None = None


class DrainRequest(BaseModel):
    node: str = Field(min_length=1)
    request_id: str = Field(min_length=1)


def _validation_details(exc: RequestValidationError) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for error in exc.errors():
        details.append(
            {
                "location": [str(part) for part in error.get("loc", [])],
                "message": error.get("msg", "invalid input"),
                "type": error.get("type", "validation_error"),
            }
        )
    return details


def create_app(db_path: str | Path | None = None) -> FastAPI:
    service = EngineService(db_path=db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.initialize()
        yield

    application = FastAPI(
        title="PDB Drain Adjudication Engine",
        version="1.0.0",
        description=(
            "Deterministic Kubernetes-style PDB eviction and node-drain simulation "
            "over local JSON samples."
        ),
        lifespan=lifespan,
    )
    application.state.engine = service

    @application.exception_handler(BusinessError)
    async def business_error_handler(request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @application.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "error": {
                    "code": "E_SPEC_INVALID",
                    "message": "request body or parameters are invalid",
                    "details": {"validation_errors": _validation_details(exc)},
                }
            },
        )

    @application.exception_handler(Exception)
    async def internal_error_handler(request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("unhandled engine error")
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "E_INTERNAL",
                    "message": "unexpected internal engine error",
                    "details": {"exception_type": type(exc).__name__},
                }
            },
        )

    @application.get("/health")
    def health() -> dict[str, Any]:
        service.initialize()
        return {
            "status": "ok",
            "database": str(service.db_path),
            "scenario_loaded": service._scenario is not None,
            "sqlite_wal": True,
            "visibility": VISIBILITY_RULES,
        }

    @application.post("/api/v1/samples/load")
    def load_sample(body: LoadSampleRequest | None = None) -> dict[str, Any]:
        return service.load_sample(body.sample_path if body else None)

    @application.get("/api/v1/scenario")
    def get_scenario() -> dict[str, Any]:
        return service.scenario_summary()

    @application.post("/api/v1/evictions/check")
    def check_eviction(body: EvictionDecisionRequest) -> dict[str, Any]:
        return service.check_eviction(body.pod, body.request_id)

    @application.post("/api/v1/drains")
    def drain_node(body: DrainRequest) -> dict[str, Any]:
        return service.drain_node(body.node, body.request_id)

    @application.get("/api/v1/drains")
    def list_drains() -> dict[str, Any]:
        return {"drains": service.list_drains(), "visibility": VISIBILITY_RULES}

    @application.get("/api/v1/drains/{request_id}")
    def get_drain(request_id: str) -> dict[str, Any]:
        return service.get_drain_detail(request_id)

    @application.get("/api/v1/recovery/report")
    def get_recovery_report() -> dict[str, Any]:
        return service.get_recovery_report()

    return application


app = create_app()

