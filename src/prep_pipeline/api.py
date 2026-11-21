from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .config import Settings
from .errors import PipelineError
from .models import BatchSubmitRequest, VerifyRequest
from .policy import parse_policy_snapshot
from .service import PipelineService
from .verifier import ResultVerifier


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def create_app(settings: Settings | None = None, service: PipelineService | None = None) -> FastAPI:
    resolved_settings = settings or Settings.from_env()
    resolved_service = service or PipelineService(resolved_settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.recovery_reports_at_startup = resolved_service.initialize(recover=True)
        yield

    app = FastAPI(
        title="Deterministic Preprocessing Pipeline",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = resolved_settings
    app.state.service = resolved_service
    app.state.verifier = ResultVerifier(resolved_settings, resolved_service.db)

    @app.exception_handler(PipelineError)
    async def pipeline_error_handler(_: Request, exc: PipelineError) -> JSONResponse:
        return JSONResponse(status_code=int(exc.http_status or 400), content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        error = PipelineError(
            "E_REQUEST_INVALID",
            "request body or parameters failed validation",
            {"validation_errors": _json_safe(exc.errors())},
        )
        return JSONResponse(status_code=error.http_status or 422, content=error.as_dict())

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = "E_ROUTE_NOT_FOUND" if exc.status_code == 404 else "E_METHOD_NOT_ALLOWED"
        error = PipelineError(code, str(exc.detail), {"status_code": exc.status_code})
        return JSONResponse(status_code=exc.status_code, content=error.as_dict())

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
        error = PipelineError(
            "E_INTERNAL",
            "unhandled server error",
            {"exception_type": type(exc).__name__, "detail": str(exc)},
        )
        return JSONResponse(status_code=500, content=error.as_dict())

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "policy_head": resolved_service.db.policy_head(),
            "database": str(resolved_settings.db_path),
        }

    @app.get("/v1/policies/head")
    def policy_head() -> dict[str, Any]:
        return {"current_version": resolved_service.db.policy_head()}

    @app.get("/v1/policies/{version}")
    def get_policy(version: int) -> dict[str, Any]:
        return resolved_service.db.get_policy(version).model_dump(mode="json")

    @app.post("/v1/policies")
    def submit_policy(payload: dict[str, Any]) -> dict[str, Any]:
        return resolved_service.submit_policy(parse_policy_snapshot(payload))

    @app.post("/v1/batches")
    def submit_batch(request: BatchSubmitRequest) -> dict[str, Any]:
        return resolved_service.submit_batch(request)

    @app.get("/v1/batches/{batch_id}")
    def get_batch(batch_id: str) -> dict[str, Any]:
        return resolved_service.get_batch(batch_id)

    @app.get("/v1/batches/{batch_id}/stages")
    def get_batch_stages(batch_id: str) -> dict[str, Any]:
        batch = resolved_service.get_batch(batch_id)
        return {
            "batch_id": batch_id,
            "committed_at": batch["committed_at"],
            "visibility": batch["visibility"],
            "stages": batch["stages"],
        }

    @app.get("/v1/batches/{batch_id}/shards")
    def get_batch_shards(batch_id: str) -> dict[str, Any]:
        batch = resolved_service.get_batch(batch_id)
        return {
            "batch_id": batch_id,
            "committed_at": batch["committed_at"],
            "visibility": batch["visibility"],
            "shards": batch["shards"],
        }

    @app.get("/v1/recovery")
    def recovery_reports() -> dict[str, Any]:
        reports = resolved_service.recovery_reports()
        return {"reports": reports, "report_count": len(reports)}

    @app.get("/v1/recovery/{batch_id}")
    def recovery_report(batch_id: str) -> dict[str, Any]:
        reports = resolved_service.recovery_reports(batch_id)
        if not reports:
            raise PipelineError(
                "E_BATCH_NOT_FOUND",
                "no recovery report exists for this batch",
                {"batch_id": batch_id},
            )
        return {"reports": reports, "report_count": len(reports)}

    @app.post("/v1/recovery/scan")
    def recovery_scan() -> dict[str, Any]:
        reports = resolved_service.recover()
        return {"recovered": reports, "recovered_count": len(reports)}

    @app.post("/v1/verify/{batch_id}")
    def verify_batch(batch_id: str, request: VerifyRequest | None = None) -> dict[str, Any]:
        request = request or VerifyRequest()
        return app.state.verifier.verify(
            batch_id,
            policy_version=request.policy_version,
            baseline_batch_id=request.baseline_batch_id,
        )

    return app


app = create_app()
