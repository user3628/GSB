from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import Body, FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import __version__
from .db import Database
from .errors import AppError, spec_invalid
from .service import ScalingControlService


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).resolve().parent / "static"
SAMPLES_DIR = PROJECT_ROOT / "samples"


def create_app(
    db_path: str | Path | None = None,
    samples_dir: str | Path | None = None,
) -> FastAPI:
    database_path = db_path or os.environ.get(
        "SCALE_DB_PATH", str(PROJECT_ROOT / "data" / "scaler.db")
    )
    db = Database(database_path)
    service = ScalingControlService(db)
    samples_root = Path(samples_dir) if samples_dir is not None else SAMPLES_DIR

    app = FastAPI(
        title="Inference Elastic Scaling & Placement Arbiter",
        version=__version__,
        description=(
            "Deterministic local control plane for versioned scaling policy snapshots, "
            "fixed-point replica derivation, bounded parallel placement and SQLite recovery."
        ),
    )
    app.state.db = db
    app.state.service = service
    app.state.samples_dir = samples_root

    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.exception_handler(AppError)
    async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        error = AppError(
            "E_SPEC_INVALID",
            "request body or query parameters failed schema validation",
            {"validation_errors": jsonable_encoder(exc.errors())},
            400,
        )
        return JSONResponse(status_code=400, content=error.as_dict())

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        error = AppError(
            "E_INTERNAL",
            "unexpected internal error",
            {"type": exc.__class__.__name__},
            500,
        )
        return JSONResponse(status_code=500, content=error.as_dict())

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = "E_NOT_FOUND" if exc.status_code == 404 else "E_HTTP_ERROR"
        error = AppError(
            code,
            str(exc.detail),
            {"path": request.url.path, "status_code": exc.status_code},
            exc.status_code,
        )
        return JSONResponse(status_code=exc.status_code, content=error.as_dict())

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/api/v1/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "version": __version__,
            "policy_version": db.current_policy_version(),
            "latest_revision": db.latest_revision(),
            "pending_intents": db.pending_intent_count(),
            "startup_recovery_events": len(service.startup_recovery),
            "storage": "sqlite-wal",
            "deterministic_virtual_time": True,
        }

    @app.get("/api/v1/policy/current")
    def current_policy() -> dict[str, Any]:
        row = db.get_policy_row()
        if row is None:
            raise AppError(
                "E_POLICY_NOT_FOUND",
                "no policy snapshot has been committed",
                {},
                404,
            )
        return {
            **row,
            "revision": db.latest_revision(),
            "normalized_policy": _policy_to_dict(db.get_policy(row["version"])),
        }

    @app.get("/api/v1/policies")
    def policies() -> dict[str, Any]:
        return {"items": db.list_policy_versions()}

    @app.post("/api/v1/policy")
    def submit_policy(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        return service.initialize_policy(payload)

    @app.get("/api/v1/state")
    def state(revision: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
        return service.get_state_response(revision)

    @app.get("/api/v1/revisions")
    def revisions(limit: int = Query(default=100, ge=1, le=1000)) -> dict[str, Any]:
        return {"items": db.list_revisions(limit), "latest_revision": db.latest_revision()}

    @app.post("/api/v1/rounds")
    def execute_round(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        return service.execute_round(payload)

    @app.get("/api/v1/rounds")
    def rounds(limit: int = Query(default=50, ge=1, le=500)) -> dict[str, Any]:
        return {"items": db.list_rounds(limit)}

    @app.get("/api/v1/rounds/{round_id}")
    def get_round(round_id: str) -> dict[str, Any]:
        return service.get_round_response(round_id)

    @app.post("/api/v1/derive")
    def derive(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        return service.derive(payload)

    @app.post("/api/v1/services/{service_id}/transition")
    def transition_service(
        service_id: str, payload: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        if "target_state" not in payload:
            raise spec_invalid("target_state is required")
        return service.transition_service(
            service_id,
            payload.get("target_state"),
            payload.get("expected_revision"),
            str(payload.get("reason", "operator transition")),
        )

    @app.get("/api/v1/recovery/report")
    def recovery_report() -> dict[str, Any]:
        return db.recovery_report()

    @app.get("/api/v1/samples")
    def list_samples() -> dict[str, Any]:
        items = []
        if samples_root.exists():
            for path in sorted(samples_root.glob("*.json")):
                items.append({"name": path.name, "size_bytes": path.stat().st_size})
        return {"items": items}

    @app.get("/api/v1/samples/{name}")
    def get_sample(name: str) -> JSONResponse:
        if Path(name).name != name or not name.endswith(".json"):
            raise AppError("E_SPEC_INVALID", "invalid sample name", {"name": name}, 400)
        path = samples_root / name
        if not path.exists():
            raise AppError(
                "E_SAMPLE_NOT_FOUND", "sample does not exist", {"name": name}, 404
            )
        import json

        return JSONResponse(content=json.loads(path.read_text(encoding="utf-8")))

    return app


def _policy_to_dict(policy: Any) -> dict[str, Any] | None:
    if policy is None:
        return None
    return {
        "version": policy.version,
        "generated_at": policy.generated_at,
        "runtime": {
            "max_parallel_scales": policy.runtime.max_parallel_scales,
            "round_time_budget_ms": policy.runtime.round_time_budget_ms,
            "placement_cost_ms": policy.runtime.placement_cost_ms,
            "wait_queue_limit": policy.runtime.wait_queue_limit,
        },
        "services": [
            {
                "id": item.id,
                "model": item.model,
                "current_replicas": item.current_replicas,
                "min_replicas": item.min_replicas,
                "max_replicas": item.max_replicas,
                "replica_memory_bytes": item.replica_memory_bytes,
                "replica_compute_milli": item.replica_compute_milli,
                "target_concurrency": item.target_concurrency,
                "latency_budget_ms": item.latency_budget_ms,
                "priority": item.priority,
                "max_replicas_per_fault_domain": item.max_replicas_per_fault_domain,
            }
            for item in policy.services
        ],
        "nodes": [
            {
                "id": item.id,
                "fault_domain": item.fault_domain,
                "memory_bytes": item.memory_bytes,
                "compute_milli": item.compute_milli,
                "healthy": item.healthy,
                "reserved_memory_bytes": item.reserved_memory_bytes,
                "reserved_compute_milli": item.reserved_compute_milli,
            }
            for item in policy.nodes
        ],
    }


def __getattr__(name: str) -> Any:
    # Support `uvicorn app.main:app` without creating a default database merely
    # because tests import this module for the `create_app` factory.
    if name == "app":
        value = create_app()
        globals()["app"] = value
        return value
    raise AttributeError(name)
