from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse

from app import __version__
from app.errors import WorkbenchError, workbench_error_handler
from app.models import IncrementalRequest, SnapshotRequest
from app.proxy import ALL_METHODS, ProxyEngine
from app.service import ConfigService
from app.settings import Settings


LOGGER = logging.getLogger("workbench")
STATIC_DIR = Path(__file__).resolve().parent / "static"


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    service = ConfigService(
        settings.db_path, enable_failpoints=settings.enable_failpoints
    )
    proxy = ProxyEngine(service, service.store)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await asyncio.to_thread(service.startup)
        current = service.current
        recovery = service._startup_recovery
        LOGGER.info(
            "workbench ready db=%s version=%s recovered_intents=%s",
            settings.db_path,
            current.version if current else 0,
            recovery.get("rolled_back_intent_count") if recovery else 0,
        )
        yield

    app = FastAPI(
        title="Loopback Proxy Diagnostics Workbench",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.service = service
    app.state.proxy = proxy

    app.add_exception_handler(WorkbenchError, workbench_error_handler)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        is_config = request.url.path.startswith("/workbench/api/config/")
        code = "E_SNAPSHOT_INVALID" if is_config else "E_BAD_REQUEST"
        error = WorkbenchError(
            code,
            "request validation failed",
            details={"errors": jsonable_encoder(exc.errors())},
        )
        return JSONResponse(status_code=error.http_status or 422, content=error.payload())

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("unhandled workbench error on %s", request.url.path)
        error = WorkbenchError(
            "E_INTERNAL",
            "internal workbench error",
            details={"exception_type": type(exc).__name__},
        )
        return JSONResponse(status_code=500, content=error.payload())

    @app.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "dashboard.html")

    @app.get("/dashboard", include_in_schema=False)
    async def dashboard() -> FileResponse:
        return FileResponse(STATIC_DIR / "dashboard.html")

    @app.get("/workbench/api/health")
    async def health() -> dict[str, Any]:
        current = service.current
        return {
            "ok": True,
            "version": __version__,
            "current_config_version": current.version if current else 0,
            "db_path": str(settings.db_path),
            "loopback_only": True,
        }

    @app.get("/workbench/api/state")
    async def state(
        version: int | None = Query(default=None, ge=1)
    ) -> dict[str, Any]:
        return await asyncio.to_thread(service.state, version)

    @app.get("/workbench/api/versions")
    async def versions() -> dict[str, Any]:
        return await asyncio.to_thread(service.versions)

    @app.get("/workbench/api/versions/{version}")
    async def version_state(version: int) -> dict[str, Any]:
        return await asyncio.to_thread(service.state, version)

    @app.get("/workbench/api/budgets/explain")
    async def explain_budget(
        route_id: str,
        version: int | None = Query(default=None, ge=1),
    ) -> dict[str, Any]:
        return await asyncio.to_thread(service.explain_budget, route_id, version)

    @app.get("/workbench/api/budgets/benchmark")
    async def budget_benchmark() -> dict[str, Any]:
        return service.benchmark()

    @app.get("/workbench/api/intents")
    async def intents(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, Any]:
        return await asyncio.to_thread(service.intents, limit)

    @app.get("/workbench/api/recovery")
    async def recovery() -> dict[str, Any]:
        return await asyncio.to_thread(service.recovery)

    @app.post("/workbench/api/config/snapshot")
    async def submit_snapshot(
        payload: SnapshotRequest, request: Request
    ) -> dict[str, Any]:
        failpoint = request.headers.get("x-workbench-failpoint")
        return await asyncio.to_thread(
            service.submit_snapshot, payload, failpoint=failpoint
        )

    @app.post("/workbench/api/config/incremental")
    async def submit_incremental(
        payload: IncrementalRequest, request: Request
    ) -> dict[str, Any]:
        failpoint = request.headers.get("x-workbench-failpoint")
        return await asyncio.to_thread(
            service.submit_incremental, payload, failpoint=failpoint
        )

    sample_names = (
        "routes_snapshot_v1.json",
        "routes_incremental_v2.json",
        "local_services.json",
        "loopback_ports.json",
    )

    @app.get("/workbench/api/samples")
    async def list_samples() -> dict[str, Any]:
        return {
            "samples": {
                name: json.loads((settings.samples_dir / name).read_text(encoding="utf-8"))
                for name in sample_names
            }
        }

    @app.get("/workbench/api/samples/{name}")
    async def sample(name: str) -> Any:
        if name not in sample_names:
            raise WorkbenchError(
                "E_ROUTE_NOT_FOUND", "sample not found", details={"name": name}
            )
        return json.loads((settings.samples_dir / name).read_text(encoding="utf-8"))

    @app.api_route("/forward", methods=ALL_METHODS, include_in_schema=False)
    async def forward_root(request: Request):
        return await proxy.handle_forward(request, "")

    @app.api_route("/forward/{path:path}", methods=ALL_METHODS, include_in_schema=False)
    async def forward_path(request: Request, path: str):
        return await proxy.handle_forward(request, path)

    @app.api_route("/{path:path}", methods=ALL_METHODS, include_in_schema=False)
    async def reverse_ingress(request: Request, path: str):
        return await proxy.handle_reverse(request)

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    runtime_settings = Settings.from_env()
    uvicorn.run(
        "app.main:app",
        host=runtime_settings.host,
        port=runtime_settings.port,
        reload=False,
    )
