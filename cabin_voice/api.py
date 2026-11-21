from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from .errors import CabinError
from .service import CabinService


class ImportRequest(BaseModel):
    manifest_path: str
    reset: bool = False
    config: dict[str, Any] | None = None


class DemoRequest(BaseModel):
    seed: int = 20261122
    reset: bool = True


class RunRequest(BaseModel):
    stage: str = "all"
    force_full: bool = False
    verify: bool = True


class EditRequest(BaseModel):
    operation: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    expected_version: int
    shard_id: str
    edit_id: str | None = None


class RetryRequest(BaseModel):
    expected_version: int | None = None


class StopRequest(BaseModel):
    reason: str = "user_requested"


class ExportRequest(BaseModel):
    name: str = "result.json"


def create_app(workspace_root: str | Path | None = None) -> FastAPI:
    root = Path(workspace_root or "workspace").expanduser().resolve()
    service = CabinService(root)
    app = FastAPI(
        title="Cabin Voice Arbitration Workbench",
        version="0.1.0",
        description="Deterministic multi-zone VAD, TDOA arbitration and incremental workbench",
    )
    app.state.service = service
    app.state.workspace_root = root

    @app.exception_handler(CabinError)
    async def cabin_error_handler(_request: Any, exc: CabinError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Any, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": {
                    "code": "E_INVALID_ARGUMENT",
                    "message": "request validation failed",
                    "details": {"errors": jsonable_encoder(exc.errors())},
                },
            },
        )

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        html_path = Path(__file__).with_name("static") / "index.html"
        return HTMLResponse(html_path.read_text(encoding="utf-8"))

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "status": "healthy", "workspace": str(root)}

    @app.post("/api/import")
    def import_manifest(request: ImportRequest) -> dict[str, Any]:
        return service.import_manifest(
            request.manifest_path,
            reset=request.reset,
            config=request.config,
        )

    @app.post("/api/demo/init")
    def initialize_demo(request: DemoRequest) -> dict[str, Any]:
        return service.initialize_demo(seed=request.seed, reset=request.reset)

    @app.post("/api/run")
    def run_pipeline(request: RunRequest) -> dict[str, Any]:
        return service.run(
            stage=request.stage,
            force_full=request.force_full,
            verify=request.verify,
        )

    @app.get("/api/status")
    def status() -> dict[str, Any]:
        return service.status()

    @app.get("/api/versions")
    def versions() -> dict[str, Any]:
        return service.versions()

    @app.get("/api/query/{kind}")
    def query(
        kind: str,
        shard_id: str = Query(...),
        time_ms: int = Query(..., ge=0),
        budget_ms: int | None = Query(None, ge=0),
    ) -> dict[str, Any]:
        return service.query(
            kind,
            shard_id=shard_id,
            time_ms=time_ms,
            budget_ms=budget_ms,
        )

    @app.post("/api/edits/direct")
    def direct_edit(request: EditRequest) -> dict[str, Any]:
        return service.edit(
            request.operation,
            parameters=request.parameters,
            expected_version=request.expected_version,
            shard_id=request.shard_id,
            edit_id=request.edit_id,
        )

    @app.post("/api/edits/prepare")
    def prepare_edit(request: EditRequest) -> dict[str, Any]:
        return service.prepare_edit(
            request.operation,
            parameters=request.parameters,
            expected_version=request.expected_version,
            shard_id=request.shard_id,
            edit_id=request.edit_id,
        )

    @app.post("/api/edits/{edit_id}/publish")
    def publish_edit(edit_id: str) -> dict[str, Any]:
        return service.publish_edit(edit_id)

    @app.post("/api/edits/{edit_id}/retry")
    def retry_edit(edit_id: str, request: RetryRequest) -> dict[str, Any]:
        return service.retry_edit(edit_id, expected_version=request.expected_version)

    @app.post("/api/edits/{edit_id}/discard")
    def discard_edit(edit_id: str) -> dict[str, Any]:
        return service.discard_edit(edit_id)

    @app.get("/api/edits/pending")
    def pending_edits() -> dict[str, Any]:
        return service.pending_edits()

    @app.get("/api/audit")
    def audit() -> dict[str, Any]:
        return service.audit()

    @app.post("/api/stop")
    def stop(request: StopRequest | None = None) -> dict[str, Any]:
        return service.stop(request.reason if request else "user_requested")

    @app.post("/api/resume")
    def resume() -> dict[str, Any]:
        return service.resume()

    @app.get("/api/snapshots")
    def snapshots() -> dict[str, Any]:
        return service.snapshots()

    @app.get("/api/snapshots/verify")
    @app.post("/api/snapshots/verify")
    def verify_snapshots() -> dict[str, Any]:
        return service.verify_snapshots()

    @app.post("/api/export")
    def export(request: ExportRequest) -> dict[str, Any]:
        return service.export(request.name)

    @app.get("/api/timeline/{shard_id}")
    def timeline(shard_id: str) -> dict[str, Any]:
        return service.timeline(shard_id)

    @app.get("/api/vad/frames/{shard_id}/{zone_id}")
    def vad_frames(shard_id: str, zone_id: str) -> dict[str, Any]:
        return service.vad_frames(shard_id, zone_id)

    return app


app = create_app()
