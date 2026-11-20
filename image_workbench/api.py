from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import __version__
from .errors import (
    E_BUSY,
    E_FORMAT_LOSS,
    E_LEGACY,
    E_NOT_FOUND,
    E_NUMERIC,
    E_SNAPSHOT_DIGEST,
    E_STALE_SNAPSHOT,
    E_VALIDATION,
    WorkbenchError,
)
from .service import WorkbenchService

STATUS_BY_CODE = {
    E_FORMAT_LOSS: 422,
    E_NUMERIC: 422,
    E_LEGACY: 422,
    E_VALIDATION: 400,
    E_NOT_FOUND: 404,
    E_STALE_SNAPSHOT: 409,
    E_SNAPSHOT_DIGEST: 409,
    E_BUSY: 409,
}


class ImportRequest(BaseModel):
    manifest_path: str
    legacy: bool = False
    legacy_config: dict[str, Any] | None = None


class RunRequest(BaseModel):
    force: bool = False


class ConvertRequest(BaseModel):
    fragment_id: str
    target_format: str
    output_dir: str | None = None


class ImportExchangeRequest(BaseModel):
    image_path: str
    descriptor_path: str | None = None
    image_format: str | None = None
    replace_existing: bool = True


class ExportProjectRequest(BaseModel):
    path: str
    artifact: str = "style"


class ImportInternalProjectRequest(BaseModel):
    path: str


class FormatConfigRequest(BaseModel):
    config: dict[str, Any]


class FragmentUpdateRequest(BaseModel):
    changes: dict[str, Any] = Field(default_factory=dict)


class SnapshotCandidateRequest(BaseModel):
    candidate: dict[str, Any]


def _resolve_service_path(service: WorkbenchService, value: str) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (service.root / path).resolve()


def create_app(root: str | Path | None = None, runtime_dir: str | Path | None = None) -> FastAPI:
    service_root = Path(root or os.environ.get("IWB_ROOT", os.getcwd())).resolve()
    runtime_value = runtime_dir or os.environ.get("IWB_RUNTIME")
    service = WorkbenchService(service_root, runtime_dir=runtime_value)
    app = FastAPI(title="Deterministic Image Workbench", version=__version__)
    app.state.service = service
    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.exception_handler(WorkbenchError)
    async def workbench_error_handler(request: Request, exc: WorkbenchError) -> JSONResponse:
        return JSONResponse(
            status_code=STATUS_BY_CODE.get(exc.code, 500),
            content={"ok": False, "error": exc.to_dict()},
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": {
                    "code": "E_INTERNAL",
                    "message": str(exc),
                    "fragment_id": None,
                    "pixel": None,
                    "details": {"type": type(exc).__name__},
                },
            },
        )

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "version": __version__, "root": str(service.root), "python": sys.version.split()[0]}

    @app.get("/api/project")
    def project() -> dict[str, Any]:
        return {"ok": True, "project": service.project_summary()}

    @app.post("/api/project/import")
    def import_project(request: ImportRequest) -> dict[str, Any]:
        result = service.import_manifest(
            _resolve_service_path(service, request.manifest_path),
            legacy=request.legacy,
            legacy_config=request.legacy_config,
        )
        return {"ok": True, "project": result}

    @app.post("/api/project/bootstrap-sample")
    def bootstrap_sample(run: bool = False) -> dict[str, Any]:
        manifest = service.root / "data" / "manifest.json"
        if not manifest.exists():
            from .demo_data import generate_demo_data

            generate_demo_data(service.root / "data")
        project = service.import_manifest(manifest)
        response: dict[str, Any] = {"ok": True, "project": project}
        if run:
            response["run"] = service.run(force=False)
        return response

    @app.post("/api/run")
    def run_pipeline_endpoint(request: RunRequest) -> dict[str, Any]:
        return {"ok": True, "run": service.run(force=request.force)}

    @app.get("/api/status")
    def status() -> dict[str, Any]:
        return {"ok": True, "status": service.status()}

    @app.get("/api/fragments/{fragment_id}")
    def fragment_status(fragment_id: str) -> dict[str, Any]:
        return {"ok": True, "fragment": service.fragment_status(fragment_id)}

    @app.get("/api/fragments/{fragment_id}/fingerprints")
    def fingerprints(fragment_id: str) -> dict[str, Any]:
        fragment = service.fragment_status(fragment_id)
        return {
            "ok": True,
            "fragment_id": fragment_id,
            "fingerprints": fragment.get("fingerprints_preview", {}),
            "cached": fragment.get("cached"),
            "earliest_invalidated_stage": fragment.get("preview_earliest_invalidated_stage"),
            "reason": fragment.get("preview_reason"),
        }

    @app.patch("/api/fragments/{fragment_id}")
    def update_fragment(fragment_id: str, request: FragmentUpdateRequest) -> dict[str, Any]:
        return {"ok": True, "fragment": service.update_fragment(fragment_id, request.changes)}

    @app.post("/api/control/stop")
    def stop() -> dict[str, Any]:
        return {"ok": True, "control": service.request_stop()}

    @app.post("/api/control/resume")
    def resume() -> dict[str, Any]:
        return {"ok": True, "control": service.resume()}

    @app.post("/api/convert")
    def convert(request: ConvertRequest) -> dict[str, Any]:
        output_dir = _resolve_service_path(service, request.output_dir) if request.output_dir else None
        return {"ok": True, "conversion": service.convert(request.fragment_id, request.target_format, output_dir)}

    @app.post("/api/import-exchange")
    def import_exchange(request: ImportExchangeRequest) -> dict[str, Any]:
        return {
            "ok": True,
            "fragment": service.import_exchange(
                _resolve_service_path(service, request.image_path),
                descriptor_path=_resolve_service_path(service, request.descriptor_path) if request.descriptor_path else None,
                image_format=request.image_format,
                replace_existing=request.replace_existing,
            ),
        }

    @app.post("/api/project/export")
    def export_project(request: ExportProjectRequest) -> dict[str, Any]:
        return {
            "ok": True,
            "export": service.export_project(_resolve_service_path(service, request.path), artifact=request.artifact),
        }

    @app.post("/api/project/import-internal")
    def import_internal_project(request: ImportInternalProjectRequest) -> dict[str, Any]:
        return {
            "ok": True,
            "project": service.load_project_file(_resolve_service_path(service, request.path)),
        }

    @app.put("/api/config/format")
    def update_format_config(request: FormatConfigRequest) -> dict[str, Any]:
        return {"ok": True, "format_config": service.set_format_config(request.config)}

    @app.get("/api/snapshots")
    def snapshot_status() -> dict[str, Any]:
        return {"ok": True, "snapshot": service.snapshot_status()}

    @app.post("/api/snapshots/inspect")
    def inspect_snapshot(request: SnapshotCandidateRequest) -> dict[str, Any]:
        return {"ok": True, "inspection": service.inspect_snapshot(request.candidate)}

    @app.post("/api/snapshots/publish")
    def publish_snapshot(request: SnapshotCandidateRequest) -> dict[str, Any]:
        return {"ok": True, "publication": service.publish_snapshot(request.candidate)}

    @app.get("/api/exports/{filename}")
    def exported_file(filename: str) -> FileResponse:
        if Path(filename).name != filename:
            raise WorkbenchError(E_VALIDATION, "Invalid export filename")
        path = service.runtime_dir / "exports" / filename
        if not path.exists():
            raise WorkbenchError(E_NOT_FOUND, "Export file not found", details={"path": str(path)})
        return FileResponse(path)

    return app


app = create_app()

