from __future__ import annotations

import os
import signal
import threading
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import WorkbenchConfig
from .errors import WorkbenchError
from .workspace import Workspace


class ImportWorkspaceRequest(BaseModel):
    manifest_path: str


class EditRequest(BaseModel):
    base_version: int = Field(ge=1)
    operations: list[dict[str, Any]]


class ForkRequest(BaseModel):
    base_version: int = Field(ge=1)
    parent_version: int = Field(ge=1)
    operations: list[dict[str, Any]]


class ImportTextRequest(BaseModel):
    base_version: int = Field(ge=1)
    format: Literal["json", "srt", "vtt", "webvtt"]
    content: str
    chunk_id: str | None = None


class MergeRequest(BaseModel):
    base_version: int = Field(ge=1)
    left_version: int = Field(ge=1)
    right_version: int = Field(ge=1)
    strategy: Literal["last-wins", "mark-conflict"] = "last-wins"


class ExportRequest(BaseModel):
    version: int | None = Field(default=None, ge=1)
    format: Literal["json", "srt", "vtt", "webvtt"]
    chunk_id: str | None = None


class RoundTripRequest(BaseModel):
    version: int | None = Field(default=None, ge=1)
    chunk_id: str | None = None


def create_app(
    workspace_root: str | Path | None = None,
    *,
    config: WorkbenchConfig | None = None,
    lock_timeout_s: float = 10.0,
    exit_on_stop: bool = False,
) -> FastAPI:
    root = Path(
        workspace_root
        or os.environ.get("WORKBENCH_ROOT", "artifacts/workspace")
    ).resolve()
    workspace = Workspace(
        root,
        config=config or WorkbenchConfig(),
        lock_timeout_s=lock_timeout_s,
    )
    app = FastAPI(
        title="Local Transcript Workbench",
        version="1.0.0",
        description="Local deterministic word-level transcript editing service.",
    )
    app.state.workspace = workspace
    app.state.exit_on_stop = exit_on_stop

    @app.exception_handler(WorkbenchError)
    async def workbench_error_handler(
        request: Request, exc: WorkbenchError
    ) -> JSONResponse:
        del request
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        del request
        error = WorkbenchError(
            "E_INVALID_REQUEST",
            "HTTP request validation failed",
            {"errors": jsonable_encoder(exc.errors())},
        )
        return JSONResponse(status_code=error.http_status, content=error.to_dict())

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "local-transcript-workbench",
            "root": str(workspace.root),
            "snapshot_exists": workspace.snapshot_path.exists(),
        }

    @app.post("/api/workspaces/import")
    def import_workspace(request: ImportWorkspaceRequest) -> dict[str, Any]:
        return workspace.bootstrap(request.manifest_path)

    @app.get("/api/state")
    def state(version: int | None = None) -> dict[str, Any]:
        selected = workspace.load_state() if version is None else workspace.load_version(version)
        response = workspace._state_response(selected)
        response["chunks"] = [
            selected.chunks[chunk_id].to_dict()
            for chunk_id in sorted(
                selected.chunks,
                key=lambda cid: (selected.chunks[cid].order, cid),
            )
        ]
        response["words"] = [word.to_dict() for word in selected.sorted_words()]
        return response

    @app.get("/api/versions")
    def versions() -> dict[str, Any]:
        history = workspace.version_history()
        return {"ok": True, "versions": history, "latest_version": history[-1]["version"]}

    @app.get("/api/versions/{version}")
    def version_detail(version: int) -> dict[str, Any]:
        selected = workspace.load_version(version)
        response = workspace._state_response(selected)
        response["words"] = [word.to_dict() for word in selected.sorted_words()]
        response["migrations"] = selected.migrations
        response["merge_records"] = selected.merge_records
        response["changes"] = selected.metadata.get("changes", {})
        return response

    @app.get("/api/versions/{left_version}/diff/{right_version}")
    def version_diff(left_version: int, right_version: int) -> dict[str, Any]:
        return workspace.diff_versions(left_version, right_version)

    @app.post("/api/edits")
    def edit(request: EditRequest) -> dict[str, Any]:
        return workspace.edit(request.operations, expected_version=request.base_version)

    @app.post("/api/forks")
    def fork(request: ForkRequest) -> dict[str, Any]:
        return workspace.fork_edit(
            request.operations,
            parent_version=request.parent_version,
            expected_version=request.base_version,
        )

    @app.post("/api/imports")
    def import_text(request: ImportTextRequest) -> dict[str, Any]:
        return workspace.import_text(
            content=request.content,
            fmt=request.format,
            expected_version=request.base_version,
            chunk_id=request.chunk_id,
        )

    @app.post("/api/exports")
    def export(request: ExportRequest) -> dict[str, Any]:
        return workspace.export(
            fmt=request.format,
            version=request.version,
            chunk_id=request.chunk_id,
        )

    @app.post("/api/merge")
    def merge(request: MergeRequest) -> dict[str, Any]:
        return workspace.merge_versions(
            left_version=request.left_version,
            right_version=request.right_version,
            strategy=request.strategy,
            expected_version=request.base_version,
        )

    @app.post("/api/roundtrip")
    def roundtrip(request: RoundTripRequest) -> dict[str, Any]:
        return workspace.roundtrip_check(
            version=request.version,
            chunk_id=request.chunk_id,
        )

    @app.post("/api/stop")
    def stop() -> dict[str, Any]:
        response = workspace.request_stop()
        if exit_on_stop:
            def terminate() -> None:
                if os.name == "nt":
                    os.kill(os.getpid(), signal.SIGTERM)
                else:
                    os.kill(os.getpid(), signal.SIGTERM)

            timer = threading.Timer(0.25, terminate)
            timer.daemon = True
            timer.start()
        return response

    @app.post("/api/recover")
    def recover() -> dict[str, Any]:
        return workspace.recover()

    @app.get("/api/snapshot")
    def snapshot() -> dict[str, Any]:
        return workspace.snapshot_info()

    return app


app = create_app(exit_on_stop=os.environ.get("WORKBENCH_EXIT_ON_STOP") == "1")
