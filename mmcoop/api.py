from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import __version__
from .errors import AppError, business_error
from .service import WorkspaceService


class BackgroundBuildRunner:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._result: dict[str, Any] | None = None
        self._error: dict[str, Any] | None = None
        self._run_id = 0

    def running(self) -> bool:
        thread = self._thread
        return bool(thread and thread.is_alive())

    def start(self, function: Callable[[], dict[str, Any]]) -> int:
        with self._lock:
            if self.running():
                raise business_error(
                    "E_BUILD_RUNNING",
                    "build",
                    "a background build is already running",
                    http_status=409,
                )
            self._run_id += 1
            run_id = self._run_id
            self._result = None
            self._error = None

            def target() -> None:
                try:
                    result = function()
                    with self._lock:
                        self._result = result
                except AppError as error:
                    with self._lock:
                        self._error = error.to_dict()
                except Exception as error:  # pragma: no cover - guarded API boundary
                    with self._lock:
                        self._error = {
                            "ok": False,
                            "error": {
                                "code": "E_INTERNAL",
                                "stage": "build",
                                "message": str(error),
                            },
                        }

            self._thread = threading.Thread(target=target, name=f"mmcoop-build-{run_id}", daemon=True)
            self._thread.start()
            return run_id

    def status(self) -> dict[str, Any]:
        with self._lock:
            if self.running():
                status = "running"
            elif self._error is not None:
                status = "failed"
            elif self._result is not None:
                status = self._result.get("status", "completed")
            else:
                status = "idle"
            return {
                "status": status,
                "run_id": self._run_id,
                "result": self._result,
                "error": self._error,
            }


def _build_payload_injections(payload: dict[str, Any] | None) -> Any:
    if not payload:
        return None
    if payload.get("injections") is not None:
        return payload["injections"]
    if payload.get("injection") is not None:
        return [payload["injection"]]
    return None


def create_app(
    config_path: str | Path | None = None,
    data_dir: str | Path | None = None,
    workspace_dir: str | Path | None = None,
) -> FastAPI:
    root = Path.cwd()
    config = Path(config_path or os.environ.get("MMCOOP_CONFIG", root / "config" / "default.yaml"))
    data = Path(data_dir or os.environ.get("MMCOOP_DATA", root / "data"))
    workspace = Path(workspace_dir or os.environ.get("MMCOOP_WORKSPACE", root / "workspace"))
    service = WorkspaceService(config, data, workspace)
    runner = BackgroundBuildRunner()
    service.ensure_directories()

    app = FastAPI(
        title="MMCoop deterministic multimodal workspace",
        version=__version__,
        description="Local deterministic multimodal cooperative reasoning data editor.",
    )
    app.state.service = service
    app.state.runner = runner

    @app.exception_handler(AppError)
    async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": {
                    "code": "E_INTERNAL",
                    "stage": "api",
                    "message": str(exc),
                },
            },
        )

    @app.get("/")
    def root_info() -> dict[str, Any]:
        return {
            "ok": True,
            "data": {
                "name": "mmcoop",
                "version": __version__,
                "config_path": str(service.config_path),
                "data_dir": str(service.data_dir),
                "workspace_dir": str(service.workspace_dir),
                "docs": "/docs",
            },
        }

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "data": {"status": "healthy"}}

    @app.get("/api/status")
    def status() -> dict[str, Any]:
        return {"ok": True, "data": service.status()}

    @app.get("/api/version")
    def version() -> dict[str, Any]:
        value = service.status()
        return {
            "ok": True,
            "data": {
                "status": value["status"],
                "config_version": value["config"]["config_version"],
                "data_fingerprint": value["data_fingerprint"],
                **value["version"],
            },
        }

    @app.get("/api/results")
    def results() -> dict[str, Any]:
        return {"ok": True, "data": service.results()}

    @app.get("/api/invalidations")
    def invalidations() -> dict[str, Any]:
        return {"ok": True, "data": service.invalidations()}

    @app.get("/api/checkpoints")
    def checkpoints() -> dict[str, Any]:
        return {"ok": True, "data": service.checkpoints()}

    @app.get("/api/checkpoints/integrity")
    def integrity() -> dict[str, Any]:
        return {"ok": True, "data": service.integrity()}

    @app.get("/api/build/status")
    def background_status() -> dict[str, Any]:
        return {"ok": True, "data": runner.status()}

    @app.post("/api/demo/reset")
    def demo_reset(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        seed = int(payload.get("seed", 20261123))
        fresh = bool(payload.get("fresh_workspace", False))
        return {"ok": True, "data": service.generate_demo(seed=seed, fresh_workspace=fresh)}

    @app.post("/api/data/import")
    def data_import(payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "data": service.import_payload(payload)}

    @app.patch("/api/data/texts/{text_id}")
    def edit_text(text_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "data": service.edit_text(text_id, payload)}

    @app.patch("/api/data/images/{image_id}")
    def edit_image(image_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "data": service.edit_image(image_id, payload)}

    @app.post("/api/build")
    def build(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        injections = _build_payload_injections(payload)
        verify = bool(payload.get("verify_incremental", True))
        through_stage = str(payload.get("through_stage", "aggregation"))
        background = bool(payload.get("background", False))
        if background:
            run_id = runner.start(
                lambda: service.build(
                    injections=injections,
                    verify_incremental=verify,
                    through_stage=through_stage,
                )
            )
            return {"ok": True, "data": {"status": "accepted", "run_id": run_id, "background": True}}
        return {
            "ok": True,
            "data": service.build(injections=injections, verify_incremental=verify, through_stage=through_stage),
        }

    @app.post("/api/stop")
    def stop() -> dict[str, Any]:
        return {"ok": True, "data": service.request_stop()}

    @app.post("/api/resume")
    def resume(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        verify = bool(payload.get("verify_incremental", True))
        through_stage = str(payload.get("through_stage", "aggregation"))
        background = bool(payload.get("background", False))
        if background:
            run_id = runner.start(lambda: service.resume(verify_incremental=verify, through_stage=through_stage))
            return {"ok": True, "data": {"status": "accepted", "run_id": run_id, "background": True}}
        return {"ok": True, "data": service.resume(verify_incremental=verify, through_stage=through_stage)}

    @app.post("/api/inject")
    def inject(payload: dict[str, Any]) -> dict[str, Any]:
        injection = {"kind": payload.get("kind"), "sample_id": payload.get("sample_id")}
        if injection["sample_id"] is None:
            injection.pop("sample_id")
        return {"ok": True, "data": service.build(injections=[injection], verify_incremental=True)}

    return app


app = create_app()
