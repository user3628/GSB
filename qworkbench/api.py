from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from . import __version__
from .config import WorkbenchConfig
from .errors import BusinessError
from .pipeline import Pipeline
from .samples import (
    SampleDataset,
    dataset_from_directory,
    load_imported_spec,
    write_imported_dataset,
)


class ImportRequest(BaseModel):
    samples: list[dict[str, Any]] = Field(default_factory=list)
    annotations: dict[str, float] | list[dict[str, Any]] | None = None


class RunRequest(BaseModel):
    wait: bool = False
    start_stage: str | None = None


class ResumeRequest(BaseModel):
    wait: bool = False


class StopRequest(BaseModel):
    reason: str = "http_operator_request"


@dataclass
class Settings:
    config_path: Path
    data_dir: Path | None
    annotation_file: Path | None
    workdir: Path
    default_data_dir: Path | None = None


class ServiceContext:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.workdir = settings.workdir.resolve()
        self.config = WorkbenchConfig.load(settings.config_path)
        self.lock = threading.RLock()
        self.job: dict[str, Any] = {
            "job_id": None,
            "status": "IDLE",
            "action": None,
            "result": None,
            "error": None,
        }
        self._job_thread: threading.Thread | None = None
        self._job_counter = 0

    def load_dataset(self) -> SampleDataset:
        imported = load_imported_spec(self.workdir)
        if imported is not None:
            files, annotation = imported
            return SampleDataset.load(files, annotation)
        if self.settings.data_dir is not None:
            annotation = self.settings.annotation_file
            if annotation is None:
                candidate = self.settings.data_dir / "annotations.jsonl"
                annotation = candidate if candidate.exists() else None
            return SampleDataset.load(
                sorted(
                    path
                    for path in self.settings.data_dir.glob("*.jsonl")
                    if path.name != "annotations.jsonl"
                ),
                annotation,
            )
        if self.settings.default_data_dir is not None:
            return dataset_from_directory(self.settings.default_data_dir)
        raise BusinessError(
            "E_INVALID_INPUT",
            "no dataset configured; import samples or provide --data-dir",
            {"workdir": str(self.workdir)},
        )

    def pipeline(self, **kwargs: Any) -> Pipeline:
        return Pipeline(self.config, self.load_dataset(), self.workdir, **kwargs)

    def job_snapshot(self) -> dict[str, Any]:
        with self.lock:
            return dict(self.job)

    def start_job(self, action: str, *, start_stage: str | None = None) -> dict[str, Any]:
        with self.lock:
            if self._job_thread is not None and self._job_thread.is_alive():
                raise BusinessError("E_BUSY", "a pipeline job is already running", {"job": dict(self.job)})
            self._job_counter += 1
            job_id = f"job-{self._job_counter:06d}"
            self.job = {
                "job_id": job_id,
                "status": "RUNNING",
                "action": action,
                "start_stage": start_stage,
                "result": None,
                "error": None,
            }
            self._job_thread = threading.Thread(
                target=self._run_job,
                args=(action, start_stage),
                name=job_id,
                daemon=True,
            )
            self._job_thread.start()
            return dict(self.job)

    def _run_job(self, action: str, start_stage: str | None) -> None:
        try:
            pipeline = self.pipeline()
            if action == "resume":
                result = pipeline.resume()
            else:
                result = pipeline.run(start_stage=start_stage or "normalize")
            with self.lock:
                self.job["status"] = "FINISHED"
                self.job["result"] = result
        except BusinessError as exc:
            with self.lock:
                self.job["status"] = "FAILED"
                self.job["error"] = exc.as_dict()
        except Exception as exc:  # pragma: no cover - defensive job boundary
            with self.lock:
                self.job["status"] = "FAILED"
                self.job["error"] = {
                    "ok": False,
                    "code": "E_INTERNAL",
                    "message": str(exc),
                    "details": {},
                }


def _error_status(code: str) -> int:
    return {
        "E_CONFIG": 422,
        "E_NUMERIC": 422,
        "E_INVALID_INPUT": 422,
        "E_INVALID_REQUEST": 422,
        "E_INVALID_STAGE": 409,
        "E_STALE_CHECKPOINT": 409,
        "E_BUSY": 409,
        "E_LOCK_TIMEOUT": 503,
    }.get(code, 500)


def create_app(settings: Settings) -> FastAPI:
    app = FastAPI(
        title="Instruction Quality Workbench",
        version=__version__,
        description="Deterministic local instruction-data quality, diagnostics, and mixture workbench.",
    )
    context = ServiceContext(settings)
    app.state.context = context

    @app.exception_handler(BusinessError)
    async def business_error_handler(request: Request, exc: BusinessError) -> JSONResponse:
        return JSONResponse(status_code=_error_status(exc.code), content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "code": "E_INVALID_REQUEST",
                "message": "request validation failed",
                "details": {"errors": exc.errors()},
            },
        )

    @app.get("/")
    def root() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "instruction-quality-workbench",
            "version": __version__,
            "config_version": context.config.version,
            "workdir": str(context.workdir),
            "docs": "/docs",
        }

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "status": "healthy", "version": __version__}

    @app.post("/api/v1/import")
    def import_samples(payload: ImportRequest) -> dict[str, Any]:
        if not payload.samples:
            raise BusinessError("E_INVALID_INPUT", "at least one sample is required")
        result = write_imported_dataset(context.workdir, payload.samples, payload.annotations)
        return {"ok": True, "import": result}

    @app.post("/api/v1/run")
    def run_pipeline(payload: RunRequest) -> dict[str, Any]:
        if payload.wait:
            pipeline = context.pipeline()
            return {"ok": True, "job": {"status": "FINISHED", "result": pipeline.run(start_stage=payload.start_stage or "normalize")}}
        job = context.start_job("run", start_stage=payload.start_stage)
        return {"ok": True, "job": job}

    @app.post("/api/v1/control/stop")
    def stop(payload: StopRequest) -> dict[str, Any]:
        result = context.pipeline().request_stop(payload.reason)
        return {"ok": True, "stop": result}

    @app.post("/api/v1/control/resume")
    def resume(payload: ResumeRequest) -> dict[str, Any]:
        if payload.wait:
            result = context.pipeline().resume()
            return {"ok": True, "job": {"status": "FINISHED", "result": result}}
        job = context.start_job("resume")
        return {"ok": True, "job": job}

    @app.get("/api/v1/jobs/latest")
    def latest_job() -> dict[str, Any]:
        return {"ok": True, "job": context.job_snapshot()}

    @app.get("/api/v1/status")
    def status() -> dict[str, Any]:
        payload = context.pipeline().status()
        payload["job"] = context.job_snapshot()
        return payload

    @app.get("/api/v1/fingerprints")
    def fingerprints() -> dict[str, Any]:
        payload = context.pipeline().status()
        return {
            "ok": True,
            "config_version": payload["config_version"],
            "config_fingerprint": payload["config_fingerprint"],
            "algorithm_version": payload["algorithm_version"],
            "dataset_fingerprint": payload["dataset"]["sample_fingerprint"],
            "annotation_fingerprint": payload["dataset"]["annotation_fingerprint"],
            "stage_fingerprints": {
                stage: {
                    "valid": details["valid"],
                    "checkpoint_fingerprint": details["checkpoint_fingerprint"],
                    "expected_fingerprint": details["expected_fingerprint"],
                    "input_fingerprint": details["input_fingerprint"],
                    "config_fingerprint": details["config_fingerprint"],
                    "seq": details["seq"],
                }
                for stage, details in payload["stages"].items()
            },
            "earliest_invalid_stage": payload["earliest_invalid_stage"],
            "last_invalidation": payload["last_invalidation"],
        }

    @app.get("/api/v1/diagnostics/numeric")
    def numeric_diagnostics() -> dict[str, Any]:
        return {"ok": True, "diagnostics": context.pipeline().diagnostics()}

    @app.get("/api/v1/allocation")
    def allocation_details() -> dict[str, Any]:
        return {"ok": True, "allocation": context.pipeline().allocation()}

    @app.get("/api/v1/consistency")
    def consistency_details() -> dict[str, Any]:
        return {"ok": True, "consistency": context.pipeline().consistency()}

    @app.get("/api/v1/manifest")
    def manifest() -> dict[str, Any]:
        return {"ok": True, "manifest": context.pipeline().final_manifest()}

    @app.get("/api/v1/export")
    def export() -> FileResponse:
        path = context.pipeline().export_path()
        return FileResponse(path, media_type="application/x-ndjson", filename="mixture.jsonl")

    return app
