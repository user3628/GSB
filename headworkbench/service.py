from __future__ import annotations

import os
import threading
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .canonical import digest_object
from .config import WorkbenchConfig, current_config_path, load_config
from .data import install_dataset, load_dataset
from .errors import WorkbenchError, stale_version
from .pipeline import Pipeline
from .versioning import VersionStore


class ImportRequest(BaseModel):
    manifest_path: str
    shard_paths: list[str] = Field(default_factory=list)
    config_path: str | None = None


class RunRequest(BaseModel):
    window_name: str | None = None
    mode: str = "incremental"
    verify_consistency: bool = True


class CommitRequest(BaseModel):
    head_widths: dict[str, int]
    expected_base_version: int = Field(ge=0)
    window_name: str | None = None
    adjudication_digest: str | None = None
    note: str | None = None
    verify_full: bool = True


class RollbackRequest(BaseModel):
    target_version: int = Field(ge=1)
    expected_base_version: int = Field(ge=0)
    note: str | None = None


class QueryCache:
    def __init__(self, capacity: int = 32) -> None:
        self.capacity = capacity
        self._values: OrderedDict[str, Any] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            value = self._values.get(key)
            if value is not None:
                self._values.move_to_end(key)
            return value

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            self._values[key] = value
            self._values.move_to_end(key)
            while len(self._values) > self.capacity:
                self._values.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._values.clear()


class WorkbenchService:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.store = VersionStore(self.root)
        self.cache = QueryCache()
        self.store.ensure_layout()

    def config(self) -> WorkbenchConfig:
        return load_config(current_config_path(self.root))

    def pipeline(self, config: WorkbenchConfig | None = None) -> Pipeline:
        return Pipeline(self.root, config or self.config())

    def default_window(self, config: WorkbenchConfig) -> str:
        if "compact" in config.sampling_windows:
            return "compact"
        return sorted(config.sampling_windows)[0]

    def query_key(
        self,
        config: WorkbenchConfig,
        window_name: str,
        *,
        record_digest: str | None = None,
        mode: str = "incremental",
    ) -> dict[str, Any]:
        window = config.window_descriptor(window_name)
        material = {
            "window_name": window_name,
            "window_digest": window["window_digest"],
            "config_version": config.version,
            "config_digest": config.config_digest,
            "mode": mode,
            "record_digest": record_digest,
        }
        material["cache_key"] = digest_object(material)
        return material

    def cached_run(
        self,
        config: WorkbenchConfig,
        pipeline: Pipeline,
        window_name: str,
        *,
        mode: str = "incremental",
        record_digest: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any], bool]:
        key = self.query_key(config, window_name, record_digest=record_digest, mode=mode)
        cached = self.cache.get(key["cache_key"])
        if cached is not None:
            return cached, key, True
        result = pipeline.run(window_name, mode=mode)
        self.cache.put(key["cache_key"], result)
        return result, key, False

    @staticmethod
    def finish_query(
        payload: dict[str, Any],
        *,
        started: float,
        budget_ms: float,
        query_id: str,
        cache_key: dict[str, Any],
        cache_hit: bool,
    ) -> dict[str, Any]:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if elapsed_ms > budget_ms:
            raise WorkbenchError(
                "E_LATENCY_BUDGET",
                "query exceeded the configured interaction latency budget",
                status_code=408,
                details={
                    "query_id": query_id,
                    "elapsed_ms": elapsed_ms,
                    "budget_ms": budget_ms,
                    "cache_key": cache_key,
                },
            )
        response = dict(payload)
        response["query"] = {
            "query_id": query_id,
            "elapsed_ms": elapsed_ms,
            "budget_ms": budget_ms,
            "cache_key": cache_key,
            "cache_hit": cache_hit,
        }
        return response

    def _live_consistency(
        self,
        record: dict[str, Any],
        config: WorkbenchConfig,
        pipeline: Pipeline,
        window_name: str,
    ) -> tuple[dict[str, Any], dict[str, Any], bool, str]:
        result, key, cache_hit = self.cached_run(
            config,
            pipeline,
            window_name,
            record_digest=record["record_digest"],
        )
        if (
            record.get("config_digest") == config.config_digest
            and record.get("data_digest") == result["payload"]["data_digest"]
            and record.get("sampling_window_digest") == result["payload"]["window"]["window_digest"]
        ):
            if record.get("result_digest") != result["result_digest"]:
                raise stale_version(
                    "record adjudication summary does not match the current full pipeline result",
                    cause="record_adjudication_mismatch",
                    version=record["version"],
                    record_result_digest=record.get("result_digest"),
                    live_result_digest=result["result_digest"],
                )
            consistency = {
                "checked": True,
                "consistent": True,
                "record_result_digest": record["result_digest"],
                "live_result_digest": result["result_digest"],
                "full_run_verified_at_commit": bool(record.get("full_run_verified", False)),
            }
        else:
            consistency = {
                "checked": False,
                "consistent": None,
                "reason": "record config, data, or sampling window differs from the live pipeline",
                "record_result_digest": record.get("result_digest"),
                "live_result_digest": result["result_digest"],
            }
        return result, key, cache_hit, consistency

    def adjudication_query(self, window_name: str | None, query_id: str | None = None) -> dict[str, Any]:
        started = time.perf_counter()
        query_id = query_id or uuid.uuid4().hex
        config = self.config()
        window_name = window_name or self.default_window(config)
        pipeline = self.pipeline(config)
        record = self.store.current()
        if record is not None:
            record_window = record["sampling_window_name"]
            if window_name == record_window:
                result, key, cache_hit, consistency = self._live_consistency(
                    record, config, pipeline, window_name
                )
                payload = {
                    "schema": "headworkbench.adjudication-query/v1",
                    "source": "version_record",
                    "version": record["version"],
                    "version_status": record["status"],
                    "effective_head_widths": record["head_widths"],
                    "attempted_head_widths": record.get("attempted_head_widths", record["head_widths"]),
                    "window": record["sampling_window"],
                    "config_version": config.version,
                    "adjudication": record.get("adjudication_summary", result["payload"]["adjudication"]),
                    "consistency": consistency,
                }
                return self.finish_query(
                    payload,
                    started=started,
                    budget_ms=config.latency_budget_ms,
                    query_id=query_id,
                    cache_key=key,
                    cache_hit=cache_hit,
                )
        result, key, cache_hit = self.cached_run(config, pipeline, window_name)
        payload = {
            "schema": "headworkbench.adjudication-query/v1",
            "source": "live_pipeline",
            "version": record["version"] if record else None,
            "version_status": record["status"] if record else None,
            "effective_head_widths": record["head_widths"] if record else None,
            "window": result["payload"]["window"],
            "config_version": config.version,
            "adjudication": result["payload"]["adjudication"],
            "consistency": {
                "checked": False,
                "consistent": None,
                "reason": "requested window does not match the current version record",
            },
        }
        return self.finish_query(
            payload,
            started=started,
            budget_ms=config.latency_budget_ms,
            query_id=query_id,
            cache_key=key,
            cache_hit=cache_hit,
        )

    def width_query(self, window_name: str | None, query_id: str | None = None) -> dict[str, Any]:
        started = time.perf_counter()
        query_id = query_id or uuid.uuid4().hex
        config = self.config()
        pipeline = self.pipeline(config)
        record = self.store.current()
        if record is not None:
            window_name = window_name or record["sampling_window_name"]
            if window_name == record["sampling_window_name"]:
                result, key, cache_hit, consistency = self._live_consistency(
                    record, config, pipeline, window_name
                )
                payload = {
                    "schema": "headworkbench.width-query/v1",
                    "source": "version_record",
                    "version": record["version"],
                    "version_status": record["status"],
                    "head_widths": record["head_widths"],
                    "attempted_head_widths": record.get("attempted_head_widths", record["head_widths"]),
                    "effective_composition_analysis": record.get("effective_composition_analysis"),
                    "allocation": record.get("allocation_summary", result["payload"]["allocation"]),
                    "rollback": record.get("rollback"),
                    "consistency": consistency,
                }
                return self.finish_query(
                    payload,
                    started=started,
                    budget_ms=config.latency_budget_ms,
                    query_id=query_id,
                    cache_key=key,
                    cache_hit=cache_hit,
                )
        window_name = window_name or self.default_window(config)
        result, key, cache_hit = self.cached_run(config, pipeline, window_name)
        allocation = result["payload"]["allocation"]
        payload = {
            "schema": "headworkbench.width-query/v1",
            "source": "live_pipeline",
            "version": record["version"] if record else None,
            "version_status": record["status"] if record else None,
            "head_widths": allocation["suggested_head_widths"],
            "attempted_head_widths": None,
            "effective_composition_analysis": None,
            "allocation": allocation,
            "rollback": record.get("rollback") if record else None,
            "consistency": {
                "checked": False,
                "consistent": None,
                "reason": "requested window does not match the current version record",
            },
        }
        return self.finish_query(
            payload,
            started=started,
            budget_ms=config.latency_budget_ms,
            query_id=query_id,
            cache_key=key,
            cache_hit=cache_hit,
        )


def _resolve_workspace_path(root: Path, value: str) -> Path:
    path = (root / value).resolve() if not Path(value).is_absolute() else Path(value).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise WorkbenchError(
            "E_PATH",
            "all import paths must reside inside the workspace",
            details={"path": str(path), "workspace": str(root)},
        ) from exc
    return path


def create_app(root: Path | None = None) -> FastAPI:
    root = (root or Path(os.environ.get("HEADWORKBENCH_ROOT", Path.cwd()))).resolve()
    service = WorkbenchService(root)
    app = FastAPI(
        title="Attention-Head Truncation Workbench",
        version="1.0.0",
        description="Local deterministic per-head activation truncation adjudication and version rollback.",
    )
    app.state.service = service
    app.state.root = root
    app.state.stop_requested = False
    app.state.uvicorn_server = None

    @app.exception_handler(WorkbenchError)
    async def workbench_error_handler(_: Request, exc: WorkbenchError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.as_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "code": "E_REQUEST",
                    "message": "request validation failed",
                    "details": exc.errors(),
                }
            },
        )

    @app.get("/health")
    def health() -> dict[str, Any]:
        config = service.config()
        try:
            record = service.store.current()
        except WorkbenchError:
            record = None
        return {
            "status": "ok",
            "workspace": str(root),
            "python": os.sys.version.split()[0],
            "config_version": config.version,
            "config_digest": config.config_digest,
            "sampling_windows": config.sampling_windows,
            "candidate_bitwidths": list(config.candidate_bitwidths),
            "latency_budget_ms": config.latency_budget_ms,
            "current_version": record["version"] if record else None,
            "stop_requested": app.state.stop_requested,
        }

    @app.get("/api/config")
    def get_config() -> dict[str, Any]:
        config = service.config()
        dataset = load_dataset(root)
        return {
            "config_path": str(config.config_path.relative_to(root)),
            "config_version": config.version,
            "config_digest": config.config_digest,
            "data_digest": dataset.data_digest,
            "manifest_digest": dataset.manifest["manifest_digest"],
            "head_ids": [head.head_id for head in dataset.heads],
            "candidate_bitwidths": list(config.candidate_bitwidths),
            "sampling_windows": config.sampling_windows,
            "sample_cap_per_head": config.sample_cap_per_head,
            "min_samples_per_head": config.min_samples_per_head,
            "sensitivity_threshold": config.sensitivity_threshold,
            "global_error_budget": config.global_error_budget,
            "per_head_error_tolerance": config.per_head_error_tolerance,
            "latency_budget_ms": config.latency_budget_ms,
        }

    @app.post("/api/import")
    def import_data(request: ImportRequest) -> dict[str, Any]:
        manifest = _resolve_workspace_path(root, request.manifest_path)
        shards = [_resolve_workspace_path(root, item) for item in request.shard_paths]
        config = _resolve_workspace_path(root, request.config_path) if request.config_path else None
        result = install_dataset(root, manifest_path=manifest, shard_paths=shards, config_path=config)
        service.cache.clear()
        return result

    @app.post("/api/run")
    def run_pipeline(request: RunRequest) -> dict[str, Any]:
        config = service.config()
        window_name = request.window_name or service.default_window(config)
        pipeline = service.pipeline(config)
        result = pipeline.run(window_name, mode=request.mode)
        consistency = None
        if request.mode == "incremental" and request.verify_consistency:
            consistency = pipeline.verify_full_consistency(window_name)
        payload = result["payload"]
        return {
            "window_name": window_name,
            "mode": request.mode,
            "result_digest": result["result_digest"],
            "pipeline_fingerprint": payload["pipeline_fingerprint"],
            "stage_digests": {name: entry["digest"] for name, entry in payload["stages"].items()},
            "recomputed_stages": result["recomputed_stages"],
            "cache_hits": result["cache_hits"],
            "ranking": payload["adjudication"]["ranking"],
            "sampling_insufficient_head_ids": payload["adjudication"]["sampling_insufficient_head_ids"],
            "suggested_head_widths": payload["allocation"]["suggested_head_widths"],
            "allocation_total_error_estimate": payload["allocation"]["total_error_estimate"],
            "global_error_budget": payload["allocation"]["global_error_budget"],
            "budget_limited_head_ids": payload["allocation"]["budget_limited_head_ids"],
            "downgraded_head_ids": payload["allocation"]["downgraded_head_ids"],
            "consistency": consistency,
        }

    @app.post("/api/verify")
    def verify(request: RunRequest) -> dict[str, Any]:
        config = service.config()
        window_name = request.window_name or service.default_window(config)
        pipeline = service.pipeline(config)
        report = pipeline.verify_full_consistency(window_name)
        report["window_name"] = window_name
        return report

    @app.get("/api/adjudication")
    def adjudication(
        window: str | None = Query(default=None),
        query_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        return service.adjudication_query(window, query_id)

    @app.get("/api/widths")
    def widths(
        window: str | None = Query(default=None),
        query_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        return service.width_query(window, query_id)

    @app.get("/api/versions")
    def versions() -> dict[str, Any]:
        return service.store.history()

    @app.get("/api/versions/{version}")
    def version_detail(version: int) -> dict[str, Any]:
        return service.store.get_record(version)

    @app.get("/api/rollbacks")
    def rollbacks() -> dict[str, Any]:
        return service.store.rollback_records()

    @app.post("/api/versions/commit")
    def commit(request: CommitRequest) -> dict[str, Any]:
        config = service.config()
        pipeline = service.pipeline(config)
        record = service.store.current()
        window_name = request.window_name or (
            record["sampling_window_name"] if record else service.default_window(config)
        )
        result = service.store.commit(
            pipeline,
            request.head_widths,
            expected_base_version=request.expected_base_version,
            window_name=window_name,
            adjudication_digest=request.adjudication_digest,
            note=request.note,
            verify_full=request.verify_full,
        )
        service.cache.clear()
        return result

    @app.post("/api/versions/rollback")
    def rollback(request: RollbackRequest) -> dict[str, Any]:
        result = service.store.rollback_to(
            target_version=request.target_version,
            expected_base_version=request.expected_base_version,
            note=request.note,
        )
        service.cache.clear()
        return result

    @app.post("/api/recover")
    def recover() -> dict[str, Any]:
        result = service.store.recover()
        service.cache.clear()
        return result

    @app.post("/api/control/stop")
    def stop() -> dict[str, Any]:
        app.state.stop_requested = True
        server = app.state.uvicorn_server

        def request_exit() -> None:
            if server is not None:
                server.should_exit = True

        if server is not None:
            threading.Timer(0.15, request_exit).start()
        return {
            "status": "stopping",
            "graceful": True,
            "server_attached": server is not None,
            "message": "the local HTTP server will stop after this response",
        }

    @app.get("/api/diagnostics")
    def diagnostics() -> dict[str, Any]:
        config = service.config()
        dataset = load_dataset(root)
        layout = service.store.ensure_layout()
        return {
            "workspace": str(root),
            "layout": layout,
            "dataset": {
                "manifest_path": str(dataset.manifest_path.relative_to(root)),
                "data_digest": dataset.data_digest,
                "shard_count": len(dataset.manifest["shards"]),
                "head_count": len(dataset.heads),
                "heads": [
                    {
                        "head_id": head.head_id,
                        "layer_name": head.layer_name,
                        "head_index": head.head_index,
                        "shard_file": head.shard_file,
                        "activation_shape": list(head.activation.shape),
                        "query_proj_shape": list(head.query_proj.shape),
                        "key_proj_shape": list(head.key_proj.shape),
                    }
                    for head in dataset.heads
                ],
            },
            "config": {
                "path": str(config.config_path.relative_to(root)),
                "version": config.version,
                "digest": config.config_digest,
                "sampling_windows": config.sampling_windows,
                "candidate_bitwidths": list(config.candidate_bitwidths),
            },
        }

    return app

