"""FastAPI application for the local interactive RoPE diagnostics UI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from .checkpoints import CheckpointStore
from .config import RopeConfig
from .errors import RopeError
from .session import DiagnosticSession


def create_app(workspace: str | Path) -> FastAPI:
    workspace_path = Path(workspace).resolve()
    app = FastAPI(
        title="RoPE Diagnostics",
        version="1.0.0",
        description="Local deterministic RoPE operator diagnostics",
    )
    app.state.workspace = workspace_path
    app.state.session = DiagnosticSession.load(workspace_path) if workspace_path.exists() else None

    def session() -> DiagnosticSession:
        current = app.state.session
        if current is None:
            raise RopeError(
                "E_NOT_FOUND",
                "尚未导入会话，请先初始化 manifest 与配置",
                {"workspace": str(workspace_path)},
            )
        return current

    @app.exception_handler(RopeError)
    async def rope_error_handler(_request: Request, exc: RopeError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        error = RopeError(
            "E_INVALID_INPUT",
            "HTTP 请求参数校验失败",
            {"errors": exc.errors()},
        )
        return JSONResponse(status_code=error.http_status, content=error.to_dict())

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        path = Path(__file__).with_name("static") / "index.html"
        return HTMLResponse(path.read_text(encoding="utf-8"))

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "data": {
                "service": "rope-diagnostics",
                "version": "1.0.0",
                "workspace": str(workspace_path),
                "session_loaded": app.state.session is not None,
            },
        }

    @app.post("/api/session/init")
    def init_session(body: dict[str, Any]) -> dict[str, Any]:
        manifest_path = _required_string(body, "manifest_path")
        config_path = _required_string(body, "config_path")
        reset = bool(body.get("reset", False))
        app.state.session = DiagnosticSession.create(
            workspace_path, manifest_path, config_path, reset=reset
        )
        return {"ok": True, "data": app.state.session.status_payload()}

    @app.get("/api/status")
    def status(
        shard_id: str | None = Query(default=None),
        stage: str | None = Query(default=None),
    ) -> dict[str, Any]:
        current = session()
        return {
            "ok": True,
            "data": current.status_payload(shard_id=shard_id, stage=stage),
        }

    @app.get("/api/shards")
    def shards() -> dict[str, Any]:
        return {"ok": True, "data": session().shards_payload()}

    @app.get("/api/fingerprints")
    def fingerprints() -> dict[str, Any]:
        return {"ok": True, "data": session().fingerprints()}

    @app.get("/api/checkpoints")
    def checkpoints(shard_id: str | None = Query(default=None)) -> dict[str, Any]:
        current = session()
        store = CheckpointStore(current.workspace)
        selected = [current.shard_by_id[shard_id]] if shard_id else current.shards
        records: dict[str, Any] = {}
        for shard in selected:
            progress = store.progress(
                shard.shard_id, current.config.version, shard.content_fingerprint
            )
            records[shard.shard_id] = [record.to_dict() for record in progress]
        return {
            "ok": True,
            "data": {
                "config_version": current.config.version,
                "records": records,
            },
        }

    @app.post("/api/config")
    def update_config(body: dict[str, Any]) -> dict[str, Any]:
        current = session()
        patch = body.get("patch", body)
        if not isinstance(patch, dict):
            raise RopeError("E_INVALID_CONFIG", "patch 必须是配置对象")
        should_run = bool(body.get("run", True))
        verify = bool(body.get("verify", True))
        stop_after = body.get("stop_after_stage")
        data = current.update_config(
            patch,
            run=should_run,
            verify=verify,
            stop_after_stage=stop_after,
        )
        return {"ok": True, "data": data}

    @app.post("/api/config/validate")
    def validate_config(body: dict[str, Any]) -> dict[str, Any]:
        config = RopeConfig.from_mapping(dict(body))
        return {
            "ok": True,
            "data": {"valid": True, "config": config.to_mapping(), "version": config.version},
        }

    @app.post("/api/run")
    def run(body: dict[str, Any] | None = None) -> dict[str, Any]:
        current = session()
        payload = body or {}
        data = current.run(stop_after_stage=payload.get("stop_after_stage"))
        return {"ok": True, "data": data}

    @app.post("/api/stop")
    def stop(body: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = body or {}
        data = session().request_stop(reason=str(payload.get("reason", "http_request")))
        return {"ok": True, "data": data}

    @app.post("/api/resume")
    def resume() -> dict[str, Any]:
        return {"ok": True, "data": session().resume()}

    @app.post("/api/verify")
    def verify() -> dict[str, Any]:
        return {"ok": True, "data": session().verify()}

    @app.post("/api/query")
    def query(body: dict[str, Any]) -> dict[str, Any]:
        current = session()
        shard_id = _required_string(body, "shard_id")
        query_type = str(body.get("query_type", "angles"))
        options = body.get("options", {})
        if not isinstance(options, dict):
            raise RopeError("E_INVALID_INPUT", "query options 必须是对象")
        data = current.query(
            shard_id=shard_id,
            query_type=query_type,
            options=options,
            latency_budget_ms=body.get("latency_budget_ms"),
            bypass_cache=bool(body.get("bypass_cache", False)),
        )
        return {"ok": True, "data": data}

    @app.get("/api/schedule")
    def schedule() -> dict[str, Any]:
        current = session()
        return {
            "ok": True,
            "data": {
                "memory_budget_bytes": current.config.memory_budget_bytes,
                "row_chunk_size": current.config.row_chunk_size,
                "recompute_report": current.recompute_report,
            },
        }

    @app.post("/api/export")
    def export(body: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = body or {}
        output = payload.get("output_path")
        if output is not None and not isinstance(output, str):
            raise RopeError("E_INVALID_INPUT", "output_path 必须是字符串")
        data = session().export(output)
        data["download_url"] = f"/api/export/download/{data['filename']}"
        return {"ok": True, "data": data}

    @app.get("/api/export/download/{filename}")
    def export_download(filename: str) -> FileResponse:
        if Path(filename).name != filename or not filename.endswith(".zip"):
            raise RopeError("E_INVALID_INPUT", "非法导出文件名", {"filename": filename})
        path = workspace_path / "exports" / filename
        if not path.exists():
            raise RopeError("E_NOT_FOUND", "导出文件不存在", {"filename": filename})
        return FileResponse(path, media_type="application/zip", filename=filename)

    return app


def _required_string(body: dict[str, Any], key: str) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value:
        raise RopeError("E_INVALID_INPUT", f"缺少必填字符串字段 {key}", {"field": key})
    return value
