"""Local HTTP interface (FastAPI).  No external services are involved."""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .errors import PipelineError
from .workbench import Workbench


def create_app(workdir: str) -> FastAPI:
    app = FastAPI(title="xfpipe", version="1.0.0")
    app.state.workbench = Workbench(workdir)

    @app.exception_handler(PipelineError)
    async def pipeline_error_handler(_: Request, exc: PipelineError) -> JSONResponse:
        return JSONResponse(status_code=422, content=exc.to_dict())

    def wb() -> Workbench:
        return app.state.workbench

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.post("/samples/import")
    def import_sample(body: dict) -> dict:
        path = wb().import_sample(body["path"])
        return {"sample_path": path, "sample_fingerprint": wb().pipeline.sample_fingerprint}

    @app.get("/config")
    def get_config() -> dict:
        return wb().config

    @app.put("/config")
    def put_config(body: dict) -> dict:
        wb().update_config(body)
        return {"config_version": wb().pipeline.config_version}

    @app.post("/pipeline/run")
    def run_pipeline(body: dict | None = None) -> dict:
        body = body or {}
        return wb().pipeline.run(stop_after_stage=body.get("stop_after_stage"))

    @app.post("/pipeline/recover")
    def recover_pipeline() -> dict:
        return wb().pipeline.recover()

    @app.post("/pipeline/stop")
    def request_stop() -> dict:
        wb().pipeline.request_stop()
        return {"stop_requested": True}

    @app.post("/pipeline/stop/clear")
    def clear_stop() -> dict:
        wb().pipeline.clear_stop()
        return {"stop_requested": False}

    @app.get("/pipeline/status")
    def pipeline_status() -> dict:
        return wb().pipeline.status()

    @app.post("/query/{kind}")
    def run_query(kind: str, body: dict | None = None) -> dict:
        return wb().query_engine.query(kind, **(body or {}))

    @app.get("/cache/stats")
    def cache_stats() -> dict:
        return wb().cache_stats()

    @app.get("/query/traces")
    def query_traces() -> dict:
        return {"traces": wb().latency_traces()}

    @app.get("/checkpoints")
    def checkpoints() -> dict:
        store = wb().pipeline.checkpoints
        return {
            "snapshot": store.classify_snapshot(),
            "records": store.records(),
        }

    @app.post("/verify")
    def verify() -> dict:
        return wb().pipeline.verify_against_full_rebuild()

    return app
