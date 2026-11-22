"""Local HTTP workbench (loopback only, no external exposure)."""

import threading
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .errors import WorkbenchError
from .pipeline import Pipeline


class ShardPayload(BaseModel):
    records: list[dict]


class ImportPayload(BaseModel):
    shard_id: str
    records: list[dict]


class ConfigChange(BaseModel):
    key: str
    value: object


def create_app(workspace):
    app = FastAPI(title="Query Understanding Workbench")

    def pipe():
        return Pipeline(workspace)

    @app.exception_handler(WorkbenchError)
    async def workbench_error_handler(_: Request, exc: WorkbenchError):
        return JSONResponse(status_code=409, content={"error": exc.to_dict()})

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/status")
    def status():
        return pipe().status()

    @app.post("/run")
    def run():
        return pipe().run()

    @app.post("/run/full")
    def run_full():
        return pipe().run_full()

    @app.post("/shards/import")
    def import_shard(payload: ImportPayload):
        p = pipe()
        p.write_shard(payload.shard_id, payload.records)
        return p.run()

    @app.post("/shards/{shard_id}/replace")
    def replace_shard(shard_id: str, payload: ShardPayload):
        return pipe().replace_shard(shard_id, payload.records)

    @app.post("/config/change")
    def config_change(payload: ConfigChange):
        return pipe().change_config(payload.key, payload.value)

    @app.get("/queries/{query_id}/slots")
    def slot_diagnostics(query_id: str):
        result = pipe().slot_diagnostics(query_id)
        if result is None:
            return JSONResponse(status_code=404, content={"error": {"code": "E_NOT_FOUND", "query_id": query_id}})
        return result

    @app.get("/invalidations")
    def invalidations():
        return {"events": pipe().invalidations()}

    @app.post("/verify")
    def verify():
        return pipe().verify_against_full()

    @app.post("/recover")
    def recover():
        return {"report": pipe().recover(), "status": pipe().status()}

    @app.post("/shutdown")
    def shutdown():
        server = getattr(app.state, "server", None)

        def _stop():
            time.sleep(0.2)
            if server is not None:
                server.should_exit = True

        threading.Thread(target=_stop, daemon=True).start()
        return {"stopping": True}

    return app


def serve(workspace, host="127.0.0.1", port=8737):
    import uvicorn

    app = create_app(workspace)
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    app.state.server = server
    server.run()
