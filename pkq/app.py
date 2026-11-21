"""FastAPI 入口：提交点查、拉取分页、状态查询、快照加载、恢复报告。"""
from __future__ import annotations

import argparse
import os

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .columnar import SnapshotStore
from .config import Config
from .errors import PkqError
from .service import PkqService
from .store import Store


class SubmitReq(BaseModel):
    query_id: str
    keys: list[int]
    columns: list[str] | None = None
    page_size: int | None = None
    deadline_sec: float | None = None


class PullReq(BaseModel):
    handle: str
    page_seq: int


class LoadSnapshotReq(BaseModel):
    root: str


def create_app(config: Config | None = None, db_path: str = "data/pkq.db",
               snapshot_root: str | None = None) -> FastAPI:
    config = config or DEFAULT_CONFIG
    store = Store(db_path)
    snapshots = SnapshotStore()
    service = PkqService(config, store, snapshots)
    if snapshot_root and os.path.exists(os.path.join(snapshot_root, "manifest.json")):
        snapshots.load(snapshot_root)

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        service.start()
        yield
        service.shutdown()

    app = FastAPI(title="pkq-point-lookup", lifespan=lifespan)
    app.state.service = service
    app.state.snapshots = snapshots
    app.state.config = config

    @app.exception_handler(PkqError)
    async def pkq_error_handler(_: Request, exc: PkqError):
        return JSONResponse(status_code=exc.http_status, content=exc.to_payload())

    @app.post("/query")
    def submit(req: SubmitReq):
        return service.submit(req.query_id, req.keys, req.columns, req.page_size,
                              req.deadline_sec)

    @app.post("/pull")
    def pull(req: PullReq):
        return service.pull(req.handle, req.page_seq)

    @app.get("/sessions/{handle}")
    def session_status(handle: str):
        return service.session_status(handle)

    @app.get("/sessions/{handle}/trace")
    def session_trace(handle: str):
        return {"handle": handle, "trace": service.last_traces.get(handle, [])}

    @app.post("/snapshots/load")
    def load_snapshot(req: LoadSnapshotReq):
        snap = snapshots.load(req.root)
        return {"version": snap.version, "generated_at": snap.generated_at,
                "blocks": len(snap.blocks)}

    @app.get("/snapshots/current")
    def current_snapshot():
        snap = snapshots.current()
        if snap is None:
            return {"version": None}
        return {"version": snap.version, "generated_at": snap.generated_at,
                "blocks": len(snap.blocks),
                "columns": snap.columns}

    @app.get("/recovery-report")
    def recovery_report():
        return service.recovery_report or {"sessions": []}

    @app.get("/config")
    def get_config():
        return config.to_dict()

    return app


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/pkq.db")
    parser.add_argument("--snapshot", default=None)
    parser.add_argument("--port", type=int, default=18311)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--config-json", default=None,
                        help="覆盖配置的 JSON 字符串或文件路径")
    args = parser.parse_args()
    os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    os.makedirs(os.path.join(os.path.dirname(os.path.abspath(args.db)), "pages"),
                exist_ok=True)  # 分页结果缓冲临时目录
    config = Config()
    if args.config_json:
        import json as _json
        text = args.config_json
        if os.path.exists(text):
            text = open(text, encoding="utf-8").read()
        config = Config(**{**Config().to_dict(), **_json.loads(text)})
    app = create_app(config=config, db_path=args.db, snapshot_root=args.snapshot)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
