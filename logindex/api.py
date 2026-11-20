"""本地回环 HTTP API（FastAPI）。

所有业务异常统一序列化为 ``{"error": {code, message, detail}}``。
只读端点直接走引擎读方法，与写者通过 SQLite WAL 并发。
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

from .errors import HTTP_STATUS, LogIndexError
from .engine import LogIndexEngine
from .store import default_db_path


class SubmitRequest(BaseModel):
    batch_id: str = Field(..., min_length=1)
    lines: List[str]
    base_version: Optional[int] = None


class RetimestampRequest(BaseModel):
    batch_id: str = Field(..., min_length=1)
    line_no: int = Field(..., ge=1)
    new_timestamp_ms: int = Field(..., ge=0)


class InvalidateRequest(BaseModel):
    batch_id: str = Field(..., min_length=1)


class AbortRequest(BaseModel):
    batch_id: str = Field(..., min_length=1)


def create_app(db_path=None, *, recovery_policy: str = "requeue") -> FastAPI:
    import os

    resolved_db = Path(db_path) if db_path else Path(
        os.environ.get("LOGINDEX_DB", default_db_path("runtime"))
    )
    app = FastAPI(title="本地日志增量索引服务", version="1.0.0")
    app.state.engine = LogIndexEngine(
        resolved_db, recovery_policy=recovery_policy
    )

    @app.exception_handler(LogIndexError)
    async def handle_business_error(_request, exc: LogIndexError):
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=HTTP_STATUS.get(exc.code, 500),
            content={"error": exc.to_dict()},
        )

    @app.get("/health")
    def health():
        engine: LogIndexEngine = app.state.engine
        return {"status": "ok", "current_version": engine.current_version()}

    @app.post("/batches")
    def submit_batch(payload: SubmitRequest):
        engine: LogIndexEngine = app.state.engine
        result = engine.submit_batch(
            payload.batch_id, payload.lines, payload.base_version
        )
        return result.__dict__

    @app.post("/retimestamp")
    def retimestamp(payload: RetimestampRequest):
        engine: LogIndexEngine = app.state.engine
        return engine.retimestamp(
            payload.batch_id, payload.line_no, payload.new_timestamp_ms
        )

    @app.post("/batches/invalidate")
    def invalidate(payload: InvalidateRequest):
        engine: LogIndexEngine = app.state.engine
        return engine.invalidate_batch(payload.batch_id)

    @app.post("/batches/abort")
    def abort(payload: AbortRequest):
        engine: LogIndexEngine = app.state.engine
        return engine.mark_aborted(payload.batch_id)

    @app.get("/batches")
    def list_batches():
        return {"batches": app.state.engine.list_batches()}

    @app.get("/records/query")
    def query_records(
        service: Optional[str] = None,
        level: Optional[str] = None,
        start_ms: Optional[int] = Query(None),
        end_ms: Optional[int] = Query(None),
        version: Optional[int] = Query(None),
    ):
        rows = app.state.engine.query_records(
            service=service, level=level, start_ms=start_ms,
            end_ms=end_ms, as_of_version=version,
        )
        return {"hit_count": len(rows), "records": rows}

    @app.get("/index-state")
    def index_state(version: Optional[int] = Query(None)):
        return app.state.engine.index_state(as_of_version=version)

    @app.get("/version-events")
    def version_events():
        return {"events": app.state.engine.version_events()}

    @app.get("/recovery-report")
    def recovery_report():
        return app.state.engine.get_recovery_report()

    @app.post("/test/arm-crash")
    def arm_crash():
        app.state.engine.arm_crash_after_indexing()
        return {"armed": True,
                "note": "下一次批次提交将在 INDEXING 意图落盘后强制终止"}

    return app


def main() -> None:  # pragma: no cover - 进程入口
    import uvicorn

    uvicorn.run(create_app(), host="127.0.0.1", port=8765, log_level="info")


if __name__ == "__main__":  # pragma: no cover
    main()
