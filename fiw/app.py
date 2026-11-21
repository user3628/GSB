"""Local HTTP interface for the fault injection workbench."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .engine import Engine
from .errors import BizError


class LoadRequest(BaseModel):
    endpoints_path: str
    plan_path: str


class TransitionRequest(BaseModel):
    target: str


def create_app(db_path):
    app = FastAPI(title="Fault Injection Workbench")
    engine = Engine(db_path)
    app.state.engine = engine
    app.state.startup_recovery = engine.recover()

    @app.exception_handler(BizError)
    async def biz_error_handler(_: Request, exc: BizError):
        status = 404 if exc.code == "E_NOT_FOUND" else 400
        return JSONResponse(status_code=status, content={"error": exc.to_dict()})

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/plans/load")
    def load_plan(req: LoadRequest):
        return engine.load_plan(req.endpoints_path, req.plan_path)

    @app.get("/plans")
    def list_plans():
        return {"plans": engine.store.list_plans()}

    @app.get("/plans/{plan_id}")
    def get_plan(plan_id: str):
        return engine.get_plan(plan_id)

    @app.post("/plans/{plan_id}/transitions")
    def transition(plan_id: str, req: TransitionRequest):
        return engine.transition(plan_id, req.target)

    @app.post("/plans/{plan_id}/rounds/{round_no}/run")
    def run_round(plan_id: str, round_no: int):
        return engine.run_round(plan_id, round_no)

    @app.get("/plans/{plan_id}/rounds/{round_no}")
    def get_round(plan_id: str, round_no: int):
        return engine.get_round(plan_id, round_no)

    @app.post("/recover")
    def recover():
        return engine.recover()

    @app.get("/recovery/latest")
    def latest_recovery():
        report = engine.store.latest_recovery_report()
        return {"report": report}

    @app.get("/recovery/startup")
    def startup_recovery():
        return {"report": app.state.startup_recovery}

    return app


def main():
    import os
    import uvicorn

    db_path = os.environ.get("FIW_DB", "data/fiw.db")
    port = int(os.environ.get("FIW_PORT", "18080"))
    uvicorn.run(create_app(db_path), host="127.0.0.1", port=port,
                log_level="warning")


if __name__ == "__main__":
    main()
