"""Local FastAPI workbench.

Every read endpoint loads persisted state from the workspace; nothing is
served from process memory.  Business errors are rendered with their stable
codes and locating fields.
"""
from __future__ import annotations

import json
import os
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel

from . import engine
from .config import parse_config
from .errors import BenchError
from .generator import DEFAULT_MODEL_CONFIG
from .model import ModelConfig
from .state import load_state

app = FastAPI(title="LoRA Parallel Scheduling Workbench", version="1.0.0")


class ImportRequest(BaseModel):
    workspace: str
    config_path: str


class WorkspaceRequest(BaseModel):
    workspace: str


class ResumeRequest(BaseModel):
    workspace: str
    task_id: str | None = None


def _model_config() -> ModelConfig:
    return DEFAULT_MODEL_CONFIG


def _error_response(exc: BenchError) -> JSONResponse:
    status = {
        "E_INVALID_TASK": 400,
        "E_BUDGET_EXCEEDED": 409,
        "E_STALE_CHECKPOINT": 409,
        "E_WEIGHT_MISMATCH": 409,
        "E_INVALID_PHASE": 409,
        "E_CORRUPT_CHECKPOINT": 409,
        "E_NOT_FOUND": 404,
    }.get(exc.code, 500)
    return JSONResponse(status_code=status, content={"error": exc.to_dict()})


@app.exception_handler(BenchError)
async def bench_error_handler(_request, exc: BenchError):
    return _error_response(exc)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/import")
def import_work(payload: ImportRequest) -> dict[str, Any]:
    state = engine.import_work(
        payload.workspace, payload.config_path, _model_config(), reimport=True
    )
    return state.to_dict()


@app.post("/run")
def run(payload: WorkspaceRequest) -> dict[str, Any]:
    try:
        return engine.run(payload.workspace)
    except BenchError as exc:
        return JSONResponse(
            status_code=409 if exc.code != "E_NOT_FOUND" else 404,
            content={"error": exc.to_dict()},
        )


@app.post("/stop")
def stop(payload: WorkspaceRequest) -> dict[str, str]:
    try:
        engine.stop(payload.workspace)
    except BenchError as exc:
        return JSONResponse(status_code=409, content={"error": exc.to_dict()})
    return {"status": "stop_requested"}


@app.post("/resume")
def resume(payload: ResumeRequest) -> dict[str, Any]:
    try:
        return engine.resume(payload.workspace, task_id=payload.task_id)
    except BenchError as exc:
        status = 404 if exc.code == "E_NOT_FOUND" else 409
        return JSONResponse(status_code=status, content={"error": exc.to_dict()})


@app.get("/state")
def state(workspace: str) -> dict[str, Any]:
    current = load_state(workspace)
    if current is None:
        return JSONResponse(
            status_code=404,
            content={"error": {"code": "E_NOT_FOUND", "message": "no state", "task_id": None}},
        )
    return current.to_dict()


@app.get("/tasks/{task_id}")
def task_detail(task_id: str, workspace: str) -> dict[str, Any]:
    current = load_state(workspace)
    if current is None or task_id not in current.tasks:
        return JSONResponse(
            status_code=404,
            content={
                "error": {
                    "code": "E_NOT_FOUND",
                    "message": f"task {task_id} unknown",
                    "task_id": task_id,
                }
            },
        )
    return current.tasks[task_id].to_dict()


@app.get("/budget")
def budget(workspace: str) -> dict[str, Any]:
    current = load_state(workspace)
    if current is None:
        return JSONResponse(
            status_code=404, content={"error": {"code": "E_NOT_FOUND", "message": "no state"}}
        )
    ledger_path = os.path.join(workspace, "admission.json")
    ledger = {}
    if os.path.exists(ledger_path):
        with open(ledger_path, "r", encoding="utf-8") as handle:
            ledger = json.load(handle)
    return {
        "budget": current.budget,
        "global_cap": current.global_cap,
        "global_consumed": current.global_consumed,
        "admission": ledger,
        "tasks": [
            {
                "task_id": tid,
                "estimated_samples": current.tasks[tid].estimated_samples,
                "consumed_samples": current.tasks[tid].consumed_samples,
                "status": current.tasks[tid].status,
                "rejection_reason": current.tasks[tid].rejection_reason,
            }
            for tid in current.task_order
        ],
    }


@app.get("/summary")
def summary(workspace: str) -> dict[str, Any]:
    try:
        return engine.build_summary(workspace)
    except BenchError as exc:
        return JSONResponse(status_code=404, content={"error": exc.to_dict()})


@app.get("/export/loss")
def export_loss(workspace: str) -> Any:
    try:
        summary = engine.build_summary(workspace)
    except BenchError as exc:
        return JSONResponse(status_code=404, content={"error": exc.to_dict()})
    lines = ["task_id,step,loss"]
    for task in summary["tasks"]:
        for step, loss in enumerate(task["loss_history"], start=1):
            lines.append(f"{task['task_id']},{step},{loss:.12g}")
    return PlainTextResponse("\n".join(lines) + "\n")
