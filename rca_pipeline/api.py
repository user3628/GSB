"""HTTP routes exposing the pipeline over a loopback-only FastAPI app."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .errors import PipelineError
from .service import Pipeline

router = APIRouter()


def _pipeline(request: Request) -> Pipeline:
    return request.app.state.pipeline


def _error(exc: PipelineError) -> JSONResponse:
    return JSONResponse(status_code=exc.http_status, content=exc.to_dict())


@router.get("/health")
def health(request: Request):
    return {"ok": True, "service": "rca-pipeline"}


@router.post("/samples/batches")
async def submit_samples(request: Request):
    payload = await request.json()
    try:
        return {"ok": True, "data": _pipeline(request).submit_samples(payload)}
    except PipelineError as exc:
        return _error(exc)


@router.post("/topology/changes")
async def submit_topology(request: Request):
    payload = await request.json()
    try:
        return {"ok": True, "data": _pipeline(request).submit_topology(payload)}
    except PipelineError as exc:
        return _error(exc)


@router.get("/alerts")
def query_alerts(request: Request, status: str | None = None, service: str | None = None):
    try:
        return {"ok": True, "data": _pipeline(request).query_alerts(status, service)}
    except PipelineError as exc:
        return _error(exc)


@router.post("/alerts/{alert_id}/transitions")
async def transition_alert(alert_id: str, request: Request):
    payload = await request.json()
    to_status = payload.get("to_status")
    try:
        return {"ok": True, "data": _pipeline(request).transition_alert(alert_id, to_status)}
    except PipelineError as exc:
        return _error(exc)


@router.get("/aggregations/quantiles")
def query_quantiles(request: Request, service: str | None = None, metric: str | None = None):
    try:
        return {"ok": True, "data": _pipeline(request).query_quantiles(service, metric)}
    except PipelineError as exc:
        return _error(exc)


@router.get("/suppressions")
def query_suppressions(request: Request):
    try:
        return {"ok": True, "data": _pipeline(request).query_suppression()}
    except PipelineError as exc:
        return _error(exc)


@router.get("/rca/{alert_id}")
def query_rca(alert_id: str, request: Request):
    try:
        return {"ok": True, "data": _pipeline(request).query_rca(alert_id)}
    except PipelineError as exc:
        return _error(exc)


@router.get("/diagnostics")
def diagnostics(request: Request):
    return {"ok": True, "data": _pipeline(request).diagnostic_report()}


@router.get("/recovery")
def recovery_report(request: Request):
    return {"ok": True, "data": _pipeline(request).recovery_report()}


@router.post("/faults/inject")
async def inject_fault(request: Request):
    payload = await request.json()
    mode = payload.get("mode")
    try:
        return {"ok": True, "data": _pipeline(request).inject_fault(mode)}
    except ValueError as exc:
        return JSONResponse(
            status_code=422,
            content={"ok": False, "error": {"code": "E_SAMPLE_INVALID",
                                            "message": str(exc), "context": {}}},
        )
