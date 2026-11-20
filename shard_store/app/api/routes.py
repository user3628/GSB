"""All HTTP endpoints. Errors share one JSON envelope via BusinessError."""
from __future__ import annotations

import base64
import binascii

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.api.schemas import CreateSessionRequest, MergeRequest, SubmitPartRequest
from app.errors import BusinessError
from app.services.merge_service import MergeService, SimulatedCrash
from app.services.object_service import ObjectService
from app.services.part_service import PartService
from app.services.recovery_service import RecoveryService
from app.services.session_service import SessionService

router = APIRouter(prefix="/api/v1")


def _services(request: Request):
    state = request.app.state
    return (
        SessionService(state.settings),
        PartService(state.settings),
        MergeService(state.settings),
        ObjectService(state.settings),
        RecoveryService(state.settings),
    )


@router.post("/sessions")
def create_session(payload: CreateSessionRequest, request: Request):
    sessions, _, _, _, _ = _services(request)
    result = sessions.create_session(
        object_key=payload.object_key,
        min_part_size=payload.min_part_size,
        max_part_size=payload.max_part_size,
        expected_total_size=payload.expected_total_size,
        merge_strategy=payload.merge_strategy,
        conflict_policy=payload.conflict_policy,
    )
    return {"ok": True, "data": result}


@router.get("/sessions")
def list_sessions(request: Request):
    sessions, _, _, _, _ = _services(request)
    return {"ok": True, "data": sessions.list_sessions()}


@router.get("/sessions/{session_id}")
def get_session(session_id: str, request: Request):
    sessions, _, _, _, _ = _services(request)
    return {"ok": True, "data": sessions.get_session(session_id)}


@router.post("/sessions/{session_id}/parts/{part_number}")
def submit_part(session_id: str, part_number: int,
                payload: SubmitPartRequest, request: Request):
    _, parts, _, _, _ = _services(request)
    try:
        content = base64.b64decode(payload.content_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise BusinessError("E_PART_INVALID",
                            "content_base64 is not valid base64", 400) from exc
    result = parts.submit_part(
        session_id, part_number, content,
        client_sha256=payload.sha256.lower() if payload.sha256 else None,
    )
    return {"ok": True, "data": result}


@router.get("/sessions/{session_id}/parts")
def list_parts(session_id: str, request: Request):
    _, parts, _, _, _ = _services(request)
    return {"ok": True, "data": parts.list_parts(session_id)}


@router.post("/sessions/{session_id}/merge")
def merge(session_id: str, payload: MergeRequest, request: Request):
    _, _, merge_service, _, _ = _services(request)
    try:
        result = merge_service.merge(
            session_id,
            strategy=payload.strategy,
            simulate_crash_after_intent=payload.simulate_crash_after_intent,
        )
    except SimulatedCrash as exc:
        # Deterministic crash-injection response. The durable state mirrors a
        # SIGKILL: pending intent + merging session, no object metadata.
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": {
                    "code": "E_SIMULATED_CRASH",
                    "message": "process terminated after intent commit, before "
                               "metadata commit (crash injection point)",
                    "details": {"intent_id": exc.args[0]},
                },
            },
        )
    return {"ok": True, "data": result}


@router.post("/sessions/{session_id}/cancel")
def cancel_session(session_id: str, request: Request):
    sessions, _, _, _, _ = _services(request)
    return {"ok": True, "data": sessions.cancel_session(session_id)}


@router.get("/objects/versions/{object_key:path}")
def versions(object_key: str, request: Request, version: int | None = None):
    # With ?version=N: one historical/current version; without it: full history.
    _, _, _, object_service, _ = _services(request)
    if version is not None:
        return {"ok": True, "data": object_service.get_version(object_key, version)}
    return {"ok": True, "data": object_service.list_versions(object_key)}


@router.get("/objects/{object_key:path}")
def get_object(object_key: str, request: Request):
    _, _, _, objects, _ = _services(request)
    return {"ok": True, "data": objects.get_current_object(object_key)}


@router.post("/recovery/run")
def run_recovery(request: Request):
    _, _, _, _, recovery = _services(request)
    return {"ok": True, "data": recovery.recover()}


@router.get("/recovery/reports")
def recovery_reports(request: Request):
    _, _, _, _, recovery = _services(request)
    return {"ok": True, "data": recovery.list_reports()}


@router.get("/health")
def health():
    return {"ok": True, "data": {"status": "healthy"}}
