"""Local HTTP cluster endpoint used by the demo and integration tests.

One process can simulate several clusters by using
``/clusters/<cluster_id>/push`` URLs.  Behaviour is controlled through local
``/control`` routes; nothing is sent to an external service.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
from collections import defaultdict
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from .codec import digest_json
from .errors import E_CONFLICTING_PUSH, E_PUSH_REJECTED


class ModeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cluster_id: str = "east"
    component_id: str
    mode: str = "success"
    delay_seconds: float = Field(default=1.2, ge=0, le=30)


class ModeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    modes: list[ModeSpec]


class PushRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    request_id: str
    snapshot_version: int
    component_id: str
    configs: dict[str, Any]
    expected_digest: str | None = None


class RollbackRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    request_id: str
    snapshot_version: int
    component_id: str


class Runtime:
    def __init__(self) -> None:
        self.modes: dict[tuple[str, str], dict[str, Any]] = {}
        self.ledger: dict[tuple[str, str], dict[str, Any]] = {}
        self.applied: dict[tuple[str, str], dict[str, Any]] = {}
        self.stats: dict[str, int] = defaultdict(int)

    def reset(self) -> None:
        self.modes.clear()
        self.ledger.clear()
        self.applied.clear()
        self.stats.clear()

    def mode_for(self, cluster_id: str, component_id: str) -> dict[str, Any]:
        return self.modes.get(
            (cluster_id, component_id),
            self.modes.get(("*", component_id), {"mode": "success", "delay_seconds": 0.0}),
        )


runtime = Runtime()
app = FastAPI(title="Local cluster endpoint", version="1.0.0")


def _error(code: str, message: str, details: dict[str, Any], status: int) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"code": code, "message": message, "details": details},
    )


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "stats": dict(runtime.stats)}


@app.post("/control/modes")
def set_modes(body: ModeRequest) -> dict[str, Any]:
    for spec in body.modes:
        runtime.modes[(spec.cluster_id, spec.component_id)] = {
            "mode": spec.mode,
            "delay_seconds": spec.delay_seconds,
        }
    return {"ok": True, "modes": len(runtime.modes)}


@app.post("/control/reset")
def reset() -> dict[str, Any]:
    runtime.reset()
    return {"ok": True}


@app.get("/control/stats")
def stats() -> dict[str, Any]:
    return {
        "stats": dict(runtime.stats),
        "ledger_entries": len(runtime.ledger),
        "applied_components": len(runtime.applied),
    }


@app.get("/clusters/{cluster_id}/state/{component_id}")
def applied_state(cluster_id: str, component_id: str) -> dict[str, Any]:
    state = runtime.applied.get((cluster_id, component_id))
    return {"cluster_id": cluster_id, "component_id": component_id, "state": state}


@app.post("/clusters/{cluster_id}/push")
async def push(cluster_id: str, body: PushRequest, request: Request) -> Any:
    runtime.stats["push_requests"] += 1
    payload_digest = digest_json(body.model_dump(mode="json"))
    ledger_key = (cluster_id, body.request_id)
    prior = runtime.ledger.get(ledger_key)
    if prior is not None:
        if prior["payload_digest"] != payload_digest:
            runtime.stats["conflicts"] += 1
            return _error(
                E_CONFLICTING_PUSH,
                "request_id was already used with a different payload",
                {"request_id": body.request_id, "cluster_id": cluster_id},
                409,
            )
        runtime.stats["idempotent_replays"] += 1
        replay = copy.deepcopy(prior["response"])
        replay["reused"] = True
        return replay

    mode = runtime.mode_for(cluster_id, body.component_id)
    if mode["mode"] == "reject":
        runtime.stats["rejections"] += 1
        return _error(
            E_PUSH_REJECTED,
            "configuration rejected by deterministic endpoint policy",
            {"component_id": body.component_id, "cluster_id": cluster_id},
            422,
        )
    if mode["mode"] == "conflict":
        runtime.stats["conflicts"] += 1
        return _error(
            E_CONFLICTING_PUSH,
            "deterministic idempotency conflict",
            {"component_id": body.component_id, "cluster_id": cluster_id},
            409,
        )
    if mode["mode"] == "timeout":
        runtime.stats["timeout_requests"] += 1
        await asyncio.sleep(float(mode.get("delay_seconds", 1.2)))
        if await request.is_disconnected():
            runtime.stats["client_disconnects"] += 1
            return _error(
                "E_ENDPOINT_CLIENT_TIMEOUT",
                "client closed the connection before the delayed response",
                {"component_id": body.component_id},
                499,
            )

    state_key = (cluster_id, body.component_id)
    previous = runtime.applied.get(state_key)
    history = copy.deepcopy(previous.get("history", [])) if previous else []
    if previous:
        history.append(
            {
                "snapshot_version": previous["snapshot_version"],
                "expected_digest": previous["expected_digest"],
                "configs": previous["configs"],
            }
        )
    response = {
        "ok": True,
        "cluster_id": cluster_id,
        "component_id": body.component_id,
        "snapshot_version": body.snapshot_version,
        "expected_digest": body.expected_digest,
        "request_id": body.request_id,
        "reused": False,
    }
    runtime.applied[state_key] = {
        "snapshot_version": body.snapshot_version,
        "expected_digest": body.expected_digest,
        "configs": body.configs,
        "history": history,
        "request_id": body.request_id,
    }
    runtime.ledger[ledger_key] = {
        "payload_digest": payload_digest,
        "response": response,
    }
    runtime.stats["applied"] += 1
    return response


@app.post("/clusters/{cluster_id}/rollback")
async def rollback(cluster_id: str, body: RollbackRequest) -> Any:
    runtime.stats["rollback_requests"] += 1
    state_key = (cluster_id, body.component_id)
    previous = runtime.applied.get(state_key)
    request_payload = body.model_dump(mode="json")
    ledger_key = (cluster_id, body.request_id)
    prior = runtime.ledger.get(ledger_key)
    if prior is not None:
        if prior["payload_digest"] != digest_json(request_payload):
            runtime.stats["conflicts"] += 1
            return _error(
                E_CONFLICTING_PUSH,
                "rollback request_id conflicts with a different payload",
                {"request_id": body.request_id},
                409,
            )
        replay = copy.deepcopy(prior["response"])
        replay["reused"] = True
        return replay
    if previous and previous.get("history"):
        restored = previous["history"].pop()
        runtime.applied[state_key] = {
            "snapshot_version": restored["snapshot_version"],
            "expected_digest": restored["expected_digest"],
            "configs": restored["configs"],
            "history": previous["history"],
            "request_id": body.request_id,
        }
        rolled_back_to = restored["snapshot_version"]
    else:
        runtime.applied.pop(state_key, None)
        rolled_back_to = None
    response = {
        "ok": True,
        "cluster_id": cluster_id,
        "component_id": body.component_id,
        "snapshot_version": body.snapshot_version,
        "rolled_back_to": rolled_back_to,
        "request_id": body.request_id,
    }
    runtime.ledger[ledger_key] = {
        "payload_digest": digest_json(request_payload),
        "response": response,
    }
    runtime.stats["rolled_back"] += 1
    return response


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local cluster endpoint service")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18765)
    parser.add_argument("--log-level", default="warning")
    args = parser.parse_args()
    uvicorn.run(
        "config_coordinator.endpoint_server:app",
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()

