from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .workloads import WorkloadRegistry, generate_sample


class SampleRequest(BaseModel):
    nonce: int
    profile: str | None = None
    concurrency: int = Field(gt=0)
    round_id: str = Field(min_length=1, max_length=200)


def _workload_path() -> Path | None:
    configured = os.getenv("CAPACITY_WORKLOADS")
    return Path(configured) if configured else None


def create_app(workload_path: str | Path | None = None) -> FastAPI:
    app = FastAPI(title="Deterministic loopback load target", version="0.1.0")
    registry = WorkloadRegistry.load(workload_path or _workload_path())
    app.state.registry = registry
    app.state.stats = {}

    def stats_for(round_id: str) -> dict[str, int]:
        stats = app.state.stats.setdefault(
            round_id,
            {
                "requests": 0,
                "active": 0,
                "max_active": 0,
                "errors": 0,
                "timeouts": 0,
                "client_disconnects": 0,
            },
        )
        return stats

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "profiles": sorted(registry.profiles)}

    @app.post("/sample")
    async def sample(payload: SampleRequest, request: Request) -> JSONResponse:
        stats = stats_for(payload.round_id)
        stats["requests"] += 1
        stats["active"] += 1
        stats["max_active"] = max(stats["max_active"], stats["active"])
        try:
            try:
                profile = registry.get(payload.profile)
            except KeyError as exc:
                stats["errors"] += 1
                return JSONResponse(
                    status_code=422,
                    content={"error": {"code": "E_WORKLOAD_UNKNOWN", "message": str(exc)}},
                )
            generated = generate_sample(
                profile, payload.concurrency, payload.nonce
            )
            if generated.timeout:
                stats["timeouts"] += 1
                await asyncio.sleep(profile.timeout_sleep_ms / 1000.0)
                if await request.is_disconnected():
                    stats["client_disconnects"] += 1
                stats["errors"] += 1
                return JSONResponse(
                    status_code=504,
                    headers={"X-Sample-Latency-Ms": str(generated.latency_ms)},
                    content={
                        "ok": False,
                        "injected": "timeout",
                        "latency_ms": generated.latency_ms,
                    },
                )
            if generated.error:
                stats["errors"] += 1
                return JSONResponse(
                    status_code=503,
                    headers={"X-Sample-Latency-Ms": str(generated.latency_ms)},
                    content={
                        "ok": False,
                        "injected": "error",
                        "latency_ms": generated.latency_ms,
                    },
                )
            return JSONResponse(
                status_code=200,
                headers={"X-Sample-Latency-Ms": str(generated.latency_ms)},
                content={
                    "ok": True,
                    "latency_ms": generated.latency_ms,
                    "distribution": generated.kind,
                },
            )
        finally:
            stats["active"] -= 1

    @app.get("/__stats")
    async def all_stats() -> dict[str, Any]:
        return {"rounds": app.state.stats}

    @app.get("/__stats/{round_id}")
    async def one_stats(round_id: str) -> dict[str, Any]:
        return {"round_id": round_id, **stats_for(round_id)}

    @app.post("/__stats/reset")
    async def reset_stats() -> dict[str, bool]:
        app.state.stats.clear()
        return {"ok": True}

    return app


app = create_app()

