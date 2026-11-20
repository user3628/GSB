from __future__ import annotations

import asyncio
from typing import Any

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field, field_validator

from .scheduler import validate_loopback_url


class DispatchItem(BaseModel):
    node_id: str
    shard_id: int = Field(ge=0)
    sample_count: int = Field(ge=0)
    gradient: list[float]
    delay_seconds: float = Field(default=0.0, ge=0.0, le=60.0)


class DispatchRequest(BaseModel):
    scheduler_url: str
    round_id: str
    submissions: list[DispatchItem] = Field(min_length=1)

    @field_validator("scheduler_url")
    @classmethod
    def validate_scheduler_url(cls, value: str) -> str:
        validate_loopback_url(value, field="scheduler_url")
        return value.rstrip("/")


def create_worker_app(name: str) -> FastAPI:
    app = FastAPI(title=f"GradSync local test worker {name}", version="0.1.0")
    app.state.name = name
    app.state.results = {}

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "worker": name, "stored_results": len(app.state.results)}

    @app.post("/run")
    async def run_batch(payload: DispatchRequest) -> dict[str, Any]:
        async def send(item: DispatchItem) -> dict[str, Any]:
            if item.delay_seconds:
                await asyncio.sleep(item.delay_seconds)
            body = {
                "node_id": item.node_id,
                "shard_id": item.shard_id,
                "sample_count": item.sample_count,
                "gradient": item.gradient,
            }
            url = f"{payload.scheduler_url}/rounds/{payload.round_id}/submissions"
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.post(url, json=body)
                try:
                    response_body = response.json()
                except Exception:
                    response_body = {"text": response.text[:500]}
                return {
                    "ok": response.is_success,
                    "http_status": response.status_code,
                    "node_id": item.node_id,
                    "shard_id": item.shard_id,
                    "response": response_body,
                }
            except Exception as exc:
                return {
                    "ok": False,
                    "http_status": None,
                    "node_id": item.node_id,
                    "shard_id": item.shard_id,
                    "error": f"{type(exc).__name__}: {exc}",
                }

        results = await asyncio.gather(*(send(item) for item in payload.submissions))
        return {
            "ok": all(item["ok"] for item in results),
            "worker": name,
            "round_id": payload.round_id,
            "submissions": results,
        }

    @app.post("/results")
    async def receive_result(payload: dict[str, Any]) -> dict[str, Any]:
        round_id = payload.get("round_id", "unknown")
        app.state.results[round_id] = payload
        return {
            "ok": True,
            "worker": name,
            "round_id": round_id,
            "digest_sha256": payload.get("digest_sha256"),
            "element_sum": payload.get("element_sum"),
        }

    @app.get("/results")
    async def list_results() -> dict[str, Any]:
        return {"ok": True, "worker": name, "results": app.state.results}

    return app

