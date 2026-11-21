from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator


class WriteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str
    object_key: str
    seq: int = Field(ge=1)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")

    @field_validator("sha256")
    @classmethod
    def normalize(cls, value: str) -> str:
        return value.lower()


class ModeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["normal", "mismatch", "hang", "fail"]
    hang_seconds: float = Field(default=5.0, gt=0, le=300)


class BackendState:
    def __init__(
        self,
        backend_id: str,
        capacity_bytes: int,
        max_concurrency: int,
        max_parts_per_session: int,
        default_mode: str = "normal",
    ) -> None:
        self.backend_id = backend_id
        self.capacity_bytes = capacity_bytes
        self.max_concurrency = max_concurrency
        self.max_parts_per_session = max_parts_per_session
        self.mode = default_mode
        self.hang_seconds = 5.0
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.lock = asyncio.Lock()
        self.used_bytes = 0
        self.objects: dict[tuple[str, int], dict[str, Any]] = {}
        self.session_parts: dict[str, set[int]] = {}
        self.active_writes = 0
        self.peak_active_writes = 0
        self.total_writes = 0
        self.rejected = 0

    async def reset(self) -> None:
        async with self.lock:
            self.used_bytes = 0
            self.objects.clear()
            self.session_parts.clear()
            self.active_writes = 0
            self.peak_active_writes = 0
            self.total_writes = 0
            self.rejected = 0

    async def snapshot(self) -> dict[str, Any]:
        async with self.lock:
            return {
                "backend_id": self.backend_id,
                "mode": self.mode,
                "hang_seconds": self.hang_seconds,
                "capacity_bytes": self.capacity_bytes,
                "used_bytes": self.used_bytes,
                "remaining_bytes": self.capacity_bytes - self.used_bytes,
                "max_concurrency": self.max_concurrency,
                "active_writes": self.active_writes,
                "peak_active_writes": self.peak_active_writes,
                "total_writes": self.total_writes,
                "rejected": self.rejected,
                "session_part_counts": {
                    session_id: len(seqs) for session_id, seqs in sorted(self.session_parts.items())
                },
                "objects": [
                    {
                        "session_id": session_id,
                        "seq": seq,
                        "object_key": entry["object_key"],
                        "size_bytes": entry["size_bytes"],
                        "sha256": entry["stored_sha256"],
                    }
                    for (session_id, seq), entry in sorted(self.objects.items())
                ],
            }


def _load_spec() -> tuple[dict[str, Any], dict[str, Any]]:
    config_path = Path(os.environ.get("BACKEND_CONFIG_FILE", "samples/backends.json"))
    backend_id = os.environ["BACKEND_ID"]
    with config_path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    for spec in config["backends"]:
        if spec["id"] == backend_id:
            return spec, config
    raise RuntimeError(f"backend {backend_id} is absent from {config_path}")


spec, global_config = _load_spec()
state = BackendState(
    backend_id=spec["id"],
    capacity_bytes=int(spec["capacity_bytes"]),
    max_concurrency=int(spec["max_concurrency"]),
    max_parts_per_session=int(spec["max_parts_per_session"]),
    default_mode=os.environ.get("BACKEND_DEFAULT_MODE", "normal"),
)
app = FastAPI(title=f"Local backend simulator: {spec['id']}")


def _error(code: str, message: str, status: int, **context: Any) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"code": code, "message": message, "context": context},
    )


async def _apply_mode_delay() -> str:
    mode = state.mode
    if mode == "hang":
        await asyncio.sleep(state.hang_seconds)
    return mode


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "backend_id": state.backend_id, "mode": state.mode}


@app.post("/admin/mode")
async def set_mode(request: ModeRequest) -> dict[str, Any]:
    state.mode = request.mode
    state.hang_seconds = request.hang_seconds
    return {
        "backend_id": state.backend_id,
        "mode": state.mode,
        "hang_seconds": state.hang_seconds,
    }


@app.post("/admin/reset")
async def reset() -> dict[str, Any]:
    await state.reset()
    return await state.snapshot()


@app.get("/admin/state")
async def admin_state() -> dict[str, Any]:
    return await state.snapshot()


@app.post("/v1/backend/write", response_model=None)
async def write_part(request: WriteRequest) -> JSONResponse | dict[str, Any]:
    started = time.monotonic()
    try:
        await asyncio.wait_for(state.semaphore.acquire(), timeout=5.0)
    except asyncio.TimeoutError:
        async with state.lock:
            state.rejected += 1
        return _error(
            "E_BACKEND_BUSY",
            "backend concurrent-write limit is saturated",
            429,
            backend_id=state.backend_id,
            max_concurrency=state.max_concurrency,
        )
    async with state.lock:
        state.active_writes += 1
        state.peak_active_writes = max(state.peak_active_writes, state.active_writes)
    try:
        mode = await _apply_mode_delay()
        if mode == "fail":
            async with state.lock:
                state.rejected += 1
            return _error("E_BACKEND_IO", "injected backend write failure", 503, seq=request.seq)
        async with state.lock:
            key = (request.session_id, request.seq)
            existing = state.objects.get(key)
            if existing is not None:
                if (
                    existing["object_key"] != request.object_key
                    or existing["size_bytes"] != request.size_bytes
                    or existing["submitted_sha256"] != request.sha256
                ):
                    state.rejected += 1
                    return _error(
                        "E_BACKEND_IDEMPOTENCY_CONFLICT",
                        "backend already stored a different value for this session part",
                        409,
                        session_id=request.session_id,
                        seq=request.seq,
                    )
                if mode == "normal" and existing["stored_sha256"] != request.sha256:
                    existing["stored_sha256"] = request.sha256
                    state.total_writes += 1
                    return {
                        "code": "OK",
                        "duplicate": False,
                        "repaired": True,
                        "storage_token": existing["storage_token"],
                        "stored_sha256": existing["stored_sha256"],
                    }
                return {
                    "code": "OK",
                    "duplicate": True,
                    "storage_token": existing["storage_token"],
                    "stored_sha256": existing["stored_sha256"],
                }
            session_seqs = state.session_parts.setdefault(request.session_id, set())
            if request.seq not in session_seqs and len(session_seqs) >= state.max_parts_per_session:
                state.rejected += 1
                return _error(
                    "E_BACKEND_SESSION_PART_LIMIT",
                    "backend per-session part limit exceeded",
                    409,
                    session_id=request.session_id,
                    limit=state.max_parts_per_session,
                )
            if state.used_bytes + request.size_bytes > state.capacity_bytes:
                state.rejected += 1
                return _error(
                    "E_QUOTA_EXCEEDED",
                    "backend capacity budget exceeded",
                    409,
                    remaining_bytes=state.capacity_bytes - state.used_bytes,
                    requested_bytes=request.size_bytes,
                )
            stored_sha = request.sha256
            if mode == "mismatch":
                stored_sha = hashlib.sha256(
                    f"corrupt:{request.sha256}".encode("ascii")
                ).hexdigest()
            storage_token = hashlib.sha256(
                f"{state.backend_id}:{request.session_id}:{request.seq}:{request.sha256}".encode(
                    "ascii"
                )
            ).hexdigest()[:24]
            state.objects[key] = {
                "object_key": request.object_key,
                "size_bytes": request.size_bytes,
                "submitted_sha256": request.sha256,
                "stored_sha256": stored_sha,
                "storage_token": storage_token,
                "stored_at": time.time(),
            }
            session_seqs.add(request.seq)
            state.used_bytes += request.size_bytes
            state.total_writes += 1
            return {
                "code": "OK",
                "duplicate": False,
                "storage_token": storage_token,
                "stored_sha256": stored_sha,
                "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
            }
    finally:
        async with state.lock:
            state.active_writes -= 1
        state.semaphore.release()


@app.get("/v1/backend/read/{session_id}/{seq}", response_model=None)
async def read_part(
    session_id: str,
    seq: int,
    object_key: str = Query(..., min_length=1),
) -> JSONResponse | dict[str, Any]:
    mode = await _apply_mode_delay()
    if mode == "fail":
        return _error("E_BACKEND_IO", "injected backend read failure", 503, seq=seq)
    async with state.lock:
        entry = state.objects.get((session_id, seq))
        if entry is None or entry["object_key"] != object_key:
            return _error(
                "E_NOT_FOUND",
                "stored part not found",
                404,
                session_id=session_id,
                seq=seq,
            )
        return {
            "code": "OK",
            "session_id": session_id,
            "seq": seq,
            "object_key": object_key,
            "size_bytes": entry["size_bytes"],
            "sha256": entry["stored_sha256"],
            "storage_token": entry["storage_token"],
        }
