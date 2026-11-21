from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import threading
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import Body, FastAPI, Header
from fastapi.responses import JSONResponse


def stable_latency_ms(model_id: str, sample_count: int) -> int:
    material = f"{model_id}:{sample_count}".encode("utf-8")
    value = int.from_bytes(hashlib.sha256(material).digest()[:4], "big")
    return 25 + value % 36


class BackendState:
    def __init__(self, behaviors: dict[str, Any]) -> None:
        self.behaviors = behaviors
        self.lock = threading.Lock()
        self.active = 0
        self.peak_active = 0
        self.completed = 0
        self.failed = 0
        self.hanging = 0
        self.by_model: dict[str, dict[str, int]] = {}
        self.faults: dict[str, str] = {}

    def begin(self, model_id: str) -> None:
        with self.lock:
            self.active += 1
            self.peak_active = max(self.peak_active, self.active)
            entry = self.by_model.setdefault(
                model_id,
                {"active": 0, "completed": 0, "failed": 0, "hanging": 0, "peak_active": 0},
            )
            entry["active"] += 1
            entry["peak_active"] = max(entry["peak_active"], entry["active"])

    def end(self, model_id: str, outcome: str) -> None:
        with self.lock:
            self.active = max(0, self.active - 1)
            entry = self.by_model.setdefault(model_id, {"active": 0})
            entry["active"] = max(0, entry.get("active", 0) - 1)
            if outcome == "success":
                self.completed += 1
                entry["completed"] = entry.get("completed", 0) + 1
            elif outcome == "fail":
                self.failed += 1
                entry["failed"] = entry.get("failed", 0) + 1
            elif outcome == "hang":
                self.hanging += 1
                entry["hanging"] = entry.get("hanging", 0) + 1

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "active": self.active,
                "peak_active": self.peak_active,
                "completed": self.completed,
                "failed": self.failed,
                "hanging": self.hanging,
                "by_model": json.loads(json.dumps(self.by_model)),
                "configured_faults": dict(self.faults),
            }

    def resolve(self, model_id: str, sample_count: int, header_fault: str | None) -> dict[str, Any]:
        behavior = dict(self.behaviors.get("default", {}))
        behavior.update(self.behaviors.get("models", {}).get(model_id, {}))
        fault = header_fault or self.faults.get(model_id) or behavior.get("mode", "success")
        latency = int(behavior.get("latency_ms", stable_latency_ms(model_id, sample_count)))
        if fault == "hang":
            latency = max(latency, int(self.behaviors.get("hang_seconds", 30)) * 1000)
        return {
            "mode": fault,
            "latency_ms": latency,
            "vram_per_sample_mb": float(behavior.get("vram_per_sample_mb", 1.0)),
            "model_id": model_id,
        }


def _load_behaviors(path: str | None) -> dict[str, Any]:
    if path:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    env_path = os.getenv("BACKEND_BEHAVIORS_FILE")
    if env_path:
        with open(env_path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    return {
        "default": {"mode": "success", "vram_per_sample_mb": 1.0},
        "hang_seconds": 30,
        "models": {},
    }


def create_app(behaviors_path: str | None = None) -> FastAPI:
    state = BackendState(_load_behaviors(behaviors_path))
    app = FastAPI(title="Deterministic Local Model Backend", version="0.1.0")
    app.state.backend_state = state

    @app.get("/health")
    def health() -> dict[str, Any]:
        snapshot = state.snapshot()
        return {"ok": True, "service": "mock-backend", **snapshot}

    @app.get("/metrics")
    def metrics() -> dict[str, Any]:
        return {"ok": True, **state.snapshot()}

    @app.get("/admin/behaviors")
    def get_behaviors() -> dict[str, Any]:
        return {"ok": True, "behaviors": state.behaviors}

    @app.post("/admin/faults")
    def set_fault(body: dict[str, Any] = Body(...)) -> dict[str, Any]:
        model_id = str(body.get("model_id", "*"))
        mode = str(body.get("mode", "success"))
        if mode not in {"success", "fail", "hang"}:
            return JSONResponse(
                status_code=422,
                content={"ok": False, "code": "E_INVALID_FAULT", "message": "unsupported fault mode"},
            )
        if model_id == "*":
            state.faults.clear()
        else:
            state.faults[model_id] = mode
        return {"ok": True, "faults": dict(state.faults)}

    @app.post("/admin/reset")
    def reset() -> dict[str, Any]:
        with state.lock:
            state.active = 0
            state.peak_active = 0
            state.completed = 0
            state.failed = 0
            state.hanging = 0
            state.by_model.clear()
            state.faults.clear()
        return {"ok": True, **state.snapshot()}

    @app.post("/infer")
    async def infer(
        body: dict[str, Any] = Body(...),
        x_fault_mode: str | None = Header(default=None),
    ) -> JSONResponse:
        model_id = str(body.get("model_id", "default"))
        sample_count = max(1, int(body.get("sample_count", 1)))
        request_id = str(body.get("request_id", "unknown"))
        behavior = state.resolve(model_id, sample_count, x_fault_mode or body.get("fault_mode"))
        vram_mb = round(sample_count * behavior["vram_per_sample_mb"], 6)
        state.begin(model_id)
        try:
            await asyncio.sleep(behavior["latency_ms"] / 1000.0)
            if behavior["mode"] == "fail":
                state.end(model_id, "fail")
                return JSONResponse(
                    status_code=500,
                    content={
                        "ok": False,
                        "code": "E_BACKEND_FAILURE",
                        "message": "injected deterministic backend failure",
                        "request_id": request_id,
                        "model_id": model_id,
                    },
                )
            outcome = "hang" if behavior["mode"] == "hang" else "success"
            state.end(model_id, outcome)
            return JSONResponse(
                status_code=200,
                content={
                    "ok": True,
                    "request_id": request_id,
                    "model_id": model_id,
                    "sample_count": sample_count,
                    "latency_ms": behavior["latency_ms"],
                    "vram_mb": vram_mb,
                    "outcome": outcome,
                    "deterministic": True,
                },
            )
        except asyncio.CancelledError:
            state.end(model_id, "hang" if behavior["mode"] == "hang" else "fail")
            raise
        except Exception:
            state.end(model_id, "fail")
            raise

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic local model backend")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19101)
    parser.add_argument("--behaviors", default=None)
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args()
    app = create_app(args.behaviors)
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()

