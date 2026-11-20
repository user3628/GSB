from __future__ import annotations

import asyncio
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .errors import ConfigHubError
from .models import SampleSpec
from .pipeline import compute_pipeline


class EndpointState:
    def __init__(
        self,
        delays: dict[str, float] | None = None,
        modes: dict[str, str] | None = None,
    ) -> None:
        self.delays = {"default": 0.0, **(delays or {})}
        self.modes = {"default": "normal", **(modes or {})}
        self.cached: dict[str, dict[str, Any]] = {}
        self.stats: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    def stats_for(self, profile: str) -> dict[str, Any]:
        return self.stats.setdefault(
            profile,
            {
                "request_count": 0,
                "completed_count": 0,
                "delay_seconds": self.delays.get(profile, 0.0),
                "mode": self.modes.get(profile, "normal"),
                "last_picked_version": None,
            },
        )

    async def handle(self, profile: str, payload: dict[str, Any]) -> dict[str, Any]:
        async with self._lock:
            stats = self.stats_for(profile)
            stats["request_count"] += 1
            delay = float(self.delays.get(profile, 0.0))
            mode = self.modes.get(profile, "normal")

        if delay > 0:
            await asyncio.sleep(delay)

        async with self._lock:
            cached = self.cached.get(profile)
            if mode == "lagging" and cached is not None:
                picked_version = int(cached["version"])
                config = dict(cached["config"])
                sample = dict(cached["sample"])
            else:
                picked_version = int(payload["version"])
                config = dict(payload["config"])
                sample = dict(payload["sample"])
                self.cached[profile] = {
                    "version": picked_version,
                    "config": config,
                    "sample": sample,
                }

            spec = SampleSpec(**sample)
            result = compute_pipeline(spec, picked_version, config)
            stats["completed_count"] += 1
            stats["last_picked_version"] = picked_version
            return {
                "status": "ACK",
                "client_id": payload["client_id"],
                "picked_version": picked_version,
                "target_version": int(payload["version"]),
                "profile": profile,
                "delay_seconds": delay,
                "mode": mode,
                "result": result,
            }


def create_client_app(
    delays: dict[str, float] | None = None,
    modes: dict[str, str] | None = None,
) -> FastAPI:
    app = FastAPI(title="Local deterministic client test endpoint", version="1.0")
    state = EndpointState(delays=delays, modes=modes)
    app.state.endpoint = state

    @app.exception_handler(ConfigHubError)
    async def config_error_handler(_request: Any, exc: ConfigHubError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.as_dict())

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "service": "client-test-endpoint"}

    @app.post("/push")
    async def push_default(payload: dict[str, Any]) -> dict[str, Any]:
        return await state.handle("default", payload)

    @app.post("/push/{profile}")
    async def push_profile(profile: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await state.handle(profile, payload)

    @app.post("/admin/delay")
    async def set_delay(payload: dict[str, Any]) -> dict[str, Any]:
        profile = str(payload.get("profile", "default"))
        delay = float(payload.get("delay_seconds", 0.0))
        if delay < 0 or delay > 300:
            return JSONResponse(
                status_code=422,
                content={"ok": False, "code": "E_VALIDATION", "message": "invalid delay"},
            )
        async with state._lock:
            state.delays[profile] = delay
            state.stats_for(profile)["delay_seconds"] = delay
        return {"ok": True, "profile": profile, "delay_seconds": delay}

    @app.post("/admin/mode")
    async def set_mode(payload: dict[str, Any]) -> dict[str, Any]:
        profile = str(payload.get("profile", "default"))
        mode = str(payload.get("mode", "normal"))
        if mode not in {"normal", "lagging"}:
            return JSONResponse(
                status_code=422,
                content={"ok": False, "code": "E_VALIDATION", "message": "invalid mode"},
            )
        async with state._lock:
            state.modes[profile] = mode
            state.stats_for(profile)["mode"] = mode
        return {"ok": True, "profile": profile, "mode": mode}

    @app.get("/stats")
    async def stats() -> dict[str, Any]:
        async with state._lock:
            return {"ok": True, "profiles": dict(state.stats)}

    return app

