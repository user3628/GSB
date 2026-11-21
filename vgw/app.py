from __future__ import annotations

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from .errors import GatewayError
from .gateway import Gateway
from .models import (
    BatchSubmission,
    FrameDecodeRequest,
    FrameSubmission,
    PatchDocument,
)
from .snapshot import compute_digest
from .store import Store
from .transport import LocalTestNetwork, Topology


ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = Path(__file__).resolve().parent / "static"
SAMPLES_DIR = ROOT / "samples"


@dataclass(slots=True)
class AppSettings:
    db_path: Path
    topology_path: Path
    samples_dir: Path = SAMPLES_DIR
    auto_load_sample: bool = True


def default_settings() -> AppSettings:
    return AppSettings(
        db_path=Path(os.getenv("VGW_DB", ROOT / "artifacts" / "vgw.db")),
        topology_path=Path(os.getenv("VGW_TOPOLOGY", SAMPLES_DIR / "topology.json")),
        samples_dir=Path(os.getenv("VGW_SAMPLES", SAMPLES_DIR)),
        auto_load_sample=os.getenv("VGW_AUTO_SAMPLE", "1") != "0",
    )


def create_app(settings: AppSettings | None = None) -> FastAPI:
    settings = settings or default_settings()
    topology = Topology.from_file(settings.topology_path)
    network = LocalTestNetwork(topology)
    store = Store(settings.db_path)
    gateway = Gateway(store)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await network.start()
        await gateway.start()
        if (
            settings.auto_load_sample
            and store.latest_snapshot() is None
            and (settings.samples_dir / "snapshot_v1.json").exists()
        ):
            sample = json.loads(
                (settings.samples_dir / "snapshot_v1.json").read_text(encoding="utf-8")
            )
            await gateway.load_snapshot(sample)
        app.state.gateway = gateway
        app.state.network = network
        app.state.settings = settings
        app.state.topology = topology
        yield
        await gateway.stop()
        await network.stop()
        store.close()

    app = FastAPI(
        title="Loopback Tunnel Gateway",
        version="1.0.0",
        description=(
            "Local-only VPN tunnel gateway core with versioned snapshots, "
            "UDP carrier/decap test hops, bounded channel scheduling, atomic "
            "incremental rebuilds, and SQLite intent recovery."
        ),
        lifespan=lifespan,
    )

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_request: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status or 500,
            content=exc.as_payload(),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        error = GatewayError(
            "E_REQUEST_INVALID",
            "request validation failed before entering the gateway",
            {
                "errors": [
                    {
                        "loc": list(item.get("loc", ())),
                        "type": item.get("type"),
                        "msg": item.get("msg"),
                    }
                    for item in exc.errors()
                ]
            },
        )
        return JSONResponse(status_code=400, content=error.as_payload())

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        error = GatewayError(
            "E_INTERNAL",
            "unhandled gateway failure",
            {"exception": type(exc).__name__},
        )
        return JSONResponse(status_code=500, content=error.as_payload())

    @app.get("/", include_in_schema=False)
    async def dashboard() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> Response:
        return Response(status_code=204)

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        state = gateway.public_state()
        return {
            "ok": True,
            "snapshot_version": state.get("version"),
            "scheduler_running": state.get("scheduler", {}).get("running"),
            "recovery_completed": gateway.latest_recovery() is not None,
            "loopback_only": True,
        }

    @app.get("/api/v1/overview")
    async def overview() -> dict[str, Any]:
        return {
            "ok": True,
            "state": gateway.public_state(),
            "topology": topology.as_dict(),
            "network": network.status(),
            "recovery": gateway.latest_recovery(),
            "samples": {
                name: str((settings.samples_dir / name).relative_to(ROOT))
                if (settings.samples_dir / name).is_relative_to(ROOT)
                else str(settings.samples_dir / name)
                for name in (
                    "snapshot_v1.json",
                    "patch_v2.json",
                    "frames.json",
                    "expected.json",
                    "topology.json",
                )
            },
        }

    @app.get("/api/v1/snapshots/current")
    async def current_snapshot() -> dict[str, Any]:
        snapshot = gateway.public_state().get("snapshot")
        if snapshot is None:
            raise GatewayError("E_NOT_FOUND", "no active snapshot", {})
        return {"ok": True, "snapshot": snapshot}

    @app.post("/api/v1/snapshots")
    async def load_snapshot(raw: dict[str, Any]) -> dict[str, Any]:
        result = await gateway.load_snapshot(raw)
        return {"ok": True, "result": result, "state": gateway.public_state()}

    @app.post("/api/v1/routes/patches")
    async def apply_patch(raw: dict[str, Any]) -> dict[str, Any]:
        result = await gateway.apply_patch(raw)
        return {"ok": True, "result": result, "state": gateway.public_state()}

    @app.post("/api/v1/frames")
    async def submit_frame(submission: FrameSubmission) -> dict[str, Any]:
        result = await gateway.submit_frame(submission)
        return {"ok": True, "result": result, "state": gateway.public_state()}

    @app.post("/api/v1/frames/batch")
    async def submit_batch(batch: BatchSubmission) -> dict[str, Any]:
        semaphore = asyncio.Semaphore(batch.max_parallel)

        async def one(item: FrameSubmission) -> dict[str, Any]:
            async with semaphore:
                try:
                    result = await gateway.submit_frame(item)
                    return {"frame_id": item.frame_id, "ok": True, "result": result}
                except GatewayError as exc:
                    return {"frame_id": item.frame_id, "ok": False, **exc.as_payload()}

        results = await asyncio.gather(*(one(item) for item in batch.frames))
        return {
            "ok": all(item["ok"] for item in results),
            "results": results,
            "state": gateway.public_state(),
        }

    @app.post("/api/v1/frames/decode")
    async def decode_frame(request: FrameDecodeRequest) -> dict[str, Any]:
        return {
            "ok": True,
            "frame": gateway.decode_observable_frame(
                request.frame_b64, max_payload=request.max_payload
            ),
        }

    @app.get("/api/v1/frames")
    async def list_frames(limit: int = 100, session_id: str | None = None) -> dict[str, Any]:
        return {
            "ok": True,
            "frames": gateway.list_frames(limit=max(1, min(limit, 1000)), session_id=session_id),
        }

    @app.get("/api/v1/frames/{frame_id}")
    async def get_frame(frame_id: str) -> dict[str, Any]:
        return {"ok": True, "frame": gateway.get_frame(frame_id)}

    @app.get("/api/v1/traces")
    async def list_traces(limit: int = 300, session_id: str | None = None) -> dict[str, Any]:
        return {
            "ok": True,
            "traces": gateway.list_traces(
                limit=max(1, min(limit, 5000)), session_id=session_id
            ),
        }

    @app.get("/api/v1/intents")
    async def list_intents(limit: int = 100, status: str | None = None) -> dict[str, Any]:
        return {
            "ok": True,
            "intents": gateway.list_intents(
                limit=max(1, min(limit, 1000)), status=status
            ),
        }

    @app.get("/api/v1/recovery")
    async def recovery() -> dict[str, Any]:
        report = gateway.latest_recovery()
        return {"ok": True, "recovery": report, "facts_only": True}

    @app.get("/api/v1/samples/{name}")
    async def get_sample(name: str) -> dict[str, Any]:
        allowed = {
            "snapshot_v1.json",
            "patch_v2.json",
            "frames.json",
            "expected.json",
            "topology.json",
        }
        if name not in allowed:
            raise GatewayError("E_NOT_FOUND", "sample was not found", {"name": name})
        path = settings.samples_dir / name
        if not path.exists():
            raise GatewayError("E_NOT_FOUND", "sample file is missing", {"name": name})
        return json.loads(path.read_text(encoding="utf-8"))

    @app.post("/api/v1/lab/scenarios/parallel")
    async def scenario_parallel() -> dict[str, Any]:
        state = gateway.public_state()
        sessions = state.get("sessions") or {}
        missing = [name for name in ("alpha", "beta", "gamma") if name not in sessions]
        if missing:
            raise GatewayError(
                "E_SNAPSHOT_INVALID",
                "parallel scenario requires alpha, beta and gamma",
                {"missing_sessions": missing},
            )

        def make_frame(session_id: str, sequence: int, round_index: int) -> FrameSubmission:
            options = {"delay_ms": 90} if round_index == 0 else {}
            return FrameSubmission(
                frame_id=f"parallel-{session_id}-{sequence}-{uuid.uuid4().hex[:6]}",
                session_id=session_id,
                sequence=sequence,
                payload={
                    "op": "echo",
                    "body": f"{session_id}-round-{round_index + 1}",
                    "options": options,
                },
            )

        first = [
            make_frame(name, sessions[name]["next_expected_seq"], 0)
            for name in ("alpha", "beta", "gamma")
        ]
        second = [
            make_frame(name, sessions[name]["next_expected_seq"] + 1, 1)
            for name in ("alpha", "beta", "gamma")
        ]
        first_results = await _gather_frame_results(gateway, first)
        second_results = await _gather_frame_results(gateway, second)
        return {
            "ok": all(item["ok"] for item in first_results + second_results),
            "scenario": "multi-session-parallel",
            "rounds": [first_results, second_results],
            "state": gateway.public_state(),
        }

    @app.post("/api/v1/lab/scenarios/rebuild")
    async def scenario_rebuild() -> dict[str, Any]:
        state = gateway.public_state()
        snapshot = state.get("snapshot")
        if snapshot is None:
            raise GatewayError("E_SNAPSHOT_INVALID", "no active snapshot", {})
        endpoints = snapshot["egress_endpoints"]
        binding = snapshot["bindings"].get("beta")
        if binding is None or len(endpoints) < 2:
            raise GatewayError(
                "E_SNAPSHOT_INVALID",
                "scenario requires beta and at least two egress endpoints",
                {"available_sessions": sorted(snapshot["bindings"]), "endpoints": sorted(endpoints)},
            )
        alternate = next((name for name in sorted(endpoints) if name != binding["egress"]), None)
        if alternate is None:
            raise GatewayError("E_SNAPSHOT_INVALID", "no alternate egress endpoint", {})
        patch_raw = {
            "schema": "vgw.patch/v1",
            "base_version": snapshot["version"],
            "version": snapshot["version"] + 1,
            "generated_at": _now_iso(),
            "bindings": {
                "beta": {
                    "priority": binding["priority"],
                    "channel": binding["channel"],
                    "egress": alternate,
                    "max_payload": binding["max_payload"],
                }
            },
            "egress_endpoints": {},
            "digest": "sha256:" + "0" * 64,
        }
        patch_model = PatchDocument.model_validate(patch_raw)
        patch_raw["digest"] = compute_digest(patch_model)
        result = await gateway.apply_patch(patch_raw)
        return {
            "ok": True,
            "scenario": "single-session-egress-rebuild",
            "result": result,
            "state": gateway.public_state(),
        }

    @app.post("/api/v1/lab/scenarios/pressure")
    async def scenario_pressure() -> dict[str, Any]:
        state = gateway.public_state()
        snapshot = state.get("snapshot")
        if snapshot is None:
            raise GatewayError("E_SNAPSHOT_INVALID", "no active snapshot", {})
        sessions = state["sessions"]
        required = ("alpha", "beta", "gamma", "delta", "epsilon", "zeta")
        missing = [name for name in required if name not in sessions]
        if missing:
            raise GatewayError(
                "E_SNAPSHOT_INVALID",
                "pressure scenario requires the fixed sample session set",
                {"missing_sessions": missing},
            )

        def frame_for(session_id: str, *, delay_ms: int = 0) -> FrameSubmission:
            next_seq = sessions[session_id]["next_expected_seq"]
            payload: dict[str, Any] = {
                "op": "echo",
                "body": f"{session_id}-pressure-{next_seq}",
            }
            if delay_ms:
                payload["options"] = {"delay_ms": delay_ms}
            return FrameSubmission(
                frame_id=f"pressure-{session_id}-{next_seq}-{uuid.uuid4().hex[:6]}",
                session_id=session_id,
                sequence=next_seq,
                payload=payload,
            )

        active_frames = [frame_for(name, delay_ms=650) for name in ("alpha", "beta", "gamma")]
        active_tasks = [asyncio.create_task(gateway.submit_frame(item)) for item in active_frames]
        await asyncio.sleep(0.03)
        waiting_frames = [frame_for(name) for name in ("delta", "epsilon")]
        waiting_tasks = [asyncio.create_task(gateway.submit_frame(item)) for item in waiting_frames]
        await asyncio.sleep(0.03)
        overflow_frame = frame_for("zeta")
        overflow_task = asyncio.create_task(gateway.submit_frame(overflow_frame))
        submitted_frames = active_frames + waiting_frames + [overflow_frame]
        tasks = active_tasks + waiting_tasks + [overflow_task]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        classified = [
            _classify_result(result, submitted)
            for result, submitted in zip(results, submitted_frames)
        ]
        return {
            "ok": True,
            "scenario": "bounded-channel-queue-pressure",
            "results": classified,
            "overflow_code": next(
                (
                    item.get("error", {}).get("code")
                    for item in classified
                    if not item.get("ok")
                ),
                None,
            ),
            "state": gateway.public_state(),
        }

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return app


async def _gather_frame_results(
    gateway: Gateway, frames: list[FrameSubmission]
) -> list[dict[str, Any]]:
    async def one(frame: FrameSubmission) -> dict[str, Any]:
        try:
            result = await gateway.submit_frame(frame)
            return {"frame_id": frame.frame_id, "ok": True, "result": result}
        except GatewayError as exc:
            return {"frame_id": frame.frame_id, **exc.as_payload()}

    return list(await asyncio.gather(*(one(frame) for frame in frames)))


def _classify_result(result: Any, frame: FrameSubmission) -> dict[str, Any]:
    if isinstance(result, GatewayError):
        return {
            "ok": False,
            "frame_id": frame.frame_id,
            "session_id": frame.session_id,
            "sequence": frame.sequence,
            **result.as_payload(),
        }
    if isinstance(result, Exception):
        return {
            "ok": False,
            "frame_id": frame.frame_id,
            "session_id": frame.session_id,
            "sequence": frame.sequence,
            "error": {
                "code": "E_INTERNAL",
                "message": str(result),
                "context": {"exception": type(result).__name__},
            },
        }
    return {
        "ok": True,
        "frame_id": result["frame_id"],
        "session_id": result["session_id"],
        "sequence": result["sequence"],
        "channel_id": result["channel_id"],
        "endpoint": result["response"].get("endpoint"),
    }


def _now_iso() -> str:
    from .store import utc_now

    return utc_now()


