from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import ConfigHubError
from .service import ConfigService


class ClientBody(BaseModel):
    client_id: str
    config_keys: list[str]
    endpoint_url: str
    timeout_seconds: float = 1.0
    sample_seed: int = 20240501
    sample_size: int = 64
    feature_count: int = 3


class SnapshotBody(BaseModel):
    version: int
    config: dict[str, float]
    digest: str | None = None
    checksum: str | None = None
    generated_at: str | None = None


class PushBody(BaseModel):
    version: int
    client_ids: list[str] | None = None
    timeout_seconds: float | None = Field(default=None, gt=0, le=60)
    content_tag: str = "default"


class AckBody(BaseModel):
    client_id: str
    version: int
    result: dict[str, Any]
    batch_id: str | None = None


def create_app(
    db_path: str | Path,
    *,
    recover_on_start: bool = True,
    service: ConfigService | None = None,
) -> FastAPI:
    core = service or ConfigService(db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if recover_on_start:
            app.state.startup_recovery = core.recover()
        else:
            app.state.startup_recovery = None
        app.state.service = core
        yield

    app = FastAPI(title="Local Config Hot-Reload Middleware", version="1.0", lifespan=lifespan)
    app.state.service = core
    app.state.db_path = str(core.storage.db_path)
    app.state.startup_recovery = None

    @app.exception_handler(ConfigHubError)
    async def config_error_handler(_request: Request, exc: ConfigHubError) -> JSONResponse:
        payload = exc.as_dict()
        payload["details"] = exc.details
        return JSONResponse(status_code=exc.status_code, content=payload)

    def svc(request: Request) -> ConfigService:
        return request.app.state.service

    @app.get("/health")
    def health(request: Request) -> dict[str, Any]:
        core_service = svc(request)
        return {
            "ok": True,
            "service": "config-hub",
            "db_path": str(core_service.storage.db_path),
            "current_version": core_service.latest_version(),
            "visibility_violations": core_service.storage.visibility_invariant_violations(),
        }

    @app.post("/clients")
    def register_client(body: ClientBody, request: Request) -> dict[str, Any]:
        return svc(request).register_client(
            body.client_id,
            body.config_keys,
            body.endpoint_url,
            timeout_seconds=body.timeout_seconds,
            sample_seed=body.sample_seed,
            sample_size=body.sample_size,
            feature_count=body.feature_count,
        )

    @app.get("/clients")
    def list_clients(request: Request) -> dict[str, Any]:
        return {"ok": True, "clients": svc(request).list_clients()}

    @app.get("/clients/{client_id}")
    def get_client(client_id: str, request: Request) -> dict[str, Any]:
        client = svc(request).get_client(client_id)
        if client is None:
            raise ConfigHubError("E_NOT_FOUND", "client not found", status_code=404)
        return client

    @app.post("/snapshots")
    def submit_snapshot(body: SnapshotBody, request: Request) -> dict[str, Any]:
        return svc(request).submit_snapshot(
            body.version,
            body.config,
            digest=body.digest if body.digest is not None else body.checksum,
            generated_at=body.generated_at,
        )

    @app.get("/snapshots/latest")
    def latest_snapshot(request: Request) -> dict[str, Any]:
        snapshot = svc(request).latest_snapshot()
        if snapshot is None:
            raise ConfigHubError("E_NOT_FOUND", "no snapshot exists", status_code=404)
        return snapshot

    @app.get("/snapshots/{version}")
    def get_snapshot(version: int, request: Request) -> dict[str, Any]:
        snapshot = svc(request).get_snapshot(version)
        if snapshot is None:
            raise ConfigHubError("E_NOT_FOUND", "snapshot not found", status_code=404)
        return snapshot

    @app.post("/pushes")
    async def push(body: PushBody, request: Request) -> dict[str, Any]:
        return await svc(request).push_version(
            body.version,
            timeout_seconds=body.timeout_seconds,
            client_ids=body.client_ids,
            content_tag=body.content_tag,
        )

    @app.post("/acks")
    def acknowledge(body: AckBody, request: Request) -> dict[str, Any]:
        return svc(request).apply_ack(
            body.client_id,
            body.version,
            body.result,
            batch_id=body.batch_id,
            source="external_http",
        )

    @app.get("/batches/{batch_id}")
    def get_batch(batch_id: str, request: Request) -> dict[str, Any]:
        batch = svc(request).get_batch(batch_id)
        if batch is None:
            raise ConfigHubError("E_NOT_FOUND", "batch not found", status_code=404)
        return batch

    @app.get("/batches/{batch_id}/consistency")
    def consistency(batch_id: str, request: Request) -> dict[str, Any]:
        return svc(request).verify_batch(batch_id)

    @app.post("/recovery/scan")
    def recovery_scan(request: Request) -> dict[str, Any]:
        return svc(request).recover()

    @app.get("/recovery/latest")
    def recovery_latest(request: Request) -> dict[str, Any]:
        report = svc(request).storage.latest_recovery_report()
        if report is None:
            return {"ok": True, "conclusion": "NO_RECOVERY_REPORT"}
        return report

    return app

