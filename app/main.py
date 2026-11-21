from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app import __version__
from app.config import Settings
from app.domain import ROUNDING_NOTE
from app.errors import DomainError
from app.models import (
    BatchIn,
    EventIn,
    PolicyPatchIn,
    PolicySnapshotIn,
    ResumeIn,
    SimulationIn,
    SuspendIn,
    TenantPatchIn,
)
from app.seed import seed_if_empty
from app.service import GatewayService
from app.store import Store


def create_app(settings: Settings | None = None) -> FastAPI:
    runtime_settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        store = Store(runtime_settings.database_path)
        service = GatewayService(store, runtime_settings)
        application.state.store = store
        application.state.service = service
        if runtime_settings.auto_recover:
            service.recover_pending("startup")
        application.state.seed_result = None
        if runtime_settings.auto_seed:
            application.state.seed_result = seed_if_empty(service, runtime_settings.samples_dir)
        try:
            yield
        finally:
            store.close()

    application = FastAPI(
        title="Local Object Gateway Quota Control Plane",
        version=__version__,
        description=(
            "Deterministic tenant quota, hierarchical token-bucket and policy snapshot "
            "operations backed by SQLite. All endpoints are intended for loopback/local use."
        ),
        lifespan=lifespan,
    )

    @application.exception_handler(DomainError)
    async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.as_dict())

    @application.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "code": "E_SPEC_INVALID",
                "message": "request body or query parameters failed schema validation",
                "context": {"errors": jsonable_encoder(exc.errors())},
            },
        )

    @application.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "code": "E_SPEC_INVALID",
                "message": str(exc),
                "context": {"exception": type(exc).__name__},
            },
        )

    def service(request: Request) -> GatewayService:
        return request.app.state.service

    @application.get("/")
    def root() -> dict[str, Any]:
        return {
            "name": "local-object-gateway-quota",
            "version": __version__,
            "docs": "/docs",
            "health": "/health",
            "loopback_only": True,
            "accounting": {"scale": 6, "rounding": "ROUND_FLOOR", "note": ROUNDING_NOTE},
        }

    @application.get("/health")
    @application.get("/healthz")
    def health(request: Request) -> dict[str, Any]:
        svc = service(request)
        return {
            "status": "ok",
            "database": str(svc.store.database_path),
            "policy_version": svc.store.current_version(),
            "last_event_ms": svc.store.last_event_ms(),
            "pending_intents": svc.store.scalar(
                "SELECT COUNT(*) FROM intents WHERE status='PENDING'"
            ),
        }

    @application.get("/api/v1/meta")
    def meta(request: Request) -> dict[str, Any]:
        return {
            "accounting": {
                "scale": 6,
                "unit": "1e-6",
                "rounding": "ROUND_FLOOR",
                "note": ROUNDING_NOTE,
                "compensated_aggregation": (
                    "All accepted usage is represented by integer 1e-6 units. Rebuilds sum "
                    "direct integer facts, avoiding large-plus-small binary floating drift."
                ),
            },
            "meter_modes": {
                "BYTES": "deduct capacity bytes only",
                "REQUESTS": "deduct request-weight tokens only",
                "COMPOSITE": "require and atomically deduct both",
            },
            "directions": "READ/WRITE is persisted and echoed; metering is determined by meter_mode.",
            "validation_order": {
                "batch": [
                    "E_SPEC_INVALID",
                    "E_UNKNOWN_TENANT",
                    "E_CLOCK_REWIND",
                    "E_BATCH_ABORTED",
                    "E_QUOTA_EXCEEDED|E_PARENT_QUOTA_EXCEEDED",
                    "E_RATE_LIMITED|E_PARENT_QUOTA_EXCEEDED",
                ],
                "policy_submit": ["E_SPEC_INVALID", "E_STALE_SNAPSHOT", "E_TENANT_CYCLE"],
                "incremental_patch": [
                    "E_SPEC_INVALID",
                    "E_STALE_BASE",
                    "E_UNKNOWN_TENANT",
                    "E_TENANT_CYCLE",
                ],
            },
            "state_machine": {
                "ACTIVE": ["THROTTLED", "QUOTA_BLOCKED", "SUSPENDED"],
                "THROTTLED": ["ACTIVE", "QUOTA_BLOCKED", "SUSPENDED"],
                "QUOTA_BLOCKED": ["ACTIVE", "THROTTLED", "SUSPENDED"],
                "SUSPENDED": ["ACTIVE"],
                "recovery_condition": (
                    "A suspended tenant resumes only by explicit action when no ancestor is "
                    "suspended, remaining capacity > 0, and tokens >= min(1 token, burst)."
                ),
                "automatic_recovery": (
                    "THROTTLED returns to ACTIVE after a successful admission; QUOTA_BLOCKED "
                    "returns after an edit rebuild makes aggregate usage lower than capacity."
                ),
            },
        }

    @application.get("/api/v1/policy")
    @application.get("/api/v1/policies")
    def get_policy(request: Request) -> dict[str, Any]:
        return service(request).get_policy()

    @application.post("/api/v1/policy", status_code=201)
    @application.post("/api/v1/policies", status_code=201)
    def submit_policy(request: Request, payload: PolicySnapshotIn) -> dict[str, Any]:
        return service(request).submit_policy(payload)

    @application.patch("/api/v1/policy")
    @application.patch("/api/v1/policies")
    def patch_policy(request: Request, payload: PolicyPatchIn) -> dict[str, Any]:
        return service(request).patch_policy(payload)

    @application.get("/api/v1/state")
    def get_state(request: Request) -> dict[str, Any]:
        return service(request).get_state()

    @application.get("/api/v1/tenants")
    def list_tenants(request: Request) -> dict[str, Any]:
        return service(request).get_state()

    @application.get("/api/v1/tenants/{tenant_id}")
    def get_tenant(request: Request, tenant_id: str) -> dict[str, Any]:
        return service(request).get_tenant(tenant_id)

    @application.patch("/api/v1/tenants/{tenant_id}")
    def patch_tenant(
        request: Request, tenant_id: str, payload: TenantPatchIn
    ) -> dict[str, Any]:
        return service(request).patch_tenant(tenant_id, payload)

    @application.post("/api/v1/tenants/{tenant_id}/suspend")
    def suspend_tenant(request: Request, tenant_id: str, payload: SuspendIn) -> dict[str, Any]:
        return service(request).suspend_tenant(tenant_id, payload.reason)

    @application.post("/api/v1/tenants/{tenant_id}/resume")
    def resume_tenant(request: Request, tenant_id: str, payload: ResumeIn) -> dict[str, Any]:
        return service(request).resume_tenant(tenant_id, payload.at_ms)

    @application.post("/api/v1/batches", status_code=201)
    def process_batch(request: Request, payload: BatchIn) -> dict[str, Any]:
        return service(request).process_batch(payload)

    @application.get("/api/v1/batches")
    def list_batches(request: Request, limit: int = 50) -> dict[str, Any]:
        return {"batches": service(request).list_batches(limit)}

    @application.get("/api/v1/batches/{batch_id}")
    def get_batch(request: Request, batch_id: str) -> dict[str, Any]:
        return service(request).get_batch(batch_id)

    @application.post("/api/v1/admissions")
    def admit_one(request: Request, payload: EventIn) -> dict[str, Any]:
        svc = service(request)
        result = svc.process_batch(BatchIn(events=[payload]))
        return {
            "accepted": result["events"][0]["accepted"],
            "code": result["events"][0]["code"],
            "result": result["events"][0],
            "batch_id": result["batch_id"],
        }

    @application.post("/api/v1/simulations", status_code=201)
    def simulate(request: Request, payload: SimulationIn) -> dict[str, Any]:
        return service(request).simulate(payload)

    @application.get("/api/v1/simulations/{run_id}")
    def get_simulation(request: Request, run_id: str) -> dict[str, Any]:
        return service(request).get_simulation(run_id)

    @application.post("/api/v1/recover")
    @application.post("/api/v1/admin/recover")
    def recover(request: Request) -> dict[str, Any]:
        return service(request).recover_pending("manual")

    @application.get("/api/v1/recovery/report")
    def recovery_report(request: Request) -> dict[str, Any]:
        return service(request).latest_recovery_report()

    @application.get("/api/v1/intents")
    def list_intents(request: Request, status: str | None = None) -> dict[str, Any]:
        return {"intents": service(request).list_intents(status)}

    @application.get("/api/v1/deductions")
    def list_deductions(
        request: Request, tenant_id: str | None = None, limit: int = 500
    ) -> dict[str, Any]:
        return {"deductions": service(request).list_usage_ledger(tenant_id, limit)}

    @application.get("/api/v1/faults/scenarios")
    def fault_scenarios() -> dict[str, Any]:
        return {
            "deterministic_order": [
                "E_SPEC_INVALID",
                "E_UNKNOWN_TENANT",
                "E_CLOCK_REWIND",
                "E_BATCH_ABORTED",
            ],
            "scenarios": {
                "invalid_payload": {
                    "endpoint": "POST /api/v1/batches",
                    "request": {"events": ["... valid event ...", "..."], "faults": {"invalid_payload": True}},
                    "code": "E_SPEC_INVALID",
                },
                "unknown_tenant": {
                    "endpoint": "POST /api/v1/batches",
                    "request": {"events": ["... valid event ...", "..."], "faults": {"unknown_tenant": True}},
                    "code": "E_UNKNOWN_TENANT",
                },
                "version_regression": {
                    "endpoint": "POST /api/v1/policy",
                    "request": {"version": "<= current", "generated_at_ms": 0, "tenants": ["..."]},
                    "code": "E_STALE_SNAPSHOT",
                },
                "parent_cycle": {
                    "endpoint": "POST /api/v1/policy",
                    "request": {"version": "current+1", "tenants": ["... cycle ..."]},
                    "code": "E_TENANT_CYCLE",
                },
                "clock_rewind": {
                    "endpoint": "POST /api/v1/batches",
                    "request": {"events": ["... valid event ...", "..."], "faults": {"clock_rewind": True}},
                    "code": "E_CLOCK_REWIND",
                },
                "mid_batch_abort": {
                    "endpoint": "POST /api/v1/batches",
                    "request": {"events": ["... valid event ...", "..."], "faults": {"abort_midway": True}},
                    "code": "E_BATCH_ABORTED",
                    "note": "A durable PENDING intent is left for POST /api/v1/recover.",
                },
            },
        }

    return application


app = create_app()
