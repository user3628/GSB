"""Local FastAPI HTTP surface bound to the loopback interface."""
from __future__ import annotations

import json

from fastapi import FastAPI, Request
from fastapi.responses import Response

from app.errors import WorkbenchError
from app.models import FormatName, IncrementFragment, RoundingRule
from app.service import OrchestrationService

_STATUS_BY_CODE = {
    "E_FORMAT_INVALID": 422,
    "E_VERSION_UNSUPPORTED": 422,
    "E_SNAPSHOT_INVALID": 422,
    "E_DEPENDENCY_CYCLE": 409,
    "E_STALE_BASE": 409,
    "E_NOT_FOUND": 404,
    "E_CONFLICT": 409,
}


def create_app(service: OrchestrationService) -> FastAPI:
    app = FastAPI(title="Resource Orchestration Workbench", version="1.0.0")

    @app.exception_handler(WorkbenchError)
    async def handle_workbench_error(_: Request, exc: WorkbenchError):
        return Response(
            content=json.dumps(exc.to_dict()).encode("utf-8"),
            status_code=_STATUS_BY_CODE.get(exc.code, 400),
            media_type="application/json",
        )

    @app.get("/health")
    async def health():
        return {"ok": True, "active_snapshot_seq": service.store.active_snapshot_seq()}

    @app.post("/snapshots/import")
    async def import_snapshot(request: Request, rounding_rule: RoundingRule = RoundingRule.HALF_EVEN):
        body = await request.body()
        return service.import_snapshot(body, rounding_rule).model_dump()

    @app.post("/snapshots/dry-run")
    async def dry_run(request: Request, rounding_rule: RoundingRule = RoundingRule.HALF_EVEN):
        body = await request.body()
        return service.dry_run(body, rounding_rule)

    @app.get("/snapshots")
    async def list_snapshots():
        return service.snapshots()

    @app.get("/snapshots/active")
    async def active_snapshot():
        return service.diagnostics()

    @app.get("/snapshots/{snapshot_seq}")
    async def get_snapshot(snapshot_seq: int):
        stats, resources, batches = service.store.load_snapshot(snapshot_seq)
        return {
            "ok": True,
            "stats": stats.model_dump(),
            "resources": [item.model_dump() for item in resources],
            "batches": [item.model_dump() for item in batches],
        }

    @app.get("/snapshots/export/{target_format}")
    async def export_snapshot(target_format: FormatName, snapshot_seq: int | None = None):
        payload = service.export_snapshot(target_format, snapshot_seq)
        media_types = {
            FormatName.BINARY: "application/octet-stream",
            FormatName.JSONL: "application/x-ndjson",
            FormatName.YAML: "text/yaml",
        }
        return Response(content=payload, media_type=media_types[target_format])

    @app.get("/diagnostics/impact/{resource_id}")
    async def impact(resource_id: str):
        return service.impact(resource_id)

    @app.post("/increments")
    async def apply_increment(fragment: IncrementFragment):
        return service.apply_increment(fragment).model_dump()

    @app.post("/rebuild")
    async def rebuild(submit_id: str):
        return service.rebuild_layers(submit_id)

    @app.get("/recovery")
    async def recovery():
        return service.recovery_report()

    return app
