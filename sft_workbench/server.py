"""Local HTTP interface for the instruction-data workbench.

Exposes the library pipeline over JSON endpoints: sample import, staged
execution, quality/mixture queries, version fingerprint queries, stop and
resume requests, and mixture export.  Business errors are returned as
``{"error": {"code", "message", "details"}}`` with the same code/message
that is written to the logs.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from .checkpoint import STAGES
from .errors import WorkbenchError
from .pipeline import Pipeline, PipelineConfig

logger = logging.getLogger("sft_workbench.server")


def create_app(workspace: str | Path, config: PipelineConfig | None = None) -> FastAPI:
    pipeline = Pipeline(workspace, config=config)
    app = FastAPI(title="sft-workbench", version="1.0.0")

    @app.exception_handler(WorkbenchError)
    async def workbench_error_handler(_request: Request, exc: WorkbenchError) -> JSONResponse:
        exc.log()
        return JSONResponse(status_code=409, content=exc.to_dict())

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/samples/import")
    def import_samples(payload: dict[str, Any]) -> dict[str, Any]:
        shards = payload.get("shards")
        if not isinstance(shards, list) or not shards:
            raise WorkbenchError(
                "E_INVALID_INPUT",
                "request body must contain a non-empty 'shards' list",
                {"field": "shards"},
            )
        return pipeline.run(shard_paths=[str(p) for p in shards], until="import")

    @app.post("/stages/run")
    def run_stage(payload: dict[str, Any]) -> dict[str, Any]:
        stage = str(payload.get("stage", "all"))
        until = "export" if stage == "all" else stage
        shards = payload.get("shards")
        return pipeline.run(
            shard_paths=[str(p) for p in shards] if shards else None,
            until=until,
        )

    @app.post("/stop")
    def request_stop() -> dict[str, str]:
        pipeline.request_stop()
        return {"status": "stop_requested"}

    @app.post("/resume")
    def resume() -> dict[str, Any]:
        return pipeline.resume()

    @app.get("/quality")
    def quality() -> dict[str, Any]:
        path = pipeline.store.artifacts_dir / "quality.json"
        if not path.exists():
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "quality artifact is missing; run the score stage first",
                {"stage": "score", "missing_artifact": "quality.json"},
            )
        import json

        return json.loads(path.read_bytes().decode("utf-8"))

    @app.get("/mixture")
    def mixture() -> dict[str, Any]:
        path = pipeline.store.artifacts_dir / "allocation.json"
        if not path.exists():
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "allocation artifact is missing; run the sample stage first",
                {"stage": "sample", "missing_artifact": "allocation.json"},
            )
        import json

        return json.loads(path.read_bytes().decode("utf-8"))

    @app.get("/export/mixture")
    def export_mixture() -> PlainTextResponse:
        path = pipeline.store.artifacts_dir / "mixture.jsonl"
        if not path.exists():
            raise WorkbenchError(
                "E_INVALID_STAGE",
                "mixture artifact is missing; run the export stage first",
                {"stage": "export", "missing_artifact": "mixture.jsonl"},
            )
        return PlainTextResponse(
            path.read_bytes().decode("utf-8"), media_type="application/x-ndjson"
        )

    @app.get("/fingerprint")
    def fingerprint() -> dict[str, Any]:
        head = pipeline.store.head()
        return {
            "version_fingerprint": (head or {}).get("version_fingerprint", ""),
            "head": head,
            "stages": list(STAGES),
        }

    @app.get("/checkpoints")
    def checkpoints() -> dict[str, Any]:
        verification = pipeline.store.verify()
        return {
            "verification": verification.to_dict(),
            "records": [record.to_dict() for record in pipeline.store.list_records()],
        }

    @app.get("/errors")
    def errors() -> dict[str, Any]:
        import json

        result: dict[str, Any] = {"import_errors": [], "dedup_errors": []}
        import_path = pipeline.store.artifacts_dir / "import_errors.json"
        if import_path.exists():
            result["import_errors"] = json.loads(import_path.read_bytes().decode("utf-8"))["errors"]
        dedup_path = pipeline.store.artifacts_dir / "dedup_summary.json"
        if dedup_path.exists():
            result["dedup_errors"] = json.loads(dedup_path.read_bytes().decode("utf-8"))["errors"]
        return result

    return app


app = create_app(os.environ.get("SFT_WORKSPACE", "workspace"))


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("SFT_PORT", "8377")))


if __name__ == "__main__":
    main()
