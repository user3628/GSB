from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .formats import (
    canonical_format_name,
    decode_format,
    encode_format,
)
from .models import CURRENT_SCHEMA_VERSION
from .pipeline import Pipeline
from .errors import REEError


class ImportRequest(BaseModel):
    kind: str
    paths: list[str]


class RunRequest(BaseModel):
    stop_after_stage: str | None = None


class ConvertRequest(BaseModel):
    doc_id: str
    from_format: str
    to_format: str


class CompatRequest(BaseModel):
    payload: dict[str, Any]


class RoundtripRequest(BaseModel):
    doc_id: str


def create_app(root: str | Path | None = None) -> FastAPI:
    app = FastAPI(
        title="REE Interoperability Pipeline",
        version="1.0.0",
        description="Local deterministic relation/event extraction and format compatibility service.",
    )
    workspace_root = Path(root).resolve() if root is not None else Path.cwd().resolve()
    app.state.pipeline = Pipeline(workspace_root)

    @app.exception_handler(REEError)
    async def ree_error_handler(_request: Request, exc: REEError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"error": exc.as_dict()})

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "service": "ree-pipeline",
            "schema_version": CURRENT_SCHEMA_VERSION,
            "workspace": ".",
        }

    @app.post("/import")
    def import_inputs(request: ImportRequest) -> dict[str, Any]:
        return app.state.pipeline.catalog.import_files(request.kind, request.paths)

    @app.get("/status")
    def status() -> dict[str, Any]:
        return app.state.pipeline.status()

    @app.get("/fingerprints")
    def fingerprints() -> dict[str, Any]:
        state = app.state.pipeline.status()
        return {
            "config_version": state["config_version"],
            "last_checkpoint_seq": state["last_checkpoint_seq"],
            "stages": {
                stage: {
                    "state": item["state"],
                    "fingerprint": item["fingerprint"],
                    "expected_fingerprint": item["expected_fingerprint"],
                    "checkpoint_seq": item["checkpoint_seq"],
                }
                for stage, item in state["stages"].items()
            },
            "invalidations": state["invalidations"],
        }

    @app.post("/run")
    def run(request: RunRequest) -> dict[str, Any]:
        return app.state.pipeline.run(stop_after_stage=request.stop_after_stage)

    @app.post("/resume")
    def resume(request: RunRequest) -> dict[str, Any]:
        return app.state.pipeline.resume(stop_after_stage=request.stop_after_stage)

    @app.post("/stop")
    def stop() -> dict[str, Any]:
        payload = app.state.pipeline.request_stop()
        return {"status": "stop_requested", **payload}

    @app.post("/run-stage/{stage}")
    def run_stage(stage: str) -> dict[str, Any]:
        return app.state.pipeline.run_stage(stage)

    @app.get("/artifacts/{stage}")
    def artifact(stage: str) -> dict[str, Any]:
        return app.state.pipeline.get_stage_artifact(stage)

    @app.get("/results")
    def results(doc_id: str | None = None) -> dict[str, Any]:
        if doc_id is not None:
            document = app.state.pipeline.get_document(doc_id)
            return {"schema_version": 2, "document": document.model_dump(mode="json")}
        artifact = app.state.pipeline.get_stage_artifact("extract")
        return artifact

    @app.get("/export")
    def export_ir() -> dict[str, Any]:
        artifact = app.state.pipeline.get_stage_artifact("extract")
        return {
            "schema_version": 2,
            "format": "UNIFIED_IR",
            "documents": artifact["documents"],
            "summary": artifact["summary"],
        }

    @app.get("/diagnostics")
    def diagnostics() -> dict[str, Any]:
        pipeline = app.state.pipeline
        parse_artifact = pipeline.get_stage_artifact("parse")
        validate_artifact = pipeline.get_stage_artifact("validate")
        failed_checks = [
            {
                "doc_id": check["doc_id"],
                "chains": [chain for chain in check["chains"] if not chain["ok"]],
            }
            for check in validate_artifact["checks"]
            if not check["ok"]
        ]
        return {
            "parse": parse_artifact["diagnostics"],
            "parse_summary": parse_artifact["summary"],
            "validation_summary": validate_artifact["summary"],
            "validation_failures": failed_checks,
            "upgrades": {
                check["doc_id"]: {
                    "source_annotation_version": check["source_annotation_version"],
                    "source_upgrades": check["source_upgrades"],
                    "roundtrip_upgrades": sorted(
                        {
                            upgrade
                            for chain in check["chains"]
                            for upgrade in chain.get("upgrades", [])
                        }
                    ),
                }
                for check in validate_artifact["checks"]
                if check["source_annotation_version"] == 1
            },
            "invalidations": pipeline.status()["invalidations"],
        }

    @app.post("/convert")
    def convert(request: ConvertRequest) -> dict[str, Any]:
        pipeline = app.state.pipeline
        source_format = canonical_format_name(request.from_format)
        target_format = canonical_format_name(request.to_format)
        document = pipeline.get_document(request.doc_id)
        source_payload = encode_format(source_format, document)
        decoded = decode_format(source_format, source_payload, pipeline.load_config())
        converted = encode_format(target_format, decoded.document)
        return {
            "doc_id": request.doc_id,
            "from_format": source_format,
            "to_format": target_format,
            "diagnostics": decoded.diagnostics,
            "upgrades": decoded.upgrades,
            "payload": converted,
        }

    @app.post("/roundtrip")
    def roundtrip(request: RoundtripRequest) -> dict[str, Any]:
        pipeline = app.state.pipeline
        validation = pipeline.get_stage_artifact("validate")
        for check in validation["checks"]:
            if check["doc_id"] == request.doc_id:
                return check
        raise REEError(
            "E_DOCUMENT_NOT_FOUND",
            "query",
            f"unknown validation document: {request.doc_id}",
            doc_id=request.doc_id,
        )

    @app.post("/compat/check")
    def compat_check(request: CompatRequest) -> dict[str, Any]:
        decoded = decode_format(
            request.payload.get("format", "JSON_INTERVALS"),
            request.payload,
            app.state.pipeline.load_config(),
        )
        return {
            "ok": True,
            "source_version": decoded.source_version,
            "upgrades": decoded.upgrades,
            "diagnostics": decoded.diagnostics,
            "document": decoded.document.model_dump(mode="json"),
        }

    return app


app = create_app()
