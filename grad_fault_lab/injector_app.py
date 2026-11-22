from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from .config import LabConfig
from .errors import AppError, ErrorCode
from .http_errors import install_error_handlers
from .injection import normal_submission, perform_injection, validate_injection_against_config
from .models import InjectionSpec


class InjectionRequest(BaseModel):
    spec: dict[str, Any]
    attempt_no: int = Field(default=1, ge=1, le=20)
    block_count: int = Field(default=2, ge=1, le=32)


class NormalSubmissionRequest(BaseModel):
    node_id: str
    round: int = Field(ge=0)
    block_count: int = Field(default=2, ge=1, le=32)


def create_injector_app(config: LabConfig) -> FastAPI:
    app = FastAPI(title="gradient-fault-injector", version="1.0.0")
    app.state.config = config

    install_error_handlers(app, request_validation_code=ErrorCode.INJECTION_SPEC_INVALID)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "service": "injector", "host": config.host}

    @app.get("/v1/nodes")
    def nodes() -> dict[str, Any]:
        return {
            "ok": True,
            "nodes": [
                {
                    "node_id": node.node_id,
                    "port": node.port,
                    "refused_port": node.refused_port,
                }
                for node in config.nodes
            ],
        }

    @app.post("/v1/inject")
    def inject(request: InjectionRequest) -> dict[str, Any]:
        spec = InjectionSpec.from_payload(request.spec)
        validate_injection_against_config(config, spec, spec.round)
        if request.block_count != config.shards_per_node:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "block_count must equal shards_per_node",
                {
                    "block_count": request.block_count,
                    "shards_per_node": config.shards_per_node,
                },
            )
        observation = perform_injection(
            config,
            spec,
            attempt_no=request.attempt_no,
            block_count=request.block_count,
        )
        return {
            "ok": True,
            "data": {
                "attempt_no": request.attempt_no,
                "injected": spec.applies_on_attempt(request.attempt_no),
                "spec": spec.to_dict(),
                "observation": observation.to_dict(include_sequence=True),
            },
        }

    @app.post("/v1/submit")
    def submit(request: NormalSubmissionRequest) -> dict[str, Any]:
        if request.node_id not in config.node_ids:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "Submission node is not registered",
                {"node_id": request.node_id},
            )
        if request.block_count != config.shards_per_node:
            raise AppError(
                ErrorCode.INJECTION_SPEC_INVALID,
                "block_count must equal shards_per_node",
                {
                    "block_count": request.block_count,
                    "shards_per_node": config.shards_per_node,
                },
            )
        observation = normal_submission(config, node_id=request.node_id, round_no=request.round)
        return {
            "ok": True,
            "data": {
                "node_id": request.node_id,
                "round": request.round,
                "observation": observation.to_dict(include_sequence=True),
            },
        }

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the loopback fault injection endpoint")
    parser.add_argument("--config", default="config/default.json")
    args = parser.parse_args()
    import uvicorn

    config = LabConfig.load(Path(args.config))
    uvicorn.run(
        create_injector_app(config),
        host=config.host,
        port=config.injector_port,
        log_level="warning",
        access_log=False,
    )


if __name__ == "__main__":
    main()

