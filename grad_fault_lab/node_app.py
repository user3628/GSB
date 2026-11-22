from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import Response

from .config import LabConfig
from .errors import AppError, ErrorCode
from .http_errors import install_error_handlers
from .samples import generate_block


def create_node_app(config: LabConfig, node_id: str) -> FastAPI:
    node = config.node(node_id)
    app = FastAPI(title=f"gradient-test-node-{node_id}", version="1.0.0")
    app.state.config = config
    app.state.node = node

    install_error_handlers(app)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "node_id": node_id, "port": node.port}

    @app.get("/v1/blocks/{round_no}/{shard_id}")
    def block(round_no: int, shard_id: int) -> dict[str, Any]:
        if round_no < 0:
            raise AppError(
                code=ErrorCode.BLOCK_VALIDATION_FAILED,
                message="Round must be non-negative",
            )
        try:
            generated = generate_block(config, node, round_no, shard_id)
        except ValueError as exc:
            raise AppError(
                code=ErrorCode.BLOCK_VALIDATION_FAILED,
                message=str(exc),
            ) from exc
        return generated.to_payload()

    @app.get("/debug/hang")
    async def hang() -> dict[str, Any]:
        await asyncio.sleep(config.hang_seconds)
        return {"ok": True, "warning": "the client should have timed out already"}

    @app.get("/debug/truncated")
    def truncated() -> Response:
        declared_body = {
            "ok": True,
            "node_id": node_id,
            "round": 0,
            "shard_id": 0,
            "padding": "x" * 96,
            "note": "this body is deliberately cut before Content-Length bytes arrive",
        }
        encoded = json.dumps(declared_body, separators=(",", ":")).encode()
        emitted = encoded[: max(16, len(encoded) // 2)]
        return Response(
            content=emitted,
            media_type="application/json",
            headers={"content-length": str(len(encoded) + 32), "connection": "close"},
        )

    @app.get("/debug/info")
    def debug_info() -> dict[str, Any]:
        return {
            "node_id": node_id,
            "port": node.port,
            "refused_port": node.refused_port,
            "tensor_shape": list(config.tensor_shape),
            "seed": config.seed,
        }

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one loopback gradient test node")
    parser.add_argument("--config", default="config/default.json")
    parser.add_argument("--node-id", required=True)
    args = parser.parse_args()
    import uvicorn

    config = LabConfig.load(Path(args.config))
    node = config.node(args.node_id)
    uvicorn.run(
        create_node_app(config, node.node_id),
        host=config.host,
        port=node.port,
        log_level="warning",
        access_log=False,
    )


if __name__ == "__main__":
    main()

