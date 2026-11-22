from __future__ import annotations

import argparse
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .config import LabConfig
from .errors import HTTP_STATUS, AppError, ErrorCode
from .http_errors import install_error_handlers
from .orchestrator import BatchOrchestrator, BatchRequest
from .store import Store


SUCCESS_CODES = {ErrorCode.OK, ErrorCode.DEGRADED_AGGREGATION}


def batch_response_status(record: dict[str, Any]) -> int:
    code = ErrorCode(record["final_code"])
    if code in SUCCESS_CODES:
        return 200
    return HTTP_STATUS.get(code, 500)


def batch_response_body(record: dict[str, Any]) -> dict[str, Any]:
    code = ErrorCode(record["final_code"])
    if code in SUCCESS_CODES:
        return {"ok": True, "data": record}
    error = AppError(
        code,
        f"Batch finished in {record['state']} with business code {code.value}",
        {
            "batch_id": record["batch_id"],
            "state": record["state"],
            "final_code": record["final_code"],
            "fault_codes": [fault["code"] for fault in record.get("faults", [])],
            "aggregation_status": (record.get("aggregation") or {}).get("status"),
        },
        retryable=False,
    )
    return error.to_envelope(batch_id=record["batch_id"])


def create_validator_app(
    config: LabConfig,
    *,
    store: Store | None = None,
    recover_on_start: bool = True,
) -> FastAPI:
    store = store or Store(config.database_path)
    orchestrator = BatchOrchestrator(config, store)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.recovery_reports = (
            store.recover_incomplete() if recover_on_start else []
        )
        yield

    app = FastAPI(title="gradient-fault-verification-bench", version="1.0.0", lifespan=lifespan)
    app.state.config = config
    app.state.store = store
    app.state.orchestrator = orchestrator
    app.state.recovery_reports = []

    install_error_handlers(app)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "validator",
            "host": config.host,
            "validator_port": config.validator_port,
            "database_path": str(config.database_path),
            "recovered_this_start": len(app.state.recovery_reports),
        }

    @app.get("/v1/config")
    def public_config() -> dict[str, Any]:
        return {"ok": True, "data": config.public_dict()}

    @app.post("/v1/batches")
    def create_batch(request: BatchRequest) -> JSONResponse:
        record = orchestrator.execute(request)
        return JSONResponse(
            status_code=batch_response_status(record),
            content=batch_response_body(record),
        )

    @app.get("/v1/batches/{batch_id}")
    def get_batch(batch_id: str) -> dict[str, Any]:
        record = store.get_final_batch(batch_id)
        return {"ok": True, "data": record}

    @app.get("/v1/batches/{batch_id}/faults")
    def get_batch_faults(batch_id: str) -> dict[str, Any]:
        record = store.get_final_batch(batch_id)
        return {
            "ok": True,
            "data": {
                "batch_id": batch_id,
                "state": record["state"],
                "faults": record["faults"],
                "retry_sequence": record["retry_sequence"],
                "aggregation": record["aggregation"],
            },
        }

    @app.get("/v1/batches")
    def list_batches(limit: int = 100) -> dict[str, Any]:
        if not 1 <= limit <= 1000:
            raise AppError(
                ErrorCode.REQUEST_VALIDATION,
                "limit must be between 1 and 1000",
                {"limit": limit},
            )
        return {"ok": True, "data": store.list_final_batches(limit)}

    @app.get("/v1/recoveries")
    def list_recoveries() -> dict[str, Any]:
        return {"ok": True, "data": store.list_recovery_reports()}

    @app.get("/v1/diagnostics/error-codes")
    def error_codes() -> dict[str, Any]:
        return {
            "ok": True,
            "data": [
                {"code": code.value, "http_status": HTTP_STATUS[code]}
                for code in ErrorCode
            ],
        }

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the loopback gradient verification bench")
    parser.add_argument("--config", default="config/default.json")
    args = parser.parse_args()
    import uvicorn

    config = LabConfig.load(Path(args.config))
    uvicorn.run(
        create_validator_app(config),
        host=config.host,
        port=config.validator_port,
        log_level="warning",
        access_log=False,
    )


if __name__ == "__main__":
    main()

