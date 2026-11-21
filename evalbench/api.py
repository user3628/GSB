"""Local HTTP API. Error responses carry the same stable business codes
as the CLI: {"error": {"code", "message", "details"}} with HTTP 409 for
checkpoint/stage conflicts and 400 for everything else."""
from __future__ import annotations

import json
import threading
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .checkpoint import CheckpointStore
from .config import load_config
from .errors import EvalBenchError
from .pipeline import Pipeline

_HTTP_STATUS = {
    "E_STALE_CHECKPOINT": 409,
    "E_INVALID_STAGE": 409,
    "E_VERSION_MISMATCH": 400,
    "E_FIT_FAILED": 422,
    "E_CONFIG_INVALID": 400,
    "E_ROUNDTRIP_VIOLATION": 422,
    "E_SHARD_NOT_FOUND": 404,
}


class RunRequest(BaseModel):
    workspace: str
    data_dir: str
    config_path: str
    shard_ids: list[str] | None = None
    unit_delay_ms: int = 0


class StopRequest(BaseModel):
    workspace: str


class PublishRequest(BaseModel):
    workspace: str
    shard_id: str
    record: dict


class UpgradeRequest(BaseModel):
    config_path: str


class RoundtripRequest(BaseModel):
    data_dir: str
    config_path: str


def create_app(default_data_dir: str = "data/shards",
               default_config: str = "data/configs/config_v2.json") -> FastAPI:
    app = FastAPI(title="evalbench", version="0.1.0")
    stop_events: dict[str, threading.Event] = {}

    @app.exception_handler(EvalBenchError)
    async def _business_error(_req: Request, exc: EvalBenchError):
        status = _HTTP_STATUS.get(exc.code, 400)
        return JSONResponse(status_code=status, content=exc.to_dict())

    def _pipeline(workspace: str, config_path: str) -> Pipeline:
        return Pipeline(workspace, load_config(config_path))

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.post("/run")
    def run(req: RunRequest):
        stop_event = stop_events.setdefault(req.workspace, threading.Event())
        stop_event.clear()
        pipe = _pipeline(req.workspace, req.config_path)
        return pipe.run(req.data_dir, stop_event=stop_event,
                        unit_delay_ms=req.unit_delay_ms, shard_ids=req.shard_ids)

    @app.post("/stop")
    def stop(req: StopRequest):
        event = stop_events.setdefault(req.workspace, threading.Event())
        event.set()
        return {"workspace": req.workspace, "stop_requested": True}

    @app.get("/status")
    def status(workspace: str, config_path: str, data_dir: str | None = None):
        pipe = _pipeline(workspace, config_path)
        return pipe.status(data_dir=data_dir)

    @app.get("/exports/reliability_report")
    def export(workspace: str):
        path = Path(workspace) / "exports" / "reliability_report.json"
        if not path.is_file():
            return JSONResponse(status_code=404, content={
                "error": {"code": "E_SHARD_NOT_FOUND",
                          "message": "no export yet; run the pipeline first",
                          "details": {"path": str(path)}}})
        return json.loads(path.read_text(encoding="utf-8"))

    @app.post("/publish")
    def publish(req: PublishRequest):
        store = CheckpointStore(req.workspace)
        record = dict(req.record)
        record.setdefault("shard_id", req.shard_id)
        return store.publish_record(record, target_shard_id=req.shard_id)

    @app.post("/validate/roundtrip")
    def roundtrip(req: RoundtripRequest):
        from . import codec
        cfg = load_config(req.config_path)
        results = {}
        for path in sorted(Path(req.data_dir).glob("*.jsonl")):
            shard_id = path.stem
            count = 0
            with open(path, "r", encoding="utf-8") as fh:
                for line_no, line in enumerate(fh.read().splitlines(), start=1):
                    if not line.strip():
                        continue
                    obj = json.loads(line)
                    version = codec.detect_declared_version(
                        obj, shard_id=shard_id, line_no=line_no)
                    if version == codec.SCHEMA_V2:
                        codec.roundtrip_chain_from_v2(
                            obj, shard_id=shard_id, line_no=line_no,
                            n_bins=cfg.n_bins, rounding=cfg.rounding,
                            id_delimiter=cfg.id_delimiter)
                    else:
                        codec.roundtrip_chain_from_v1(
                            obj, shard_id=shard_id, line_no=line_no,
                            n_bins=cfg.n_bins, rounding=cfg.rounding,
                            id_delimiter=cfg.id_delimiter)
                    count += 1
            results[shard_id] = {"chains_verified": count, "ok": True}
        return {"shards": results}

    @app.post("/configs/upgrade")
    def upgrade(req: UpgradeRequest):
        cfg = load_config(req.config_path)
        return {"upgraded_config": cfg.to_v2_dict(),
                "upgrade_notes": cfg.upgrade_notes_dicts()}

    return app


app = create_app()
