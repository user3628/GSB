"""本地 HTTP 接口（仅绑定 127.0.0.1，端口可配置）。

端点：
* POST /samples/import        导入样本目录
* POST /rebuild               触发分阶段重建（可 stop_after）
* GET  /stages                阶段状态与版本指纹
* GET  /invalidation          失效原因
* POST /stop                  请求优雅停止
* POST /recover               执行恢复
* GET  /query/{type}          交互查询（带延迟预算）
* GET  /export                导出点云与验证摘要信息

所有业务异常统一渲染为 ``{"error": {...}}`` 并带对应 HTTP 状态码。
"""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .canonical import read_json
from .errors import ReconError
from .generator import generate_samples
from .pipeline import Pipeline
from .query import QueryService

app = FastAPI(title="recon3d", version="1.0.0")
STATE: dict[str, Pipeline] = {}


class ImportRequest(BaseModel):
    sample_dir: str = "samples"
    run_dir: str = "runs/api"
    generate_if_missing: bool = True
    seed: int = 20261121


class RebuildRequest(BaseModel):
    sample_dir: str = "samples"
    run_dir: str = "runs/api"
    force: bool = False
    stop_after: str | None = None


class StopRequest(BaseModel):
    run_dir: str = "runs/api"
    sample_dir: str = "samples"


class RecoverRequest(BaseModel):
    sample_dir: str = "samples"
    run_dir: str = "runs/api"


class QueryRequest(BaseModel):
    sample_dir: str = "samples"
    run_dir: str = "runs/api"
    query_type: str
    budget: float = 1.0
    query_id: str | None = None
    params: dict = Field(default_factory=dict)


def _raise(error: ReconError) -> None:
    raise HTTPException(status_code=error.http_status, detail=error.to_dict())


def _pipeline(sample_dir: str, run_dir: str) -> Pipeline:
    return Pipeline(sample_dir, run_dir)


@app.get("/health")
def health():
    return {"status": "ok", "service": "recon3d"}


@app.post("/samples/import")
def import_samples(req: ImportRequest):
    manifest_path = os.path.join(req.sample_dir, "manifest.json")
    if req.generate_if_missing and not os.path.exists(manifest_path):
        generate_samples(req.sample_dir, req.seed)
    if not os.path.exists(manifest_path):
        raise HTTPException(status_code=404, detail={"code": "E_NOT_FOUND",
                          "message": "manifest.json not found", "sample_dir": req.sample_dir})
    manifest = read_json(manifest_path)
    pipeline = _pipeline(req.sample_dir, req.run_dir)
    STATE[req.run_dir] = pipeline
    return {
        "sample_fingerprint": pipeline.sample_fp,
        "tiles": len(manifest["tiles"]),
        "truth_points": len(manifest["truth_points"]),
        "run_dir": req.run_dir,
    }


@app.post("/rebuild")
def rebuild(req: RebuildRequest):
    pipeline = _pipeline(req.sample_dir, req.run_dir)
    try:
        return pipeline.run(stop_after=req.stop_after, force=req.force)
    except ReconError as error:
        _raise(error)


@app.get("/stages")
def stages(sample_dir: str = "samples", run_dir: str = "runs/api"):
    pipeline = _pipeline(sample_dir, run_dir)
    return {
        "sample_fingerprint": pipeline.sample_fp,
        "config_version": pipeline.manifest["config_version"],
        "stages": pipeline.stage_fingerprints(),
        "journal": pipeline.store.stage_status(),
    }


@app.get("/invalidation")
def invalidation(sample_dir: str = "samples", run_dir: str = "runs/api",
                 previous_fingerprints: str | None = None):
    pipeline = _pipeline(sample_dir, run_dir)
    if not previous_fingerprints or not os.path.exists(previous_fingerprints):
        return {"reasons": [], "note": "provide previous_fingerprints file path"}
    import json

    previous = json.loads(open(previous_fingerprints, "rb").read())
    return {"reasons": pipeline.invalidation_reason(previous)}


@app.post("/stop")
def stop(req: StopRequest):
    pipeline = _pipeline(req.sample_dir, req.run_dir)
    pipeline.request_stop()
    return {"stop_requested": True, "run_dir": req.run_dir}


@app.post("/recover")
def recover(req: RecoverRequest):
    pipeline = _pipeline(req.sample_dir, req.run_dir)
    try:
        return pipeline.run(force=False)
    except ReconError as error:
        _raise(error)


@app.post("/query")
def query(req: QueryRequest):
    pipeline = _pipeline(req.sample_dir, req.run_dir)
    service = QueryService(req.run_dir, pipeline.sample_fp,
                           pipeline.manifest["config_version"], req.budget)
    service.stage_digests = {r["stage"]: r["artifact_digest"]
                             for r in pipeline.store.stage_status()}
    try:
        return service.query(req.query_type, params=req.params,
                             budget=req.budget, query_id=req.query_id)
    except ReconError as error:
        _raise(error)


@app.get("/export")
def export(run_dir: str = "runs/api"):
    ply = os.path.join(run_dir, "exports", "point_cloud.ply")
    summary = os.path.join(run_dir, "exports", "validation_summary.json")
    return {
        "point_cloud": ply,
        "validation_summary": summary,
        "exists": os.path.exists(ply) and os.path.exists(summary),
    }
