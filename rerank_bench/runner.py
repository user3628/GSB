"""High-level run orchestration: schedule, execute, reduce, export."""

from __future__ import annotations

import json
import os

from . import errors
from .checkpoints import atomic_write_json
from .errors import PipelineError
from .executor import execute_shards
from .hashing import file_digest, sha256_hex
from .pipeline import (GLOBAL_SHARD, Workspace, aggregate_fingerprint,
                       detect_and_invalidate, shard_fingerprints,
                       _shard_infos)
from .scheduler import plan_schedule
from .stages import SHARD_STAGES, STAGE_ORDER

SUMMARY_SCHEMA = "rerank-bench.summary.v1"


def _enrich_admitted(ws: Workspace, plan: dict) -> list:
    files = {s["shard_id"]: s["file"] for s in ws.manifest()["shards"]}
    return [{**a, "shard_file": files[a["shard_id"]]} for a in plan["admitted"]]


def run_pipeline(ws: Workspace, serial: bool = False, stage_limit: str = None,
                 repo_root: str = None, stop_after: int = None) -> dict:
    """Run the pipeline. Returns a run report (operational, not exported)."""
    if stage_limit is not None and stage_limit not in STAGE_ORDER:
        raise PipelineError(
            errors.E_INVALID_STAGE, f"unknown stage {stage_limit}",
            stage=stage_limit)
    config = ws.config()
    invalidations = detect_and_invalidate(ws, config)
    plan = plan_schedule(_shard_infos(ws), config)
    admitted = _enrich_admitted(ws, plan)
    shard_stage_limit = stage_limit if stage_limit in SHARD_STAGES else None
    report = {
        "mode": "serial" if serial else "parallel",
        "stage_limit": stage_limit,
        "invalidations": invalidations,
        "plan": plan,
        "stopped": False,
        "pending": [],
        "shard_results": {},
        "aggregated": False,
    }
    if shard_stage_limit is None or shard_stage_limit in SHARD_STAGES:
        upto = (SHARD_STAGES.index(shard_stage_limit) + 1
                if shard_stage_limit else len(SHARD_STAGES))
        stages = SHARD_STAGES[:upto]
        outcome = execute_shards(
            ws, admitted, config.raw, config.shard_latency_budget_ms,
            plan["concurrency"], ws.stop_requested,
            serial=serial, repo_root=repo_root, stages=stages,
            stop_after=stop_after)
        report["shard_results"] = outcome["results"]
        report["stopped"] = outcome["stopped"]
        report["pending"] = outcome["pending"]
    want_aggregate = stage_limit in (None, "aggregate")
    if want_aggregate and not report["stopped"] and not report["pending"]:
        report["aggregated"] = aggregate_and_export(
            ws, config, plan, report["shard_results"])
    state = ws.load_state()
    state["last_plan"] = plan
    state["last_run"] = {
        "mode": report["mode"],
        "stopped": report["stopped"],
        "pending": report["pending"],
        "aggregated": report["aggregated"],
        "invalidations": invalidations,
        "shard_results": report["shard_results"],
    }
    ws.save_state(state)
    return report


def aggregate_and_export(ws: Workspace, config, plan: dict,
                         shard_results: dict = None) -> bool:
    """Deterministic reduction: sort by shard index, then candidate rank."""
    manifest = ws.manifest()
    index_of = {s["shard_id"]: s["shard_index"] for s in manifest["shards"]}
    if shard_results is None:
        state = ws.load_state()
        shard_results = state.get("last_run", {}).get("shard_results", {})
    shard_fps = {
        s["shard_id"]: shard_fingerprints(
            file_digest(ws.shard_path(s["file"])), config)
        for s in manifest["shards"]
    }
    agg_fp = aggregate_fingerprint(shard_fps, plan, config)
    summary_shards = []
    consumed = 0
    run_errors = []
    for entry in sorted(plan["admitted"], key=lambda a: a["shard_index"]):
        shard_id = entry["shard_id"]
        result = shard_results.get(shard_id, {})
        if result.get("status") == "error":
            summary_shards.append({
                "shard_id": shard_id,
                "shard_index": index_of[shard_id],
                "status": "error",
                "error": result["error"],
            })
            run_errors.append({"shard_id": shard_id, **result["error"]})
            continue
        artifact = ws.store.get_artifact(shard_id, "rerank")
        if artifact is None or not ws.store.is_valid(
                shard_id, "rerank", shard_fps[shard_id]["rerank"]):
            raise PipelineError(
                errors.E_INVALID_STAGE,
                f"rerank artifact for {shard_id} missing or invalid; "
                f"cannot aggregate",
                shard_id=shard_id, stage="aggregate")
        groups = []
        for group in artifact["query_groups"]:
            groups.append({
                "query_id": group["query_id"],
                "locale": group["locale"],
                "channel": group["channel"],
                "effective_window": group["effective_window"],
                "window_truncated": group["window_truncated"],
                "results": [{
                    "rank": item["rank"],
                    "doc_id": item["doc_id"],
                    "category": item["category"],
                    "language": item["language"],
                    "precision_score": item["precision_score"],
                } for item in group["selected"]],
            })
            consumed += len(group["selected"])
        summary_shards.append({
            "shard_id": shard_id,
            "shard_index": index_of[shard_id],
            "status": "done",
            "query_groups": groups,
        })
    unprocessed = [
        {
            "shard_id": r["shard_id"],
            "shard_index": r["shard_index"],
            "code": r["code"],
            "reason": r["reason"],
        }
        for r in sorted(plan["rejected"], key=lambda x: x["shard_index"])
    ]
    summary = {
        "schema": SUMMARY_SCHEMA,
        "config_version": config.config_version,
        "budget": {
            "global_eval_budget": plan["global_eval_budget"],
            "reserved_evaluations": plan["reserved_evaluations"],
            "consumed_evaluations": consumed,
            "concurrency": plan["concurrency"],
        },
        "shards": summary_shards,
        "unprocessed": unprocessed,
        "errors": run_errors,
    }
    ws.store.publish(GLOBAL_SHARD, "aggregate", agg_fp,
                     config.config_version, summary)
    blob = json.dumps(summary, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")) + "\n"
    export_path = os.path.join(ws.export_dir, "summary.json")
    tmp = f"{export_path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(blob)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, export_path)
    digest = sha256_hex(blob)
    atomic_write_json(os.path.join(ws.export_dir, "summary.digest.json"),
                      {"sha256": digest, "schema": SUMMARY_SCHEMA})
    return True


def export_summary(ws: Workspace) -> dict:
    path = os.path.join(ws.export_dir, "summary.json")
    if not os.path.exists(path):
        raise PipelineError(
            errors.E_INVALID_STAGE,
            "aggregate stage has not completed; summary unavailable",
            stage="aggregate")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def status(ws: Workspace) -> dict:
    config = ws.config()
    manifest = ws.manifest()
    plan = plan_schedule(_shard_infos(ws), config)
    shard_fps = {
        s["shard_id"]: shard_fingerprints(
            file_digest(ws.shard_path(s["file"])), config)
        for s in manifest["shards"]
    }
    shards = []
    for shard in manifest["shards"]:
        shard_id = shard["shard_id"]
        stages = {}
        for stage in SHARD_STAGES:
            rec = ws.store.get_record(shard_id, stage)
            expected = shard_fps[shard_id][stage]
            stages[stage] = {
                "status": ("valid" if ws.store.is_valid(shard_id, stage, expected)
                           else ("stale" if rec else "pending")),
                "expected_fingerprint": expected,
                "recorded_fingerprint": (rec or {}).get("input_fingerprint"),
            }
        shards.append({
            "shard_id": shard_id,
            "shard_index": shard["shard_index"],
            "candidates": shard["candidates"],
            "stages": stages,
        })
    state = ws.load_state()
    last_run = state.get("last_run", {})
    agg_rec = ws.store.get_record(GLOBAL_SHARD, "aggregate")
    return {
        "shards": shards,
        "plan": plan,
        "aggregate": {
            "status": "done" if agg_rec else "pending",
            "record": agg_rec,
        },
        "stop_requested": ws.stop_requested(),
        "last_run": {
            "mode": last_run.get("mode"),
            "stopped": last_run.get("stopped"),
            "pending": last_run.get("pending"),
            "aggregated": last_run.get("aggregated"),
            "invalidations": last_run.get("invalidations", []),
            "shard_results": last_run.get("shard_results", {}),
        },
    }


def rerank_trace(ws: Workspace, shard_id: str) -> dict:
    artifact = ws.store.get_artifact(shard_id, "rerank")
    if artifact is None:
        raise PipelineError(
            errors.E_INVALID_STAGE,
            f"rerank artifact for {shard_id} not available",
            shard_id=shard_id, stage="rerank")
    return {
        "shard_id": shard_id,
        "query_groups": [{
            "query_id": g["query_id"],
            "requested_window": g["requested_window"],
            "effective_window": g["effective_window"],
            "window_truncated": g["window_truncated"],
            "trace": g["trace"],
        } for g in artifact["query_groups"]],
    }
