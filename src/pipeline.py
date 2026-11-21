"""Pipeline orchestration: staged execution, fingerprint invalidation and
incremental checkpoint reuse.

Stages form a strict linear order:
    ingest -> plan -> schedule -> execute -> checkpoint -> aggregate

Requests that skip a missing prerequisite stage, jump illegally or use an
out-of-range stage name fail with ``E_INVALID_STAGE``.  Each shard input
fingerprint combines the shard content hash and the configuration
fingerprint; a change in data, method, window geometry or budgets changes
the fingerprint of the earliest affected shard and invalidates that shard
plus every downstream shard and the aggregate report.
"""
from __future__ import annotations

import hashlib
import os
from typing import Any, Dict, List, Optional, Tuple

from .checkpoints import (clear_stop, list_checkpoints, publish_checkpoint,
                          read_state, stop_requested, write_state)
from .config import BacktestConfig, canonical_json
from .data import ShardData, load_shards
from .engine import EngineRun, run_parallel, run_serial
from .errors import StageError
from .metrics import FoldResult, aggregate
from .scheduler import FoldTask, schedule_folds
from .windows import build_shard_plan, estimate_fold_count


STAGES = ["ingest", "plan", "schedule", "execute", "checkpoint",
          "aggregate"]


def stage_index(name: str) -> int:
    try:
        return STAGES.index(name)
    except ValueError:
        raise StageError(f"unknown stage {name!r}",
                         context={"valid_stages": STAGES})


def assert_stage(state: Dict[str, Any], minimum_stage: str) -> None:
    current = state.get("stage", "ingest")
    if stage_index(current) < stage_index(minimum_stage):
        raise StageError(
            f"operation requires stage {minimum_stage!r}, current stage is "
            f"{current!r}",
            context={"current": current, "required": minimum_stage})


def input_fingerprint(shard: ShardData, cfg: BacktestConfig) -> str:
    payload = canonical_json({
        "shard_content": shard.content_fingerprint,
        "config": cfg.fingerprint(),
    })
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def result_fingerprint(fold_records: List[Dict[str, Any]]) -> str:
    return hashlib.sha256(
        canonical_json(fold_records).encode("utf-8")).hexdigest()


def _manifest_hash(manifest: Dict[str, Any]) -> str:
    return hashlib.sha256(
        canonical_json(manifest).encode("utf-8")).hexdigest()


def _new_state(manifest: Dict[str, Any], cfg: BacktestConfig) -> Dict[str, Any]:
    return {
        "stage": "ingest",
        "config_fingerprint": cfg.fingerprint(),
        "config_version": cfg.version,
        "method": cfg.method,
        "manifest_fingerprint": _manifest_hash(manifest),
        "shards": [],
        "invalidation": [],
        "rejected_folds": [],
        "series_errors": [],
        "run_status": "running",
        "stop_requested": False,
    }


def load_or_init(workspace: str, manifest_path: str,
                 cfg: BacktestConfig) -> Tuple[Dict[str, Any],
                                                List[ShardData],
                                                Dict[str, Any],
                                                List[Dict[str, Any]]]:
    manifest, shards = load_shards(manifest_path)
    state = read_state(workspace)
    drift: List[Dict[str, Any]] = []
    if state is None:
        state = _new_state(manifest, cfg)
        return manifest, shards, state, drift

    if state.get("config_fingerprint") != cfg.fingerprint():
        drift.append({
            "scope": "config",
            "reason": "configuration fingerprint changed",
            "previous": state.get("config_fingerprint"),
            "current": cfg.fingerprint(),
        })
    if state.get("manifest_fingerprint") != _manifest_hash(manifest):
        drift.append({
            "scope": "manifest",
            "reason": "manifest/shard set changed",
            "previous": state.get("manifest_fingerprint"),
            "current": _manifest_hash(manifest),
        })
    state["config_fingerprint"] = cfg.fingerprint()
    state["config_version"] = cfg.version
    state["method"] = cfg.method
    return manifest, shards, state, drift


def _to_fold_results(records: List[Dict[str, Any]]) -> List[FoldResult]:
    return [FoldResult(
        shard_id=item["shard_id"], series_id=item["series_id"],
        fold_index=item["fold_index"], worker_id=item["worker_id"],
        train_start=item["train_start"], train_end=item["train_end"],
        test_start=item["test_start"], test_end=item["test_end"],
        has_changepoint=item["has_changepoint"], method=item["method"],
        params=item["params"], predictions=item["predictions"],
        actual=item["actual"], timestamps=item["timestamps"],
        errors=item["errors"], abs_errors=item["abs_errors"],
        squared_errors=item["squared_errors"],
        absolute_percentage_errors=item["absolute_percentage_errors"],
        interval_covered=item["interval_covered"],
        interval_halfwidth=item["interval_halfwidth"],
        zero_denominator_count=item["zero_denominator_count"])
        for item in records]


def run_pipeline(workspace: str, manifest_path: str, cfg: BacktestConfig,
                 parallel: bool = True,
                 stop_after_folds: Optional[int] = None) -> Dict[str, Any]:
    """Execute (or resume) the full rolling-backtest pipeline."""
    # a resume entry explicitly clears any previous cooperative stop flag
    if stop_requested(workspace):
        clear_stop(workspace)
    manifest, shards, state, drift = load_or_init(
        workspace, manifest_path, cfg)
    shard_by_id = {shard.shard_id: shard for shard in shards}
    ordered_ids = [shard.shard_id for shard in shards]

    # ---- ingest + plan per shard, with fingerprint invalidation ----------
    planned: Dict[str, Dict[str, Any]] = {}
    existing = {record["shard_id"]: record
                for record in list_checkpoints(workspace)}
    invalidated: List[str] = []
    invalidation_log = list(state.get("invalidation", []))
    for index, shard in enumerate(shards):
        fingerprint = input_fingerprint(shard, cfg)
        plan = build_shard_plan(shard, cfg)
        series_errors = [item.error.to_dict()
                         for item in plan.values() if item.error]
        estimated = estimate_fold_count(manifest["shards"][index], cfg)
        planned[shard.shard_id] = {
            "shard_id": shard.shard_id,
            "sequence": index,
            "input_fingerprint": fingerprint,
            "estimated_folds": estimated,
            "eligible_folds": sum(len(item.folds)
                                  for item in plan.values()),
            "excluded_folds": sum(len(item.excluded)
                                  for item in plan.values()),
            "series_errors": series_errors,
        }
        old = existing.get(shard.shard_id)
        if old is not None and old.get("input_fingerprint") != fingerprint:
            invalidated.append(shard.shard_id)
            invalidation_log = [
                record for record in invalidation_log
                if record.get("shard_id") != shard.shard_id]
            invalidation_log.append({
                "shard_id": shard.shard_id,
                "reason": "input/config fingerprint changed",
                "previous_fingerprint": old.get("input_fingerprint"),
                "current_fingerprint": fingerprint,
            })
            for downstream in ordered_ids[index + 1:]:
                if downstream not in invalidated:
                    invalidated.append(downstream)

    for shard_id in invalidated:
        path = os.path.join(workspace, "checkpoints",
                            f"shard_{shard_id}.json")
        if os.path.exists(path):
            os.remove(path)
    state["invalidation"] = invalidation_log
    state["shards"] = [planned[shard_id] for shard_id in ordered_ids]

    # ---- schedule across all eligible folds (global budget) --------------
    folds_by_shard: Dict[str, List] = {}
    all_series_errors: List[Dict[str, Any]] = []
    for shard in shards:
        plan = build_shard_plan(shard, cfg)
        folds_by_shard[shard.shard_id] = [
            fold for item in plan.values() for fold in item.folds]
        for item in plan.values():
            if item.error:
                all_series_errors.append(item.error.to_dict())
    scheduling = schedule_folds(folds_by_shard, cfg)
    state["stage"] = "schedule"
    state["rejected_folds"] = scheduling.rejected
    state["series_errors"] = all_series_errors
    state["scheduler"] = {
        "estimated_folds": scheduling.estimated_folds,
        "accepted_folds": scheduling.accepted_count,
        "rejected_folds": len(scheduling.rejected),
        "worker_loads": scheduling.worker_loads,
        "global_fit_budget": cfg.global_fit_budget,
        "worker_fit_quota": cfg.worker_fit_quota,
        "max_workers": cfg.max_workers,
    }
    write_state(workspace, state)

    # ---- resume: a shard is reusable only when every accepted fold is
    # present in its checkpoint AND its fingerprint still matches ----------
    accepted_by_shard: Dict[str, int] = {}
    for task in scheduling.accepted:
        accepted_by_shard[task.shard_id] = \
            accepted_by_shard.get(task.shard_id, 0) + 1
    reusable_shards = set()
    stale_partial = []
    for record in list_checkpoints(workspace):
        shard_id = record["shard_id"]
        shard = shard_by_id.get(shard_id)
        if shard is None:
            continue
        if record.get("input_fingerprint") != input_fingerprint(shard, cfg):
            continue
        if record.get("completed_folds", 0) \
                < accepted_by_shard.get(shard_id, 0):
            stale_partial.append(shard_id)
            continue
        reusable_shards.add(shard_id)
    for shard_id in stale_partial:
        path = os.path.join(workspace, "checkpoints",
                            f"shard_{shard_id}.json")
        if os.path.exists(path):
            os.remove(path)
    remaining: List[FoldTask] = [
        task for task in scheduling.accepted
        if task.shard_id not in reusable_shards]

    # ---- execute one shard at a time; shard is the atomic checkpoint unit
    state["stage"] = "execute"
    all_new_results: List[Dict[str, Any]] = []
    all_new_failures: List[Dict[str, Any]] = []
    stopped = False
    stop_budget = stop_after_folds

    for index, shard in enumerate(shards):
        shard_id = shard.shard_id
        if shard_id in reusable_shards:
            continue
        shard_tasks = [task for task in remaining
                       if task.shard_id == shard_id]
        if not shard_tasks:
            continue
        if stop_requested(workspace):
            stopped = True
            break

        runner = run_parallel if parallel else run_serial
        engine_run: EngineRun = runner(
            shard_tasks, shard_by_id, cfg, workspace=workspace,
            stop_after_folds=stop_budget)
        all_new_results.extend(engine_run.results)
        all_new_failures.extend(engine_run.failures)
        if stop_budget is not None:
            stop_budget -= (len(engine_run.results)
                            + len(engine_run.failures))

        if engine_run.stopped:
            stopped = True
            state["not_started_folds"] = engine_run.not_started
            state["run_status"] = "stopped"
            state["stop_requested"] = True
            write_state(workspace, state)
            return state

        shard_results = [record for record in all_new_results
                         if record["shard_id"] == shard_id]
        shard_failures = [record for record in all_new_failures
                          if record["shard_id"] == shard_id]
        record = {
            "shard_id": shard_id,
            "sequence": index,
            "prev_shard_id": ordered_ids[index - 1] if index > 0 else "",
            "input_fingerprint": input_fingerprint(shard, cfg),
            "config_fingerprint": cfg.fingerprint(),
            "config_version": cfg.version,
            "completed_folds": len(shard_results),
            "failed_folds": len(shard_failures),
            "product_fingerprint": result_fingerprint(shard_results),
            "results": shard_results,
            "failures": shard_failures,
            "plan": planned[shard_id],
        }
        publish_checkpoint(workspace, record)
        state["stage"] = "checkpoint"
        write_state(workspace, state)

    # ---- aggregate only when every shard checkpoint is present -----------
    checkpoints = list_checkpoints(workspace)
    done_ids = {record["shard_id"] for record in checkpoints}
    if stopped or any(shard_id not in done_ids for shard_id in ordered_ids):
        state["run_status"] = "stopped" if stopped else "incomplete"
        state["stop_requested"] = stopped
        write_state(workspace, state)
        return state

    # a resumed run reaches completion: clear transient stop bookkeeping so
    # the final state is byte-identical to a clean end-to-end run
    state.pop("not_started_folds", None)
    all_results = [item for record in checkpoints
                   for item in record["results"]]
    all_failures = [item for record in checkpoints
                    for item in record["failures"]]
    all_results.sort(key=lambda item: (item["shard_id"], item["series_id"],
                                       item["fold_index"]))
    metrics = aggregate(_to_fold_results(all_results))
    fold_curve = []
    for fold in all_results:
        abs_errors = fold["abs_errors"]
        fold_curve.append({
            "shard_id": fold["shard_id"],
            "series_id": fold["series_id"],
            "fold_index": fold["fold_index"],
            "mae": sum(abs_errors) / len(abs_errors),
            "has_changepoint": fold["has_changepoint"],
        })
    fold_curve.sort(key=lambda item: (item["shard_id"], item["series_id"],
                                      item["fold_index"]))
    state["stage"] = "aggregate"
    state["run_status"] = "completed"
    state["stop_requested"] = False
    state["metrics"] = metrics
    state["fold_curve"] = fold_curve
    state["failures"] = all_failures
    state["aggregate_fingerprint"] = result_fingerprint(all_results)
    state["checkpoints"] = [{
        "shard_id": record["shard_id"],
        "sequence": record["sequence"],
        "input_fingerprint": record["input_fingerprint"],
        "product_fingerprint": record["product_fingerprint"],
        "completed_folds": record["completed_folds"],
    } for record in checkpoints]
    write_state(workspace, state)
    clear_stop(workspace)
    return state
