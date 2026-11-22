"""Pipeline orchestration, process-pool execution, exports, and verification."""

from __future__ import annotations

import copy
import multiprocessing
import shutil
import traceback
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from .aggregate import aggregate_match_artifacts
from .canonical import (
    atomic_write_bytes,
    canonical_bytes,
    digest_obj,
    locked,
)
from .checkpoints import CheckpointStore, checkpoint_summary
from .config import (
    aggregate_stage_input,
    budget_config,
    config_fingerprint,
    decode_stage_input,
    matching_config,
    match_stage_input,
    schedule_stage_input,
    stage_config,
)
from .decode import decode_shard
from .errors import WorkbenchError, invalid_stage
from .matching import match_shard
from .scheduler import make_budget_plan
from .shards import (
    import_shard,
    list_shards,
    load_shard_file,
    shard_content_digest,
    shard_path,
)
from .workspace import Workspace, create_workspace, utc_now


TARGET_STAGES = ("all", "decode", "match", "aggregate", "schedule")


def _stage_config_digest(config: dict[str, Any], stage: str) -> str:
    return digest_obj({"schema": f"segval-{stage}-semantic-config-v1", "config": stage_config(config, stage)})


def _stage_record(plan: dict[str, Any], stage: str) -> dict[str, Any]:
    return next(item for item in plan["stages"] if item["stage"] == stage)


def _checkpoint_brief(record: dict[str, Any] | None) -> dict[str, Any] | None:
    return checkpoint_summary(record) if record is not None else None


def plan_shard(shard: dict[str, Any], config: dict[str, Any], store: CheckpointStore) -> dict[str, Any]:
    content_digest = shard_content_digest(shard)
    decode_input = decode_stage_input(config, content_digest)
    decode_input_fingerprint = digest_obj(decode_input)
    decode_checkpoint = store.get(shard["shard_index"], "decode")
    decode_stage: dict[str, Any] = {
        "stage": "decode",
        "status": "MISSING",
        "expected_input": decode_input,
        "expected_input_fingerprint": decode_input_fingerprint,
        "existing": _checkpoint_brief(decode_checkpoint),
        "invalid_reasons": ["MISSING_STAGE"],
    }
    decode_reasons: list[str] = []
    if decode_checkpoint is None:
        decode_reasons.append("MISSING_STAGE")
    else:
        if decode_checkpoint.get("input_fingerprint") != decode_input_fingerprint:
            if decode_checkpoint.get("shard_content_digest") != content_digest:
                decode_reasons.append("SHARD_CONTENT_CHANGED")
            if decode_checkpoint.get("config_fingerprint") != _stage_config_digest(config, "decode"):
                decode_reasons.append("DECODE_CONFIG_CHANGED")
            if not decode_reasons:
                decode_reasons.append("INPUT_FINGERPRINT_CHANGED")
        if not isinstance(decode_checkpoint.get("artifact"), dict):
            decode_reasons.append("MISSING_ARTIFACT")
    if not decode_reasons:
        decode_stage["status"] = "VALID"
        decode_stage["invalid_reasons"] = []
    else:
        decode_stage["status"] = "MISSING" if decode_checkpoint is None else "STALE"
        decode_stage["invalid_reasons"] = decode_reasons
    stages = [decode_stage]

    decode_artifact = decode_checkpoint.get("artifact") if decode_checkpoint and not decode_reasons else None
    if decode_artifact is None:
        match_stage = {
            "stage": "match",
            "status": "BLOCKED_UPSTREAM",
            "expected_input": None,
            "expected_input_fingerprint": None,
            "existing": _checkpoint_brief(store.get(shard["shard_index"], "match")),
            "invalid_reasons": ["UPSTREAM_DECODE_INVALID"],
        }
        aggregate_stage = {
            "stage": "aggregate",
            "status": "BLOCKED_UPSTREAM",
            "expected_input": None,
            "expected_input_fingerprint": None,
            "existing": _checkpoint_brief(store.get(shard["shard_index"], "aggregate")),
            "invalid_reasons": ["UPSTREAM_MATCH_INVALID"],
        }
        stages.extend([match_stage, aggregate_stage])
        earliest = "decode"
        affected = ["decode", "match", "aggregate"]
        reasons = {"decode": decode_reasons, "match": ["UPSTREAM_DECODE_INVALID"], "aggregate": ["UPSTREAM_MATCH_INVALID"]}
        return {
            **_shard_base(shard, content_digest),
            "stages": stages,
            "all_valid": False,
            "earliest_invalid_stage": earliest,
            "affected_stages": affected,
            "invalidation": {
                "earliest_stage": earliest,
                "affected_stages": affected,
                "reasons": reasons,
            },
            "decode_artifact_digest": None,
            "match_artifact_digest": None,
            "aggregate_digest": None,
        }

    class_set = list(decode_artifact.get("class_set", []))
    match_input = match_stage_input(config, decode_artifact["artifact_digest"], class_set)
    match_input_fingerprint = digest_obj(match_input)
    match_checkpoint = store.get(shard["shard_index"], "match")
    match_reasons: list[str] = []
    match_stage = {
        "stage": "match",
        "status": "MISSING",
        "expected_input": match_input,
        "expected_input_fingerprint": match_input_fingerprint,
        "existing": _checkpoint_brief(match_checkpoint),
        "invalid_reasons": [],
    }
    if match_checkpoint is None:
        match_reasons.append("MISSING_STAGE")
    else:
        if match_checkpoint.get("input_fingerprint") != match_input_fingerprint:
            if match_checkpoint.get("upstream_digest") != decode_artifact["artifact_digest"]:
                match_reasons.append("UPSTREAM_DECODE_CHANGED")
            if match_checkpoint.get("config_fingerprint") != _stage_config_digest(config, "match"):
                match_reasons.append("MATCH_CONFIG_CHANGED")
            existing_class_set = list((match_checkpoint.get("artifact") or {}).get("class_set", []))
            if existing_class_set != sorted(class_set):
                match_reasons.append("CLASS_SET_CHANGED")
            if not match_reasons:
                match_reasons.append("INPUT_FINGERPRINT_CHANGED")
        if not isinstance(match_checkpoint.get("artifact"), dict):
            match_reasons.append("MISSING_ARTIFACT")
    if not match_reasons:
        match_stage["status"] = "VALID"
    else:
        match_stage["status"] = "MISSING" if match_checkpoint is None else "STALE"
    match_stage["invalid_reasons"] = match_reasons
    stages.append(match_stage)

    match_artifact = match_checkpoint.get("artifact") if match_checkpoint and not match_reasons else None
    if match_artifact is None:
        aggregate_stage = {
            "stage": "aggregate",
            "status": "BLOCKED_UPSTREAM",
            "expected_input": None,
            "expected_input_fingerprint": None,
            "existing": _checkpoint_brief(store.get(shard["shard_index"], "aggregate")),
            "invalid_reasons": ["UPSTREAM_MATCH_INVALID"],
        }
        stages.append(aggregate_stage)
        earliest = "match"
        affected = ["match", "aggregate"]
        reasons = {"match": match_reasons, "aggregate": ["UPSTREAM_MATCH_INVALID"]}
        return {
            **_shard_base(shard, content_digest),
            "stages": stages,
            "all_valid": False,
            "earliest_invalid_stage": earliest,
            "affected_stages": affected,
            "invalidation": {
                "earliest_stage": earliest,
                "affected_stages": affected,
                "reasons": reasons,
            },
            "decode_artifact_digest": decode_artifact["artifact_digest"],
            "match_artifact_digest": None,
            "aggregate_digest": None,
        }

    aggregate_input = aggregate_stage_input(config, match_artifact["artifact_digest"])
    aggregate_input_fingerprint = digest_obj(aggregate_input)
    aggregate_checkpoint = store.get(shard["shard_index"], "aggregate")
    aggregate_reasons: list[str] = []
    aggregate_stage = {
        "stage": "aggregate",
        "status": "MISSING",
        "expected_input": aggregate_input,
        "expected_input_fingerprint": aggregate_input_fingerprint,
        "existing": _checkpoint_brief(aggregate_checkpoint),
        "invalid_reasons": [],
    }
    if aggregate_checkpoint is None:
        aggregate_reasons.append("MISSING_STAGE")
    else:
        if aggregate_checkpoint.get("input_fingerprint") != aggregate_input_fingerprint:
            if aggregate_checkpoint.get("upstream_digest") != match_artifact["artifact_digest"]:
                aggregate_reasons.append("UPSTREAM_MATCH_CHANGED")
            if aggregate_checkpoint.get("config_fingerprint") != _stage_config_digest(config, "aggregate"):
                aggregate_reasons.append("AGGREGATE_CONFIG_CHANGED")
            if not aggregate_reasons:
                aggregate_reasons.append("INPUT_FINGERPRINT_CHANGED")
        if not isinstance(aggregate_checkpoint.get("artifact"), dict):
            aggregate_reasons.append("MISSING_ARTIFACT")
    if not aggregate_reasons:
        aggregate_stage["status"] = "VALID"
    else:
        aggregate_stage["status"] = "MISSING" if aggregate_checkpoint is None else "STALE"
    aggregate_stage["invalid_reasons"] = aggregate_reasons
    stages.append(aggregate_stage)

    if aggregate_reasons:
        earliest = "aggregate"
        affected = ["aggregate"]
        reasons = {"aggregate": aggregate_reasons}
    else:
        earliest = None
        affected = []
        reasons = {}
    return {
        **_shard_base(shard, content_digest),
        "stages": stages,
        "all_valid": not aggregate_reasons,
        "earliest_invalid_stage": earliest,
        "affected_stages": affected,
        "invalidation": {
            "earliest_stage": earliest,
            "affected_stages": affected,
            "reasons": reasons,
        },
        "decode_artifact_digest": decode_artifact["artifact_digest"],
        "match_artifact_digest": match_artifact["artifact_digest"],
        "aggregate_digest": aggregate_checkpoint.get("artifact_digest") if aggregate_checkpoint else None,
    }


def _shard_base(shard: dict[str, Any], content_digest: str) -> dict[str, Any]:
    return {
        "shard_id": shard["shard_id"],
        "shard_index": shard["shard_index"],
        "sample_count": len(shard["samples"]),
        "prediction_count": sum(len(sample["predictions"]) for sample in shard["samples"]),
        "class_set": sorted(
            {
                class_name
                for sample in shard["samples"]
                for class_name in (
                    list(sample["ground_truth"].keys())
                    + [prediction["class_name"] for prediction in sample["predictions"]]
                )
            }
        ),
        "shard_content_digest": content_digest,
    }


def plan_all(workspace: Workspace, config: dict[str, Any], shards: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    store = CheckpointStore(workspace.root)
    loaded = shards if shards is not None else list_shards(workspace.root)
    return [plan_shard(shard, config, store) for shard in loaded]


def _schedule_validation(workspace: Workspace, config: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    state = workspace.state()
    last_run = state.get("last_run") or {}
    previous = last_run.get("schedule_fingerprint")
    current = plan["schedule_fingerprint"]
    reasons: list[str] = []
    if previous is None:
        reasons.append("MISSING_STAGE")
    elif previous != current:
        previous_budget = last_run.get("budget_fingerprint")
        current_budget = digest_obj(budget_config(config))
        if previous_budget != current_budget:
            reasons.append("BUDGET_CONFIG_CHANGED")
        if last_run.get("accepted_shard_ids") != plan["accepted_shard_ids"]:
            reasons.append("ACCEPTED_SHARD_SET_CHANGED")
        if last_run.get("aggregate_digests") != plan["aggregate_digests"]:
            reasons.append("UPSTREAM_AGGREGATE_CHANGED")
        if not reasons:
            reasons.append("SCHEDULE_INPUT_CHANGED")
    return {
        "stage": "schedule",
        "status": "VALID" if not reasons else ("MISSING" if previous is None else "STALE"),
        "previous_fingerprint": previous,
        "current_fingerprint": current,
        "invalid_reasons": reasons,
        "affected_stages": ["schedule"] if reasons else [],
    }


def build_plan_bundle(workspace: Workspace, config: dict[str, Any] | None = None) -> dict[str, Any]:
    config = config or workspace.config()
    shards = list_shards(workspace.root)
    shard_plans = plan_all(workspace, config, shards)
    budget_plan = make_budget_plan(shards, config)
    plan_by_id = {item["shard_id"]: item for item in shard_plans}
    accepted = []
    for item in budget_plan["accepted"]:
        plan = plan_by_id[item["shard_id"]]
        accepted.append({**item, "aggregate_digest": plan.get("aggregate_digest")})
    budget_plan["accepted"] = accepted
    schedule_input = schedule_stage_input(config, accepted, budget_plan["unprocessed"])
    schedule_fingerprint = digest_obj(schedule_input)
    budget_plan["schedule_input"] = schedule_input
    budget_plan["schedule_fingerprint"] = schedule_fingerprint
    budget_plan["accepted_shard_ids"] = [item["shard_id"] for item in accepted]
    budget_plan["aggregate_digests"] = sorted(
        item.get("aggregate_digest") or "" for item in accepted
    )
    budget_plan["budget_fingerprint"] = digest_obj(budget_config(config))
    budget_plan["actual_candidate_evaluations"] = sum(
        int(
            (
                CheckpointStore(workspace.root).get(plan_by_id[item["shard_id"]]["shard_index"], "match")
                or {"artifact": {"counts": {}}}
            )["artifact"]["counts"].get("candidate_evaluations", 0)
        )
        for item in accepted
        if plan_by_id[item["shard_id"]].get("match_artifact_digest")
    )
    budget_plan["within_global_budget"] = (
        int(budget_plan["budget"]["reserved_evaluations"]) <= int(budget_config(config)["max_evaluations"])
        and budget_plan["actual_candidate_evaluations"] <= int(budget_config(config)["max_evaluations"])
        and budget_plan["actual_candidate_evaluations"] <= int(budget_plan["budget"]["reserved_evaluations"])
    )
    schedule_validation = _schedule_validation(workspace, config, budget_plan)
    return {
        "config": config,
        "config_fingerprint": config_fingerprint(config),
        "config_version": config.get("config_version", "1"),
        "shards": shards,
        "shard_plans": shard_plans,
        "plan_by_id": plan_by_id,
        "budget_plan": budget_plan,
        "schedule_validation": schedule_validation,
    }


def _publish_decode(
    store: CheckpointStore,
    workspace: Workspace,
    shard: dict[str, Any],
    config: dict[str, Any],
    artifact: dict[str, Any],
) -> dict[str, Any]:
    input_obj = decode_stage_input(config, artifact["shard_content_digest"])
    return store.publish(
        shard_id=shard["shard_id"],
        shard_index=shard["shard_index"],
        stage="decode",
        input_fingerprint=digest_obj(input_obj),
        config_fingerprint=_stage_config_digest(config, "decode"),
        config_version=config["config_version"],
        artifact=artifact,
        upstream_digest=None,
    )


def _publish_match(
    store: CheckpointStore,
    shard: dict[str, Any],
    config: dict[str, Any],
    decode_artifact: dict[str, Any],
    artifact: dict[str, Any],
) -> dict[str, Any]:
    input_obj = match_stage_input(config, decode_artifact["artifact_digest"], decode_artifact["class_set"])
    return store.publish(
        shard_id=shard["shard_id"],
        shard_index=shard["shard_index"],
        stage="match",
        input_fingerprint=digest_obj(input_obj),
        config_fingerprint=_stage_config_digest(config, "match"),
        config_version=config["config_version"],
        artifact=artifact,
        upstream_digest=decode_artifact["artifact_digest"],
    )


def _publish_aggregate(
    store: CheckpointStore,
    shard: dict[str, Any],
    config: dict[str, Any],
    match_artifact: dict[str, Any],
    artifact: dict[str, Any],
) -> dict[str, Any]:
    input_obj = aggregate_stage_input(config, match_artifact["artifact_digest"])
    return store.publish(
        shard_id=shard["shard_id"],
        shard_index=shard["shard_index"],
        stage="aggregate",
        input_fingerprint=digest_obj(input_obj),
        config_fingerprint=_stage_config_digest(config, "aggregate"),
        config_version=config["config_version"],
        artifact=artifact,
        upstream_digest=match_artifact["artifact_digest"],
    )


def process_shard_job(job: dict[str, Any]) -> dict[str, Any]:
    """Top-level process-pool entry point. Never shares mutable state across runs."""

    shard_index = int(job["shard_index"])
    try:
        workspace = Workspace(Path(job["workspace"]))
        config = job["config"]
        target_stage = job["target_stage"]
        run_token = job.get("run_token")
        stop_after_samples = job.get("stop_after_samples")
        shard = load_shard_file(shard_path(workspace.shards_dir, shard_index))
        store = CheckpointStore(workspace.root)
        stages_run: list[str] = []
        checkpoint_sequences: list[int] = []
        def stop_requested() -> bool:
            return workspace.stop_requested()

        def sample_boundary() -> None:
            if run_token is not None:
                workspace.record_sample_boundary(run_token, stop_after_samples)

        plan = plan_shard(shard, config, store)
        existing_decode = _stage_record(plan, "decode")
        decode_artifact = (
            store.get(shard_index, "decode")["artifact"]
            if existing_decode["status"] == "VALID"
            else None
        )

        if target_stage in ("all", "decode") and existing_decode["status"] != "VALID":
            decoded = decode_shard(shard, config, stop_requested=stop_requested)
            if decoded is None:
                return {
                    "ok": True,
                    "shard_id": shard["shard_id"],
                    "shard_index": shard_index,
                    "stopped": True,
                    "stages_run": stages_run,
                    "checkpoint_sequences": checkpoint_sequences,
                }
            record = _publish_decode(store, workspace, shard, config, decoded)
            decode_artifact = decoded
            stages_run.append("decode")
            checkpoint_sequences.append(record["sequence"])
        if target_stage == "decode":
            return {
                "ok": True,
                "shard_id": shard["shard_id"],
                "shard_index": shard_index,
                "stopped": stop_requested(),
                "stages_run": stages_run,
                "checkpoint_sequences": checkpoint_sequences,
            }
        if stop_requested():
            return {
                "ok": True,
                "shard_id": shard["shard_id"],
                "shard_index": shard_index,
                "stopped": True,
                "stages_run": stages_run,
                "checkpoint_sequences": checkpoint_sequences,
            }
        if decode_artifact is None:
            current = _stage_record(plan_shard(shard, config, store), "decode")
            if current["status"] != "VALID":
                raise invalid_stage(
                    "Decode stage is not valid.",
                    {"shard_id": shard["shard_id"], "stage": "decode", "status": current["status"]},
                )
            decode_artifact = store.get(shard_index, "decode")["artifact"]

        plan = plan_shard(shard, config, store)
        existing_match = _stage_record(plan, "match")
        match_artifact = (
            store.get(shard_index, "match")["artifact"]
            if existing_match["status"] == "VALID"
            else None
        )
        if target_stage in ("all", "match") and existing_match["status"] != "VALID":
            matched = match_shard(
                decode_artifact,
                config,
                stop_requested=stop_requested,
                sample_boundary=sample_boundary,
            )
            if matched is None:
                return {
                    "ok": True,
                    "shard_id": shard["shard_id"],
                    "shard_index": shard_index,
                    "stopped": True,
                    "stages_run": stages_run,
                    "checkpoint_sequences": checkpoint_sequences,
                }
            record = _publish_match(store, shard, config, decode_artifact, matched)
            match_artifact = matched
            stages_run.append("match")
            checkpoint_sequences.append(record["sequence"])
        if target_stage == "match":
            return {
                "ok": True,
                "shard_id": shard["shard_id"],
                "shard_index": shard_index,
                "stopped": stop_requested(),
                "stages_run": stages_run,
                "checkpoint_sequences": checkpoint_sequences,
                "actual_evaluations": (match_artifact or {}).get("counts", {}).get("candidate_evaluations", 0),
            }
        if stop_requested():
            return {
                "ok": True,
                "shard_id": shard["shard_id"],
                "shard_index": shard_index,
                "stopped": True,
                "stages_run": stages_run,
                "checkpoint_sequences": checkpoint_sequences,
            }
        if match_artifact is None:
            current = _stage_record(plan_shard(shard, config, store), "match")
            if current["status"] != "VALID":
                raise invalid_stage(
                    "Match stage is not valid.",
                    {"shard_id": shard["shard_id"], "stage": "match", "status": current["status"]},
                )
            match_artifact = store.get(shard_index, "match")["artifact"]

        plan = plan_shard(shard, config, store)
        existing_aggregate = _stage_record(plan, "aggregate")
        aggregate_artifact = (
            store.get(shard_index, "aggregate")["artifact"]
            if existing_aggregate["status"] == "VALID"
            else None
        )
        if target_stage in ("all", "aggregate") and existing_aggregate["status"] != "VALID":
            from .aggregate import aggregate_match_artifacts

            combined = aggregate_match_artifacts([match_artifact])
            aggregate_artifact = {
                schema_key: value
                for schema_key, value in combined.items()
                if schema_key != "source_shards"
            }
            aggregate_artifact["shard_id"] = shard["shard_id"]
            aggregate_artifact["shard_index"] = shard["shard_index"]
            aggregate_artifact["source_shards"] = [
                {
                    "shard_id": shard["shard_id"],
                    "shard_index": shard["shard_index"],
                    "match_artifact_digest": match_artifact["artifact_digest"],
                }
            ]
            aggregate_artifact["artifact_digest"] = digest_obj(
                {key: value for key, value in aggregate_artifact.items() if key != "artifact_digest"}
            )
            record = _publish_aggregate(store, shard, config, match_artifact, aggregate_artifact)
            stages_run.append("aggregate")
            checkpoint_sequences.append(record["sequence"])
        return {
            "ok": True,
            "shard_id": shard["shard_id"],
            "shard_index": shard_index,
            "stopped": stop_requested(),
            "stages_run": stages_run,
            "checkpoint_sequences": checkpoint_sequences,
            "actual_evaluations": (match_artifact or {}).get("counts", {}).get("candidate_evaluations", 0),
        }
    except WorkbenchError as exc:
        return {
            "ok": False,
            "shard_index": shard_index,
            "error": exc.to_dict(),
            "stopped": False,
        }
    except Exception as exc:  # pragma: no cover - exercised by injected failures
        return {
            "ok": False,
            "shard_index": shard_index,
            "error": {
                "code": "E_WORKER_CRASH",
                "message": f"{type(exc).__name__}: {exc}",
                "details": {"traceback": traceback.format_exc()},
            },
            "stopped": False,
        }


def _execute_jobs(jobs: list[dict[str, Any]], workers: int) -> list[dict[str, Any]]:
    if not jobs:
        return []
    if workers <= 1 or len(jobs) == 1:
        return [process_shard_job(job) for job in jobs]
    context = multiprocessing.get_context("spawn")
    results: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=min(workers, len(jobs)), mp_context=context) as executor:
        future_map = {executor.submit(process_shard_job, job): job for job in jobs}
        for future in as_completed(future_map):
            job = future_map[future]
            try:
                results.append(future.result())
            except Exception as exc:  # process transport/runtime failure
                results.append(
                    {
                        "ok": False,
                        "shard_index": job["shard_index"],
                        "shard_id": job.get("shard_id"),
                        "error": {
                            "code": "E_WORKER_CRASH",
                            "message": f"{type(exc).__name__}: {exc}",
                            "details": {"traceback": traceback.format_exc()},
                        },
                        "stopped": False,
                    }
                )
    results.sort(key=lambda item: (int(item.get("shard_index", 0)), str(item.get("shard_id", ""))))
    return results


def _jobs_for_run(
    bundle: dict[str, Any],
    target_stage: str,
    only_shard_ids: set[str] | None,
    run_token: str,
    stop_after_samples: int | None,
    config: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    jobs: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    accepted_ids = {item["shard_id"] for item in bundle["budget_plan"]["accepted"]}
    for plan in bundle["shard_plans"]:
        shard_id = plan["shard_id"]
        if only_shard_ids is not None and shard_id not in only_shard_ids:
            continue
        if shard_id not in accepted_ids:
            skipped.append(
                {
                    "shard_id": shard_id,
                    "shard_index": plan["shard_index"],
                    "reason": "NOT_ACCEPTED_BY_BUDGET",
                }
            )
            continue
        if target_stage == "schedule":
            skipped.append(
                {
                    "shard_id": shard_id,
                    "shard_index": plan["shard_index"],
                    "reason": "SCHEDULE_STAGE_HAS_NO_PER_SHARD_WORK",
                }
            )
            continue
        if target_stage == "all":
            if plan["earliest_invalid_stage"] is None:
                skipped.append(
                    {
                        "shard_id": shard_id,
                        "shard_index": plan["shard_index"],
                        "reason": "ALREADY_VALID",
                    }
                )
                continue
        else:
            target_record = _stage_record(plan, target_stage)
            if target_record["status"] == "BLOCKED_UPSTREAM":
                raise invalid_stage(
                    f"Target stage {target_stage} is blocked by an invalid upstream stage.",
                    {
                        "shard_id": shard_id,
                        "stage": target_stage,
                        "upstream_reasons": target_record["invalid_reasons"],
                    },
                )
            if target_record["status"] == "VALID":
                skipped.append(
                    {
                        "shard_id": shard_id,
                        "shard_index": plan["shard_index"],
                        "reason": "ALREADY_VALID",
                    }
                )
                continue
        jobs.append(
            {
                "workspace": str(bundle["workspace_root"]),
                "shard_id": shard_id,
                "shard_index": plan["shard_index"],
                "config": config,
                "target_stage": target_stage,
                "run_token": run_token,
                "stop_after_samples": stop_after_samples,
                "expected_stages": plan["affected_stages"],
            }
        )
    return jobs, skipped


def run_workbench(
    root: Path,
    *,
    target_stage: str = "all",
    workers: int | None = None,
    only_shard_ids: list[str] | None = None,
    stop_after_samples: int | None = None,
    clear_stop: bool = False,
) -> dict[str, Any]:
    if target_stage not in TARGET_STAGES:
        raise invalid_stage(
            "target_stage must be one of all, decode, match, aggregate, schedule.",
            {"value": target_stage},
        )
    workspace = Workspace(root)
    workspace.initialize()
    with workspace.run_lock():
        if clear_stop:
            workspace.clear_stop()
        config = workspace.config()
        started_at = utc_now()
        shards = list_shards(workspace.root)
        bundle = build_plan_bundle(workspace, config)
        bundle["workspace_root"] = str(workspace.root)
        selected_ids = set(only_shard_ids) if only_shard_ids is not None else None
        if selected_ids is not None:
            known_ids = {shard["shard_id"] for shard in shards}
            unknown = sorted(selected_ids - known_ids)
            if unknown:
                raise WorkbenchError(
                    "E_SHARD_NOT_FOUND",
                    "One or more requested shard ids do not exist.",
                    {"shard_ids": unknown},
                    404,
                )
        run_token = workspace.new_run_token()
        if stop_after_samples is not None and int(stop_after_samples) <= 0:
            workspace.request_stop("stop_after_samples_reached")
        jobs, skipped = _jobs_for_run(
            bundle,
            target_stage,
            selected_ids,
            run_token,
            stop_after_samples,
            config,
        )
        configured_workers = int(config["budget"]["max_workers"])
        execution_workers = configured_workers if workers is None else max(1, min(int(workers), configured_workers))
        if target_stage == "schedule":
            execution_workers = 0
        results: list[dict[str, Any]] = []
        if not workspace.stop_requested() and jobs:
            results = _execute_jobs(jobs, execution_workers)
        elif jobs and workspace.stop_requested():
            results = [
                {
                    "ok": True,
                    "shard_index": job["shard_index"],
                    "shard_id": job["shard_id"],
                    "stopped": True,
                    "stages_run": [],
                    "checkpoint_sequences": [],
                }
                for job in jobs
            ]

        errors = [item["error"] | {"shard_index": item.get("shard_index"), "shard_id": item.get("shard_id")} for item in results if not item.get("ok")]
        stopped = workspace.stop_requested()
        final_bundle = build_plan_bundle(workspace, config)
        previous_schedule = (workspace.state().get("last_run") or {}).get("schedule_fingerprint")
        schedule_validation = {
            "stage": "schedule",
            "status": "VALID",
            "previous_fingerprint": previous_schedule,
            "current_fingerprint": final_bundle["budget_plan"]["schedule_fingerprint"],
            "invalid_reasons": [],
            "affected_stages": [],
        }
        state = {
            "schema_version": 1,
            "status": "stopped" if stopped else ("completed_with_errors" if errors else "completed"),
            "stop_requested": stopped,
            "stopped": stopped,
            "last_run": {
                "started_at": started_at,
                "finished_at": utc_now(),
                "target_stage": target_stage,
                "execution_workers": execution_workers,
                "accepted_shard_ids": final_bundle["budget_plan"]["accepted_shard_ids"],
                "aggregate_digests": final_bundle["budget_plan"]["aggregate_digests"],
                "schedule_fingerprint": final_bundle["budget_plan"]["schedule_fingerprint"],
                "budget_fingerprint": final_bundle["budget_plan"]["budget_fingerprint"],
                "reserved_evaluations": final_bundle["budget_plan"]["budget"]["reserved_evaluations"],
                "actual_candidate_evaluations": final_bundle["budget_plan"]["actual_candidate_evaluations"],
                "stopped": stopped,
                "errors": errors,
                "results": results,
                "skipped": skipped,
                "stop": workspace.stop_info(),
                "schedule_validation": schedule_validation,
            },
        }
        workspace.save_state(state)
        collect_exports(root, write=True)
        workspace.cleanup_progress(run_token)
        final_bundle = build_plan_bundle(workspace, config)
        return {
            "ok": not errors,
            "workspace": str(workspace.root),
            "target_stage": target_stage,
            "execution_workers": execution_workers,
            "planned_concurrency": final_bundle["budget_plan"]["planned_concurrency"],
            "stopped": stopped,
            "stop": workspace.stop_info(),
            "accepted": final_bundle["budget_plan"]["accepted"],
            "unprocessed": final_bundle["budget_plan"]["unprocessed"],
            "skipped": skipped,
            "results": results,
            "errors": errors,
            "budget": final_bundle["budget_plan"]["budget"],
            "actual_candidate_evaluations": final_bundle["budget_plan"]["actual_candidate_evaluations"],
            "schedule_fingerprint": final_bundle["budget_plan"]["schedule_fingerprint"],
            "schedule_validation": final_bundle["schedule_validation"],
        }


def _match_artifacts_for_export(bundle: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    store = CheckpointStore(Path(bundle["workspace_root"]))
    accepted_ids = set(bundle["budget_plan"]["accepted_shard_ids"])
    match_artifacts: list[dict[str, Any]] = []
    aggregate_artifacts: list[dict[str, Any]] = []
    for plan in bundle["shard_plans"]:
        if plan["shard_id"] not in accepted_ids or not plan["all_valid"]:
            continue
        match_record = store.get(plan["shard_index"], "match")
        aggregate_record = store.get(plan["shard_index"], "aggregate")
        if match_record and aggregate_record:
            match_artifacts.append(match_record["artifact"])
            aggregate_artifacts.append(aggregate_record["artifact"])
    match_artifacts.sort(key=lambda item: (item["shard_index"], item["shard_id"]))
    aggregate_artifacts.sort(key=lambda item: (item["shard_index"], item["shard_id"]))
    return match_artifacts, aggregate_artifacts


def collect_exports(root: Path, *, write: bool = False) -> dict[str, Any]:
    workspace = Workspace(root)
    config = workspace.config()
    bundle = build_plan_bundle(workspace, config)
    bundle["workspace_root"] = str(workspace.root)
    match_artifacts, aggregate_artifacts = _match_artifacts_for_export(bundle)
    shard_order = [item["shard_id"] for item in match_artifacts]
    match_details = [detail for artifact in match_artifacts for detail in artifact["details"]]
    match_details.sort(key=lambda item: (item["shard_index"], item["image_id"], item["class_name"]))
    match_payload = {
        "schema_version": 1,
        "config_version": config["config_version"],
        "config_fingerprint": config_fingerprint(config),
        "iou_threshold": float(matching_config(config)["iou_threshold"]),
        "shard_order": shard_order,
        "match_details": match_details,
    }
    aggregate_core = aggregate_match_artifacts(match_artifacts)
    aggregate_payload = {
        "schema_version": 1,
        "config_version": config["config_version"],
        "config_fingerprint": config_fingerprint(config),
        "shard_order": shard_order,
        "aggregates": aggregate_core,
    }
    state = workspace.state()
    last_run = state.get("last_run") or {}
    schedule_payload = {
        "schema_version": 1,
        "config_version": config["config_version"],
        "config_fingerprint": config_fingerprint(config),
        "schedule_fingerprint": bundle["budget_plan"]["schedule_fingerprint"],
        "schedule_validation": bundle["schedule_validation"],
        "budget": bundle["budget_plan"]["budget"],
        "accepted": bundle["budget_plan"]["accepted"],
        "unprocessed": bundle["budget_plan"]["unprocessed"],
        "worker_lanes": bundle["budget_plan"]["worker_lanes"],
        "actual_candidate_evaluations": bundle["budget_plan"]["actual_candidate_evaluations"],
        "within_global_budget": bundle["budget_plan"]["within_global_budget"],
        "errors": last_run.get("errors", []),
    }
    diagnostics_payload = {
        "schema_version": 1,
        "config_version": config["config_version"],
        "config_fingerprint": config_fingerprint(config),
        "schedule_fingerprint": bundle["budget_plan"]["schedule_fingerprint"],
        "checkpoint_status": CheckpointStore(workspace.root).status(),
        "shards": [
            {
                "shard_id": plan["shard_id"],
                "shard_index": plan["shard_index"],
                "all_valid": plan["all_valid"],
                "earliest_invalid_stage": plan["earliest_invalid_stage"],
                "invalidation": plan["invalidation"],
                "web_of_stages": [
                    {
                        "stage": stage["stage"],
                        "status": stage["status"],
                        "expected_input_fingerprint": stage["expected_input_fingerprint"],
                        "existing": stage["existing"],
                        "invalid_reasons": stage["invalid_reasons"],
                    }
                    for stage in plan["stages"]
                ],
            }
            for plan in bundle["shard_plans"]
        ],
        "worker_errors": last_run.get("errors", []),
        "stop": workspace.stop_info(),
    }
    bundle_payload = {
        "matches": match_payload,
        "aggregates": aggregate_payload,
        "schedule": schedule_payload,
        "diagnostics": diagnostics_payload,
    }
    payloads = {
        "matches": match_payload,
        "aggregates": aggregate_payload,
        "schedule": schedule_payload,
        "diagnostics": diagnostics_payload,
        "bundle": bundle_payload,
    }
    if write:
        with locked(workspace.root / ".export.lock", blocking=True):
            for kind, payload in payloads.items():
                atomic_write_bytes(workspace.exports_dir / f"{kind}.json", canonical_bytes(payload) + b"\n")
    return payloads


def status_payload(root: Path) -> dict[str, Any]:
    workspace = Workspace(root)
    workspace.initialize()
    config = workspace.config()
    bundle = build_plan_bundle(workspace, config)
    exports = collect_exports(workspace.root, write=False)
    aggregate_core = exports["aggregates"]["aggregates"]
    accepted_ids = set(bundle["budget_plan"]["accepted_shard_ids"])
    state = workspace.state()
    shard_rows = []
    for plan in bundle["shard_plans"]:
        accepted = next(
            (item for item in bundle["budget_plan"]["accepted"] if item["shard_id"] == plan["shard_id"]),
            None,
        )
        shard_rows.append(
            {
                "shard_id": plan["shard_id"],
                "shard_index": plan["shard_index"],
                "sample_count": plan["sample_count"],
                "prediction_count": plan["prediction_count"],
                "class_set": plan["class_set"],
                "accepted": plan["shard_id"] in accepted_ids,
                "lane": accepted.get("lane") if accepted else None,
                "estimated_evaluations": accepted.get("estimated_evaluations") if accepted else None,
                "all_valid": plan["all_valid"],
                "earliest_invalid_stage": plan["earliest_invalid_stage"],
                "affected_stages": plan["affected_stages"],
                "invalidation": plan["invalidation"],
                "stages": [
                    {
                        "stage": stage["stage"],
                        "status": stage["status"],
                        "expected_input_fingerprint": stage["expected_input_fingerprint"],
                        "existing": stage["existing"],
                        "invalid_reasons": stage["invalid_reasons"],
                    }
                    for stage in plan["stages"]
                ],
            }
        )
    return {
        "schema_version": 1,
        "workspace": str(workspace.root),
        "config": config,
        "config_version": config["config_version"],
        "config_fingerprint": config_fingerprint(config),
        "status": state.get("status", "new"),
        "stop_requested": workspace.stop_requested(),
        "stop": workspace.stop_info(),
        "shards": shard_rows,
        "budget": {
            **bundle["budget_plan"]["budget"],
            "accepted_count": len(bundle["budget_plan"]["accepted"]),
            "unprocessed_count": len(bundle["budget_plan"]["unprocessed"]),
            "actual_candidate_evaluations": bundle["budget_plan"]["actual_candidate_evaluations"],
            "within_global_budget": bundle["budget_plan"]["within_global_budget"],
        },
        "accepted": bundle["budget_plan"]["accepted"],
        "unprocessed": bundle["budget_plan"]["unprocessed"],
        "worker_lanes": bundle["budget_plan"]["worker_lanes"],
        "planned_concurrency": bundle["budget_plan"]["planned_concurrency"],
        "schedule_fingerprint": bundle["budget_plan"]["schedule_fingerprint"],
        "schedule_validation": bundle["schedule_validation"],
        "checkpoint_status": CheckpointStore(workspace.root).status(),
        "aggregate_totals": aggregate_core["totals"],
        "aggregate_detail_count": aggregate_core["detail_count"],
        "last_run": state.get("last_run"),
    }


def fingerprints_payload(root: Path) -> dict[str, Any]:
    workspace = Workspace(root)
    workspace.initialize()
    config = workspace.config()
    bundle = build_plan_bundle(workspace, config)
    stages = []
    for plan in bundle["shard_plans"]:
        stages.append(
            {
                "shard_id": plan["shard_id"],
                "shard_index": plan["shard_index"],
                "shard_content_digest": plan["shard_content_digest"],
                "all_valid": plan["all_valid"],
                "earliest_invalid_stage": plan["earliest_invalid_stage"],
                "affected_stages": plan["affected_stages"],
                "invalidation": plan["invalidation"],
                "stages": [
                    {
                        "stage": stage["stage"],
                        "status": stage["status"],
                        "expected_input_fingerprint": stage["expected_input_fingerprint"],
                        "existing_input_fingerprint": (stage["existing"] or {}).get("input_fingerprint"),
                        "artifact_digest": (stage["existing"] or {}).get("artifact_digest"),
                        "sequence": (stage["existing"] or {}).get("sequence"),
                        "invalid_reasons": stage["invalid_reasons"],
                    }
                    for stage in plan["stages"]
                ],
            }
        )
    return {
        "schema_version": 1,
        "config": config,
        "config_version": config["config_version"],
        "config_fingerprint": config_fingerprint(config),
        "stage_config_fingerprints": {
            "decode": _stage_config_digest(config, "decode"),
            "match": _stage_config_digest(config, "match"),
            "aggregate": _stage_config_digest(config, "aggregate"),
            "schedule": digest_obj(budget_config(config)),
        },
        "schedule_fingerprint": bundle["budget_plan"]["schedule_fingerprint"],
        "schedule_validation": bundle["schedule_validation"],
        "shards": stages,
    }


def sample_view(root: Path, shard_id: str | None = None, image_id: str | None = None) -> dict[str, Any]:
    workspace = Workspace(root)
    workspace.initialize()
    store = CheckpointStore(workspace.root)
    shards = list_shards(workspace.root)
    config = workspace.config()
    bundle = build_plan_bundle(workspace, config)
    plan_by_id = bundle["plan_by_id"]
    available = []
    for shard in shards:
        decode_record = store.get(shard["shard_index"], "decode")
        match_record = store.get(shard["shard_index"], "match")
        if decode_record:
            plan = plan_by_id[shard["shard_id"]]
            decode_valid = _stage_record(plan, "decode")["status"] == "VALID"
            match_valid = _stage_record(plan, "match")["status"] == "VALID"
            available.append(
                {
                    "shard_id": shard["shard_id"],
                    "shard_index": shard["shard_index"],
                    "images": [sample["image_id"] for sample in decode_record["artifact"]["samples"]],
                    "has_decode": True,
                    "has_match": match_record is not None,
                    "decode_valid": decode_valid,
                    "match_valid": match_valid,
                    "all_valid": plan["all_valid"],
                    "earliest_invalid_stage": plan["earliest_invalid_stage"],
                    "invalidation": plan["invalidation"],
                }
            )
    if not available:
        return {"schema_version": 1, "shards": [], "selected": None, "message": "No decoded samples yet."}
    selected_shard = next((item for item in available if item["shard_id"] == shard_id), available[0])
    selected_plan = plan_by_id[selected_shard["shard_id"]]
    decode_record = store.get(selected_shard["shard_index"], "decode")
    match_record = store.get(selected_shard["shard_index"], "match")
    decode_valid = _stage_record(selected_plan, "decode")["status"] == "VALID"
    match_valid = _stage_record(selected_plan, "match")["status"] == "VALID"
    samples = decode_record["artifact"]["samples"]
    selected_image_id = image_id if image_id and any(item["image_id"] == image_id for item in samples) else samples[0]["image_id"]
    sample = copy.deepcopy(next(item for item in samples if item["image_id"] == selected_image_id))
    details = []
    if match_record and match_valid:
        details = [item for item in match_record["artifact"]["details"] if item["image_id"] == selected_image_id]
    status_by_id: dict[str, str] = {}
    for detail in details:
        for hit in detail["hits"]:
            status_by_id[hit["prediction_id"]] = "hit"
            status_by_id[hit["ground_truth_id"]] = "hit"
        for missed in detail["missed_ground_truth"]:
            status_by_id[missed["ground_truth_id"]] = "miss"
        for prediction in detail["duplicate_predictions"]:
            status_by_id[prediction["prediction_id"]] = "duplicate"
        for prediction in detail["background_false_positives"]:
            status_by_id[prediction["prediction_id"]] = "background_false_positive"
        for prediction in detail["invalid_predictions"]:
            status_by_id[prediction["prediction_id"]] = "invalid"
        for ground_truth in detail["invalid_ground_truth"]:
            status_by_id[ground_truth["ground_truth_id"]] = "invalid"
    for instance in sample["ground_truth"]:
        instance["match_status"] = status_by_id.get(instance["instance_id"], "invalid" if not instance["valid"] else "unmatched")
    for instance in sample["predictions"]:
        instance["match_status"] = status_by_id.get(instance["instance_id"], "invalid" if not instance["valid"] else "unmatched")
    return {
        "schema_version": 1,
        "shards": available,
        "selected": {
            "shard_id": selected_shard["shard_id"],
            "shard_index": selected_shard["shard_index"],
            "image_id": selected_image_id,
            "sample": sample,
            "matches": details,
            "stale": not decode_valid or not match_valid,
            "decode_valid": decode_valid,
            "match_valid": match_valid,
            "all_valid": selected_plan["all_valid"],
            "earliest_invalid_stage": selected_plan["earliest_invalid_stage"],
            "invalidation": selected_plan["invalidation"],
            "warning": (
                "STALE: this is an old checkpoint view; rebuild the invalidated stage before using it as a conclusion."
                if (not decode_valid or not match_valid)
                else None
            ),
        },
    }


def first_difference(left: bytes, right: bytes) -> dict[str, Any] | None:
    if left == right:
        return None
    limit = min(len(left), len(right))
    offset = 0
    while offset < limit and left[offset] == right[offset]:
        offset += 1
    prefix = left[:offset].decode("utf-8", errors="replace")
    line = prefix.count("\n") + 1
    last_newline = prefix.rfind("\n")
    column = len(prefix) - last_newline
    return {
        "byte_offset": offset,
        "line": line,
        "column": column,
        "left_length": len(left),
        "right_length": len(right),
        "left_context": left[max(0, offset - 40) : offset + 60].decode("utf-8", errors="replace"),
        "right_context": right[max(0, offset - 40) : offset + 60].decode("utf-8", errors="replace"),
    }


def verify_incremental(
    root: Path,
    *,
    auto_rebuild: bool = True,
    workers: int = 1,
    inject_mismatch: bool = False,
) -> dict[str, Any]:
    workspace = Workspace(root)
    workspace.initialize()
    rebuild_result = None
    if auto_rebuild:
        rebuild_result = run_workbench(workspace.root, target_stage="all", workers=workers, clear_stop=True)
    payloads = collect_exports(workspace.root, write=True)
    if inject_mismatch:
        payloads = copy.deepcopy(payloads)
        payloads["matches"]["injected_mismatch_probe"] = True
    config = workspace.config()
    shards = list_shards(workspace.root)
    full_root = workspace.tmp_dir / f"full-{uuid.uuid4().hex}"
    try:
        full = create_workspace(full_root, config=config)
        for shard in shards:
            import_shard(full.root, shard)
        run_workbench(full.root, target_stage="all", workers=1, clear_stop=True)
        full_payloads = collect_exports(full.root, write=True)
        comparisons = {}
        mismatches = {}
        for kind in ("matches", "aggregates"):
            left = canonical_bytes(payloads[kind]) + b"\n"
            right = canonical_bytes(full_payloads[kind]) + b"\n"
            difference = first_difference(left, right)
            comparisons[kind] = {
                "incremental_sha256": digest_obj(payloads[kind]),
                "full_sha256": digest_obj(full_payloads[kind]),
                "byte_equal": difference is None,
            }
            if difference is not None:
                mismatches[kind] = difference
        report = {
            "ok": not mismatches,
            "mismatch_injected": inject_mismatch,
            "auto_rebuild": rebuild_result,
            "comparisons": comparisons,
            "first_differences": mismatches,
        }
        if mismatches:
            raise WorkbenchError(
                "E_INCREMENTAL_MISMATCH",
                "Incremental outputs differ from a fresh full build.",
                report,
                409,
            )
        return report
    finally:
        shutil.rmtree(full_root, ignore_errors=True)


def request_stop(root: Path, reason: str = "user_request") -> dict[str, Any]:
    workspace = Workspace(root)
    workspace.initialize()
    payload = workspace.request_stop(reason)
    return {"ok": True, "stop": payload}


def resume_workbench(root: Path, *, workers: int | None = None) -> dict[str, Any]:
    workspace = Workspace(root)
    workspace.initialize()
    workspace.clear_stop()
    return run_workbench(workspace.root, target_stage="all", workers=workers, clear_stop=False)


def update_shard(root: Path, shard: dict[str, Any], *, workers: int | None = None) -> dict[str, Any]:
    workspace = Workspace(root)
    workspace.initialize()
    imported = import_shard(workspace.root, shard)
    run = run_workbench(
        workspace.root,
        target_stage="all",
        workers=workers,
        only_shard_ids=[shard["shard_id"]],
        clear_stop=True,
    )
    return {"ok": run["ok"] and not run["stopped"], "import": imported, "run": run}

