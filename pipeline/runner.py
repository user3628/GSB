from __future__ import annotations

import shutil
import tempfile
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from .batch import BatchExecution, build_cache_plan, process_batch
from .checkpoints import STAGES, CheckpointManager
from .config import Config, load as load_config
from .data import Dataset, load_dataset
from .errors import E_STOPPED, PipelineError
from .aggregate import summarize_results
from .model import model_parameter_summary
from .scheduler import accepted_batches, plan_schedule
from .util import (
    atomic_move,
    atomic_write_json,
    atomic_write_text,
    canonical_digest,
    canonical_json,
    first_difference,
    json_line,
    read_json,
    read_jsonl,
    sha256_file,
)


FINAL_FILES = (
    "results.jsonl",
    "summary.json",
    "calibrations.json",
    "parameters.json",
    "schedule.json",
    "report.json",
)
DIAGNOSTIC_FILES = ("incremental_report.json",)


def _remove_final_files(workspace: Path) -> None:
    for name in FINAL_FILES + DIAGNOSTIC_FILES:
        try:
            (workspace / name).unlink()
        except FileNotFoundError:
            pass


def _move_stage_artifacts(workspace: Path, execution: BatchExecution, payload: dict[str, Any]) -> None:
    for artifact in payload["artifacts"]:
        source = execution.temp_dir / artifact["path"]
        target = workspace / artifact["path"]
        atomic_move(source, target)


def _publish_execution(manager: CheckpointManager, execution: BatchExecution, config: Config) -> None:
    plan = execution.plan
    if plan.requires_invalidation:
        first_payload = execution.stage_payloads[0]
        index = first_payload["stage_index"]
        old_record = plan.cached_records.get(index)
        manager.invalidate(
            plan.batch.batch_id,
            STAGES[index],
            "E_STAGE_FINGERPRINT_CHANGED",
            old_fingerprint=old_record.get("input_fingerprint") if old_record else None,
            new_fingerprint=first_payload["input_fingerprint"],
            config_version=config.version,
        )
    for payload in execution.stage_payloads:
        _move_stage_artifacts(manager.workspace, execution, payload)
        manager.publish(
            batch_id=plan.batch.batch_id,
            stage=payload["stage"],
            input_fingerprint=payload["input_fingerprint"],
            config_version=config.version,
            artifacts=payload["artifacts"],
        )
    shutil.rmtree(execution.temp_dir, ignore_errors=True)


def _execute_jobs(
    jobs: list[tuple[Any, Callable[[], BatchExecution]]],
    workers: int,
) -> list[BatchExecution]:
    if not jobs:
        return []
    results: dict[str, BatchExecution] = {}
    errors: dict[str, BaseException] = {}
    executor = ThreadPoolExecutor(max_workers=min(workers, len(jobs)), thread_name_prefix="batch")
    futures: dict[Future[BatchExecution], str] = {}
    try:
        for batch_id, function in jobs:
            futures[executor.submit(function)] = batch_id
        for future, batch_id in futures.items():
            try:
                results[batch_id] = future.result()
            except BaseException as exc:
                errors[batch_id] = exc
    finally:
        executor.shutdown(wait=True, cancel_futures=False)
    if errors:
        first_batch = sorted(errors)[0]
        raise errors[first_batch]
    return [results[batch_id] for batch_id, _function in jobs]


def _write_global_outputs(
    workspace: Path,
    config: Config,
    dataset: Dataset,
    accepted_batch_ids: list[str],
    schedule: dict[str, Any],
    checkpoints_contiguous: bool,
) -> dict[str, Any]:
    result_rows: list[dict[str, Any]] = []
    for batch_id in accepted_batch_ids:
        result_rows.extend(read_jsonl(workspace / "results" / f"{batch_id}.jsonl"))
    result_rows.sort(key=lambda row: row["doc_id"])
    result_doc_ids = [row["doc_id"] for row in result_rows]
    if len({row["doc_id"] for row in result_rows}) != len(result_rows):
        raise PipelineError("E_INVALID_CONFIG", "duplicate doc_id in global result set")
    results_text = "".join(json_line(row) for row in result_rows)
    atomic_write_text(workspace / "results.jsonl", results_text)
    results_digest = canonical_digest(result_rows)
    summary = summarize_results(
        result_rows,
        labels=tuple(sorted(config.labels)),
        languages=dataset.languages,
        channels=dataset.channels,
        class_weights=config.class_weights,
    )
    atomic_write_json(workspace / "summary.json", summary)

    calibrations: list[dict[str, Any]] = []
    for batch_id in accepted_batch_ids:
        calibration_path = workspace / "calibration" / f"{batch_id}.json"
        calibration = read_json(calibration_path)
        classes = {
            label: {
                "threshold": info["threshold"],
                "hit_rate": info["hit_rate"],
                "false_positive_rate": info["false_positive_rate"],
                "target_met": info["target_met"],
                "review_required": info["review_required"],
                "review_reasons": info["review_reasons"],
                "positive_sample_count": info["positive_sample_count"],
                "negative_sample_count": info["negative_sample_count"],
                "sample_range": info["samples"]["range"],
                "positive_ids_digest": info["samples"]["positive_ids_digest"],
                "negative_ids_digest": info["samples"]["negative_ids_digest"],
            }
            for label, info in sorted(calibration["classes"].items())
        }
        calibrations.append(
            {
                "batch_id": batch_id,
                "artifact": f"calibration/{batch_id}.json",
                "artifact_digest": sha256_file(calibration_path),
                "calibration_digest": calibration["calibration_digest"],
                "fold": calibration["fold"],
                "classes": classes,
            }
        )
    calibration_bundle = {
        "schema": "calibration-bundle-v1",
        "target_fpr": config.target_fpr,
        "threshold_max": config.threshold_max,
        "batches": calibrations,
    }
    calibration_bundle["bundle_digest"] = canonical_digest(calibration_bundle)
    atomic_write_json(workspace / "calibrations.json", calibration_bundle)

    parameter_batches: list[dict[str, Any]] = []
    for batch_id in accepted_batch_ids:
        full_model = read_json(workspace / "models" / f"{batch_id}.json")
        holdout_model = read_json(workspace / "models" / f"{batch_id}.holdout.json")
        parameter_batches.append(
            {
                "batch_id": batch_id,
                "full": model_parameter_summary(full_model),
                "holdout": model_parameter_summary(holdout_model),
            }
        )
    parameters = {
        "schema": "parameter-summary-v1",
        "config_version": config.version,
        "config_digest": config.digest,
        "config": config.stage_config(),
        "batches": parameter_batches,
    }
    parameters["parameters_digest"] = canonical_digest(parameters)
    atomic_write_json(workspace / "parameters.json", parameters)
    atomic_write_json(workspace / "schedule.json", schedule)

    recomputed = summarize_results(
        result_rows,
        labels=tuple(sorted(config.labels)),
        languages=dataset.languages,
        channels=dataset.channels,
        class_weights=config.class_weights,
    )
    summary_match = canonical_json(summary) == canonical_json(recomputed)
    aggregate_difference = first_difference(summary, recomputed)
    output_digests = {
        "results.jsonl": sha256_file(workspace / "results.jsonl"),
        "summary.json": sha256_file(workspace / "summary.json"),
        "calibrations.json": sha256_file(workspace / "calibrations.json"),
        "parameters.json": sha256_file(workspace / "parameters.json"),
        "schedule.json": sha256_file(workspace / "schedule.json"),
    }
    report = {
        "schema": "pipeline-report-v1",
        "status": "ok",
        "config_version": config.version,
        "config_digest": config.digest,
        "accepted_batch_ids": accepted_batch_ids,
        "rejected_batch_ids": [item["batch_id"] for item in schedule["rejected"]],
        "doc_count": len(result_rows),
        "decision_counts": {
            decision: sum(1 for row in result_rows if row.get("decision") == decision)
            for decision in sorted({row.get("decision") for row in result_rows})
        },
        "error_counts": {
            code: sum(1 for row in result_rows if row.get("error_code") == code)
            for code in sorted({row.get("error_code") for row in result_rows if row.get("error_code")})
        },
        "review_required_count": sum(1 for row in result_rows if row.get("review_required")),
        "calibration_review_class_count": sum(
            1
            for calibration in calibrations
            for info in calibration["classes"].values()
            if info["review_required"]
        ),
        "verification": {
            "results_sorted_by_doc_id": result_doc_ids == sorted(result_doc_ids),
            "results_digest": results_digest,
            "summary_results_digest_matches": summary.get("results_digest") == results_digest,
            "aggregate_recheck_equal": summary_match,
            "aggregate_first_difference": (
                {
                    "field": aggregate_difference[0],
                    "recomputed": aggregate_difference[1],
                    "stored": aggregate_difference[2],
                }
                if aggregate_difference
                else None
            ),
            "aggregate_error_bound": 1e-9,
            "checkpoint_sequence_contiguous": checkpoints_contiguous,
        },
        "artifact_digests": output_digests,
    }
    report["report_digest"] = canonical_digest(report)
    atomic_write_json(workspace / "report.json", report)
    return {
        "status": "ok",
        "doc_count": len(result_rows),
        "accepted_batches": accepted_batch_ids,
        "rejected_batches": report["rejected_batch_ids"],
        "results_digest": results_digest,
        "report_digest": report["report_digest"],
    }


def run_pipeline(
    workspace: Path | str,
    config_path: Path | str,
    dataset_index_path: Path | str,
    stop_event: threading.Event | None = None,
    stop_after_batches: int | None = None,
    execution_workers: int | None = None,
) -> dict[str, Any]:
    workspace_path = Path(workspace).resolve()
    workspace_path.mkdir(parents=True, exist_ok=True)
    (workspace_path / ".tmp").mkdir(parents=True, exist_ok=True)
    config = load_config(config_path)
    dataset = load_dataset(dataset_index_path)
    worker_count = min(
        config.workers,
        execution_workers if execution_workers is not None else config.workers,
    )
    if worker_count < 1:
        raise PipelineError("E_INVALID_CONFIG", "execution worker count must be positive")
    unknown_dataset_labels = sorted(set(dataset.labels) - set(config.labels))
    if unknown_dataset_labels:
        raise PipelineError(
            "E_INVALID_LABEL",
            "dataset labels are absent from configuration",
            {"labels": unknown_dataset_labels},
        )
    accepted = accepted_batches(dataset, config.evaluation_budget)
    accepted_ids = [batch.batch_id for batch in accepted]
    schedule = plan_schedule(dataset, config.evaluation_budget, config.workers)
    _remove_final_files(workspace_path)
    atomic_write_json(workspace_path / "schedule.json", schedule)
    atomic_write_json(
        workspace_path / "run_manifest.json",
        {
            "config_version": config.version,
            "config_digest": config.digest,
            "dataset_index": str(dataset.index_path),
            "config_path": str(Path(config_path).resolve()),
            "batch_ids": accepted_ids,
        },
    )
    manager = CheckpointManager(workspace_path)
    checkpoint_state = manager.state()
    plans = [
        build_cache_plan(batch, config, workspace_path, checkpoint_state)
        for batch in accepted
    ]
    event = stop_event or threading.Event()
    stopped = False
    handled = 0
    temp_root = Path(tempfile.mkdtemp(prefix="run-", dir=workspace_path / ".tmp"))
    try:
        index = 0
        while index < len(plans):
            if event.is_set():
                stopped = True
                break
            if stop_after_batches is not None and handled >= stop_after_batches:
                stopped = True
                break
            remaining_for_stop = (
                stop_after_batches - handled
                if stop_after_batches is not None
                else worker_count
            )
            window_size = max(1, min(worker_count, remaining_for_stop))
            window = plans[index : index + window_size]
            jobs: list[tuple[Any, Callable[[], BatchExecution]]] = []
            for plan in window:
                if plan.fully_cached:
                    continue
                jobs.append(
                    (
                        plan.batch.batch_id,
                        lambda plan=plan: process_batch(plan, config, workspace_path, temp_root),
                    )
                )
            executions = _execute_jobs(jobs, worker_count)
            execution_by_batch = {execution.plan.batch.batch_id: execution for execution in executions}
            for plan in window:
                execution = execution_by_batch.get(plan.batch.batch_id)
                if execution is not None:
                    _publish_execution(manager, execution, config)
                handled += 1
            index += len(window)
            if event.is_set():
                stopped = True
                break
        if index >= len(plans):
            stopped = False
        if stopped:
            return {
                "status": "stopped",
                "error_code": E_STOPPED,
                "completed_batches": handled,
                "accepted_batches": accepted_ids,
                "checkpoint_sequence": manager.state()["sequence"],
            }
        for plan in plans:
            if manager.next_stage_index(plan.batch.batch_id) != 4:
                raise PipelineError(
                    "E_INVALID_STAGE",
                    "batch is missing one or more completed stages",
                    {"batch_id": plan.batch.batch_id, "next_stage_index": manager.next_stage_index(plan.batch.batch_id)},
                )
        state = manager.state()
        integrity = manager.verify_integrity()
        result = _write_global_outputs(
            workspace_path,
            config,
            dataset,
            accepted_ids,
            schedule,
            bool(integrity["contiguous"]),
        )
        invalidations = [
            {
                "sequence": record["sequence"],
                "batch_id": record["batch_id"],
                "earliest_stage": record["stage"],
                "reason": record["reason"],
                "old_fingerprint": record.get("old_fingerprint"),
                "new_fingerprint": record.get("new_fingerprint"),
            }
            for batch_info in state["batches"].values()
            for record in batch_info.get("invalidations", [])
        ]
        invalidations.sort(key=lambda item: item["sequence"])
        atomic_write_json(
            workspace_path / "incremental_report.json",
            {
                "schema": "incremental-report-v1",
                "invalidations": invalidations,
                "checkpoint_sequence": state["sequence"],
                "checkpoint_integrity": manager.verify_integrity(),
            },
        )
        result["checkpoint_sequence"] = state["sequence"]
        return result
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
