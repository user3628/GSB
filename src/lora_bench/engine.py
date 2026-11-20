"""Orchestration engine: admission, parallel pool, deterministic reduction, resume."""
from __future__ import annotations

import multiprocessing as mp
import os
from typing import Any

from .checkpoint import (
    invalidate_from,
    latest_sequence,
    load_latest,
    list_committed,
    load_latest_consistent,
)
from .config import (
    ExperimentConfig,
    TaskConfig,
    budget_fingerprint,
    parse_config,
    task_config_fingerprint,
)
from .data import Sample, load_jsonl, samples_fingerprint
from .determinism import atomic_write_json, file_lock, sha256_text, stable_json
from .errors import (
    InvalidPhaseError,
    InvalidTaskError,
    NotFoundError,
)
from .model import ModelConfig
from .runner import WorkerJob, run_worker
from .scheduler import (
    AdmissionDecision,
    admit_tasks,
    estimated_sample_consumption,
    select_task_samples,
)
from .state import (
    PHASE_COMPLETED,
    PHASE_FAILED,
    PHASE_NOT_STARTED,
    PHASE_PENDING,
    PHASE_RUNNING,
    PHASE_STOPPED,
    TaskState,
    WorkspaceState,
    clear_stop,
    load_state,
    mutate_state,
    request_stop,
    save_state,
    stop_requested,
)

SUMMARY_NAME = "summary.json"
ADMISSION_NAME = "admission.json"


def _mp_context() -> mp.context.BaseContext:
    # "spawn" is the only start method available on Windows and keeps workers
    # from inheriting transient parent RNG state.
    method = "spawn" if os.name == "nt" else "spawn"
    return mp.get_context(method)


def import_work(
    workspace: str,
    config_path: str,
    model_config: ModelConfig,
    *,
    reimport: bool = False,
) -> WorkspaceState:
    """(Re)import a config, invalidate affected checkpoints and reschedule.

    Returns the freshly persisted state.  Existing checkpoints of unchanged
    tasks are retained; tasks whose configuration or samples changed are
    invalidated from the earliest affected checkpoint; budget changes always
    trigger a fresh admission pass and previously budget-rejected records are
    never reused.
    """
    config = parse_config(config_path)
    samples = load_jsonmap(config)
    sample_fp = samples_fingerprint(samples)
    config_fp = sha256_text(stable_json(config.to_dict()))
    budget_fp = budget_fingerprint(config.budget)

    previous = load_state(workspace)
    state = WorkspaceState(
        experiment=config.experiment,
        run_phase=PHASE_PENDING,
        seed=config.seed,
        config_fingerprint=config_fp,
        budget_fingerprint=budget_fp,
        samples_fingerprint=sample_fp,
        budget=config.budget.to_dict(),
        global_cap=config.budget.global_sample_cap,
    )

    admitted, decisions, consumptions = admit_tasks(config, samples)
    admitted_ids = {task.task_id for task in admitted}
    decision_by_id = {item.task_id: item for item in decisions}
    previous_by_id = previous.tasks if previous else {}

    for index, task in enumerate(config.tasks):
        cfg_task_fp = task_config_fingerprint(task)
        selected = select_task_samples(task, samples)
        task_sample_fp = samples_fingerprint(selected)
        estimate = estimated_sample_consumption(task, len(selected))
        item = TaskState(
            task_id=task.task_id,
            index=index,
            config_fingerprint=cfg_task_fp,
            sample_fingerprint=task_sample_fp,
            estimated_samples=estimate,
            max_steps=task.max_steps,
        )
        prior = previous_by_id.get(task.task_id)
        if prior is not None:
            item.consumed_samples = prior.consumed_samples
            item.completed_steps = prior.completed_steps
            item.last_sequence = prior.last_sequence
            item.loss_history = list(prior.loss_history)
            item.adapter_digest = dict(prior.adapter_digest)
        if task.task_id not in admitted_ids:
            decision = decision_by_id[task.task_id]
            item.status = PHASE_NOT_STARTED
            item.rejection_reason = decision.reason
            item.rejection_detail = decision.detail
            item.consumed_samples = 0
            item.completed_steps = 0
            item.last_sequence = None
            item.loss_history = []
            item.adapter_digest = {}
            # Budget rejection must never leave reusable training state.
            invalidate_from(workspace, task.task_id, 0)
        elif prior is not None and (
            prior.config_fingerprint != cfg_task_fp
            or prior.sample_fingerprint != task_sample_fp
        ):
            affected_sequence = _earliest_affected_sequence(
                workspace, task.task_id, cfg_task_fp, task_sample_fp
            )
            removed = invalidate_from(workspace, task.task_id, affected_sequence)
            item.status = PHASE_PENDING
            item.invalidation = {
                "reason": "config_or_samples_changed",
                "affected_sequence": affected_sequence,
                "removed_checkpoints": removed,
                "previous_config_fingerprint": prior.config_fingerprint,
                "previous_sample_fingerprint": prior.sample_fingerprint,
                "config_fingerprint": cfg_task_fp,
                "sample_fingerprint": task_sample_fp,
            }
            restored = load_latest(workspace, task.task_id)
            if restored is not None:
                record, _ = restored
                item.completed_steps = record.step
                item.last_sequence = record.sequence
                item.loss_history = list(record.loss_history)
                item.adapter_digest = dict(record.adapter_digest)
            else:
                item.completed_steps = 0
                item.last_sequence = None
                item.loss_history = []
                item.adapter_digest = {}
        elif prior is not None and prior.status == PHASE_COMPLETED:
            item.status = PHASE_COMPLETED
        if task.task_id in admitted_ids:
            item.consumed_samples = estimate
        state.tasks[task.task_id] = item
        state.task_order.append(task.task_id)

    # Budget accounting is fixed at admission: every admitted task reserves its
    # full estimated consumption, so the ledger never changes as steps finish.
    state.global_consumed = sum(
        state.tasks[tid].estimated_samples for tid in admitted_ids
    )
    # Persist admission ledger separately for diagnostic queries.
    atomic_write_json(
        os.path.join(workspace, ADMISSION_NAME),
        {
            "budget_fingerprint": budget_fp,
            "decisions": [item.to_dict() for item in decisions],
        },
    )
    save_state(workspace, state)
    _persist_jobs(workspace, config, model_config, samples, admitted)
    return state


def load_jsonmap(config: ExperimentConfig) -> list[Sample]:
    return load_jsonl(config.samples_path)


def _earliest_affected_sequence(
    workspace: str, task_id: str, cfg_fp: str, sample_fp: str
) -> int:
    """Smallest committed sequence whose fingerprints no longer match."""
    from .checkpoint import load_checkpoint_dir

    for sequence, path in list_committed(workspace, task_id):
        try:
            record, _ = load_checkpoint_dir(path)
        except Exception:
            return sequence
        if (
            record.config_fingerprint != cfg_fp
            or record.sample_fingerprint != sample_fp
        ):
            return sequence
    return 1


def _persist_jobs(
    workspace: str,
    config: ExperimentConfig,
    model_config: ModelConfig,
    samples: list[Sample],
    admitted: list[TaskConfig],
) -> None:
    jobs = []
    for task in admitted:
        selected = select_task_samples(task, samples)
        sample_fp = samples_fingerprint(selected)
        cfg_fp = task_config_fingerprint(task)
        job = WorkerJob.build(
            workspace,
            config.model_path,
            model_config,
            task,
            selected,
            config.checkpoint_every,
            config.seed,
            cfg_fp,
            sample_fp,
        )
        jobs.append(job.__dict__)
    atomic_write_json(os.path.join(workspace, "jobs.json"), jobs)


def run(
    workspace: str,
    *,
    max_workers: int | None = None,
    keep_stop_flag: bool = False,
) -> dict[str, Any]:
    """Run all admitted pending tasks through the process pool.

    Already completed tasks are skipped; partially trained tasks resume from
    their latest consistent checkpoint.  Results are reduced in global task
    index order so parallel and serial executions are byte-identical.
    """
    state = load_state(workspace)
    if state is None:
        raise NotFoundError("workspace has not been imported yet", field="workspace")
    if state.run_phase == PHASE_RUNNING:
        raise InvalidPhaseError(
            "a run is already in progress", field="run_phase", detail={"phase": state.run_phase}
        )
    had_stop_request = stop_requested(workspace)
    if not keep_stop_flag:
        clear_stop(workspace)
    jobs = _load_jobs(workspace)
    if not jobs:
        raise InvalidTaskError("no admitted tasks to run", field="tasks")
    state_for_filter = load_state(workspace)
    jobs = [
        job
        for job in jobs
        if state_for_filter.tasks[job["task"]["task_id"]].status != PHASE_COMPLETED
    ]
    if not jobs:
        return build_summary(workspace)

    def mark_running(inner: WorkspaceState) -> None:
        inner.run_phase = PHASE_RUNNING
        for job in jobs:
            tid = job["task"]["task_id"]
            if inner.tasks[tid].status != PHASE_COMPLETED:
                inner.tasks[tid].status = PHASE_RUNNING
                consistent = load_latest_consistent(workspace, tid)
                if consistent is None:
                    inner.tasks[tid].loss_history = []
                    inner.tasks[tid].completed_steps = 0
                    inner.tasks[tid].adapter_digest = {}
                    inner.tasks[tid].last_sequence = None
                else:
                    record, _ = consistent
                    inner.tasks[tid].loss_history = list(record.loss_history)
                    inner.tasks[tid].completed_steps = record.step
                    inner.tasks[tid].last_sequence = record.sequence

    mutate_state(workspace, mark_running)

    worker_limit = max_workers or state.budget["max_workers"]
    worker_limit = min(worker_limit, state.budget["max_workers"], len(jobs))
    results: list[dict[str, Any]] = []
    ctx = _mp_context()
    if worker_limit <= 1:
        for job in jobs:
            results.append(run_worker(WorkerJob(**job)))
            _absorb_result(workspace, results[-1])
    else:
        with ctx.Pool(processes=worker_limit) as pool:
            futures = [
                pool.apply_async(run_worker, (WorkerJob(**job),))
                for job in jobs
            ]
            for future in futures:
                outcome = future.get()
                results.append(outcome)
                _absorb_result(workspace, outcome)

    ordered = sorted(results, key=lambda item: state.tasks[item["task_id"]].index)
    stopped = stop_requested(workspace) or (keep_stop_flag and had_stop_request)

    def finalize(inner: WorkspaceState) -> None:
        for outcome in ordered:
            task_state = inner.tasks[outcome["task_id"]]
            task_state.status = (
                PHASE_STOPPED if outcome["status"] == "stopped" else outcome["status"]
            )
            task_state.completed_steps = outcome["completed_steps"]
            task_state.loss_history = list(outcome["loss_history"])
            task_state.adapter_digest = dict(outcome["adapter_digest"])
            task_state.last_sequence = latest_sequence(workspace, outcome["task_id"])
            if outcome["error"] is not None:
                task_state.last_error = outcome["error"]
                task_state.status = PHASE_FAILED
            elif task_state.status != PHASE_STOPPED:
                task_state.status = (
                    PHASE_COMPLETED
                    if task_state.completed_steps >= task_state.max_steps
                    else PHASE_PENDING
                )
        if stopped:
            inner.run_phase = PHASE_STOPPED
        elif any(item["error"] is not None for item in ordered):
            inner.run_phase = PHASE_FAILED
        else:
            inner.run_phase = PHASE_COMPLETED

    mutate_state(workspace, finalize)
    summary = build_summary(workspace)
    if not had_stop_request:
        clear_stop(workspace)
    return summary


def _absorb_result(workspace: str, outcome: dict[str, Any]) -> None:
    """Persist progress incrementally so queries always see durable state."""

    def apply(inner: WorkspaceState) -> None:
        task_state = inner.tasks[outcome["task_id"]]
        task_state.completed_steps = outcome["completed_steps"]
        task_state.loss_history = list(outcome["loss_history"])
        task_state.adapter_digest = dict(outcome["adapter_digest"])
        task_state.last_sequence = latest_sequence(workspace, outcome["task_id"])
        if outcome["error"] is not None:
            task_state.last_error = outcome["error"]

    mutate_state(workspace, apply)


def _load_jobs(workspace: str) -> list[dict[str, Any]]:
    import json

    path = os.path.join(workspace, "jobs.json")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def stop(workspace: str) -> None:
    state = load_state(workspace)
    if state is None or state.run_phase != PHASE_RUNNING:
        raise InvalidPhaseError(
            "stop is only valid while a run is in progress",
            field="run_phase",
            detail={"phase": None if state is None else state.run_phase},
        )
    request_stop(workspace)


def resume(workspace: str, *, task_id: str | None = None) -> dict[str, Any]:
    state = load_state(workspace)
    if state is None:
        raise NotFoundError("workspace has not been imported yet", field="workspace")
    if state.run_phase == PHASE_RUNNING:
        raise InvalidPhaseError("cannot resume while running", field="run_phase")
    jobs = _load_jobs(workspace)
    jobs_by_id = {job["task"]["task_id"]: job for job in jobs}
    if task_id is not None:
        if task_id not in jobs_by_id:
            raise InvalidTaskError(
                f"task {task_id} has no resumable prerequisite checkpoint/admission",
                task_id=task_id,
            )
        jobs = [jobs_by_id[task_id]]
    else:
        jobs = [
            job
            for job in jobs
            if state.tasks[job["task"]["task_id"]].status != PHASE_COMPLETED
        ]
    if not jobs:
        return build_summary(workspace)
    return run(workspace)


def build_summary(workspace: str) -> dict[str, Any]:
    """Deterministic, byte-stable global summary ordered by task index."""
    state = load_state(workspace)
    if state is None:
        raise NotFoundError("workspace has not been imported yet", field="workspace")
    entries = []
    for task_id in state.task_order:
        task = state.tasks[task_id]
        entries.append(
            {
                "index": task.index,
                "task_id": task.task_id,
                "status": task.status,
                "completed_steps": task.completed_steps,
                "max_steps": task.max_steps,
                "estimated_samples": task.estimated_samples,
                "loss_history": task.loss_history,
                "adapter_digest": task.adapter_digest,
                "rejection_reason": task.rejection_reason,
                "config_fingerprint": task.config_fingerprint,
                "sample_fingerprint": task.sample_fingerprint,
                "last_error": task.last_error,
            }
        )
    summary = {
        "experiment": state.experiment,
        "run_phase": state.run_phase,
        "global_cap": state.global_cap,
        "global_consumed": state.global_consumed,
        "budget": state.budget,
        "tasks": entries,
    }
    summary["summary_fingerprint"] = sha256_text(stable_json(summary))
    atomic_write_json(os.path.join(workspace, SUMMARY_NAME), summary)

    def stamp(inner: WorkspaceState) -> None:
        inner.summary_fingerprint = summary["summary_fingerprint"]

    mutate_state(workspace, stamp)
    return summary
