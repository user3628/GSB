"""Parallel fold execution with deterministic reduction and graceful stop.

The engine accepts already-admitted :class:`FoldTask` objects.  Worker
processes are created with the ``spawn`` start method (the Windows default)
and communicate only through picklable payloads, so behaviour is identical
on Windows and Linux.  Results are always reduced in canonical order
``(shard_id, series_id, fold_index, step)``; consequently serial and
parallel runs produce byte-identical outputs.

Stopping is cooperative: when a stop flag is observed between folds the
current fold still completes and no new fold is launched.  Each fit also
honours its per-fit time budget through an explicit deadline check and is
recorded with ``E_TIME_BUDGET`` rather than being killed as a runaway task.
"""
from __future__ import annotations

import multiprocessing as mp
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

from .checkpoints import stop_requested
from .config import BacktestConfig
from .data import ShardData
from .errors import E_TIME_BUDGET, E_WORKER_FAILED
from .metrics import FoldFailure, build_fold_result
from .models import fit_window
from .scheduler import FoldTask


# ---------------------------------------------------------------------------
# Picklable worker payload
# ---------------------------------------------------------------------------
@dataclass
class WorkerPayload:
    task: FoldTask
    values: List[float]
    actual: List[float]
    timestamps: List[int]
    config: BacktestConfig
    deadline_ms: int


def _worker(payload: WorkerPayload):
    """Top-level worker function (required by the spawn start method)."""
    started = time.perf_counter()
    task = payload.task
    try:
        window = np.asarray(payload.values, dtype=np.float64)
        fit = fit_window(window, payload.config)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if payload.deadline_ms and elapsed_ms > payload.deadline_ms:
            return ("error", FoldFailure(
                shard_id=task.shard_id, series_id=task.series_id,
                fold_index=task.fold_index, worker_id=task.worker_id,
                code=E_TIME_BUDGET,
                message=(f"fit took {elapsed_ms:.3f} ms and exceeded the "
                         f"{payload.deadline_ms} ms budget")).to_dict())
        result = build_fold_result(task, fit, payload.actual,
                                   payload.timestamps, payload.config)
        return ("ok", result.to_dict())
    except Exception as exc:  # structured failure, never kills the run
        return ("error", FoldFailure(
            shard_id=task.shard_id, series_id=task.series_id,
            fold_index=task.fold_index, worker_id=task.worker_id,
            code=E_WORKER_FAILED,
            message=f"{type(exc).__name__}: {exc}").to_dict())


def _result_sort_key(record: Dict[str, Any]) -> Tuple[str, str, int]:
    return (record["shard_id"], record["series_id"], record["fold_index"])


@dataclass
class EngineRun:
    results: List[Dict[str, Any]] = field(default_factory=list)
    failures: List[Dict[str, Any]] = field(default_factory=list)
    stopped: bool = False
    not_started: List[Dict[str, Any]] = field(default_factory=list)


def _payload_for(task: FoldTask, shards: Dict[str, ShardData],
                 cfg: BacktestConfig) -> WorkerPayload:
    shard = shards[task.shard_id]
    series = shard.series[task.series_id]
    train_values = [float(x) for x in
                    series.target[task.train_start:task.train_end]]
    actual = [float(x) for x in
              series.target[task.test_start:task.test_end]]
    timestamps = list(series.timestamps[task.test_start:task.test_end])
    return WorkerPayload(task=task, values=train_values, actual=actual,
                         timestamps=timestamps, config=cfg,
                         deadline_ms=cfg.per_fit_time_budget_ms)


def run_serial(tasks: Sequence[FoldTask], shards: Dict[str, ShardData],
               cfg: BacktestConfig, workspace: Optional[str] = None,
               stop_after_folds: Optional[int] = None) -> EngineRun:
    run = EngineRun()
    for index, task in enumerate(tasks):
        if workspace and stop_requested(workspace):
            run.stopped = True
            run.not_started = [_task_summary(item) for item in tasks[index:]]
            break
        if stop_after_folds is not None and index >= stop_after_folds:
            run.stopped = True
            run.not_started = [_task_summary(item) for item in tasks[index:]]
            break
        status, payload = _worker(_payload_for(task, shards, cfg))
        if status == "ok":
            run.results.append(payload)
        else:
            run.failures.append(payload)
    _sort_run(run)
    return run


def run_parallel(tasks: Sequence[FoldTask], shards: Dict[str, ShardData],
                 cfg: BacktestConfig, workspace: Optional[str] = None,
                 stop_after_folds: Optional[int] = None) -> EngineRun:
    run = EngineRun()
    if not tasks:
        return run

    context = mp.get_context("spawn")
    pool = context.Pool(processes=min(cfg.max_workers, len(tasks)))
    in_flight: Dict[Any, FoldTask] = {}
    submitted = 0
    completed_folds = 0
    total = len(tasks)
    stopping = False

    def report_not_started() -> List[Dict[str, Any]]:
        # every admitted fold beyond the ones launched is recorded, whether
        # it was blocked by the fold budget or the cooperative stop flag
        return [_task_summary(item) for item in tasks[submitted:]]

    try:
        def budget_reached() -> bool:
            return stop_after_folds is not None and \
                completed_folds >= stop_after_folds

        def can_submit() -> bool:
            if submitted >= total:
                return False
            if len(in_flight) >= cfg.max_workers:
                return False
            if stop_after_folds is not None and \
                    submitted >= stop_after_folds:
                return False
            return True

        # initial fill already honours the fold budget via can_submit()
        while can_submit():
            task = tasks[submitted]
            in_flight[pool.apply_async(
                _worker, (_payload_for(task, shards, cfg),))] = task
            submitted += 1

        while in_flight:
            cooperative_stop = bool(workspace and
                                    stop_requested(workspace))
            if cooperative_stop or budget_reached():
                # finish every fold already in flight, launch nothing new
                for pending in list(in_flight):
                    status, payload = in_flight.pop(pending).get()
                    (run.results if status == "ok"
                     else run.failures).append(payload)
                    completed_folds += 1
                stopping = True
                run.stopped = True
                run.not_started = report_not_started()
                break

            ready = [async_result for async_result in in_flight
                     if async_result.ready()]
            if not ready:
                time.sleep(0.005)
                continue

            for async_result in ready:
                task = in_flight.pop(async_result)
                status, payload = async_result.get()
                (run.results if status == "ok"
                 else run.failures).append(payload)
                completed_folds += 1
                if budget_reached():
                    stopping = True
                    run.stopped = True
                    break

            if stopping:
                # do not launch any further fold; drain what is in flight
                for pending in list(in_flight):
                    status, payload = in_flight.pop(pending).get()
                    (run.results if status == "ok"
                     else run.failures).append(payload)
                    completed_folds += 1
                run.not_started = report_not_started()
                break

            # refill exactly the slots that became free
            while can_submit() and not stopping:
                task = tasks[submitted]
                in_flight[pool.apply_async(
                    _worker, (_payload_for(task, shards, cfg),))] = task
                submitted += 1
    finally:
        pool.terminate()
        pool.join()
    _sort_run(run)
    return run


def _task_summary(task: FoldTask) -> Dict[str, Any]:
    return {
        "shard_id": task.shard_id,
        "series_id": task.series_id,
        "fold_index": task.fold_index,
        "worker_id": task.worker_id,
        "code": "E_STOPPED",
        "reason": "fold not started because the run was stopped",
    }


def _sort_run(run: EngineRun) -> None:
    run.results.sort(key=_result_sort_key)
    run.failures.sort(key=_result_sort_key)
    run.not_started.sort(key=lambda item: (item["shard_id"],
                                           item["series_id"],
                                           item["fold_index"]))
