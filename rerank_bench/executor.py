"""Parallel shard execution with a bounded local process pool.

Exactly N worker processes pull shard tasks from a queue. The dispatcher
only feeds a new shard when a worker slot frees and no stop has been
requested, so a stop request lets in-flight (indivisible) shards finish
and leaves the rest untouched for resume.
"""

from __future__ import annotations

import multiprocessing as mp
import queue
import sys

from .pipeline import run_shard_stages


def _worker_loop(task_queue, result_queue, workspace_root, config_raw,
                 latency_budget_ms, repo_root, stages):
    if repo_root and repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    while True:
        task = task_queue.get()
        if task is None:
            return
        shard_id, shard_file = task
        result = run_shard_stages(
            workspace_root, shard_id, shard_file, config_raw,
            latency_budget_ms, stages=stages)
        result_queue.put(result)


def execute_shards(workspace, admitted, config_raw, latency_budget_ms,
                   concurrency, stop_checker, serial=False, repo_root=None,
                   stages=None, stop_after=None):
    """Execute admitted shards; returns {results, stopped, pending}."""
    tasks = [(a["shard_id"], a["shard_file"]) for a in admitted]
    results = {}
    if serial or concurrency <= 1:
        pending = []
        stopped = False
        for shard_id, shard_file in tasks:
            if stop_checker():
                pending.append(shard_id)
                stopped = True
                continue
            results[shard_id] = run_shard_stages(
                workspace.root, shard_id, shard_file, config_raw,
                latency_budget_ms, stages=stages)
            if stop_after is not None and len(results) >= stop_after:
                stopped = True
                pending.extend(sid for sid, _ in tasks[len(results):])
                break
        return {"results": results, "stopped": stopped, "pending": pending}

    ctx = mp.get_context("spawn")
    task_queue = ctx.Queue()
    result_queue = ctx.Queue()
    workers = [
        ctx.Process(
            target=_worker_loop,
            args=(task_queue, result_queue, workspace.root, config_raw,
                  latency_budget_ms, repo_root, stages),
            daemon=True,
        )
        for _ in range(concurrency)
    ]
    for proc in workers:
        proc.start()
    pending_tasks = list(tasks)
    in_flight = 0
    stopped = False
    fed = 0
    in_flight_shards = []
    try:
        while pending_tasks and in_flight < concurrency:
            if stop_checker():
                stopped = True
                break
            if stop_after is not None and fed >= stop_after:
                stopped = True
                break
            task = pending_tasks.pop(0)
            task_queue.put(task)
            in_flight_shards.append(task[0])
            in_flight += 1
            fed += 1
        while in_flight:
            try:
                result = result_queue.get(timeout=0.2)
            except queue.Empty:
                if stop_checker():
                    stopped = True
                if not any(proc.is_alive() for proc in workers):
                    # workers died without returning results: record
                    # structured errors instead of waiting forever
                    from .errors import E_WORKER_FAILURE, error_dict
                    for shard_id in in_flight_shards:
                        results[shard_id] = {
                            "shard_id": shard_id,
                            "status": "error",
                            "error": error_dict(
                                E_WORKER_FAILURE,
                                "worker process exited before returning a "
                                "result", shard_id=shard_id),
                        }
                    in_flight = 0
                    stopped = True
                continue
            results[result["shard_id"]] = result
            in_flight_shards.remove(result["shard_id"])
            in_flight -= 1
            hit_limit = stop_after is not None and fed >= stop_after
            if pending_tasks and not stop_checker() and not hit_limit:
                task = pending_tasks.pop(0)
                task_queue.put(task)
                in_flight_shards.append(task[0])
                in_flight += 1
                fed += 1
            elif pending_tasks:
                stopped = True
    finally:
        for _ in workers:
            task_queue.put(None)
        for proc in workers:
            proc.join(timeout=30)
            if proc.is_alive():
                proc.terminate()
    return {
        "results": results,
        "stopped": stopped,
        "pending": [sid for sid, _ in pending_tasks],
    }
