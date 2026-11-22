"""Process-pool execution, bounded stop handling and deterministic reduction."""

from __future__ import annotations

import multiprocessing
import os
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Any

from .errors import E_INTERNAL, PipelineError
from .manifest import load_manifest
from .scheduler import schedule_shards
from .util import atomic_write_json, digest_value
from .worker import process_shard


def _touch(path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"stop\n")


def _clear(path: str | Path | None) -> None:
    if path:
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass


def _parent_error(shard: dict[str, Any], exc: BaseException) -> dict[str, Any]:
    return {
        "shard_id": shard["spec"]["shard_id"],
        "shard_ordinal": int(shard["spec"]["shard_ordinal"]),
        "status": "error",
        "stage": "worker",
        "error": PipelineError(
            code=E_INTERNAL,
            message=f"worker process failed: {type(exc).__name__}: {exc}",
            stage="worker",
            shard_id=shard["spec"]["shard_id"],
            details={"exception_type": type(exc).__name__},
        ).to_dict(),
    }


def _execute_accepted(
    accepted: list[dict[str, Any]],
    *,
    workspace: str,
    budget: dict[str, int],
    concurrency: int,
    stop_file: str | None,
    stop_after_completed: int | None,
) -> tuple[list[dict[str, Any]], bool]:
    if not accepted:
        return [], False
    pending = deque(accepted)
    futures: dict[Future[dict[str, Any]], dict[str, Any]] = {}
    results: list[dict[str, Any]] = []
    completed_count = 0
    stop_requested = bool(stop_file and os.path.exists(stop_file))

    def submit_until_full(executor: ProcessPoolExecutor) -> None:
        nonlocal stop_requested
        while (
            pending
            and len(futures) < max(1, int(concurrency))
            and not stop_requested
            and not (stop_file and os.path.exists(stop_file))
        ):
            shard = pending.popleft()
            task = {
                "workspace": workspace,
                "budget": budget,
                "stop_file": stop_file,
                "spec": shard["spec"],
            }
            future = executor.submit(process_shard, task)
            futures[future] = shard

    context = multiprocessing.get_context("spawn" if os.name == "nt" else "fork")
    with ProcessPoolExecutor(
        max_workers=max(1, int(concurrency)), mp_context=context
    ) as executor:
        submit_until_full(executor)
        while futures:
            done, _ = wait(tuple(futures), return_when=FIRST_COMPLETED)
            for future in sorted(done, key=lambda item: futures[item]["spec"]["shard_ordinal"]):
                shard = futures.pop(future)
                try:
                    result = future.result()
                except BaseException as exc:
                    result = _parent_error(shard, exc)
                results.append(result)
                completed_count += 1
                if result.get("status") == "stopped":
                    stop_requested = True
            if (
                stop_after_completed is not None
                and completed_count >= int(stop_after_completed)
                and not stop_requested
            ):
                if stop_file:
                    _touch(stop_file)
                stop_requested = True
            if stop_file and os.path.exists(stop_file):
                stop_requested = True
            submit_until_full(executor)

    if stop_requested:
        for shard in pending:
            results.append(
                {
                    "shard_id": shard["spec"]["shard_id"],
                    "shard_ordinal": int(shard["spec"]["shard_ordinal"]),
                    "status": "not_started",
                }
            )
    return sorted(results, key=lambda item: item["shard_ordinal"]), stop_requested


def build_summary(
    manifest: dict[str, Any],
    schedule: dict[str, Any],
    worker_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Reduce results strictly by shard ordinal; no completion-order metadata."""

    by_ordinal: dict[int, dict[str, Any]] = {}
    for item in schedule["unprocessed"]:
        by_ordinal[int(item["shard_ordinal"])] = {
            "shard_id": item["shard_id"],
            "shard_ordinal": int(item["shard_ordinal"]),
            "status": "unprocessed",
            "error": item["error"],
        }
    for result in worker_results:
        ordinal = int(result["shard_ordinal"])
        if result["status"] == "success":
            by_ordinal[ordinal] = {
                "shard_id": result["shard_id"],
                "shard_ordinal": ordinal,
                "status": "success",
                "result": result["result"],
            }
        elif result["status"] == "error":
            by_ordinal[ordinal] = {
                "shard_id": result["shard_id"],
                "shard_ordinal": ordinal,
                "status": "error",
                "error": result["error"],
            }
        elif result["status"] in {"stopped", "not_started"}:
            by_ordinal[ordinal] = {
                "shard_id": result["shard_id"],
                "shard_ordinal": ordinal,
                "status": result["status"],
            }

    entries = [by_ordinal[index] for index in sorted(by_ordinal)]
    success_entries = [entry for entry in entries if entry["status"] == "success"]
    error_entries = [entry for entry in entries if entry["status"] == "error"]
    unprocessed_entries = [entry for entry in entries if entry["status"] == "unprocessed"]
    body = {
        "schema_version": 1,
        "manifest_digest": manifest["manifest_digest"],
        "shards": entries,
        "stats": {
            "declared_shards": len(manifest["shards"]),
            "accepted_shards": len(schedule["accepted"]),
            "successful_shards": len(success_entries),
            "error_shards": len(error_entries),
            "unprocessed_shards": len(unprocessed_entries),
            "elements_evaluated": sum(
                int(entry["result"]["evaluated_elements"]) for entry in success_entries
            ),
            "global_element_budget": int(schedule["global_elements"]),
            "single_worker_element_quota": int(schedule["worker_elements"]),
            "peak_memory_limit_bytes": int(schedule["peak_memory_bytes"]),
            "accepted_estimated_elements": int(schedule["consumed_elements"]),
        },
    }
    return {**body, "output_sha256": digest_value(body)}


def export_summary(path: str | Path, summary: dict[str, Any]) -> None:
    atomic_write_json(path, summary)


def run_manifest(
    manifest_path: str | Path,
    workspace: str | Path,
    *,
    stop_file: str | Path | None = None,
    stop_after_completed: int | None = None,
    reset_stop_file: bool = False,
    workers: int | None = None,
) -> dict[str, Any]:
    """Execute a manifest and return either a complete summary or a stop result."""

    manifest = load_manifest(manifest_path)
    if workers is not None:
        manifest["workers"] = int(workers)
    workspace_path = Path(workspace).resolve()
    workspace_path.mkdir(parents=True, exist_ok=True)
    (workspace_path / "locks").mkdir(parents=True, exist_ok=True)
    schedule = schedule_shards(manifest)
    resolved_stop_file = (
        str(Path(stop_file).resolve())
        if stop_file is not None
        else str(workspace_path / "STOP_REQUESTED")
    )
    if reset_stop_file:
        _clear(resolved_stop_file)

    results, stopped = _execute_accepted(
        schedule["accepted"],
        workspace=str(workspace_path),
        budget=manifest["budget"],
        concurrency=schedule["concurrency"],
        stop_file=resolved_stop_file,
        stop_after_completed=stop_after_completed,
    )
    summary = build_summary(manifest, schedule, results)
    return {
        "stopped": bool(stopped),
        "summary": summary,
        "schedule": schedule,
        "worker_results": results,
        "workspace": str(workspace_path),
        "stop_file": resolved_stop_file,
        "manifest": manifest,
    }
