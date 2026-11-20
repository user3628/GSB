"""Parallel shard runner with budgets, graceful stop and deterministic reduce."""

from __future__ import annotations

import multiprocessing as mp
from pathlib import Path
from typing import Any, Dict, List, Optional

from .checkpoint import (
    CheckpointRecord,
    CheckpointStore,
)
from .fingerprints import combine_fingerprints
from .io_util import atomic_write_json, ensure_dir
from .manifest import Manifest, load_manifest, manifest_config_fingerprint
from .scheduler import (
    enforce_runtime_admission,
    estimate_shard,
    plan_schedule,
)
from .worker import process_shard_task

STOP_FILE = "STOP"
SUMMARY_FILE = "summary.json"
SCHEDULE_FILE = "schedule.json"


class DeterministicPublisher:
    """Commits stage records in strict global sequence order.

    Worker processes compute shards concurrently and return pending record
    payloads.  The parent process funnels them through this queue so commits
    always land as 0, 1, 2, ... regardless of which worker finished first.
    File locks inside :class:`CheckpointStore` additionally protect against
    multiple processes publishing into the same workspace.
    """

    def __init__(self, store: CheckpointStore):
        self.store = store
        self._buffered: Dict[int, Dict[str, Any]] = {}
        self._next = store.next_sequence()

    def publish_result(self, result: Dict[str, Any]) -> None:
        for payload in result.get("pending_records", []):
            self._buffered[int(payload["sequence"])] = payload
        self._flush()

    def _flush(self) -> None:
        while self._next in self._buffered:
            payload = self._buffered.pop(self._next)
            record = CheckpointRecord(**payload)
            self.store.commit(record)
            self._next += 1


def run_fingerprint(manifest: Manifest, data_dir: Path) -> str:
    parts = [manifest_config_fingerprint(manifest), str(data_dir)]
    return combine_fingerprints(parts)


def request_stop(run_dir: str | Path) -> Path:
    path = Path(run_dir) / STOP_FILE
    ensure_dir(path.parent)
    path.write_text("stop requested\n", encoding="utf-8")
    return path


def stop_requested(run_dir: str | Path) -> bool:
    return (Path(run_dir) / STOP_FILE).exists()


def _tasks(manifest: Manifest, schedule, data_dir: Path, run_dir: Path,
           run_fp: str) -> List[Dict[str, Any]]:
    accepted_ids = {item.shard_id for item in schedule.accepted}
    tasks = []
    for shard in manifest.shards:
        if shard.shard_id not in accepted_ids:
            continue
        tasks.append({
            "shard_id": shard.shard_id,
            "shard_index": shard.index,
            "manifest_path": manifest.source_path,
            "data_dir": str(data_dir),
            "run_dir": str(run_dir),
            "run_fingerprint": run_fp,
            "check_tolerance": True,
        })
    tasks.sort(key=lambda item: item["shard_index"])
    return tasks


def run(manifest_path: str | Path, data_dir: str | Path,
        run_dir: str | Path, *, workers: Optional[int] = None,
        serial: bool = False,
        stop_poll: bool = True) -> "RunOutcome":
    manifest = load_manifest(manifest_path)
    data_dir = Path(data_dir)
    run_dir = ensure_dir(run_dir)
    store = CheckpointStore(run_dir, enforce_contiguous=True)
    publisher = DeterministicPublisher(store)
    run_fp = run_fingerprint(manifest, data_dir)

    schedule = plan_schedule(manifest, max_workers=workers)
    atomic_write_json(run_dir / SCHEDULE_FILE, schedule.to_dict())

    already = set(store.completed_shards())
    input_rejected = {
        shard_id for shard_id in store.shards_dir_names()
        if store.load_stage_record(shard_id, "validate") is not None
        and store.load_stage_record(shard_id, "verify") is None
        and _record_flags_input_error(store, shard_id)
    }
    settled = already | input_rejected
    pending_tasks = [task for task in
                     _tasks(manifest, schedule, data_dir, run_dir, run_fp)
                     if task["shard_id"] not in settled]

    used_compute = sum(
        estimate_shard(shard).compute_elements
        for shard in manifest.shards if shard.shard_id in already)

    results: List[Dict[str, Any]] = []
    stopped = False

    def _collect(result: Dict[str, Any]) -> None:
        publisher.publish_result(result)
        results.append(result)

    if serial or schedule.worker_count <= 1:
        for task in pending_tasks:
            if stop_poll and stop_requested(run_dir):
                stopped = True
                break
            shard = manifest.shard_by_id(task["shard_id"])
            enforce_runtime_admission(
                estimate_shard(shard), manifest.budget, used_compute)
            result = process_shard_task(task)
            _collect(result)
            if result["status"] == "completed":
                used_compute += estimate_shard(shard).compute_elements
    else:
        ctx = mp.get_context("spawn")
        with ctx.Pool(processes=schedule.worker_count) as pool:
            in_flight: Dict[Any, Dict[str, Any]] = {}
            task_iter = iter(pending_tasks)

            def _fill() -> None:
                while len(in_flight) < schedule.worker_count:
                    try:
                        task = next(task_iter)
                    except StopIteration:
                        return
                    if stop_poll and stop_requested(run_dir):
                        return
                    shard = manifest.shard_by_id(task["shard_id"])
                    enforce_runtime_admission(
                        estimate_shard(shard), manifest.budget, used_compute)
                    in_flight[pool.apply_async(
                        process_shard_task, (task,))] = task

            _fill()
            while in_flight:
                if stop_poll and stop_requested(run_dir):
                    # Let every already-dispatched indivisible shard finish;
                    # admit nothing new afterwards.
                    stopped = True
                finished = [ar for ar in in_flight if ar.ready()]
                if not finished:
                    first = next(iter(in_flight))
                    first.wait()
                    finished = [first]
                for async_result in finished:
                    task = in_flight.pop(async_result)
                    result = async_result.get()
                    _collect(result)
                    if result["status"] == "completed":
                        used_compute += estimate_shard(
                            manifest.shard_by_id(task["shard_id"])
                        ).compute_elements
                if not stopped:
                    _fill()

    summary = build_summary(manifest, schedule, store, results, run_fp,
                            stopped=stopped)
    atomic_write_json(run_dir / SUMMARY_FILE, summary)
    return RunOutcome(run_dir=run_dir, summary=summary, stopped=stopped)


class RunOutcome:
    def __init__(self, run_dir: Path, summary: Dict[str, Any],
                 stopped: bool = False):
        self.run_dir = run_dir
        self.summary = summary
        self.stopped = stopped

    def to_dict(self) -> Dict[str, Any]:
        return {"run_dir": str(self.run_dir), "stopped": self.stopped,
                "summary": self.summary}



def build_summary(manifest: Manifest, schedule, store: CheckpointStore,
                  fresh_results: List[Dict[str, Any]], run_fp: str,
                  *, stopped: bool) -> Dict[str, Any]:
    records = dict(store.completed_shards())
    input_rejected = {
        shard_id for shard_id in store.shards_dir_names()
        if store.load_stage_record(shard_id, "validate") is not None
        and store.load_stage_record(shard_id, "verify") is None
        and _record_flags_input_error(store, shard_id)
    }
    shard_summaries: List[Dict[str, Any]] = []
    fresh_by_id = {item["shard_id"]: item for item in fresh_results}

    for shard in manifest.shards:
        if shard.shard_id in records:
            record = records[shard.shard_id]
            verify = store.load_stage_record(shard.shard_id, "verify")
            shard_summaries.append({
                "shard_id": shard.shard_id,
                "index": shard.index,
                "status": "completed",
                "sequence": record["sequence"],
                "stage_fingerprint": record["stage_fingerprint"],
                "input_fingerprint": record["input_fingerprint"],
                "product_summary": verify["product_summary"]
                if verify else record["product_summary"],
                "invalidations": fresh_by_id.get(shard.shard_id, {})
                .get("invalidations", []),
            })
        elif shard.shard_id in fresh_by_id:
            fresh = dict(fresh_by_id[shard.shard_id])
            fresh.setdefault("index", shard.index)
            fresh.pop("pending_records", None)
            shard_summaries.append(fresh)
        elif shard.shard_id in input_rejected:
            validate_record = store.load_stage_record(
                shard.shard_id, "validate")
            shard_summaries.append({
                "shard_id": shard.shard_id,
                "shard_index": shard.index,
                "index": shard.index,
                "status": "rejected_input",
                "sequence": validate_record["sequence"],
                "stages": [{
                    "stage": "validate",
                    "reused": False,
                    "stage_fingerprint":
                    validate_record["stage_fingerprint"],
                    "product_summary":
                    validate_record["product_summary"],
                }],
                "invalidations": [],
                "final_summary": {
                    "input_fingerprint":
                    validate_record["input_fingerprint"]},
                "error": _input_error_from_record(validate_record, shard),
            })
        else:
            rejection = next((item for item in schedule.rejected
                              if item.shard_id == shard.shard_id), None)
            entry = {"shard_id": shard.shard_id, "index": shard.index,
                     "status": "not_scheduled"}
            if rejection is not None:
                entry["status"] = "rejected"
                entry["reason_code"] = rejection.reason_code
                entry["reason"] = rejection.reason
                entry["budget_detail"] = rejection.budget_detail
            elif stopped:
                entry["status"] = "pending_stop"
            shard_summaries.append(entry)

    shard_summaries.sort(key=lambda item: item["index"])
    completed = [item for item in shard_summaries
                 if item["status"] == "completed"]
    errors = [item["error"] for item in shard_summaries
              if item.get("error")]
    errors.extend(item["error"] for item in fresh_results
                  if item.get("error"))
    seen = set()
    unique_errors = []
    for error in errors:
        key = _stable_key(error)
        if key not in seen:
            seen.add(key)
            unique_errors.append(error)

    summary = {
        "run_fingerprint": run_fp,
        "config_version": manifest.config_version,
        "manifest_fingerprint": manifest_config_fingerprint(manifest),
        "schedule": schedule.to_dict(),
        "stopped": stopped,
        "totals": {
            "shards_declared": len(manifest.shards),
            "shards_completed": len(completed),
            "shards_rejected": len(schedule.rejected),
            "shards_pending": sum(1 for item in shard_summaries
                                  if item["status"] in
                                  ("not_scheduled", "pending_stop")),
            "shards_with_input_error": sum(
                1 for item in shard_summaries
                if item.get("status") == "rejected_input"),
        },
        "shards": shard_summaries,
        "errors": unique_errors,
        "reduction_order": [item["shard_id"] for item in shard_summaries],
    }
    summary["summary_fingerprint"] = combine_fingerprints([
        summary["manifest_fingerprint"],
        str([item["shard_id"] for item in shard_summaries]),
        str([item.get("stage_fingerprint", "") for item in shard_summaries]),
        str([item.get("status") for item in shard_summaries]),
    ])
    return summary


def _stable_key(obj: Any) -> str:
    from .fingerprints import canonical_json_bytes

    return canonical_json_bytes(obj).hex()


def _record_flags_input_error(store: CheckpointStore,
                              shard_id: str) -> bool:
    record = store.load_stage_record(shard_id, "validate")
    if record is None:
        return False
    summary = record.get("product_summary", {})
    return bool(summary.get("non_finite"))


def _input_error_from_record(record: Dict[str, Any],
                              shard) -> Dict[str, Any]:
    from .errors import E_NUMERIC_INPUT
    non_finite = record.get("product_summary", {}).get("non_finite", {})
    return {
        "code": E_NUMERIC_INPUT,
        "message": (f"shard {shard.shard_id} contains NaN/Inf inputs; "
                    "IEEE propagation would poison downstream stages"),
        "shard_id": shard.shard_id,
        "stage": "validate",
        "details": {"non_finite": non_finite,
                    "shard_id": shard.shard_id,
                    "stage": "validate"},
    }
