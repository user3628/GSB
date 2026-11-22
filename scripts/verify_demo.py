#!/usr/bin/env python3
"""End-to-end acceptance run with a machine-readable verification artifact."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from topk_pipeline.checkpoints import CheckpointStore
from topk_pipeline.demo_data import generate_demo_data
from topk_pipeline.errors import E_INVALID_STAGE, PipelineError
from topk_pipeline.manifest import load_manifest
from topk_pipeline.runner import export_summary, run_manifest
from topk_pipeline.topk import StopRequested, chunked_topk


DATA_DIR = ROOT / "data"
DEMO_DIR = ROOT / "artifacts" / "demo"
VERIFICATION_PATH = ROOT / "artifacts" / "verification.json"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def clean_dir(path: Path) -> Path:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def relative(path: Path) -> str:
    return str(path.resolve().relative_to(ROOT))


def data_corpus_digest() -> str:
    hasher = hashlib.sha256()
    for path in sorted(path for path in DATA_DIR.rglob("*") if path.is_file()):
        hasher.update(str(path.relative_to(DATA_DIR)).encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(path.read_bytes())
        hasher.update(b"\0")
    return hasher.hexdigest()


def run_to_summary(manifest: Path, workspace: Path, *, workers: int) -> tuple[dict[str, Any], Path, bytes]:
    workspace = clean_dir(workspace)
    result = run_manifest(
        manifest,
        workspace,
        workers=workers,
        reset_stop_file=True,
    )
    if result["stopped"]:
        raise RuntimeError(f"unexpected stop for {workspace}")
    summary_path = workspace / "summary.json"
    export_summary(summary_path, result["summary"])
    return result, summary_path, summary_path.read_bytes()


def relocated_manifest(raw: dict[str, Any], target: Path) -> Path:
    """Write a modified manifest with relocatable paths relative to its new home."""

    clone = copy.deepcopy(raw)
    target.parent.mkdir(parents=True, exist_ok=True)
    for shard in clone["shards"]:
        source = (DATA_DIR / shard["path"]).resolve()
        shard["path"] = Path(os.path.relpath(source, start=target.parent.resolve())).as_posix()
    target.write_text(json.dumps(clone, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return target


def verify_injections(cases_path: Path) -> list[dict[str, Any]]:
    cases = json.loads(cases_path.read_text(encoding="utf-8"))["cases"]
    records: list[dict[str, Any]] = []
    for case in cases:
        workspace = clean_dir(DEMO_DIR / "injections" / case["name"])
        result = run_manifest(DATA_DIR / case["manifest"], workspace, reset_stop_file=True)
        entry = next(
            item for item in result["summary"]["shards"] if item["shard_id"] == case["shard_id"]
        )
        actual: dict[str, Any] = {
            "name": case["name"],
            "shard_id": case["shard_id"],
            "status": entry["status"],
            "code": entry.get("error", {}).get("code"),
            "stage": entry.get("error", {}).get("stage"),
            "row": entry.get("error", {}).get("row"),
        }
        passed = entry["status"] == case["status"]
        if case.get("code") is not None:
            passed = passed and actual["code"] == case["code"] and actual["stage"] == case["stage"]
        if case.get("row") is not None:
            passed = passed and actual["row"] == case["row"]
        if case["name"] == "all_tied_max":
            row = entry["result"]["rows"][0]
            tokens = [candidate["token"] for candidate in row["topk_candidates"]]
            scores = [candidate["score"] for candidate in row["topk_candidates"]]
            actual["tie_tokens"] = tokens
            actual["tie_scores"] = scores
            passed = (
                passed
                and tokens == case["tie_tokens_per_row"]
                and all(score == case["score"] for score in scores)
            )
        if case["name"] == "all_neg_inf_row":
            checkpoint = CheckpointStore(workspace).load_state(case["shard_id"])
            actual["checkpoint_stages"] = [record["stage"] for record in checkpoint["records"]]
            passed = passed and actual["checkpoint_stages"] == ["filter"]
        actual["passed"] = passed
        records.append(actual)
    return records


def verify_checkpoint_race() -> dict[str, Any]:
    workspace = clean_dir(DEMO_DIR / "checkpoint_race")
    probe = ROOT / "scripts" / "checkpoint_publish_probe.py"
    shard_id = "race_shard"
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                str(probe),
                str(workspace),
                f"publisher-{index}",
                "--shard-id",
                shard_id,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(ROOT),
        )
        for index in range(2)
    ]
    outputs = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=30)
        if not stdout.strip():
            raise RuntimeError(f"probe produced no output: {stderr}")
        outputs.append(json.loads(stdout.strip()))
    winners = [item for item in outputs if item["ok"]]
    losers = [item for item in outputs if not item["ok"]]
    state = CheckpointStore(workspace).load_state(shard_id)
    state_path = workspace / "checkpoints" / shard_id / "state.json"
    before = state_path.read_bytes()
    (state_path.parent / ".state.json.tmp.simulated_crash").write_bytes(b'{"records": [')
    after = CheckpointStore(workspace).load_state(shard_id)
    atomic_ok = (
        state_path.read_bytes() == before
        and after["last_sequence"] == state["last_sequence"]
        and len(after["records"]) == 1
    )

    cross_workspace = clean_dir(DEMO_DIR / "checkpoint_cross_stage")
    cross_code = None
    try:
        CheckpointStore(cross_workspace).publish(
            "cross_stage",
            sequence=1,
            stage="sampling",
            input_fingerprint="fp",
            config_version="cfg",
            content_fingerprint="content",
            artifact={"bad": "order"},
        )
    except PipelineError as exc:
        cross_code = exc.code

    return {
        "passed": (
            len(winners) == 1
            and len(losers) == 1
            and losers[0]["error"]["code"] == "E_STALE_CHECKPOINT"
            and len(state["records"]) == 1
            and atomic_ok
            and cross_code == E_INVALID_STAGE
        ),
        "winner_count": len(winners),
        "stale_code": losers[0]["error"]["code"] if losers else None,
        "record_count": len(state["records"]),
        "interrupted_temp_ignored": atomic_ok,
        "cross_stage_code": cross_code,
    }


def verify_row_block_stop() -> dict[str, Any]:
    logits = np.arange(32 * 16, dtype=np.float64).reshape(32, 16)
    calls = {"count": 0}

    def stop_check() -> bool:
        calls["count"] += 1
        return calls["count"] > 1

    completed_rows = None
    try:
        chunked_topk(
            logits,
            k=4,
            shard_id="row_block_stop",
            row_block=8,
            col_block=5,
            stop_check=stop_check,
        )
    except StopRequested as exc:
        completed_rows = exc.completed_rows
    return {
        "passed": completed_rows == 8,
        "completed_rows_before_stop": completed_rows,
        "row_block": 8,
    }


def verify_incremental() -> dict[str, Any]:
    workspace = clean_dir(DEMO_DIR / "incremental")
    manifest_path = DATA_DIR / "manifest.json"
    first = run_manifest(manifest_path, workspace, workers=2, reset_stop_file=True)
    store = CheckpointStore(workspace)
    before = store.load_state("shard_00_small")
    before_sequences = {record["stage"]: record["sequence"] for record in before["records"]}
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))

    changed_raw = copy.deepcopy(raw)
    changed_raw["shards"][0]["temperature"] = float(changed_raw["shards"][0]["temperature"]) + 0.15
    temperature_manifest = relocated_manifest(
        changed_raw, DEMO_DIR / "incremental_manifests" / "temperature.json"
    )
    temperature_run = run_manifest(
        temperature_manifest, workspace, workers=2, reset_stop_file=True
    )
    temperature_worker = next(
        item for item in temperature_run["worker_results"] if item["shard_id"] == "shard_00_small"
    )
    after_temperature = store.load_state("shard_00_small")
    temperature_sequences = {
        record["stage"]: record["sequence"] for record in after_temperature["records"]
    }

    budget_raw = copy.deepcopy(changed_raw)
    budget_raw["budget"]["global_elements"] += 1
    budget_manifest = relocated_manifest(
        budget_raw, DEMO_DIR / "incremental_manifests" / "budget.json"
    )
    budget_run = run_manifest(budget_manifest, workspace, workers=2, reset_stop_file=True)
    budget_worker = next(
        item for item in budget_run["worker_results"] if item["shard_id"] == "shard_00_small"
    )
    after_budget = store.load_state("shard_00_small")
    budget_sequences = {
        record["stage"]: record["sequence"] for record in after_budget["records"]
    }

    passed = (
        temperature_worker["earliest_invalid_stage"] == "normalize"
        and temperature_worker["reused_stages"] == ["filter"]
        and temperature_sequences["filter"] == before_sequences["filter"]
        and temperature_sequences["normalize"] > before_sequences["normalize"]
        and temperature_sequences["sampling"] > before_sequences["sampling"]
        and temperature_sequences["validation"] > before_sequences["validation"]
        and budget_worker["earliest_invalid_stage"] == "validation"
        and budget_worker["reused_stages"] == ["filter", "normalize", "sampling"]
        and budget_sequences["filter"] == temperature_sequences["filter"]
        and budget_sequences["normalize"] == temperature_sequences["normalize"]
        and budget_sequences["sampling"] == temperature_sequences["sampling"]
        and budget_sequences["validation"] > temperature_sequences["validation"]
    )
    return {
        "passed": passed,
        "temperature_change": {
            "earliest_invalid_stage": temperature_worker["earliest_invalid_stage"],
            "reused_stages": temperature_worker["reused_stages"],
            "before_sequences": before_sequences,
            "after_sequences": temperature_sequences,
        },
        "budget_change": {
            "earliest_invalid_stage": budget_worker["earliest_invalid_stage"],
            "reused_stages": budget_worker["reused_stages"],
            "after_sequences": budget_sequences,
        },
    }


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    generate_info = generate_demo_data(DATA_DIR)
    DEMO_DIR.mkdir(parents=True, exist_ok=True)

    full_result, full_path, full_bytes = run_to_summary(
        DATA_DIR / "manifest.json", DEMO_DIR / "full", workers=2
    )
    sequential_result, sequential_path, sequential_bytes = run_to_summary(
        DATA_DIR / "manifest.json", DEMO_DIR / "sequential", workers=1
    )
    schedule = full_result["schedule"]
    budgets_ok = (
        schedule["consumed_elements"] <= schedule["global_elements"]
        and all(
            item["estimate"]["elements"] <= schedule["worker_elements"]
            for item in schedule["accepted"]
        )
        and schedule["scheduled_peak_memory_bytes"] <= schedule["peak_memory_bytes"]
    )

    resume_workspace = clean_dir(DEMO_DIR / "resume")
    stopped = run_manifest(
        DATA_DIR / "manifest.json",
        resume_workspace,
        workers=1,
        stop_after_completed=2,
        reset_stop_file=True,
    )
    stop_state = {
        "stopped": stopped["stopped"],
        "successful_shards": stopped["summary"]["stats"]["successful_shards"],
        "shard_statuses": {
            entry["shard_id"]: entry["status"] for entry in stopped["summary"]["shards"]
        },
    }
    export_summary(DEMO_DIR / "stop_state.json", stop_state)
    resumed = run_manifest(
        DATA_DIR / "manifest.json",
        resume_workspace,
        workers=2,
        reset_stop_file=True,
    )
    resumed_path = resume_workspace / "resumed-summary.json"
    export_summary(resumed_path, resumed["summary"])
    resumed_bytes = resumed_path.read_bytes()

    injections = verify_injections(DATA_DIR / "cases.json")
    checkpoint_race = verify_checkpoint_race()
    row_block_stop = verify_row_block_stop()
    incremental = verify_incremental()

    checks = {
        "full_run_completed": not full_result["stopped"]
        and full_result["summary"]["stats"]["successful_shards"] == 4,
        "parallel_equals_sequential_bytes": full_bytes == sequential_bytes,
        "stop_requested_after_two_shards": bool(stopped["stopped"]),
        "resume_equals_full_run_bytes": full_bytes == resumed_bytes,
        "budget_limits_respected": budgets_ok,
        "all_injections_match": all(item["passed"] for item in injections),
        "checkpoint_race_and_atomicity": checkpoint_race["passed"],
        "row_block_graceful_stop": row_block_stop["passed"],
        "incremental_invalidation": incremental["passed"],
    }
    generator_info = {
        "output_dir": relative(Path(generate_info["output_dir"])),
        "manifest": relative(Path(generate_info["manifest"])),
        "cases": relative(Path(generate_info["cases"])),
        "shard_files": generate_info["shard_files"],
    }
    verification = {
        "schema_version": 1,
        "environment": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "platform": platform.platform(),
            "multiprocessing_start_method": (
                "spawn" if os.name == "nt" else "fork"
            ),
            "gpu_used": False,
            "network_used": False,
        },
        "generator": {
            **generator_info,
            "corpus_sha256": data_corpus_digest(),
        },
        "full_run": {
            "summary": relative(full_path),
            "summary_file_sha256": sha256_bytes(full_bytes),
            "output_sha256": full_result["summary"]["output_sha256"],
            "stats": full_result["summary"]["stats"],
            "budget_plan": {
                "accepted_order": schedule["acceptance_order"],
                "consumed_elements": schedule["consumed_elements"],
                "global_elements": schedule["global_elements"],
                "single_worker_element_quota": schedule["worker_elements"],
                "scheduled_peak_memory_bytes": schedule["scheduled_peak_memory_bytes"],
                "peak_memory_bytes": schedule["peak_memory_bytes"],
            },
        },
        "sequential_run": {
            "summary": relative(sequential_path),
            "summary_file_sha256": sha256_bytes(sequential_bytes),
            "matches_full_bytes": full_bytes == sequential_bytes,
        },
        "stop_resume": {
            "stop_state": stop_state,
            "resumed_summary": relative(resumed_path),
            "resumed_summary_file_sha256": sha256_bytes(resumed_bytes),
            "matches_full_bytes": full_bytes == resumed_bytes,
        },
        "injections": injections,
        "checkpoint_race": checkpoint_race,
        "row_block_stop": row_block_stop,
        "incremental": incremental,
        "checks": checks,
        "passed": all(checks.values()),
    }
    VERIFICATION_PATH.write_text(
        json.dumps(verification, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "passed": verification["passed"],
                "checks": checks,
                "output_sha256": full_result["summary"]["output_sha256"],
                "summary_file_sha256": sha256_bytes(full_bytes),
                "verification": relative(VERIFICATION_PATH),
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0 if verification["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
