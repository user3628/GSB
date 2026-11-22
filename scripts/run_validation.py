#!/usr/bin/env python3
"""End-to-end validation scenario with deterministic local data."""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from segval.canonical import canonical_bytes
from segval.demo_generator import generate_demo_shards, generate_shard
from segval.diagnostics import checkpoint_fault_probe, doctor
from segval.engine import run_workbench, status_payload, update_shard, verify_incremental
from segval.errors import WorkbenchError
from segval.http_server import WorkbenchServer
from segval.shards import import_shard, load_shard_file
from segval.workspace import create_workspace


DEMO_DIR = PROJECT_ROOT / "demo_data"
RUNTIME_DIR = PROJECT_ROOT / "runtime" / "validation"


def setup_workspace(root: Path, shard_paths: list[Path]):
    workspace = create_workspace(root)
    for path in shard_paths:
        import_shard(workspace.root, load_shard_file(path))
    return workspace


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    shutil.rmtree(RUNTIME_DIR, ignore_errors=True)
    generated = generate_demo_shards(DEMO_DIR, seed=20261123, shard_count=4)
    shard_paths = [Path(item["path"]) for item in generated]
    report: dict = {
        "generator": generated,
        "doctor": doctor(),
        "checkpoint_probe": checkpoint_fault_probe(),
    }

    serial = setup_workspace(RUNTIME_DIR / "serial", shard_paths)
    parallel = setup_workspace(RUNTIME_DIR / "parallel", shard_paths)
    serial_run = run_workbench(serial.root, workers=1, clear_stop=True)
    parallel_run = run_workbench(parallel.root, workers=2, clear_stop=True)
    export_names = ("matches.json", "aggregates.json", "schedule.json")
    report["serial_parallel"] = {
        "serial": {
            "ok": serial_run["ok"],
            "actual_evaluations": serial_run["actual_candidate_evaluations"],
            "exports": {name: sha(serial.root / "exports" / name) for name in export_names},
        },
        "parallel": {
            "ok": parallel_run["ok"],
            "actual_evaluations": parallel_run["actual_candidate_evaluations"],
            "exports": {name: sha(parallel.root / "exports" / name) for name in export_names},
        },
        "byte_equal": {
            name: (serial.root / "exports" / name).read_bytes()
            == (parallel.root / "exports" / name).read_bytes()
            for name in export_names
        },
    }

    interrupted = setup_workspace(RUNTIME_DIR / "interrupted", shard_paths)
    stopped = run_workbench(interrupted.root, workers=2, stop_after_samples=1, clear_stop=True)
    stopped_status = status_payload(interrupted.root)
    resumed = run_workbench(interrupted.root, workers=2, clear_stop=True)
    report["stop_resume"] = {
        "stopped": stopped["stopped"],
        "stop_reason": stopped.get("stop"),
        "stopped_stage_state": [
            {
                "shard_id": row["shard_id"],
                "all_valid": row["all_valid"],
                "earliest_invalid_stage": row["earliest_invalid_stage"],
            }
            for row in stopped_status["shards"]
        ],
        "resumed": resumed["ok"] and not resumed["stopped"],
        "byte_equal_to_full": {
            name: (serial.root / "exports" / name).read_bytes()
            == (interrupted.root / "exports" / name).read_bytes()
            for name in ("matches.json", "aggregates.json")
        },
    }

    incremental = setup_workspace(RUNTIME_DIR / "incremental", shard_paths)
    run_workbench(incremental.root, workers=2, clear_stop=True)
    changed = generate_shard(3)
    changed["samples"][0]["predictions"][0]["confidence"] = 0.123456
    import_shard(incremental.root, changed)
    invalidation_status = status_payload(incremental.root)
    changed_row = next(row for row in invalidation_status["shards"] if row["shard_id"] == "shard-0003")
    update = update_shard(incremental.root, changed, workers=2)
    verify = verify_incremental(incremental.root, auto_rebuild=False)
    report["incremental"] = {
        "invalidated_earliest_stage": changed_row["earliest_invalid_stage"],
        "affected_stages": changed_row["affected_stages"],
        "reasons": changed_row["invalidation"]["reasons"],
        "rebuilt_stages": next(
            item for item in update["run"]["results"] if item["shard_id"] == "shard-0003"
        )["stages_run"],
        "verify": verify,
    }

    mismatch = None
    try:
        verify_incremental(incremental.root, auto_rebuild=False, inject_mismatch=True)
    except WorkbenchError as exc:
        mismatch = exc.to_dict()
    report["incremental_mismatch_injection"] = mismatch

    budget_ws = setup_workspace(RUNTIME_DIR / "budget", shard_paths)
    budget_ws.update_config(
        {"budget": {"max_evaluations": 180, "per_worker_quota": 100, "max_workers": 2}}
    )
    budget_run = run_workbench(budget_ws.root, workers=2, clear_stop=True)
    budget_status = status_payload(budget_ws.root)
    invalid_threshold = None
    invalid_budget = None
    try:
        budget_ws.update_config({"matching": {"iou_threshold": 1.7}})
    except WorkbenchError as exc:
        invalid_threshold = exc.to_dict()
    try:
        budget_ws.update_config({"budget": {"max_evaluations": -1}})
    except WorkbenchError as exc:
        invalid_budget = exc.to_dict()
    report["budget_and_errors"] = {
        "budget": budget_run["budget"],
        "actual_candidate_evaluations": budget_run["actual_candidate_evaluations"],
        "accepted": [
            {
                "shard_id": item["shard_id"],
                "estimate": item["estimated_evaluations"],
                "lane": item["lane"],
            }
            for item in budget_run["accepted"]
        ],
        "unprocessed": [
            {
                "shard_id": item["shard_id"],
                "estimate": item["estimated_evaluations"],
                "code": item["code"],
                "reason": item["reason"],
            }
            for item in budget_run["unprocessed"]
        ],
        "global_invariant_ok": (
            budget_run["budget"]["reserved_evaluations"] <= 180
            and budget_run["actual_candidate_evaluations"] <= 180
            and budget_status["budget"]["within_global_budget"]
        ),
        "invalid_threshold": invalid_threshold,
        "invalid_budget": invalid_budget,
    }

    server = WorkbenchServer(("127.0.0.1", 0), serial.root)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    base = f"http://{host}:{port}"
    try:
        with urllib.request.urlopen(base + "/", timeout=5) as response:
            html_ok = response.status == 200 and b"SegVal" in response.read()
        with urllib.request.urlopen(base + "/api/status", timeout=5) as response:
            http_status = json.loads(response.read())
        request = urllib.request.Request(
            base + "/api/config",
            data=json.dumps({"matching": {"iou_threshold": 1.1}}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        http_error = None
        try:
            urllib.request.urlopen(request, timeout=5)
        except urllib.error.HTTPError as exc:
            http_error = json.loads(exc.read())
            exc.close()
        report["http"] = {
            "dashboard_ok": html_ok,
            "status_ok": http_status["status"] == "completed",
            "invalid_threshold_code": (http_error or {}).get("error", {}).get("code"),
            "bind": f"{host}:{port}",
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    report["aggregate_totals"] = status_payload(serial.root)["aggregate_totals"]
    report["summary_ok"] = (
        report["doctor"]["ok"]
        and report["checkpoint_probe"]["ok"]
        and all(report["serial_parallel"]["byte_equal"].values())
        and all(report["stop_resume"]["byte_equal_to_full"].values())
        and report["incremental"]["verify"]["ok"]
        and report["incremental_mismatch_injection"]["code"] == "E_INCREMENTAL_MISMATCH"
        and report["budget_and_errors"]["global_invariant_ok"]
        and report["budget_and_errors"]["invalid_threshold"]["code"] == "E_INVALID_IOU_THRESHOLD"
        and report["budget_and_errors"]["invalid_budget"]["code"] == "E_INVALID_BUDGET"
        and report["http"]["dashboard_ok"]
        and report["http"]["status_ok"]
        and report["http"]["invalid_threshold_code"] == "E_INVALID_IOU_THRESHOLD"
    )
    output_path = PROJECT_ROOT / "VALIDATION_RESULTS.json"
    output_path.write_bytes(canonical_bytes(report) + b"\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\nreport written to {output_path}")
    return 0 if report["summary_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

