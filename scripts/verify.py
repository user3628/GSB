#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
ARTIFACTS = ROOT / "artifacts" / "verification"
QUERY_BODY = {
    "start_ms": -10_000_000_000,
    "end_ms": 10_000_000_000_000,
    "limit": 7,
    "window_types": ["tumbling", "sliding", "session"],
    "quantiles": [0.5, 0.9, 0.95, 0.99],
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def load_batches(path: Path) -> list[dict[str, Any]]:
    return json.loads(path.read_text(encoding="utf-8"))["batches"]


@dataclass
class RunningServer:
    process: subprocess.Popen[bytes]
    base_url: str
    log_path: Path

    def wait_ready(self, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"Server exited before ready (code {self.process.returncode}); log={self.log_path}"
                )
            try:
                response = httpx.get(f"{self.base_url}/health", timeout=0.5)
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        raise TimeoutError(f"Server did not become ready; log={self.log_path}")

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)


def start_server(runtime_dir: Path, *, env_extra: dict[str, str] | None = None) -> RunningServer:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    port = free_port()
    log_path = runtime_dir / "server.log"
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(ROOT),
            "DWE_CONFIG": str(SAMPLES / "config.json"),
            "DWE_RUNTIME_DIR": str(runtime_dir),
            "PYTHONUNBUFFERED": "1",
        }
    )
    if env_extra:
        env.update(env_extra)
    log_handle = log_path.open("ab", buffering=0)
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    server = RunningServer(process=process, base_url=f"http://127.0.0.1:{port}", log_path=log_path)
    server.wait_ready()
    return server


def require_ok(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Non-JSON response {response.status_code}: {response.text[:500]}") from exc
    if response.status_code != 200 or not payload.get("ok"):
        raise RuntimeError(f"HTTP {response.status_code}: {json.dumps(payload, ensure_ascii=False)}")
    return payload


def submit_and_snapshot(client: httpx.Client, batch: dict[str, Any], label: str) -> dict[str, Any]:
    batch_result = require_ok(
        client.post(
            "/v1/batches",
            json={"batch_id": batch["batch_id"], "events": batch["events"]},
            timeout=30,
        )
    )
    snapshot_result = require_ok(
        client.post("/v1/snapshots", json={"label": label}, timeout=30)
    )
    return {"batch": batch_result, "snapshot": snapshot_result}


def collect_paginated_windows(client: httpx.Client) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    items: list[dict[str, Any]] = []
    cursor: str | None = None
    pages = 0
    total: int | None = None
    as_of_version: int | None = None
    while True:
        body = dict(QUERY_BODY)
        if cursor is not None:
            body["cursor"] = cursor
        payload = require_ok(client.post("/v1/windows/query", json=body, timeout=30))
        pages += 1
        total = payload["total"]
        if as_of_version is None:
            as_of_version = payload["as_of_version"]
        elif as_of_version != payload["as_of_version"]:
            raise RuntimeError("Pinned pagination changed as_of_version")
        items.extend(payload["items"])
        cursor = payload.get("next_cursor")
        if cursor is None:
            break
    if total != len(items):
        raise RuntimeError(f"Pagination lost or duplicated rows: total={total}, collected={len(items)}")
    keys = [window_identity(item) for item in items]
    if len(keys) != len(set(keys)):
        raise RuntimeError("Pagination returned duplicate window identities")
    return items, {
        "pages": pages,
        "total": total,
        "as_of_version": as_of_version,
        "pagination_no_duplicates": True,
    }


def window_identity(item: dict[str, Any]) -> str:
    return json.dumps(
        [
            item["window_type"],
            item["window_start_ms"],
            item["window_end_ms"],
            item["group"],
            item.get("session_id"),
        ],
        separators=(",", ":"),
    )


def collect_state(client: httpx.Client) -> dict[str, Any]:
    windows, pagination = collect_paginated_windows(client)
    late = require_ok(client.get("/v1/late-events", params={"limit": 1000}, timeout=30))
    summary = require_ok(client.get("/v1/state/summary", timeout=30))["state"]
    snapshots = require_ok(client.get("/v1/snapshots", timeout=30))["snapshots"]
    recovery = require_ok(client.get("/v1/recovery/report", timeout=30))["report"]
    return {
        "windows": windows,
        "pagination": pagination,
        "late_events": late["items"],
        "late_queue_length": late["queue_length"],
        "summary": summary,
        "snapshots": snapshots,
        "recovery_report": recovery,
    }


def run_uninterrupted(work_root: Path) -> dict[str, Any]:
    runtime = work_root / "uninterrupted"
    shutil.rmtree(runtime, ignore_errors=True)
    server = start_server(runtime)
    batch_results: list[dict[str, Any]] = []
    try:
        with httpx.Client(base_url=server.base_url) as client:
            phases = [
                load_batches(SAMPLES / "phase1.json"),
                load_batches(SAMPLES / "crash_batch.json"),
                load_batches(SAMPLES / "phase2.json"),
            ]
            for phase in phases:
                for batch in phase:
                    batch_results.append(
                        submit_and_snapshot(client, batch, f"uninterrupted:{batch['batch_id']}")
                    )
            state = collect_state(client)
    finally:
        server.stop()
    state["batch_results"] = batch_results
    state["server_exit_code"] = server.process.returncode
    return state


def crash_one_batch(
    *,
    runtime: Path,
    point: str,
    crash_batch: dict[str, Any],
) -> dict[str, Any]:
    server = start_server(
        runtime,
        env_extra={
            "DWE_CRASH_POINT": point,
            "DWE_CRASH_BATCH_ID": crash_batch["batch_id"],
            "DWE_CRASH_KIND": "batch",
        },
    )
    connection_failed = False
    try:
        with httpx.Client(base_url=server.base_url) as client:
            for batch in load_batches(SAMPLES / "phase1.json"):
                submit_and_snapshot(client, batch, f"pre-crash:{batch['batch_id']}")
            try:
                client.post(
                    "/v1/batches",
                    json={"batch_id": crash_batch["batch_id"], "events": crash_batch["events"]},
                    timeout=30,
                )
            except httpx.HTTPError:
                connection_failed = True
        exit_code = server.process.wait(timeout=15)
    finally:
        server.stop()
    return {"exit_code": exit_code, "connection_failed": connection_failed}


def resume_and_finish(
    *,
    runtime: Path,
    crash_batch: dict[str, Any],
) -> dict[str, Any]:
    server = start_server(runtime)
    try:
        with httpx.Client(base_url=server.base_url) as client:
            startup_report = require_ok(client.get("/v1/recovery/report", timeout=30))["report"]
            retried = require_ok(
                client.post(
                    "/v1/batches",
                    json={"batch_id": crash_batch["batch_id"], "events": crash_batch["events"]},
                    timeout=30,
                )
            )
            require_ok(
                client.post(
                    "/v1/snapshots",
                    json={"label": f"resume:{crash_batch['batch_id']}"},
                    timeout=30,
                )
            )
            for batch in load_batches(SAMPLES / "phase2.json"):
                submit_and_snapshot(client, batch, f"post-recovery:{batch['batch_id']}")
            state = collect_state(client)
    finally:
        server.stop()
    state["startup_recovery_report"] = startup_report
    state["retried_batch"] = retried
    return state


def run_recovery_scenario(
    work_root: Path,
    *,
    name: str,
    point: str,
) -> dict[str, Any]:
    runtime = work_root / name
    shutil.rmtree(runtime, ignore_errors=True)
    crash_batch = load_batches(SAMPLES / "crash_batch.json")[0]
    crash = crash_one_batch(runtime=runtime, point=point, crash_batch=crash_batch)
    temp_files = sorted(
        path.name for path in (runtime / "snapshots").iterdir() if path.name.endswith(".tmp")
    )
    complete_snapshots = sorted(path.name for path in (runtime / "snapshots").glob("v*.json"))
    resumed = resume_and_finish(runtime=runtime, crash_batch=crash_batch)
    return {
        "scenario": name,
        "crash_point": point,
        "crash": crash,
        "temporary_snapshot_files_after_crash": temp_files,
        "complete_snapshots_after_crash": complete_snapshots,
        **resumed,
    }


def compare_states(baseline: dict[str, Any], recovered: dict[str, Any]) -> dict[str, Any]:
    base_windows = {window_identity(item): item for item in baseline["windows"]}
    rec_windows = {window_identity(item): item for item in recovered["windows"]}
    missing = sorted(set(base_windows) - set(rec_windows))
    extra = sorted(set(rec_windows) - set(base_windows))
    changed: list[dict[str, Any]] = []
    for key in sorted(set(base_windows) & set(rec_windows)):
        if base_windows[key] != rec_windows[key]:
            changed.append(
                {
                    "window": json.loads(key),
                    "uninterrupted": base_windows[key],
                    "recovered": rec_windows[key],
                }
            )
    base_late = json.dumps(baseline["late_events"], sort_keys=True, ensure_ascii=False)
    rec_late = json.dumps(recovered["late_events"], sort_keys=True, ensure_ascii=False)
    result = {
        "windows_equal_item_by_item": not missing and not extra and not changed,
        "late_queue_equal_item_by_item": base_late == rec_late,
        "uninterrupted_window_count": len(base_windows),
        "recovered_window_count": len(rec_windows),
        "missing_in_recovered": [json.loads(key) for key in missing],
        "extra_in_recovered": [json.loads(key) for key in extra],
        "changed_windows": changed,
        "baseline_windows_sha256": digest_json(baseline["windows"]),
        "recovered_windows_sha256": digest_json(recovered["windows"]),
        "baseline_late_queue_sha256": digest_json(baseline["late_events"]),
        "recovered_late_queue_sha256": digest_json(recovered["late_events"]),
        "differences": [],
    }
    if missing:
        result["differences"].append({"type": "missing_windows", "count": len(missing)})
    if extra:
        result["differences"].append({"type": "extra_windows", "count": len(extra)})
    if changed:
        result["differences"].append({"type": "changed_windows", "count": len(changed)})
    if base_late != rec_late:
        result["differences"].append(
            {
                "type": "late_queue_mismatch",
                "uninterrupted_length": len(baseline["late_events"]),
                "recovered_length": len(recovered["late_events"]),
            }
        )
    return result


def digest_json(value: Any) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_report(baseline: dict[str, Any], scenarios: list[dict[str, Any]]) -> dict[str, Any]:
    comparisons: dict[str, Any] = {}
    for scenario in scenarios:
        comparisons[scenario["scenario"]] = compare_states(baseline, scenario)
    all_equal = all(
        comparison["windows_equal_item_by_item"] and comparison["late_queue_equal_item_by_item"]
        for comparison in comparisons.values()
    )
    return {
        "generated_at": utc_now(),
        "address_scope": "127.0.0.1 only",
        "sample_config": json.loads((SAMPLES / "config.json").read_text(encoding="utf-8")),
        "uninterrupted": {
            "window_count": baseline["pagination"]["total"],
            "late_queue_length": baseline["late_queue_length"],
            "state_version": baseline["summary"]["state_version"],
            "snapshot_count": len(baseline["snapshots"]),
            "pagination": baseline["pagination"],
            "window_results_sha256": digest_json(baseline["windows"]),
            "late_queue_sha256": digest_json(baseline["late_events"]),
        },
        "baseline_windows": baseline["windows"],
        "baseline_late_events": baseline["late_events"],
        "scenarios": {
            scenario["scenario"]: {
                "crash_point": scenario["crash_point"],
                "crash": scenario["crash"],
                "temporary_snapshot_files_after_crash": scenario["temporary_snapshot_files_after_crash"],
                "complete_snapshots_after_crash": scenario["complete_snapshots_after_crash"],
                "startup_recovery_report": scenario["startup_recovery_report"],
                "retried_batch_replayed": scenario["retried_batch"].get("replayed"),
                "state_version": scenario["summary"]["state_version"],
                "window_count": scenario["pagination"]["total"],
                "late_queue_length": scenario["late_queue_length"],
            }
            for scenario in scenarios
        },
        "comparisons": comparisons,
        "all_recovery_paths_item_by_item_equal": all_equal,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run deterministic crash-recovery verification")
    parser.add_argument("--output", default=str(ARTIFACTS / "verification_report.json"))
    args = parser.parse_args()

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    baseline = run_uninterrupted(ARTIFACTS)
    scenarios = [
        run_recovery_scenario(ARTIFACTS, name="crash-after-intent", point="after_intent"),
        run_recovery_scenario(ARTIFACTS, name="crash-during-snapshot-temp", point="after_snapshot_temp"),
        run_recovery_scenario(ARTIFACTS, name="crash-after-snapshot-replace", point="after_snapshot_replace"),
    ]
    report = build_report(baseline, scenarios)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    print(json.dumps(
        {
            "all_recovery_paths_item_by_item_equal": report["all_recovery_paths_item_by_item_equal"],
            "uninterrupted_windows": report["uninterrupted"]["window_count"],
            "recovery_scenarios": {
                name: {
                    "windows_equal": comparison["windows_equal_item_by_item"],
                    "late_queue_equal": comparison["late_queue_equal_item_by_item"],
                    "differences": comparison["differences"],
                }
                for name, comparison in report["comparisons"].items()
            },
            "report": str(output),
        },
        indent=2,
        ensure_ascii=False,
    ))
    return 0 if report["all_recovery_paths_item_by_item_equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
