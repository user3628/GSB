from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
RUNTIME = ROOT / "artifacts" / "demo-runtime"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request_json(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return json.loads(exc.read().decode("utf-8"))


def wait_ready(base_url: str, process: subprocess.Popen[Any], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited before readiness with code {process.returncode}")
        try:
            health = request_json("GET", f"{base_url}/health", timeout=1.0)
            if health.get("status") == "ok":
                return
        except Exception as exc:  # noqa: BLE001 - startup polling intentionally broad
            last_error = exc
        time.sleep(0.1)
    raise RuntimeError(f"server did not become ready: {last_error}")


def start_server(db_path: Path, port: int, log_path: Path) -> subprocess.Popen[Any]:
    log_handle = log_path.open("ab")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "app.cli",
            "serve",
            "--db",
            str(db_path),
            "--samples",
            str(SAMPLES),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )
    process._codex_log_handle = log_handle  # type: ignore[attr-defined]
    return process


def stop_server(process: subprocess.Popen[Any]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    handle = getattr(process, "_codex_log_handle", None)
    if handle is not None:
        handle.close()


def transition_summary(batch: dict[str, Any]) -> dict[str, int]:
    summary: dict[str, int] = {}
    for event in batch["transitions"]:
        key = f"{event['from_state'] or 'NONE'}->{event['to_state']}"
        summary[key] = summary.get(key, 0) + 1
    return summary


def main() -> int:
    if RUNTIME.exists():
        shutil.rmtree(RUNTIME)
    RUNTIME.mkdir(parents=True, exist_ok=True)
    db_path = RUNTIME / "tiering.db"
    log_path = RUNTIME / "server.log"
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    output: dict[str, Any] = {"base_url": base_url, "database": str(db_path)}

    process = start_server(db_path, port, log_path)
    try:
        wait_ready(base_url, process)
        policy_before = request_json("GET", f"{base_url}/v1/policy/current")
        occupancy_initial = request_json("GET", f"{base_url}/v1/layers/occupancy")
        version = occupancy_initial["snapshot_version"]

        cascade = request_json(
            "POST",
            f"{base_url}/v1/policy/adjustments",
            {
                "base_version": version,
                "version": version + 1,
                "layer_capacity_updates": {
                    "cold-hdd": 20 * 1024**3,
                },
                "heat_threshold": 200,
                "cooldown_seconds": 0,
            },
        )
        policy_after = request_json("GET", f"{base_url}/v1/policy/current")
        output["capacity_and_threshold_cascade"] = {
            "previous_version": cascade["previous_version"],
            "new_version": cascade["snapshot"]["version"],
            "capacity_updates": {"cold-hdd": 20 * 1024**3},
            "threshold_before": policy_before["snapshot"]["heat_threshold"],
            "threshold_after": policy_after["snapshot"]["heat_threshold"],
            "runtime_before": cascade["cascade"]["runtime_before"],
            "runtime_after": cascade["cascade"]["runtime_after"],
            "eligibility_changed": cascade["cascade"]["eligibility_changed"],
            "recomputed_in_transaction": cascade["cascade"][
                "candidate_inputs_recomputed_in_transaction"
            ],
        }

        illegal = request_json(
            "POST",
            f"{base_url}/v1/state-machine/validate",
            {"from_state": "COMMITTED", "to_state": "PENDING"},
        )
        output["illegal_transition"] = illegal

        injection = request_json(
            "POST",
            f"{base_url}/v1/faults/injection",
            {
                "point": "after_transfer_before_commit",
                "mode": "hard_exit",
                "batch_id": "demo-crash",
            },
        )
        output["fault_injection"] = injection
        crash_request_error = None
        crash_preview = request_json(
            "POST",
            f"{base_url}/v1/batches/preview",
            {"snapshot_version": policy_after["snapshot"]["version"]},
        )
        output["interrupted_batch_plan"] = {
            "snapshot_version": crash_preview["snapshot_version"],
            "candidates_total": crash_preview["candidates_total"],
            "selected_count": crash_preview["selected_count"],
            "selected_shards": [item["shard_id"] for item in crash_preview["selected"]],
            "truncated": crash_preview["truncated"],
        }
        try:
            request_json(
                "POST",
                f"{base_url}/v1/batches",
                {"batch_id": "demo-crash", "snapshot_version": policy_after["snapshot"]["version"]},
                timeout=10.0,
            )
        except Exception as exc:  # noqa: BLE001 - dropped connection is expected
            crash_request_error = f"{type(exc).__name__}: {exc}"
        try:
            crash_exit_code = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            crash_exit_code = process.wait(timeout=10)
        output["forced_termination"] = {
            "request_result": crash_request_error or "no client-side exception observed",
            "process_exit_code": crash_exit_code,
            "expected_hard_exit_code": 73,
        }
    finally:
        stop_server(process)

    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    recovered_process = start_server(db_path, port, log_path)
    try:
        wait_ready(base_url, recovered_process)
        occupancy_after_recovery = request_json("GET", f"{base_url}/v1/layers/occupancy")
        crash_batch = request_json("GET", f"{base_url}/v1/batches/demo-crash")
        recovery_report = request_json("GET", f"{base_url}/v1/recovery/report")
        recovery = recovery_report["latest_startup_recovery"]
        output["restart_recovery"] = {
            "forced_termination_detected": recovery["forced_termination_detected"],
            "safe_exit_summary_available": recovery["safe_exit_summary_available"],
            "unfinished_batch_count": recovery["unfinished_batch_count"],
            "intent_scan": recovery["intent_scan"],
            "recovered_batches": recovery["recovered_batches"],
            "batch_state_after_restart": crash_batch["state"],
            "batch_conclusion_matches_items": crash_batch["conclusion_matches_items"],
            "item_states": {
                item["shard_id"]: item["state"] for item in crash_batch["items"]
            },
            "transition_summary_after_recovery": transition_summary(crash_batch),
            "layer_before_recovery": recovery["layer_occupancy_before"],
            "layer_after_recovery": recovery["layer_occupancy_after"],
            "occupancy_difference": recovery["occupancy_difference"],
            "recovered_occupancy": [
                {
                    "layer_id": layer["layer_id"],
                    "used_bytes": layer["used_bytes"],
                    "shard_count": layer["shard_count"],
                }
                for layer in occupancy_after_recovery["layers"]
            ],
            "recovery_basis": recovery["recovery_basis"],
            "caveat": recovery["caveat"],
        }

        preview = request_json(
            "POST",
            f"{base_url}/v1/batches/preview",
            {"snapshot_version": policy_after["snapshot"]["version"]},
        )
        full_batch = request_json(
            "POST",
            f"{base_url}/v1/batches",
            {
                "batch_id": "demo-full",
                "snapshot_version": policy_after["snapshot"]["version"],
            },
        )
        occupancy_after_full = request_json("GET", f"{base_url}/v1/layers/occupancy")
        output["full_migration_batch"] = {
            "snapshot_version": policy_after["snapshot"]["version"],
            "candidates_total": preview["candidates_total"],
            "selected_count": preview["selected_count"],
            "truncated_count": preview["truncated_count"],
            "moved_count": full_batch["result"]["moved_count"],
            "selected_shards": [item["shard_id"] for item in full_batch["items"]],
            "layer_before": [
                {
                    "layer_id": layer["layer_id"],
                    "used_bytes": layer["used_bytes"],
                    "shard_count": layer["shard_count"],
                }
                for layer in occupancy_after_recovery["layers"]
            ],
            "layer_after": [
                {
                    "layer_id": layer["layer_id"],
                    "used_bytes": layer["used_bytes"],
                    "shard_count": layer["shard_count"],
                }
                for layer in occupancy_after_full["layers"]
            ],
            "transition_summary": transition_summary(full_batch),
            "conclusion_matches_items": full_batch["conclusion_matches_items"],
        }

        safe = request_json(
            "POST",
            f"{base_url}/v1/admin/safe-shutdown",
            {"reason": "demo_complete"},
        )
        output["safe_shutdown"] = {
            "report_type": safe["report_type"],
            "forced_termination_detected": safe["forced_termination_detected"],
            "completed_batches": safe["completed_batches"],
            "aborted_during_shutdown": safe["aborted_during_shutdown"],
        }
    finally:
        stop_server(recovered_process)

    result_path = RUNTIME / "demo-result.json"
    result_path.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, indent=2))
    print(f"\nDemo artifact: {result_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
