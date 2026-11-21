from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import httpx


ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RUNTIME = ROOT / "runtime" / "demo"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_health(client: httpx.Client, timeout: float = 15.0) -> dict[str, Any]:
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            response = client.get("/health", timeout=1.0)
            if response.status_code == 200:
                return response.json()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(0.1)
    raise RuntimeError(f"server did not become healthy: {last_error}")


@contextmanager
def run_server(db_path: Path, *, crash_point: str | None = None) -> Iterator[tuple[str, httpx.Client, subprocess.Popen]]:
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env.update(
        {
            "MIRROR_DB_PATH": str(db_path),
            "MIRROR_REPLAY_TARGET": f"{base_url}/_test/replay",
            "MIRROR_ENABLE_TEST_HOOKS": "1" if crash_point else "0",
        }
    )
    if crash_point:
        env["MIRROR_CRASH_POINT"] = crash_point
    log_path = RUNTIME / f"server-{port}.log"
    log = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "scripts" / "run_server.py"),
            "--port",
            str(port),
            "--db",
            str(db_path),
            "--replay-target",
            f"{base_url}/_test/replay",
        ],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    client = httpx.Client(base_url=base_url, timeout=60.0)
    try:
        health = wait_health(client)
        print(f"[server] {base_url} db={db_path} version={health['current_version']}")
        yield base_url, client, process
    finally:
        client.close()
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        log.close()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def compact_state(state: dict[str, Any]) -> dict[str, Any]:
    commit = state["commit"]
    selection = state["selection_summary"]
    batches = state["batch_summary"]
    bias = state["bias_summary"]
    return {
        "version": commit["version"],
        "kind": commit["kind"],
        "snapshot_version": commit["snapshot_version"],
        "policy_version": commit["policy_version"],
        "snapshot_hash": commit["snapshot_hash"],
        "records": commit["record_count"],
        "selection": {
            "artifact_id": selection["artifact_id"],
            "selected": selection["selected_count"],
            "coverage": selection["coverage"],
            "rejections": selection["rejection_counts"],
        },
        "batches": {
            "artifact_id": batches["artifact_id"],
            "count": batches["batch_count"],
            "operations": batches["operation_count"],
            "target_multiplier": batches["target_multiplier"],
            "effective_multiplier": batches["effective_multiplier"],
            "downshift": batches["downshift"],
        },
        "bias": {
            "artifact_id": bias["artifact_id"],
            "online_success_rate": bias["online_original"]["success_rate"],
            "sample_success_rate": bias["sample"]["success_rate"],
            "sampling_success_delta": bias["sampling_bias"]["success_rate_delta_vs_online"],
            "replay_present": bias["replay_amplification_bias"] is not None,
            "timeout_count": bias["attribution"]["timeout_component"]["count"]
            if bias["attribution"]["timeout_component"]
            else 0,
        },
        "cascade": {
            "atomic_rebuild": commit["cascade"].get("atomic_rebuild"),
            "reason": commit["cascade"].get("reason"),
            "changed_rules": commit["cascade"].get("changed_rules"),
            "invalidated": commit["cascade"].get("invalidated_previous_artifacts", []),
            "rebuilt": commit["cascade"].get("rebuilt_artifacts", []),
        },
    }


def assert_ok(response: httpx.Response, expected: int = 201) -> dict[str, Any]:
    if response.status_code != expected:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
    return response.json()


def main() -> int:
    parser = argparse.ArgumentParser(description="End-to-end local demonstration")
    parser.add_argument("--keep-runtime", action="store_true", help="keep the demo SQLite database")
    args = parser.parse_args()
    if RUNTIME.exists() and not args.keep_runtime:
        shutil.rmtree(RUNTIME)
    RUNTIME.mkdir(parents=True, exist_ok=True)
    db_path = RUNTIME / "demo.sqlite"
    records = read_jsonl(DATA / "mirror_records.jsonl")
    increment = read_jsonl(DATA / "mirror_records_increment.jsonl")
    baseline_policy = read_json(DATA / "policy_baseline.json")
    weight_increment = read_json(DATA / "policy_increment_checkout_weight.json")
    report: dict[str, Any] = {"steps": [], "versions": {}}

    with run_server(db_path) as (base_url, client, _process):
        v1 = assert_ok(
            client.post(
                "/api/v1/snapshots",
                json={
                    "base_version": None,
                    "snapshot_version": "demo-snapshot-1",
                    "batch_id": "demo-base-records",
                    "records": records,
                    "policy": baseline_policy,
                },
            )
        )
        report["versions"]["v1_full_import"] = compact_state(v1)
        print("[1] full deterministic sample import -> v1")
        print(json.dumps(compact_state(v1), ensure_ascii=False, indent=2))

        v2 = assert_ok(
            client.post(
                "/api/v1/samples/batches",
                json={
                    "base_version": v1["commit"]["version"],
                    "batch_id": "demo-increment-records",
                    "records": increment,
                },
            )
        )
        report["versions"]["v2_sample_increment"] = compact_state(v2)
        print("\n[2] idempotent incremental sample batch -> v2")
        print(json.dumps(compact_state(v2), ensure_ascii=False, indent=2))

        before_policy = compact_state(v2)
        v3 = assert_ok(
            client.post(
                "/api/v1/policies/increments",
                json={
                    "base_version": v2["commit"]["version"],
                    "base_policy_version": weight_increment["base_policy_version"],
                    "policy_version": weight_increment["policy_version"],
                    "patch": weight_increment["patch"],
                },
            )
        )
        after_policy = compact_state(v3)
        report["versions"]["v3_weight_increment"] = after_policy
        report["weight_increment_effect"] = {
            "selected_before": before_policy["selection"]["selected"],
            "selected_after": after_policy["selection"]["selected"],
            "artifact_ids_changed": {
                key: before_policy[key]["artifact_id"] != after_policy[key]["artifact_id"]
                for key in ("selection", "batches", "bias")
            },
        }
        print("\n[3] only checkout-service sampling weight increment -> atomic cascade at v3")
        print(json.dumps(after_policy, ensure_ascii=False, indent=2))
        print(json.dumps(report["weight_increment_effect"], ensure_ascii=False, indent=2))

        replay = assert_ok(
            client.post(
                "/api/v1/replays",
                json={"source_version": v3["commit"]["version"], "fault_mode": "mirror", "timeout_ms": 1200},
            )
        )
        replay_bias = client.get(f"/api/v1/bias?version={replay['result_version']}").json()
        report["versions"]["v4_replay_verdict"] = {
            "run_id": replay["run_id"],
            "result_version": replay["result_version"],
            "attempted": replay["metrics"]["attempted"],
            "success_rate": replay["metrics"]["success_rate"],
            "timeout_count": replay["metrics"]["timeout_count"],
            "bias_artifact_id": replay_bias["bias"]["artifact_id"],
            "sampling_component": replay_bias["bias"]["attribution"]["sampling_component"],
            "replay_failure_component": replay_bias["bias"]["attribution"]["replay_failure_component"],
        }
        print("\n[4] normal replay -> immutable verdict version v4")
        print(json.dumps(report["versions"]["v4_replay_verdict"], ensure_ascii=False, indent=2))

        source_batches = client.get(f"/api/v1/batches?version={replay['result_version']}").json()["batches"]["batches"]
        first_batch_id = source_batches[0]["batch_id"]
        timeout_response = client.post(
            "/api/v1/replays",
            json={
                "source_version": replay["result_version"],
                "batch_id": first_batch_id,
                "timeout_ms": 80,
                "fault_mode": "hang",
                "hang_ms": 1500,
                "raise_on_timeout": True,
            },
        )
        if timeout_response.status_code != 504:
            raise RuntimeError(f"expected timeout business error, got {timeout_response.status_code}: {timeout_response.text}")
        timeout_body = timeout_response.json()
        timeout_version = timeout_body["details"]["result_version"]
        timeout_bias = client.get(f"/api/v1/bias?version={timeout_version}").json()["bias"]
        report["versions"]["v5_timeout_verdict"] = {
            "error_code": timeout_body["code"],
            "run_id": timeout_body["details"]["run_id"],
            "result_version": timeout_version,
            "timeout_count": timeout_bias["replay"]["timeout_count"],
            "failure_count": timeout_bias["replay"]["failure_count"],
            "timeout_component": timeout_bias["attribution"]["timeout_component"],
            "replay_failure_component": timeout_bias["attribution"]["replay_failure_component"],
            "sampling_component": timeout_bias["attribution"]["sampling_component"],
        }
        print("\n[5] forced replay timeout -> E_REPLAY_TIMEOUT and separate attribution at v5")
        print(json.dumps(report["versions"]["v5_timeout_verdict"], ensure_ascii=False, indent=2))
        current_version = timeout_version

    print("\n[6] stopping server, then starting a test-hook server that exits during cascade rebuild")
    crash_port = free_port()
    crash_url = f"http://127.0.0.1:{crash_port}"
    crash_env = os.environ.copy()
    crash_env.update(
        {
            "MIRROR_DB_PATH": str(db_path),
            "MIRROR_REPLAY_TARGET": f"{crash_url}/_test/replay",
            "MIRROR_ENABLE_TEST_HOOKS": "1",
            "MIRROR_CRASH_POINT": "before_commit",
        }
    )
    crash_log_path = RUNTIME / "crash-server.log"
    crash_log = crash_log_path.open("w", encoding="utf-8")
    crash_process = subprocess.Popen(
        [
            sys.executable,
            str(ROOT / "scripts" / "run_server.py"),
            "--port",
            str(crash_port),
            "--db",
            str(db_path),
            "--replay-target",
            f"{crash_url}/_test/replay",
        ],
        cwd=ROOT,
        env=crash_env,
        stdout=crash_log,
        stderr=subprocess.STDOUT,
    )
    try:
        with httpx.Client(base_url=crash_url, timeout=5.0) as crash_client:
            wait_health(crash_client)
            try:
                crash_client.post(
                    "/api/v1/policies/increments",
                    json={
                        "base_version": current_version,
                        "base_policy_version": weight_increment["policy_version"],
                        "policy_version": "p-should-be-aborted",
                        "patch": {"service_weights": {"checkout-service": 0.35}},
                    },
                )
            except httpx.HTTPError:
                pass
        return_code = crash_process.wait(timeout=10)
        if return_code != 97:
            raise RuntimeError(f"crash process exited with {return_code}, expected 97")
    finally:
        if crash_process.poll() is None:
            crash_process.kill()
            crash_process.wait(timeout=5)
        crash_log.close()
    print(f"[kill] process exited at persistence hook with code 97; current committed version was v{current_version}")

    with run_server(db_path) as (_base_url, client, _process):
        recovery = client.get("/api/v1/recovery/latest").json()["recovery"]
        last_version = client.get("/api/v1/commits").json()["current_version"]
        if last_version != current_version:
            raise RuntimeError(f"recovery changed visible version: {last_version} != {current_version}")
        report["kill_recovery"] = recovery
        print("\n[7] restart recovery report (persisted facts only)")
        print(json.dumps(recovery, ensure_ascii=False, indent=2))

        recovered_increment = assert_ok(
            client.post(
                "/api/v1/policies/increments",
                json={
                    "base_version": last_version,
                    "base_policy_version": weight_increment["policy_version"],
                    "policy_version": "p-after-recovery",
                    "patch": {"service_weights": {"checkout-service": 0.35}},
                },
            )
        )
        report["post_recovery_commit"] = compact_state(recovered_increment)
        print("\n[8] a fresh commit succeeds after recovery")
        print(json.dumps(compact_state(recovered_increment), ensure_ascii=False, indent=2))

    report_path = RUNTIME / "demo_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"\n[done] report: {report_path}")
    print(f"[done] sqlite: {db_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
