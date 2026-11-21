"""Run the full HTTP + local-endpoint acceptance scenario and save a factual report."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from delay_queue.mock_endpoint import start_server  # noqa: E402
from delay_queue.config import Settings  # noqa: E402
from delay_queue.service import DelayQueueService  # noqa: E402
from delay_queue.storage import Storage  # noqa: E402
from delay_queue.verify import validate_round  # noqa: E402


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_http(client: httpx.Client, url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if client.get(url, timeout=0.5).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError(f"service did not become ready: {url}")


def post(client: httpx.Client, path: str, payload: dict) -> tuple[int, dict]:
    response = client.post(path, json=payload)
    return response.status_code, response.json()


def main() -> None:
    data_dir = ROOT / "data"
    intent_dir = ROOT / "intent_logs" / "demo"
    artifact_dir = ROOT / "artifacts"
    data_dir.mkdir(parents=True, exist_ok=True)
    intent_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "demo.db"
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()

    mock_server, _ = start_server(port=0)
    mock_port = mock_server.server_address[1]
    endpoint = f"http://127.0.0.1:{mock_port}/dispatch"
    api_port = free_port()
    base_url = f"http://127.0.0.1:{api_port}"
    env = dict(os.environ)
    env.update(
        {
            "PYTHONPATH": str(ROOT),
            "DQT_DB_PATH": str(db_path),
            "DQT_INTENT_DIR": str(intent_dir),
            "DQT_DEFAULT_ENDPOINT": endpoint,
            "DQT_GLOBAL_CONCURRENCY": "8",
            "DQT_MAX_CONCURRENCY": "3",
            "DQT_TIME_BUDGET_MS": "700",
            "DQT_MAX_TASK_RESOURCE": "6",
            "DQT_WAITING_QUEUE_CAPACITY": "20",
            "DQT_DISPATCH_TIMEOUT_MS": "120",
            "DQT_RETRY_BASE_MS": "60",
        }
    )
    log_path = data_dir / "demo_uvicorn.log"
    log_file = log_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "delay_queue.api:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(api_port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=log_file,
        stderr=subprocess.STDOUT,
        text=True,
    )
    report: dict = {"seed": 20261122, "base_url": base_url, "endpoint": endpoint}
    try:
        with httpx.Client(base_url=base_url, timeout=30, trust_env=False) as client:
            wait_http(client, "/healthz")
            validation_before = client.get("/api/v1/queues/demo/status").json()
            invalid_code, invalid_body = post(
                client,
                "/api/v1/queues/demo/tasks",
                json.loads((ROOT / "samples" / "tasks_invalid.json").read_text()),
            )
            validation_after = client.get("/api/v1/queues/demo/status").json()
            sample = json.loads(
                (ROOT / "samples" / "tasks_deterministic.json").read_text()
            )
            submit_code, submit_body = post(
                client, "/api/v1/queues/demo/tasks", sample
            )
            duplicate_code, duplicate_body = post(
                client, "/api/v1/queues/demo/tasks", sample
            )
            round_code, round_body = post(
                client,
                "/api/v1/queues/demo/rounds/demo-r1",
                {
                    "max_concurrency": 3,
                    "time_budget_ms": 700,
                    "max_task_resource": 6,
                    "waiting_queue_capacity": 20,
                    "dispatch_timeout_ms": 120,
                },
            )
            zero_code, zero_body = post(
                client,
                "/api/v1/queues/demo/rounds/demo-r2-budget-zero",
                {
                    "max_concurrency": 3,
                    "time_budget_ms": 0,
                    "max_task_resource": 6,
                    "waiting_queue_capacity": 20,
                    "dispatch_timeout_ms": 120,
                },
            )
            timeout_sample = json.loads(
                (ROOT / "samples" / "tasks_timeout.json").read_text()
            )
            post(client, "/api/v1/queues/timeout-demo/tasks", timeout_sample)
            timeout_code, timeout_body = post(
                client,
                "/api/v1/queues/timeout-demo/rounds/timeout-r1",
                {
                    "max_concurrency": 1,
                    "time_budget_ms": 500,
                    "max_task_resource": 10,
                    "waiting_queue_capacity": 5,
                    "dispatch_timeout_ms": 60,
                },
            )
            time.sleep(0.12)
            timeout2_code, timeout2_body = post(
                client,
                "/api/v1/queues/timeout-demo/rounds/timeout-r2",
                {
                    "max_concurrency": 1,
                    "time_budget_ms": 500,
                    "max_task_resource": 10,
                    "waiting_queue_capacity": 5,
                    "dispatch_timeout_ms": 60,
                },
            )
            full_tasks = [
                {
                    "task_id": f"full-{index}",
                    "due_at_ms": 0,
                    "priority": index,
                    "remaining_retries": 0,
                    "resource_requirement": 1,
                    "payload": {"mode": "success"},
                }
                for index in range(3)
            ]
            post(client, "/api/v1/queues/full-demo/tasks", {"tasks": full_tasks})
            full_code, full_body = post(
                client,
                "/api/v1/queues/full-demo/rounds/full-r1",
                {
                    "max_concurrency": 1,
                    "time_budget_ms": 300,
                    "max_task_resource": 10,
                    "waiting_queue_capacity": 1,
                    "dispatch_timeout_ms": 60,
                },
            )
            conflict_code, conflict_body = post(
                client,
                "/api/v1/queues/demo/rounds/demo-r1",
                {
                    "max_concurrency": 2,
                    "time_budget_ms": 700,
                    "max_task_resource": 6,
                    "waiting_queue_capacity": 20,
                    "dispatch_timeout_ms": 120,
                },
            )
            completed_task = next(
                item["task_id"]
                for item in round_body["dispatched"]
                if item["state_after"] in {"COMPLETED", "DEAD_LETTERED"}
            )
            transition_response = client.post(
                f"/api/v1/queues/demo/tasks/{completed_task}/transitions",
                json={"to_state": "READY", "reason": "demo illegal transition"},
            )
            trajectory = client.get(
                f"/api/v1/queues/demo/tasks/{completed_task}"
            ).json()
            final_status = client.get("/api/v1/queues/demo/status").json()
            recovery = client.get("/api/v1/recovery/report").json()
            post(
                client,
                "/api/v1/queues/crash-demo/tasks",
                {
                    "tasks": [
                        {
                            "task_id": "crash-demo-task",
                            "due_at_ms": 0,
                            "priority": 0,
                            "remaining_retries": 1,
                            "resource_requirement": 1,
                            "payload": {"mode": "hang", "hang_seconds": 30},
                        }
                    ]
                },
            )

        crash_process = subprocess.Popen(
            [
                sys.executable,
                str(ROOT / "scripts" / "crash_round.py"),
                "--db",
                str(db_path),
                "--intent-dir",
                str(intent_dir),
                "--queue",
                "crash-demo",
                "--round-id",
                "crash-demo-round",
                "--endpoint",
                endpoint,
                "--hang-ms",
                "30000",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            observer = Storage(db_path)
            deadline = time.time() + 10
            crash_ready = False
            while time.time() < deadline:
                crash_snapshot = observer.queue_snapshot("crash-demo")
                if (
                    crash_snapshot["state_counts"]["DISPATCHED"] == 1
                    and crash_snapshot["open_intents"] == 1
                ):
                    crash_ready = True
                    break
                if crash_process.poll() is not None:
                    break
                time.sleep(0.02)
            if not crash_ready:
                raise RuntimeError(
                    "crash worker did not reach DISPATCHED + IN_FLIGHT + OPEN intent"
                )
            crash_process.kill()
            crash_process.wait(timeout=5)
        finally:
            if crash_process.poll() is None:
                crash_process.kill()
                crash_process.wait(timeout=5)
            if crash_process.stdout is not None:
                crash_process.stdout.close()
            if crash_process.stderr is not None:
                crash_process.stderr.close()

        restarted = DelayQueueService(
            Settings(
                db_path=db_path,
                intent_log_dir=intent_dir,
                default_endpoint=endpoint,
                global_concurrency=8,
                retry_backoff_base_ms=60,
                round_wait_timeout_ms=2_000,
            )
        )
        crash_recovery = restarted.startup()
        crash_trajectory = restarted.get_task_trajectory(
            "crash-demo", "crash-demo-task"
        )
        crash_round = restarted.storage.get_round("crash-demo-round")
        validation_crash = validate_round(db_path, "crash-demo-round", 60)

        validation_r1 = validate_round(db_path, "demo-r1", 60)
        validation_r2 = validate_round(db_path, "demo-r2-budget-zero", 60)
        validation_timeout1 = validate_round(db_path, "timeout-r1", 60)
        validation_timeout2 = validate_round(db_path, "timeout-r2", 60)
        crash_attempt = crash_trajectory["attempts"][0]
        crash_action = crash_recovery["actions"][0]
        checks = {
            "invalid_batch_code": invalid_code == 422
            and invalid_body.get("code") == "E_TASK_INVALID",
            "invalid_batch_was_atomic": validation_before["total_tasks"] == 0
            and validation_after["total_tasks"] == 0,
            "batch_submit": submit_code == 201 and submit_body.get("inserted") == 12,
            "idempotent_resubmit": duplicate_code == 201
            and duplicate_body.get("inserted") == 0
            and len(duplicate_body.get("duplicates", [])) == 12,
            "round_completed": round_code == 200 and round_body.get("status") == "COMPLETED",
            "priority_waiting_bound": round_body["counts"]["dispatched"] == 3
            and round_body["counts"]["waiting"] == 8,
            "zero_budget_skips": zero_code == 200
            and zero_body["counts"]["dispatched"] == 0
            and all(
                item["reason"] in {"TIME_BUDGET_EXHAUSTED", "WAITING_QUEUE"}
                for item in zero_body["skipped"]
            ),
            "timeout_code": timeout_code == 200
            and timeout_body.get("error_codes") == ["E_DISPATCH_TIMEOUT"]
            and timeout_body["dispatched"][0]["outcome"] == "TIMEOUT",
            "timeout_retry": timeout_body["dispatched"][0]["state_after"]
            == "RETRY_WAITING",
            "timeout_dead_letter": timeout2_code == 200
            and timeout2_body["dispatched"][0]["state_after"] == "DEAD_LETTERED",
            "queue_full_code": full_code == 429
            and full_body.get("code") == "E_QUEUE_FULL",
            "conflicting_round_code": conflict_code == 409
            and conflict_body.get("code") == "E_CONFLICTING_ROUND",
            "invalid_transition_code": transition_response.status_code == 409
            and transition_response.json().get("code") == "E_INVALID_TRANSITION",
            "query_consistency": final_status["consistency"]["ok"] is True
            and final_status["dispatching_without_attempt"] == 0,
            "trajectory_has_facts": bool(trajectory["task"])
            and bool(trajectory["transitions"])
            and bool(trajectory["attempts"]),
            "recovery_report_factual": recovery["report"]["source"]
            == "STARTUP_RECOVERY"
            and recovery["report"][
                "graceful_exit_report_available_for_prior_process"
            ]
            is False,
            "validator_r1": validation_r1["ok"],
            "validator_r2": validation_r2["ok"],
            "validator_timeout_r1": validation_timeout1["ok"],
            "validator_timeout_r2": validation_timeout2["ok"],
            "sigkill_recovery": crash_recovery["counts"]["rolled_back_to_ready"] == 1
            and crash_trajectory["task"]["state"] == "READY"
            and crash_attempt["outcome"] == "INTERRUPTED"
            and crash_attempt["http_status"] is None
            and crash_attempt["duration_ms"] is None
            and crash_trajectory["intents"][0]["state"] == "CLOSED"
            and crash_round["status"] == "INTERRUPTED"
            and crash_action["wire_outcome_unknown"] is True
            and crash_action["wire_result_persisted"] is False
            and crash_recovery[
                "graceful_exit_report_available_for_prior_process"
            ]
            is False
            and validation_crash["ok"],
        }
        report.update(
            {
                "generated_at_unix_ms": int(time.time() * 1_000),
                "checks": checks,
                "all_checks_passed": all(checks.values()),
                "round_demo_r1": {
                    "status_code": round_code,
                    "counts": round_body.get("counts"),
                    "skipped": round_body.get("skipped"),
                    "dispatched": [
                        {
                            "task_id": item["task_id"],
                            "outcome": item["outcome"],
                            "http_status": item["http_status"],
                            "duration_ms": item["duration_ms"],
                            "state_after": item["state_after"],
                        }
                        for item in round_body.get("dispatched", [])
                    ],
                },
                "round_demo_r2_zero_budget": {
                    "counts": zero_body.get("counts"),
                    "skipped": zero_body.get("skipped"),
                },
                "timeout_round": {
                    "first": timeout_body.get("dispatched"),
                    "second": timeout2_body.get("dispatched"),
                },
                "error_responses": {
                    "invalid_batch": invalid_body,
                    "queue_full": full_body,
                    "conflicting_round": conflict_body,
                    "invalid_transition": transition_response.json(),
                },
                "final_status": {
                    "state_counts": final_status.get("state_counts"),
                    "total_tasks": final_status.get("total_tasks"),
                    "open_intents": final_status.get("open_intents"),
                    "dispatching_without_attempt": final_status.get(
                        "dispatching_without_attempt"
                    ),
                    "consistency": final_status.get("consistency"),
                },
                "recovery_report": recovery.get("report"),
                "forced_termination_recovery": {
                    "report": crash_recovery,
                    "round_status": crash_round["status"],
                    "task_state": crash_trajectory["task"]["state"],
                    "attempt": crash_attempt,
                    "intent": crash_trajectory["intents"][0],
                },
                "validators": {
                    "demo-r1": validation_r1,
                    "demo-r2-budget-zero": validation_r2,
                    "timeout-r1": validation_timeout1,
                    "timeout-r2": validation_timeout2,
                    "crash-demo-round": validation_crash,
                },
                "mock_endpoint_stats": mock_server.state.snapshot(),
            }
        )
        output = artifact_dir / "verification_report.json"
        output.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        summary = {
            "all_checks_passed": report["all_checks_passed"],
            "checks": checks,
            "report": str(output),
            "demo_round_counts": round_body.get("counts"),
            "timeout_outcomes": [
                timeout_body["dispatched"][0]["outcome"],
                timeout2_body["dispatched"][0]["outcome"],
            ],
            "validator_results": {
                key: value["ok"] for key, value in report["validators"].items()
            },
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
        if not report["all_checks_passed"]:
            raise SystemExit(1)
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        log_file.close()
        mock_server.shutdown()
        mock_server.server_close()


if __name__ == "__main__":
    main()

