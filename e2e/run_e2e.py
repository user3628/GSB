from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gateway.domain import compute_route_checksum  # noqa: E402


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_http(url: str, *, timeout: float = 20.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = requests.get(url, timeout=0.5)
            if response.status_code == 200:
                return response.json()
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(0.05)
    raise RuntimeError(f"service did not become ready: {url}: {last_error}")


def wait_terminal(base: str, request_id: str, timeout: float = 8.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        response = requests.get(f"{base}/requests/{request_id}", timeout=2)
        if response.status_code == 200:
            last = response.json()["request"]
            if last["status"] in {"COMPLETED", "FAILED", "SHED", "ABORTED"}:
                return last
        time.sleep(0.03)
    raise AssertionError(f"{request_id} did not become terminal: {last}")


def wait_status(base: str, request_id: str, status: str, timeout: float = 5.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        response = requests.get(f"{base}/requests/{request_id}", timeout=2)
        if response.status_code == 200:
            last = response.json()["request"]
            if last["status"] == status:
                return last
        time.sleep(0.02)
    raise AssertionError(f"{request_id} did not reach {status}: {last}")


def route_document(version: int, backend_url: str) -> dict[str, Any]:
    with open(ROOT / "samples" / f"route_v{version}.json", "r", encoding="utf-8") as handle:
        document = json.load(handle)
    document.pop("checksum", None)
    for backend in document["backends"].values():
        backend["endpoint"] = backend_url
    document["checksum"] = compute_route_checksum(document)
    return document


def start_process(command: list[str], log_path: Path, env: dict[str, str]) -> subprocess.Popen:
    log = open(log_path, "ab", buffering=0)
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    return subprocess.Popen(
        command,
        cwd=str(ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
        env=env,
        creationflags=creationflags,
    )


def stop_process(process: subprocess.Popen, *, force: bool = False) -> None:
    if process.poll() is not None:
        return
    if force:
        process.kill()
    else:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def force_kill(process: subprocess.Popen) -> None:
    if process.poll() is None:
        if os.name != "nt":
            try:
                process.send_signal(signal.SIGKILL)
            except ProcessLookupError:
                pass
        else:
            process.kill()
    process.wait(timeout=10)


def start_backend(port: int, log_path: Path, env: dict[str, str]) -> subprocess.Popen:
    return start_process(
        [
            sys.executable,
            str(ROOT / "scripts" / "mock_backend.py"),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--behaviors",
            str(ROOT / "samples" / "backend_behaviors.json"),
            "--log-level",
            "warning",
        ],
        log_path,
        env,
    )


def start_gateway(port: int, db_path: Path, log_path: Path, env: dict[str, str]) -> subprocess.Popen:
    gateway_env = dict(env)
    gateway_env.update(
        {
            "GATEWAY_GLOBAL_CONCURRENCY": "2",
            "GATEWAY_QUEUE_LIMIT": "20",
            "GATEWAY_ROUND_BUDGET_MS": "600",
            "GATEWAY_DB_PATH": str(db_path),
        }
    )
    return start_process(
        [
            sys.executable,
            "-m",
            "gateway.api",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--db",
            str(db_path),
            "--log-level",
            "warning",
        ],
        log_path,
        gateway_env,
    )


def summarize_round(round_row: dict[str, Any]) -> dict[str, Any]:
    trace = round_row["trace"]
    return {
        "round_id": round_row["id"],
        "budget_ms": round_row["budget_ms"],
        "dispatch_order": [
            {
                "dispatch_seq": item["dispatch_seq"],
                "request_id": item["request_id"],
                "backend_id": item["backend_id"],
                "tenant_id": item["tenant_id"],
                "priority": item["priority"],
                "estimated_latency_ms": item["estimated_latency_ms"],
            }
            for item in trace["dispatch_order"]
        ],
        "skipped": [
            {
                "request_id": item["request_id"],
                "action": item["action"],
                "reason": item["reason"],
                "code": item.get("code"),
            }
            for item in trace["skipped"]
        ],
        "summary": trace["summary"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local two-round + crash-recovery acceptance test")
    parser.add_argument("--output-dir", default=str(ROOT / "artifacts" / "e2e"))
    args = parser.parse_args()
    output_dir = Path(args.output_dir).resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = output_dir / "gateway.db"
    env = dict(os.environ)
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    backend_port = free_port()
    gateway_port = free_port()
    backend_url = f"http://127.0.0.1:{backend_port}"
    gateway_url = f"http://127.0.0.1:{gateway_port}"
    backend_process: subprocess.Popen | None = None
    gateway_process: subprocess.Popen | None = None
    report: dict[str, Any] = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ports": {"backend": backend_port, "gateway": gateway_port},
        "assertions": [],
    }

    def assertion(name: str, condition: bool, detail: Any = None) -> None:
        report["assertions"].append(
            {"name": name, "passed": bool(condition), "detail": detail}
        )
        if not condition:
            raise AssertionError(f"assertion failed: {name}: {detail}")

    try:
        backend_process = start_backend(backend_port, output_dir / "backend.log", env)
        backend_health = wait_http(backend_url + "/health")
        assertion("backend_ready", backend_health["ok"] is True)

        gateway_process = start_gateway(gateway_port, db_path, output_dir / "gateway.log", env)
        wait_http(gateway_url + "/health")
        assertion("gateway_ready", True)

        v1 = route_document(1, backend_url)
        submitted_route = requests.post(gateway_url + "/routes", json=v1, timeout=5)
        assertion("route_v1_committed", submitted_route.status_code == 200, submitted_route.text)
        stale = requests.post(gateway_url + "/routes", json=v1, timeout=5)
        assertion("stale_route_rejected", stale.status_code == 409 and stale.json()["code"] == "E_STALE_ROUTE")

        with open(ROOT / "samples" / "requests_batch.json", "r", encoding="utf-8") as handle:
            batch = json.load(handle)
        submitted_batch = requests.post(gateway_url + "/requests/batch", json=batch, timeout=5)
        assertion("batch_admitted", submitted_batch.status_code == 200 and submitted_batch.json()["accepted"] == 8)

        round1_response = requests.post(
            gateway_url + "/rounds",
            json={"time_budget_ms": 600, "degrade_excess": False},
            timeout=5,
        )
        assertion("round1_http", round1_response.status_code == 200, round1_response.text)
        round1 = round1_response.json()["round"]
        assertion(
            "round1_parallel_dispatch",
            [item["request_id"] for item in round1["trace"]["dispatch_order"]] == ["r-001", "r-002"],
            summarize_round(round1)["dispatch_order"],
        )
        r1 = wait_terminal(gateway_url, "r-001")
        r2 = wait_terminal(gateway_url, "r-002")
        assertion("r001_completed", r1["status"] == "COMPLETED", r1)
        assertion("r002_failed_backend", r2["status"] == "FAILED" and r2["error_code"] == "E_BACKEND_FAILURE", r2)

        round2_response = requests.post(
            gateway_url + "/rounds",
            json={"time_budget_ms": 200, "degrade_excess": True},
            timeout=5,
        )
        assertion("round2_http", round2_response.status_code == 200, round2_response.text)
        round2 = round2_response.json()["round"]
        assertion(
            "round2_parallel_dispatch",
            [item["request_id"] for item in round2["trace"]["dispatch_order"]] == ["r-003", "r-007"],
            summarize_round(round2)["dispatch_order"],
        )
        time_budget_shed = [
            item for item in round2["trace"]["skipped"]
            if item["reason"] == "TIME_BUDGET" and item["action"] == "SHED"
        ]
        assertion("round2_time_budget_shed", len(time_budget_shed) == 4, time_budget_shed)
        r3 = wait_terminal(gateway_url, "r-003")
        r7 = wait_terminal(gateway_url, "r-007")
        assertion("r003_completed", r3["status"] == "COMPLETED", r3)
        assertion("r007_timeout_released", r7["status"] == "FAILED" and r7["error_code"] == "E_BACKEND_TIMEOUT" and r7["vram_reserved_mb"] == 0, r7)

        backend_status = requests.get(gateway_url + "/backends", timeout=5).json()
        backend_by_id = {item["backend_id"]: item for item in backend_status["backends"]}
        assertion(
            "timeout_resources_released",
            backend_by_id["backend-b"]["in_flight"] == 0
            and backend_by_id["backend-b"]["vram_reserved_mb"] == 0
            and backend_status["active_http_connections"] == [],
            backend_by_id,
        )
        assertion("backend_circuit_degraded", backend_by_id["backend-b"]["state"] == "DEGRADED")
        degraded = requests.post(
            gateway_url + "/requests",
            json={
                "request_id": "degraded-probe",
                "model_id": "beta",
                "sample_count": 1,
                "latency_cap_ms": 200,
                "priority": 1,
                "tenant_id": "tenant-probe",
            },
            timeout=5,
        )
        assertion("degraded_business_code", degraded.status_code == 503 and degraded.json()["code"] == "E_BACKEND_DEGRADED", degraded.text)

        validation_main = requests.get(gateway_url + "/validation/report", timeout=5).json()["report"]
        assertion("validation_after_two_rounds", validation_main["verdict"] == "PASS", validation_main)
        report["two_round_traces"] = [summarize_round(round1), summarize_round(round2)]
        report["request_outcomes"] = {
            request_id: wait_terminal(gateway_url, request_id)
            for request_id in ("r-001", "r-002", "r-003", "r-004", "r-005", "r-006", "r-007", "r-008")
        }
        report["backends_after_rounds"] = backend_status
        report["validation_after_rounds"] = validation_main

        crash_request = {
            "request_id": "crash-hang",
            "model_id": "alpha",
            "sample_count": 4,
            "latency_cap_ms": 5_000,
            "priority": 100,
            "tenant_id": "tenant-recovery",
            "fault_mode": "hang",
        }
        crash_submission = requests.post(gateway_url + "/requests", json=crash_request, timeout=5)
        assertion("crash_request_admitted", crash_submission.status_code == 200, crash_submission.text)
        crash_round = requests.post(
            gateway_url + "/rounds",
            json={"time_budget_ms": 600, "degrade_excess": False},
            timeout=5,
        ).json()["round"]
        assertion("crash_request_dispatched", crash_round["trace"]["dispatch_order"][0]["request_id"] == "crash-hang")
        wait_status(gateway_url, "crash-hang", "DISPATCHED")
        assertion("force_kill_gateway", gateway_process is not None)
        assert gateway_process is not None
        old_pid = gateway_process.pid
        force_kill(gateway_process)
        report["forced_kill"] = {"pid": old_pid, "request_id": "crash-hang", "state_at_kill": "DISPATCHED"}

        recovery_port = free_port()
        recovery_url = f"http://127.0.0.1:{recovery_port}"
        gateway_process = start_gateway(recovery_port, db_path, output_dir / "gateway-recovery.log", env)
        wait_http(recovery_url + "/health", timeout=20)
        recovery_response = requests.get(recovery_url + "/recovery/report", timeout=5).json()
        recovery = recovery_response["report"]
        assertion("recovery_found_durable_dispatch", recovery["requeued_count"] == 1, recovery)
        recovered_request = requests.get(recovery_url + "/requests/crash-hang", timeout=5).json()["request"]
        assertion("recovered_request_queued", recovered_request["status"] == "QUEUED", recovered_request)
        recovered_backends = requests.get(recovery_url + "/backends", timeout=5).json()
        assertion(
            "recovery_rebuilt_backend_counters",
            all(row["in_flight"] == 0 and row["vram_reserved_mb"] == 0 for row in recovered_backends["backends"]),
            recovered_backends,
        )

        retry_round = requests.post(
            recovery_url + "/rounds",
            json={"time_budget_ms": 600, "degrade_excess": False},
            timeout=5,
        ).json()["round"]
        assertion("recovered_request_redispatched", retry_round["trace"]["dispatch_order"][0]["request_id"] == "crash-hang")
        wait_status(recovery_url, "crash-hang", "DISPATCHED")
        cancelled = requests.post(recovery_url + "/requests/crash-hang/cancel", json={"reason": "e2e_recovery_cancel"}, timeout=5).json()["request"]
        if cancelled["status"] == "DISPATCHED":
            cancelled = wait_terminal(recovery_url, "crash-hang")
        assertion("recovered_request_cancel_released", cancelled["status"] == "FAILED" and cancelled["error_code"] == "E_REQUEST_CANCELLED", cancelled)
        validation_recovery = requests.get(recovery_url + "/validation/report", timeout=5).json()["report"]
        assertion("validation_after_recovery", validation_recovery["verdict"] == "PASS", validation_recovery)
        report["recovery"] = {
            "report": recovery,
            "request_after_restart": recovered_request,
            "retry_round": summarize_round(retry_round),
            "cancelled_request": cancelled,
            "validation": validation_recovery,
        }

        v2 = route_document(2, backend_url)
        v2_response = requests.post(recovery_url + "/routes", json=v2, timeout=5)
        assertion("route_v2_committed", v2_response.status_code == 200, v2_response.text)
        stale_v1 = requests.post(recovery_url + "/routes", json=v1, timeout=5)
        assertion("route_v1_now_stale", stale_v1.status_code == 409 and stale_v1.json()["code"] == "E_STALE_ROUTE")

        final_validation = requests.get(recovery_url + "/validation/report", timeout=5).json()["report"]
        assertion("final_validation_pass", final_validation["verdict"] == "PASS", final_validation)
        report["final_validation"] = final_validation
        report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        report["verdict"] = "PASS" if all(item["passed"] for item in report["assertions"]) else "FAIL"

        (output_dir / "round1_trace.json").write_text(
            json.dumps(round1, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (output_dir / "round2_trace.json").write_text(
            json.dumps(round2, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (output_dir / "validation_report.json").write_text(
            json.dumps(final_validation, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (output_dir / "e2e_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        summary = {
            "verdict": report["verdict"],
            "round1": {
                "dispatch_order": report["two_round_traces"][0]["dispatch_order"],
                "skipped": report["two_round_traces"][0]["skipped"],
                "summary": report["two_round_traces"][0]["summary"],
            },
            "round2": {
                "dispatch_order": report["two_round_traces"][1]["dispatch_order"],
                "skipped": report["two_round_traces"][1]["skipped"],
                "summary": report["two_round_traces"][1]["summary"],
            },
            "timeout": {
                "request_id": "r-007",
                "status": report["request_outcomes"]["r-007"]["status"],
                "error_code": report["request_outcomes"]["r-007"]["error_code"],
            },
            "backend_after_timeout": backend_by_id["backend-b"],
            "recovery": {
                "requeued_count": recovery["requeued_count"],
                "open_intent_count": recovery["open_intent_count"],
                "source": recovery["source"],
            },
            "validation": {
                "verdict": final_validation["verdict"],
                "checks": {check["id"]: check["status"] for check in final_validation["checks"]},
            },
        }
        (output_dir / "e2e_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        if gateway_process is not None:
            stop_process(gateway_process, force=True)
        if backend_process is not None:
            stop_process(backend_process, force=True)


if __name__ == "__main__":
    main()

