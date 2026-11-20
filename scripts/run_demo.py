from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
VAR = ROOT / "var"
SERVER_LOG = ARTIFACTS / "demo-server.log"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def request(
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
    timeout: float = 8.0,
) -> tuple[int, dict[str, Any]]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        return exc.code, json.loads(body) if body else {"ok": False}


def start_server(database: Path, port: int, log_handle) -> subprocess.Popen:
    environment = os.environ.copy()
    environment["WORKBENCH_DB_PATH"] = str(database)
    environment["WORKBENCH_ALLOW_CRASH"] = "1"
    command = [
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
    ]
    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env=environment,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 25
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"uvicorn exited early with code {process.returncode}")
        try:
            status, body = request("GET", f"{base}/api/health", timeout=1.0)
            if status == 200 and body.get("ok"):
                return process
        except Exception:
            time.sleep(0.15)
    process.terminate()
    raise RuntimeError("uvicorn did not become ready within 25 seconds")


def stop_server(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def compact_run(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": run["run_id"],
        "state": run["state"],
        "error_code": run["error_code"],
        "deterministic_digest": run["deterministic_digest"],
        "schedule_digest": run["schedule_digest"],
        "batch_count": len(run["batches"]),
        "quantiles": [
            {
                "label": item["label"],
                "latency_ms": item["latency_ms"],
                "sample_count": item["sample_count"],
                "rank": item["rank"],
            }
            for item in run["quantiles"]
        ],
        "batches": [
            {
                "batch_uid": item["batch_uid"],
                "batch_id": item["batch_id"],
                "model_id": item["model_id"],
                "memory_display": item["memory_display"],
                "latency_ms": item["latency_ms"],
                "split_reasons": item["split_reasons"],
                "state": item["state"],
            }
            for item in run["batches"]
        ],
        "fragmentation_events": len(run["memory_plan"]["fragmentation_events"]),
        "mixed_rejections": len(run["memory_plan"]["mixed_rejections"]),
    }


def main() -> int:
    ARTIFACTS.mkdir(exist_ok=True)
    VAR.mkdir(exist_ok=True)
    database = VAR / "demo-crash-recovery.db"
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(database) + suffix)
        if path.exists():
            path.unlink()
    log_handle = SERVER_LOG.open("w", encoding="utf-8")
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    results: dict[str, Any] = {
        "bind": base,
        "database": str(database),
        "loopback_only": True,
    }
    process = start_server(database, port, log_handle)
    try:
        status, body = request("POST", f"{base}/api/demo/run/normal", {})
        if status != 201:
            raise RuntimeError(f"normal run failed: {status} {body}")
        normal = body["data"]["run"]
        results["normal_run"] = compact_run(normal)
        status, body = request(
            "GET", f"{base}/api/batches/{normal['batches'][0]['batch_uid']}/metrics"
        )
        if status != 200:
            raise RuntimeError(f"normal metrics failed: {status} {body}")
        metrics = body["data"]
        results["normal_batch_metrics"] = {
            "batch_uid": metrics["batch_uid"],
            "quantiles": [
                {
                    "label": q["label"],
                    "latency_ms": q["latency_ms"],
                    "sample_count": q["sample_count"],
                    "rank": q["rank"],
                    "rounding_error_us": q["rounding_error_us"],
                }
                for q in metrics["latency"]["quantiles"]
            ],
            "rounding": metrics["rounding"],
            "memory_budget": {
                "batch_memory_display": metrics["memory_budget"]["batch_memory_display"],
                "region_start_bytes": metrics["memory_budget"]["region_start_bytes"],
                "region_end_bytes": metrics["memory_budget"]["region_end_bytes"],
            },
        }

        status, body = request("POST", f"{base}/api/demo/run/fragment", {})
        if status != 201:
            raise RuntimeError(f"fragment run failed: {status} {body}")
        fragment = body["data"]["run"]
        results["fragment_run"] = compact_run(fragment)
        events = fragment["memory_plan"]["fragmentation_events"]
        results["fragment_evidence"] = {
            "before_bitmap": events[0]["before_bitmap"]["text"],
            "after_bitmap": events[0]["after_bitmap"]["text"],
            "mixed_rejection": events[0]["mixed_rejection"],
            "constraints": events[0]["constraint_assertions"],
        }

        drift_status, body = request("POST", f"{base}/api/demo/run/precision-drift", {})
        if drift_status != 409 or body.get("error", {}).get("code") != "E_PRECISION_DRIFT":
            raise RuntimeError(f"precision drift expectation failed: {drift_status} {body}")
        drift_run_id = body["error"]["details"]["run_id"]
        status, body = request("GET", f"{base}/api/runs/{drift_run_id}")
        results["precision_drift"] = {
            "http_status": drift_status,
            "code": "E_PRECISION_DRIFT",
            "persisted": True,
            "run": compact_run(body["data"]),
            "failed_batches": body["data"]["batches"],
        }
        status, body = request("GET", f"{base}/api/runs/{drift_run_id}/validation")
        results["precision_drift"]["validation"] = body["data"]

        status, body = request("GET", f"{base}/api/samples/dimensions-invalid")
        invalid_payload = body["data"]["payload"]
        status, body = request("POST", f"{base}/api/traces", invalid_payload)
        if status != 422 or body.get("error", {}).get("code") != "E_SPEC_INVALID":
            raise RuntimeError(f"invalid dimension expectation failed: {status} {body}")
        results["invalid_dimension"] = {
            "http_status": status,
            "code": body["error"]["code"],
            "written": body["error"]["details"]["written"],
        }

        crash_error = None
        try:
            request(
                "POST",
                f"{base}/api/traces/trace-normal-v1/organize",
                {"demo_crash_at": "mid_quantiles"},
                timeout=10,
            )
        except Exception as exc:
            crash_error = type(exc).__name__
        try:
            exit_code = process.wait(timeout=12)
        except subprocess.TimeoutExpired:
            process.kill()
            exit_code = process.wait(timeout=5)
        results["forced_termination"] = {
            "http_transport_error": crash_error,
            "exit_code": exit_code,
            "expected_checkpoint": "BATCHES_PERSISTED_QUANTILES_PENDING",
        }
    finally:
        stop_server(process)
        log_handle.close()

    log_handle = SERVER_LOG.open("a", encoding="utf-8")
    process = start_server(database, port, log_handle)
    try:
        status, body = request("GET", f"{base}/api/recovery/report")
        if status != 200:
            raise RuntimeError(f"recovery report failed: {status} {body}")
        report = body["data"]
        results["recovery_report"] = report
        if report["events"]:
            aborted_run_id = report["events"][0]["run_id"]
            status, run_body = request("GET", f"{base}/api/runs/{aborted_run_id}")
            results["aborted_run"] = {
                "http_status": status,
                "state": run_body["data"]["state"],
                "error_code": run_body["data"]["error_code"],
                "batch_states": [item["state"] for item in run_body["data"]["batches"]],
                "batch_count": len(run_body["data"]["batches"]),
            }
            status, quantile_body = request(
                "GET", f"{base}/api/runs/{aborted_run_id}/quantiles"
            )
            results["aborted_run_quantiles"] = {
                "http_status": status,
                "code": quantile_body.get("error", {}).get("code"),
            }
        status, body = request("GET", f"{base}/api/recovery/intents?limit=20")
        results["intent_stats_after_restart"] = {
            "intents": body["data"]["intents"],
        }
    finally:
        stop_server(process)
        log_handle.close()

    output = ARTIFACTS / "demo_results.json"
    output.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(
        {
            "normal_state": results.get("normal_run", {}).get("state"),
            "normal_batches": results.get("normal_run", {}).get("batch_count"),
            "fragment_events": results.get("fragment_run", {}).get("fragmentation_events"),
            "mixed_rejections": results.get("fragment_run", {}).get("mixed_rejections"),
            "drift_code": results.get("precision_drift", {}).get("code"),
            "invalid_code": results.get("invalid_dimension", {}).get("code"),
            "crash_exit_code": results.get("forced_termination", {}).get("exit_code"),
            "recovered_pending": results.get("recovery_report", {}).get("pending_intents_found"),
            "aborted_state": results.get("aborted_run", {}).get("state"),
            "aborted_quantiles_code": results.get("aborted_run_quantiles", {}).get("code"),
            "artifact": str(output),
        },
        ensure_ascii=False,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
