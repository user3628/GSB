from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from .config import LabConfig
from .errors import ErrorCode


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "default.json"


@dataclass
class ServiceProcess:
    name: str
    module: str
    args: list[str]
    log_path: Path
    process: subprocess.Popen[bytes] | None = None
    log_handle: Any = None

    def start(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_handle = self.log_path.open("ab")
        environment = os.environ.copy()
        environment["PYTHONUNBUFFERED"] = "1"
        environment["NO_PROXY"] = "127.0.0.1,localhost"
        environment["no_proxy"] = "127.0.0.1,localhost"
        self.process = subprocess.Popen(
            [sys.executable, "-m", self.module, *self.args],
            cwd=PROJECT_ROOT,
            env=environment,
            stdout=self.log_handle,
            stderr=subprocess.STDOUT,
        )

    def stop(self, timeout: float = 6.0) -> None:
        process = self.process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=timeout)
        if self.log_handle is not None:
            self.log_handle.close()
        self.process = None
        self.log_handle = None

    def kill(self) -> None:
        process = self.process
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGKILL)
            process.wait(timeout=5)
        if self.log_handle is not None:
            self.log_handle.close()
        self.process = None
        self.log_handle = None


def wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with httpx.Client(trust_env=False, timeout=0.5) as client:
                response = client.get(url)
                if response.status_code == 200:
                    return
        except Exception as exc:
            last_error = exc
        time.sleep(0.05)
    raise RuntimeError(f"Service at {url} did not become healthy: {last_error}")


def spec(
    injection_type: str,
    *,
    node_id: str = "node-1",
    shard: int = 0,
    round_no: int,
    max_retries: int,
    timeout_seconds: float,
    apply_attempts: list[int] | None = None,
) -> dict[str, Any]:
    return {
        "type": injection_type,
        "target_node": node_id,
        "target_shard": shard,
        "round": round_no,
        "retry": {
            "max_retries": max_retries,
            "backoff_seconds": 0.03,
            "backoff_multiplier": 2.0,
        },
        "params": {
            "timeout_seconds": timeout_seconds,
            "apply_attempts": apply_attempts or [1],
        },
    }


def build_scenarios() -> list[dict[str, Any]]:
    return [
        {
            "name": "timeout_then_retry_success",
            "batch_id": "demo-timeout-retry",
            "round": 0,
            "injection": spec(
                "response_timeout", round_no=0, max_retries=1, timeout_seconds=0.10
            ),
            "expect_status": 200,
            "expect_code": ErrorCode.OK.value,
            "expect_state": "VERIFIED",
        },
        {
            "name": "content_truncated_fatal_abort",
            "batch_id": "demo-content-truncated",
            "round": 1,
            "injection": spec(
                "content_truncated", round_no=1, max_retries=0, timeout_seconds=0.20
            ),
            "expect_status": 502,
            "expect_code": ErrorCode.FAULT_CONTENT_TRUNCATED.value,
            "expect_state": "ABORTED",
        },
        {
            "name": "duplicate_submission_conflict",
            "batch_id": "demo-duplicate",
            "round": 2,
            "injection": spec(
                "duplicate_submission", round_no=2, max_retries=0, timeout_seconds=0.20
            ),
            "expect_status": 409,
            "expect_code": ErrorCode.FAULT_DUPLICATE_SUBMISSION.value,
            "expect_state": "ABORTED",
        },
        {
            "name": "order_mismatch_fatal",
            "batch_id": "demo-order-mismatch",
            "round": 2,
            "injection": spec(
                "order_mismatch",
                node_id="node-2",
                shard=0,
                round_no=2,
                max_retries=0,
                timeout_seconds=0.20,
            ),
            "expect_status": 409,
            "expect_code": ErrorCode.FAULT_ORDER_MISMATCH.value,
            "expect_state": "ABORTED",
        },
        {
            "name": "connection_refused_then_retry_success",
            "batch_id": "demo-connection-refused",
            "round": 0,
            "injection": spec(
                "connection_refused", round_no=0, max_retries=1, timeout_seconds=0.20
            ),
            "expect_status": 200,
            "expect_code": ErrorCode.OK.value,
            "expect_state": "VERIFIED",
        },
        {
            "name": "retry_exhausted_shortened_set",
            "batch_id": "demo-shortened",
            "round": 3,
            "injection": spec(
                "response_timeout",
                round_no=3,
                max_retries=1,
                timeout_seconds=0.10,
                apply_attempts=[1, 2],
            ),
            "expect_status": 200,
            "expect_code": ErrorCode.DEGRADED_AGGREGATION.value,
            "expect_state": "VERIFIED",
        },
    ]


def run_scenario(base_url: str, scenario: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "batch_id": scenario["batch_id"],
        "round": scenario["round"],
        "injection": scenario["injection"],
    }
    with httpx.Client(trust_env=False, timeout=30.0) as client:
        response = client.post(f"{base_url}/v1/batches", json=payload)
        body = response.json()
        batch_id = scenario["batch_id"]
        if not body.get("ok", False):
            query = client.get(f"{base_url}/v1/batches/{batch_id}")
            batch = query.json()["data"] if query.status_code == 200 else None
        else:
            batch = body["data"]
    return {
        "name": scenario["name"],
        "request": payload,
        "http_status": response.status_code,
        "response": body,
        "batch": batch,
        "expect": {
            "http_status": scenario["expect_status"],
            "final_code": scenario["expect_code"],
            "state": scenario["expect_state"],
        },
    }


def validate_scenario(result: dict[str, Any]) -> None:
    expected = result["expect"]
    if result["http_status"] != expected["http_status"]:
        raise AssertionError(
            f"{result['name']}: status {result['http_status']} != {expected['http_status']}"
        )
    batch = result["batch"]
    if batch is None:
        raise AssertionError(f"{result['name']}: no committed batch is queryable")
    if batch["final_code"] != expected["final_code"]:
        raise AssertionError(
            f"{result['name']}: code {batch['final_code']} != {expected['final_code']}"
        )
    if batch["state"] != expected["state"]:
        raise AssertionError(
            f"{result['name']}: state {batch['state']} != {expected['state']}"
        )
    if result["name"] == "timeout_then_retry_success":
        codes = [fault["code"] for fault in batch["faults"]]
        if codes != [ErrorCode.FAULT_RESPONSE_TIMEOUT.value]:
            raise AssertionError(f"Unexpected timeout fault codes: {codes}")
        if len(batch["retry_sequence"]) != 2:
            raise AssertionError("Timeout retry sequence must contain two attempts")
    if result["name"] == "content_truncated_fatal_abort":
        if len(batch["retry_sequence"]) != 1:
            raise AssertionError("Fatal truncation must not be retried")
    if result["name"] == "retry_exhausted_shortened_set":
        retry_codes = [attempt["code"] for attempt in batch["retry_sequence"]]
        if retry_codes != [
            ErrorCode.FAULT_RESPONSE_TIMEOUT.value,
            ErrorCode.FAULT_RESPONSE_TIMEOUT.value,
        ]:
            raise AssertionError(f"Shortening scenario did not exhaust retries: {retry_codes}")
        aggregation = batch["aggregation"]
        if not aggregation["degraded"]:
            raise AssertionError("The shortened aggregate must be marked degraded")
        if aggregation["omitted_nodes"] != ["node-1"]:
            raise AssertionError("The target node should be omitted from the aggregate")
        bounds = aggregation["error_bounds"]
        if bounds["observed_deviation_l2"] > bounds["upper_bound_l2"] + 1e-14:
            raise AssertionError("Observed aggregate deviation exceeds its error bound")


def wait_for_intent(database_path: Path, batch_id: str, timeout: float = 5.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if database_path.exists():
            connection = sqlite3.connect(database_path, timeout=0.2)
            connection.row_factory = sqlite3.Row
            try:
                row = connection.execute(
                    "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
                ).fetchone()
                if row is not None:
                    return dict(row)
            finally:
                connection.close()
        time.sleep(0.03)
    raise RuntimeError(f"Intent for {batch_id} was not persisted before timeout")


def run_kill_recovery(
    config: LabConfig,
    base_url: str,
    validator: ServiceProcess,
) -> dict[str, Any]:
    batch_id = "demo-forced-kill"
    payload = {
        "batch_id": batch_id,
        "round": 4,
        "injection": spec(
            "response_timeout",
            node_id="node-1",
            shard=1,
            round_no=4,
            max_retries=0,
            timeout_seconds=5.0,
        ),
    }

    def submit() -> tuple[int, dict[str, Any]]:
        with httpx.Client(trust_env=False, timeout=20.0) as client:
            response = client.post(f"{base_url}/v1/batches", json=payload)
            return response.status_code, response.json()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(submit)
        intent = wait_for_intent(config.database_path, batch_id)
        time.sleep(0.35)
        validator.kill()
        interrupted_error: str | None = None
        try:
            future.result(timeout=6)
        except Exception as exc:
            interrupted_error = f"{type(exc).__name__}: {exc}"

    validator.start()
    wait_http(f"{base_url}/health", timeout=20)
    with httpx.Client(trust_env=False, timeout=10.0) as client:
        query = client.get(f"{base_url}/v1/batches/{batch_id}")
        if query.status_code != 200:
            raise RuntimeError(f"Recovered batch query failed: {query.status_code} {query.text}")
        batch = query.json()["data"]
        recoveries = client.get(f"{base_url}/v1/recoveries").json()["data"]
    if batch["state"] != "ABORTED":
        raise AssertionError("Killed batch must be recovered as ABORTED")
    if batch["final_code"] != ErrorCode.RECOVERED_AFTER_RESTART.value:
        raise AssertionError("Killed batch must expose E_RECOVERED_AFTER_RESTART")
    return {
        "name": "forced_kill_then_restart_recovery",
        "request": payload,
        "intent_before_kill": intent,
        "interrupted_client_error": interrupted_error,
        "recovered_batch": batch,
        "recovery_reports": recoveries,
        "expect": {
            "state": "ABORTED",
            "final_code": ErrorCode.RECOVERED_AFTER_RESTART.value,
            "exit_report_complete": False,
        },
    }


def compact_scenario(result: dict[str, Any]) -> dict[str, Any]:
    batch = result["batch"] or {}
    aggregation = batch.get("aggregation") or {}
    recovery = batch.get("recovery") or {}
    return {
        "name": result["name"],
        "http_status": result["http_status"],
        "business_code": (
            batch.get("final_code")
            or result["response"].get("error", {}).get("code")
        ),
        "state": batch.get("state"),
        "injection": result["request"]["injection"],
        "faults": [
            {"code": fault["code"], "classification": fault["classification"]}
            for fault in batch.get("faults", [])
        ],
        "retry_sequence": [
            {
                "attempt": attempt["attempt_no"],
                "code": attempt["code"],
                "backoff_ms": attempt["backoff_ms"],
                "elapsed_ms": attempt["elapsed_ms"],
            }
            for attempt in batch.get("retry_sequence", [])
        ],
        "aggregation": {
            "status": aggregation.get("status"),
            "degraded": aggregation.get("degraded"),
            "participant_nodes": aggregation.get("participant_nodes"),
            "omitted_nodes": aggregation.get("omitted_nodes"),
            "error_bounds": aggregation.get("error_bounds"),
        },
        "transitions": [
            f"{transition['from_state']}->{transition['to_state']}"
            for transition in batch.get("transitions", [])
        ],
        "recovery_diff": recovery.get("diff"),
    }


def validate_error_envelopes(results: list[dict[str, Any]]) -> None:
    for result in results:
        response = result["response"]
        if response.get("ok", True):
            continue
        if set(response) != {"ok", "error"} or response["ok"] is not False:
            raise AssertionError(f"Non-uniform error envelope in {result['name']}")
        error = response["error"]
        required = {"code", "message", "retryable", "details"}
        if not required.issubset(error):
            raise AssertionError(f"Missing error fields in {result['name']}: {error}")


def run_demo(config_path: Path, keep_services: bool = False) -> dict[str, Any]:
    config = LabConfig.load(config_path)
    config.artifact_dir.mkdir(parents=True, exist_ok=True)
    config.database_path.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(config.database_path) + suffix)
        if path.exists():
            path.unlink()
    for path in config.artifact_dir.glob("*.log"):
        path.unlink()

    nodes = [
        ServiceProcess(
            f"node-{node.node_id}",
            "grad_fault_lab.node_app",
            ["--config", str(config_path), "--node-id", node.node_id],
            config.artifact_dir / f"{node.node_id}.log",
        )
        for node in config.nodes
    ]
    injector = ServiceProcess(
        "injector",
        "grad_fault_lab.injector_app",
        ["--config", str(config_path)],
        config.artifact_dir / "injector.log",
    )
    validator = ServiceProcess(
        "validator",
        "grad_fault_lab.validator_app",
        ["--config", str(config_path)],
        config.artifact_dir / "validator.log",
    )
    base_url = f"http://127.0.0.1:{config.validator_port}"
    all_services = [*nodes, injector, validator]
    results: list[dict[str, Any]] = []
    recovery_result: dict[str, Any]
    try:
        for node in nodes:
            node.start()
        for node_config in config.nodes:
            wait_http(f"http://127.0.0.1:{node_config.port}/health")
        injector.start()
        wait_http(f"http://127.0.0.1:{config.injector_port}/health")
        validator.start()
        wait_http(f"{base_url}/health")

        for scenario in build_scenarios():
            result = run_scenario(base_url, scenario)
            validate_scenario(result)
            results.append(result)
            print(f"[OK] {result['name']} -> HTTP {result['http_status']}")

        recovery_result = run_kill_recovery(config, base_url, validator)
        print("[OK] forced_kill_then_restart_recovery")
        validate_error_envelopes(results)
    finally:
        if not keep_services:
            for service in reversed(all_services):
                if service.process is not None:
                    service.stop()

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "config": config.public_dict(),
        "scenario_count": len(results),
        "scenarios": [compact_scenario(result) for result in results],
        "full_responses": results,
        "forced_kill_recovery": {
            "name": recovery_result["name"],
            "request": recovery_result["request"],
            "intent_before_kill": recovery_result["intent_before_kill"],
            "interrupted_client_error": recovery_result["interrupted_client_error"],
            "recovered_state": recovery_result["recovered_batch"]["state"],
            "recovered_final_code": recovery_result["recovered_batch"]["final_code"],
            "recovery": recovery_result["recovered_batch"]["recovery"],
            "reports": recovery_result["recovery_reports"],
            "expect": recovery_result["expect"],
        },
        "business_code_coverage": {
            "successful_retry": ErrorCode.OK.value,
            "connection_refused": ErrorCode.FAULT_CONNECTION_REFUSED.value,
            "response_timeout": ErrorCode.FAULT_RESPONSE_TIMEOUT.value,
            "content_truncated": ErrorCode.FAULT_CONTENT_TRUNCATED.value,
            "duplicate_submission": ErrorCode.FAULT_DUPLICATE_SUBMISSION.value,
            "order_mismatch": ErrorCode.FAULT_ORDER_MISMATCH.value,
            "degraded_aggregation": ErrorCode.DEGRADED_AGGREGATION.value,
            "recovered_after_restart": ErrorCode.RECOVERED_AFTER_RESTART.value,
        },
    }
    output_path = config.artifact_dir / "demo_output.json"
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("\n=== Injection parameters, retries, bounds and recovery ===")
    print(json.dumps(report["scenarios"], indent=2))
    print("\n=== Forced-kill recovery diff ===")
    print(json.dumps(report["forced_kill_recovery"], indent=2))
    print(f"\nFull demo report: {output_path}")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the end-to-end local verification demo")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--keep-services", action="store_true")
    args = parser.parse_args()
    run_demo(Path(args.config), keep_services=args.keep_services)


if __name__ == "__main__":
    main()

