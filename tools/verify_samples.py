from __future__ import annotations

import http.client
import importlib.metadata
import json
import platform
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import ROOT, Settings
from app.engine import RuntimeEngine
from app.errors import E_RECOVERED_INTERRUPTED


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_health(port: int, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.5)
        try:
            connection.request("GET", "/health")
            response = connection.getresponse()
            response.read()
            if response.status == 200:
                return
        except OSError:
            pass
        finally:
            connection.close()
        time.sleep(0.05)
    raise RuntimeError("local test endpoint failed to start")


def _start_endpoint() -> tuple[subprocess.Popen[bytes], str]:
    port = _free_port()
    refuse_port = _free_port()
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tools.test_endpoint",
            "--port",
            str(port),
            "--refuse-port",
            str(refuse_port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    _wait_health(port)
    return process, f"http://127.0.0.1:{port}"


def _stop_endpoint(process: subprocess.Popen[bytes]) -> None:
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _load_samples() -> tuple[dict[str, Any], dict[str, Any]]:
    specs = json.loads((ROOT / "samples" / "functions.json").read_text(encoding="utf-8"))
    trace = json.loads((ROOT / "samples" / "trajectory.json").read_text(encoding="utf-8"))
    return specs, trace


def _trace_replay(
    directory: Path, endpoint: str, specs: dict[str, Any], trace: dict[str, Any]
) -> dict[str, Any]:
    engine = RuntimeEngine(
        Settings(
            db_path=directory / "trace.sqlite3",
            log_path=directory / "trace.log",
            endpoint_base=endpoint,
            start_timeout_s=1.0,
            exec_timeout_s=2.0,
            max_queue_wait_s=10.0,
        )
    )
    spec_result = engine.load_specs(specs, source="samples/functions.json")
    trace_result = engine.load_trace(trace, source="samples/trajectory.json")
    prewarm = engine.prewarm("fn-orders", 1)
    normalized_calls = []
    for call in trace["calls"]:
        result = engine.invoke(
            {
                "call_id": call["call_id"],
                "function_id": call["function_id"],
                "arrival_ms": call["arrival_ms"],
                "execution_us": call["execution_us"],
                "priority": call["priority"],
                "retry": call["retry"],
            }
        )
        normalized_calls.append(
            {
                "call_id": call["call_id"],
                "function_id": call["function_id"],
                "business_code": result["business_code"],
                "status": result["status"],
                "cold_start": result["cold_start"],
                "warm_start": result["warm_start"],
                "quota_bucket": result["quota_bucket"],
            }
        )
    runtime = {
        row["function_id"]: {
            "cold_starts": row["cold_starts"],
            "warm_starts": row["warm_starts"],
            "start_timeouts": row["start_timeouts"],
            "exec_timeouts": row["exec_timeouts"],
        }
        for row in engine.list_functions()
    }
    return {
        "spec_batch": spec_result["batch_id"],
        "trace_batch": trace_result["batch_id"],
        "prewarm_ok": bool(prewarm["outcomes"]) and all(item["ok"] for item in prewarm["outcomes"]),
        "calls": normalized_calls,
        "runtime": runtime,
        "audit": engine.audit_invariants(),
        "quota": engine.quota(),
    }


def _injection_suite(directory: Path, endpoint: str, specs: dict[str, Any]) -> dict[str, Any]:
    engine = RuntimeEngine(
        Settings(
            db_path=directory / "injections.sqlite3",
            log_path=directory / "injections.log",
            endpoint_base=endpoint,
            start_timeout_s=0.08,
            exec_timeout_s=0.08,
            max_queue_wait_s=10.0,
        )
    )
    engine.load_specs(specs, source="samples/functions.json")
    scenarios = [
        {
            "kind": "INSTANCE_CREATE_TIMEOUT",
            "function_id": "fn-image",
            "call_id": "verify-create-timeout",
            "params": {},
        },
        {
            "kind": "EXECUTION_INTERRUPT",
            "function_id": "fn-audit",
            "call_id": "verify-exec-interrupt",
            "params": {"execution_us": 500_000},
        },
        {
            "kind": "DUPLICATE_CALL_ID",
            "function_id": "fn-audit",
            "call_id": "verify-duplicate",
            "params": {"execution_us": 10_000},
        },
        {
            "kind": "QUOTA_BURST",
            "function_id": "fn-report",
            "call_id": "verify-quota-burst",
            "params": {"execution_us": 150_000},
        },
        {
            "kind": "TRACE_OUT_OF_ORDER",
            "function_id": "fn-audit",
            "call_id": "verify-trace-order",
            "params": {"arrival_ms": 500},
        },
    ]
    results = []
    for scenario in scenarios:
        result = engine.run_injection(scenario)
        recheck = engine.recheck_injection(result["injection_id"])
        results.append(
            {
                "kind": result["kind"],
                "result_code": result["result_code"],
                "recheck_consistent": recheck["consistent"],
                "checks": recheck["checks"],
            }
        )
    return {
        "results": results,
        "distinct_codes": sorted({item["result_code"] for item in results}),
        "audit": engine.audit_invariants(),
    }


def _read_call(db_path: Path, call_id: str) -> dict[str, Any] | None:
    if not db_path.exists():
        return None
    connection = sqlite3.connect(db_path, timeout=1)
    connection.row_factory = sqlite3.Row
    try:
        try:
            row = connection.execute(
                "SELECT * FROM calls WHERE call_id = ?", (call_id,)
            ).fetchone()
        except sqlite3.OperationalError:
            return None
        return dict(row) if row else None
    finally:
        connection.close()


def _recovery_check(directory: Path, endpoint: str, scenario: str) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=True)
    db_path = directory / f"crash-{scenario}.sqlite3"
    spec_path = directory / f"crash-{scenario}.json"
    function_id = f"verify-crash-{scenario}"
    spec_path.write_text(
        json.dumps(
            {
                "functions": [
                    {
                        "function_id": function_id,
                        "runtime": "python3.11",
                        "init_us": 10_000,
                        "memory_bytes": 16_777_216,
                        "per_instance_concurrency": 1,
                        "function_concurrency_limit": 2,
                        "reserved_quota": 1,
                        "init_concurrency": 1,
                        "prewarm_instances": 0,
                        "queue_capacity": 2,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    call_id = f"verify-killed-{scenario}"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tools.crash_worker",
            "--db",
            str(db_path),
            "--endpoint",
            endpoint,
            "--specs",
            str(spec_path),
            "--scenario",
            scenario,
            "--call-id",
            call_id,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        target = "EXECUTING" if scenario == "execute" else "CREATING"
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            status = (_read_call(db_path, call_id) or {}).get("status")
            if status == target:
                break
            time.sleep(0.01)
        else:
            raise RuntimeError(f"worker did not reach {target}")
        process.kill()
        process.wait(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
    settings = Settings(
        db_path=db_path,
        log_path=directory / "recovery.log",
        endpoint_base=endpoint,
        start_timeout_s=0.2,
        exec_timeout_s=0.2,
        max_queue_wait_s=2.0,
    )
    recovered = RuntimeEngine(settings)
    report = recovered.latest_recovery_report()
    call = recovered.get_call(call_id)
    released = next(
        row for row in report["quota_released"] if row["function_id"] == function_id
    )
    return {
        "scenario": scenario,
        "pending_intents_found": report["pending_intents_found"],
        "recovered_call": next(
            item for item in report["recovered_calls"] if item["call_id"] == call_id
        ),
        "terminated_instances": report["terminated_instances"],
        "quota_released": released,
        "final_call_code": call["business_code"],
        "final_instance_states": [
            item["state"] for item in recovered.list_instances(function_id)
        ],
        "audit": recovered.audit_invariants(),
        "exit_report_available": report["exit_report_available"],
        "persisted_facts_only": report["persisted_facts_only"],
    }


def _versions() -> dict[str, str]:
    packages = {}
    for name in ("fastapi", "uvicorn", "numpy", "pytest", "httpx"):
        try:
            packages[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            packages[name] = "missing"
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "packages": packages,
    }


def main() -> int:
    specs, trace = _load_samples()
    endpoint_process, endpoint = _start_endpoint()
    try:
        with tempfile.TemporaryDirectory(prefix="serverless-coldstart-") as temp:
            directory = Path(temp)
            replay_one = _trace_replay(directory / "one", endpoint, specs, trace)
            replay_two = _trace_replay(directory / "two", endpoint, specs, trace)
            normalized_one = {
                "prewarm_ok": replay_one["prewarm_ok"],
                "calls": replay_one["calls"],
                "runtime": replay_one["runtime"],
                "quota": replay_one["quota"],
            }
            normalized_two = {
                "prewarm_ok": replay_two["prewarm_ok"],
                "calls": replay_two["calls"],
                "runtime": replay_two["runtime"],
                "quota": replay_two["quota"],
            }
            injections = _injection_suite(directory / "injections", endpoint, specs)
            recovery = [
                _recovery_check(directory / "recovery", endpoint, "execute"),
                _recovery_check(directory / "recovery", endpoint, "create"),
            ]
            expected_codes = {
                "E_START_TIMEOUT",
                "E_EXEC_TIMEOUT",
                "E_CONFLICTING_CALL",
                "E_QUOTA_DENIED",
                "E_TRACE_OUT_OF_ORDER",
            }
            assertions = {
                "trace_replays_identical": normalized_one == normalized_two,
                "all_trace_calls_succeeded": all(
                    call["status"] == "SUCCEEDED" and call["business_code"] == "OK"
                    for call in replay_one["calls"]
                ),
                "cold_and_warm_observed": any(
                    call["cold_start"] for call in replay_one["calls"]
                )
                and any(call["warm_start"] for call in replay_one["calls"]),
                "all_injection_rechecks_consistent": all(
                    item["recheck_consistent"] for item in injections["results"]
                ),
                "five_distinct_injection_codes": set(injections["distinct_codes"])
                == expected_codes,
                "recovery_reports_consistent": all(
                    item["audit"]["ok"]
                    and item["exit_report_available"] is False
                    and item["persisted_facts_only"] is True
                    and item["final_call_code"] == E_RECOVERED_INTERRUPTED
                    and item["quota_released"]["on_demand_used_after"] == 0
                    and item["quota_released"]["reserved_used_after"] == 0
                    for item in recovery
                ),
            }
            result = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "environment": _versions(),
                "sample_seed": specs.get("seed"),
                "function_count": len(specs["functions"]),
                "trace_call_count": len(trace["calls"]),
                "deterministic_replay": {
                    "run_1": replay_one,
                    "run_2": normalized_two,
                    "normalized_identical": normalized_one == normalized_two,
                },
                "injections": injections,
                "recovery": recovery,
                "assertions": assertions,
                "passed": all(assertions.values()),
            }
            output = ROOT / "artifacts" / "verification.json"
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(
                json.dumps(result, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            print(json.dumps({"passed": result["passed"], "assertions": assertions}, indent=2))
            if not result["passed"]:
                return 1
            return 0
    finally:
        _stop_endpoint(endpoint_process)


if __name__ == "__main__":
    raise SystemExit(main())

