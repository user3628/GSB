from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.digest import canonical_json  # noqa: E402


def reserve_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def wait_for_server(client: httpx.Client, base_url: str, process: subprocess.Popen[Any], log_path: Path) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            log = log_path.read_text(encoding="utf-8", errors="replace") if log_path.exists() else ""
            raise RuntimeError(f"API server exited early with code {process.returncode}:\n{log}")
        try:
            response = client.get(base_url + "/api/health")
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError("API server did not become ready")


def api_call(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    expected: int,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = client.request(method, path, json=payload, timeout=30)
    if response.status_code != expected:
        raise RuntimeError(
            f"{method} {path} expected {expected}, got {response.status_code}: {response.text}"
        )
    return response.json()


def decision_hash(decisions: list[dict[str, Any]]) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(decisions).encode("utf-8")).hexdigest()


def order_summary(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "rank": item["rank"],
            "priority": item["priority"],
            "priority_score": item["priority_score"],
            "route_id": item["route_id"],
            "difference_type": item["difference_type"],
            "affected_requests": item["affected_requests"],
            "trigger_condition": item["trigger_condition"],
        }
        for item in orders
    ]


def installed_versions() -> dict[str, str]:
    from importlib.metadata import version

    return {
        "python": platform.python_version(),
        "pip": version("pip"),
        "fastapi": version("fastapi"),
        "uvicorn": version("uvicorn"),
        "httpx": version("httpx"),
        "pytest": version("pytest"),
        "sqlite": sqlite3.sqlite_version,
    }


def run_crash_recovery(
    client: httpx.Client,
    *,
    db_path: Path,
    snapshot_version: int,
    trace: dict[str, Any],
    artifacts_dir: Path,
) -> dict[str, Any]:
    prepared = api_call(
        client,
        "POST",
        "/api/simulations",
        expected=201,
        payload={
            "snapshot_version": snapshot_version,
            "trace": trace,
            "execution": "prepare",
        },
    )
    batch_id = prepared["batch"]["batch_id"]
    ready_file = artifacts_dir / f"crash-ready-{batch_id}.json"
    worker_log = artifacts_dir / f"crash-worker-{batch_id}.log"
    if ready_file.exists():
        ready_file.unlink()
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    with worker_log.open("w", encoding="utf-8") as log_handle:
        worker = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "app.crash_worker",
                "--db",
                str(db_path),
                "--batch-id",
                batch_id,
                "--stage-count",
                "5",
                "--ready-file",
                str(ready_file),
                "--hold-seconds",
                "120",
            ],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
    deadline = time.monotonic() + 20
    try:
        while time.monotonic() < deadline:
            if ready_file.exists():
                break
            if worker.poll() is not None:
                log = worker_log.read_text(encoding="utf-8", errors="replace")
                raise RuntimeError(f"crash worker exited before ready: {worker.returncode}\n{log}")
            time.sleep(0.1)
        else:
            raise RuntimeError("crash worker did not signal readiness")
        ready = load_json(ready_file)
        worker.kill()
        killed_return_code = worker.wait(timeout=10)
    finally:
        if worker.poll() is None:
            worker.kill()
            worker.wait(timeout=10)

    scan = api_call(client, "POST", "/api/recovery/scan", expected=200)
    matching = [report for report in scan["reports"] if report["batch_id"] == batch_id]
    if len(matching) != 1:
        raise RuntimeError(f"expected one recovery report for {batch_id}, got {scan}")
    report = matching[0]
    summary = api_call(client, "GET", f"/api/simulations/{batch_id}", expected=200)["batch"]
    hidden = api_call(
        client,
        "GET",
        f"/api/simulations/{batch_id}/decisions",
        expected=409,
    )
    report_by_batch = api_call(
        client,
        "GET",
        f"/api/recovery/reports/{batch_id}",
        expected=200,
    )
    if summary["status"] != "ABORTED":
        raise RuntimeError("recovered batch is not ABORTED")
    if hidden["error"]["code"] != "E_BATCH_INCOMPLETE":
        raise RuntimeError("partial conclusions were unexpectedly visible")
    if report["facts"]["persisted_staged_decisions"] != ready["staged_count"]:
        raise RuntimeError("recovery report did not preserve staged decision facts")
    return {
        "batch_id": batch_id,
        "prepared_status": prepared["batch"]["status"],
        "worker_pid": ready["pid"],
        "staged_before_kill": ready["staged_count"],
        "forced_kill_return_code": killed_return_code,
        "post_recovery_status": summary["status"],
        "recovery_report_id": report["report_id"],
        "recovery_report_matches_query": report_by_batch["report_id"] == report["report_id"],
        "persisted_final_decisions": report["facts"]["persisted_final_decisions"],
        "persisted_staged_decisions": report["facts"]["persisted_staged_decisions"],
        "complete_conclusion_available": report["facts"]["complete_conclusion_available"],
        "decision_query_error_code": hidden["error"]["code"],
        "recovery_scan": scan,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="End-to-end deterministic workbench demonstration")
    parser.add_argument("--port", type=int, default=0, help="0 selects an unused loopback port")
    parser.add_argument("--endpoint-port", type=int, default=0)
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "data" / "demo.db")
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "artifacts" / "demo-result.json",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifacts_dir = (PROJECT_ROOT / "artifacts").resolve()
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    db_path = args.db.resolve()
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    trace = load_json(PROJECT_ROOT / "samples" / "trace_baseline.json")
    policy_v1 = load_json(PROJECT_ROOT / "samples" / "policy_v1.json")
    policy_v2 = load_json(PROJECT_ROOT / "samples" / "policy_v2_conflict_ambiguous.json")
    api_port = args.port or reserve_loopback_port()
    endpoint_port = args.endpoint_port or reserve_loopback_port()
    while endpoint_port == api_port:
        endpoint_port = reserve_loopback_port()
    base_url = f"http://127.0.0.1:{api_port}"
    server_log = artifacts_dir / "demo-server.log"
    env = os.environ.copy()
    env["SIM_WORKBENCH_DB"] = str(db_path)
    env["PYTHONUNBUFFERED"] = "1"
    with server_log.open("w", encoding="utf-8") as log_handle:
        server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(api_port),
                "--log-level",
                "warning",
            ],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )

    result: dict[str, Any] = {
        "environment": installed_versions(),
        "database": str(db_path),
        "loopback_ports": {"api": api_port, "upstream_fixture": endpoint_port},
        "samples": {
            "trace_id": trace["trace_id"],
            "trace_request_count": len(trace["requests"]),
            "policy_v1_checksum": policy_v1["checksum"],
            "policy_v2_checksum": policy_v2["checksum"],
        },
    }
    try:
        with httpx.Client(base_url=base_url, timeout=10) as client:
            wait_for_server(client, base_url, server, server_log)
            health = api_call(client, "GET", "/api/health", expected=200)
            print(f"[health] python={health['python']} db={health['database']}")

            submitted_v1 = api_call(
                client,
                "POST",
                "/api/policies",
                expected=201,
                payload=policy_v1,
            )
            first = api_call(
                client,
                "POST",
                "/api/simulations",
                expected=201,
                payload={"snapshot_version": 1, "trace": trace},
            )
            second = api_call(
                client,
                "POST",
                "/api/simulations",
                expected=201,
                payload={"snapshot_version": 1, "trace": trace},
            )
            first_decisions = api_call(
                client,
                "GET",
                f"/api/simulations/{first['batch']['batch_id']}/decisions",
                expected=200,
            )["decisions"]
            second_decisions = api_call(
                client,
                "GET",
                f"/api/simulations/{second['batch']['batch_id']}/decisions",
                expected=200,
            )["decisions"]
            if first_decisions != second_decisions:
                raise RuntimeError("same snapshot/trace simulation is not deterministic")
            first_orders = api_call(
                client,
                "GET",
                f"/api/simulations/{first['batch']['batch_id']}/orders",
                expected=200,
            )["orders"]
            print(
                "[v1] decisions="
                f"{first['batch']['stats']['total_requests']} orders={len(first_orders)} "
                f"hash={decision_hash(first_decisions)}"
            )

            submitted_v2 = api_call(
                client,
                "POST",
                "/api/policies",
                expected=201,
                payload=policy_v2,
            )
            conflict_batch = api_call(
                client,
                "POST",
                "/api/simulations",
                expected=201,
                payload={"snapshot_version": 2, "trace": trace},
            )
            conflict_orders = api_call(
                client,
                "GET",
                f"/api/simulations/{conflict_batch['batch']['batch_id']}/orders",
                expected=200,
            )["orders"]
            comparison = api_call(
                client,
                "GET",
                f"/api/simulations/compare?left={first['batch']['batch_id']}&right={conflict_batch['batch']['batch_id']}",
                expected=200,
            )
            print(
                "[v2] conflicts="
                f"{conflict_batch['batch']['stats']['conflict_count']} "
                f"changed_decisions={comparison['decision_change_count']} "
                f"orders={len(conflict_orders)}"
            )

            ambiguity_payload = {
                "snapshot_version": 2,
                "path": "/api/checkout/order",
                "headers": {
                    "x-tenant": "premium",
                    "x-retry": "1",
                    "x-campaign": "launch",
                },
            }
            strict_error = api_call(
                client,
                "POST",
                "/api/routes/resolve",
                expected=409,
                payload={**ambiguity_payload, "strict": True},
            )
            resolved = api_call(
                client,
                "POST",
                "/api/routes/resolve",
                expected=200,
                payload={**ambiguity_payload, "strict": False},
            )
            if strict_error["error"]["code"] != "E_ROUTE_AMBIGUOUS":
                raise RuntimeError("strict ambiguity did not return E_ROUTE_AMBIGUOUS")
            if not resolved["tied_at_top_precedence"]:
                raise RuntimeError("expected a resolved top-precedence ambiguity")
            print(
                f"[ambiguity] strict={strict_error['error']['code']} "
                f"resolved_route={resolved['route_id']} winner={resolved['winning_rule_id']}"
            )

            recovery = run_crash_recovery(
                client,
                db_path=db_path,
                snapshot_version=2,
                trace=trace,
                artifacts_dir=artifacts_dir,
            )
            print(
                f"[recovery] staged={recovery['persisted_staged_decisions']} "
                f"status={recovery['post_recovery_status']} "
                f"query={recovery['decision_query_error_code']}"
            )

        upstream = subprocess.run(
            [sys.executable, "-m", "tools.upstream_demo", "--port", str(endpoint_port)],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        upstream_result = json.loads(upstream.stdout)
        print(
            f"[loopback] timeout={upstream_result['timeout']['handled']} "
            f"connection_failure={upstream_result['connection_failure']['handled']}"
        )

        result.update(
            {
                "submitted_policies": [submitted_v1, submitted_v2],
                "v1_batches": [first["batch"], second["batch"]],
                "determinism": {
                    "same_version_repeat_equal": first_decisions == second_decisions,
                    "decision_count": len(first_decisions),
                    "decision_hash": decision_hash(first_decisions),
                },
                "v1_stats": first["batch"]["stats"],
                "v1_orders": order_summary(first_orders),
                "v2_batch": conflict_batch["batch"],
                "v2_stats": conflict_batch["batch"]["stats"],
                "v2_conflicts": conflict_batch["batch"]["conflicts"],
                "v2_orders": order_summary(conflict_orders),
                "version_comparison": comparison,
                "route_ambiguity": {
                    "strict_error": strict_error,
                    "resolved": resolved,
                },
                "recovery": recovery,
                "loopback_upstream": upstream_result,
            }
        )
        result["assertions"] = {
            "same_version_decisions_identical": first_decisions == second_decisions,
            "v2_has_timeout_breaker_conflicts": conflict_batch["batch"]["stats"]["conflict_count"] == 3,
            "ambiguous_route_is_explainable": resolved["tied_at_top_precedence"],
            "forced_kill_batch_aborted": recovery["post_recovery_status"] == "ABORTED",
            "partial_decisions_hidden": recovery["decision_query_error_code"] == "E_BATCH_INCOMPLETE",
            "loopback_timeout_handled": upstream_result["timeout"]["handled"],
            "loopback_connection_failure_handled": upstream_result["connection_failure"]["handled"],
        }
        if not all(result["assertions"].values()):
            raise RuntimeError(f"demo assertions failed: {result['assertions']}")
        output_path = args.output.resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[result] {output_path}")
    finally:
        server.terminate()
        try:
            server.wait(timeout=7)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=7)


if __name__ == "__main__":
    main()
