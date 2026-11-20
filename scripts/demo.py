from __future__ import annotations

import argparse
import copy
import json
import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import httpx


ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
CONFIG = ROOT / "config"


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_http(url: str, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=0.5, trust_env=False)
            if response.status_code == 200:
                return
        except Exception as exc:  # startup race
            last_error = exc
        time.sleep(0.05)
    raise RuntimeError(f"service did not become ready: {url} ({last_error})")


def start_process(
    module: str, port: int, env: dict[str, str], log_path: Path
) -> subprocess.Popen:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = log_path.open("ab")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            module,
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    # Keep the file handle on the process object so it remains open while the
    # child writes, then close it after termination.
    process._demo_log = log  # type: ignore[attr-defined]
    return process


def stop_process(process: subprocess.Popen | None, *, force: bool = False) -> None:
    if process is None:
        return
    if process.poll() is None:
        if force:
            process.kill()
        else:
            process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    log = getattr(process, "_demo_log", None)
    if log is not None:
        log.close()


def load_plan(filename: str, target_url: str) -> dict[str, Any]:
    with (CONFIG / filename).open("r", encoding="utf-8") as handle:
        plan = json.load(handle)
    plan["target_url"] = target_url
    return plan


def compact_state(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "plan_status": report["plan"]["status"],
        "conclusion": report["conclusion"],
        "rounds": [
            {
                "round_id": row["id"],
                "stage_index": row["stage_index"],
                "attempt": row["attempt"],
                "round_key": row["round_key"],
                "origin": row["origin"],
                "concurrency": row["concurrency"],
                "status": row["status"],
                "aggregate": row["aggregate"],
                "judgment": row["judgment"],
            }
            for row in report["rounds"]
        ],
    }


def print_result(title: str, report: dict[str, Any]) -> None:
    print(f"\n=== {title} ===")
    print(
        "stage attempt conc samples throughput_rps   p50_ms   p95_ms   p99_ms"
        " error_rate verdict triggers"
    )
    for row in report["rounds"]:
        aggregate = row["aggregate"]
        judgment = row["judgment"]
        if aggregate is None or judgment is None:
            print(
                f"{row['stage_index']:>5} {row['attempt']:>7} {row['concurrency']:>4}"
                f" status={row['status']} round={row['round_key']}"
            )
            continue
        triggers = ",".join(
            item["threshold"] for item in judgment["triggered"]
        ) or "-"
        print(
            f"{row['stage_index']:>5} {row['attempt']:>7} {row['concurrency']:>4}"
            f" {aggregate['sample_size']:>7} {aggregate['throughput_rps']:>14.2f}"
            f" {aggregate['p50_ms']:>8.2f} {aggregate['p95_ms']:>8.2f}"
            f" {aggregate['p99_ms']:>8.2f} {aggregate['error_rate']:>10.4f}"
            f" {judgment['verdict']:>7} {triggers}"
        )
    print("conclusion:")
    print(json.dumps(report["conclusion"], ensure_ascii=False, indent=2))


@contextmanager
def clients(base_url: str) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=base_url, timeout=90.0, trust_env=False) as client:
        yield client


def create_and_run(client: httpx.Client, plan: dict[str, Any]) -> dict[str, Any]:
    response = client.post("/plans", json=plan)
    response.raise_for_status()
    plan_id = response.json()["plan"]["id"]
    response = client.post(f"/plans/{plan_id}/run")
    response.raise_for_status()
    return response.json()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db", default=str(ARTIFACTS / "demo-workbench.db"), help="SQLite path"
    )
    args = parser.parse_args()

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    db_path = Path(args.db)
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()

    target_port = free_tcp_port()
    workbench_port = free_tcp_port()
    target_url = f"http://127.0.0.1:{target_port}/sample"
    workbench_url = f"http://127.0.0.1:{workbench_port}"
    env = os.environ.copy()
    env["CAPACITY_DB"] = str(db_path)
    env["CAPACITY_WORKLOADS"] = str(CONFIG / "workloads.json")
    env["PYTHONUNBUFFERED"] = "1"

    target_process: subprocess.Popen | None = None
    workbench_process: subprocess.Popen | None = None
    output: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database": str(db_path),
        "target_url": target_url,
        "workbench_url": workbench_url,
    }
    try:
        target_process = start_process(
            "capacity_workbench.test_service:app",
            target_port,
            env,
            ARTIFACTS / "demo-target.log",
        )
        wait_http(f"http://127.0.0.1:{target_port}/health")

        workbench_process = start_process(
            "capacity_workbench.api:app",
            workbench_port,
            env,
            ARTIFACTS / "demo-workbench.log",
        )
        wait_http(f"{workbench_url}/health")

        print(f"target service:    {target_url}")
        print(f"workbench service: {workbench_url}")
        print(f"SQLite:            {db_path}")

        with clients(workbench_url) as client:
            complete = create_and_run(
                client, load_plan("sample_plan_complete.json", target_url)
            )
            print_result("1. Complete sequence progression", complete)
            output["complete_sequence"] = compact_state(complete)

            fallback = create_and_run(
                client, load_plan("sample_plan_fallback.json", target_url)
            )
            print_result("2. Mid-sequence veto and fallback", fallback)
            output["fallback_sequence"] = compact_state(fallback)

            recovery_plan = load_plan("sample_plan_recovery.json", target_url)
            response = client.post("/plans", json=recovery_plan)
            response.raise_for_status()
            recovery_plan_id = response.json()["plan"]["id"]
            holder: dict[str, Any] = {}

            def launch_long_run() -> None:
                try:
                    with httpx.Client(
                        base_url=workbench_url, timeout=120.0, trust_env=False
                    ) as run_client:
                        holder["response"] = run_client.post(
                            f"/plans/{recovery_plan_id}/run"
                        )
                except Exception as exc:  # expected when the process is killed
                    holder["error"] = exc.__class__.__name__

            runner = threading.Thread(target=launch_long_run, daemon=True)
            runner.start()
            before_snapshot: dict[str, Any] | None = None
            for _ in range(300):
                snapshot = client.get(
                    f"/plans/{recovery_plan_id}/snapshot"
                ).json()
                if any(row["status"] == "SAMPLING" for row in snapshot["rounds"]):
                    before_snapshot = snapshot
                    break
                time.sleep(0.02)
            if before_snapshot is None:
                raise RuntimeError("recovery round never reached SAMPLING")
            before_report = client.get(f"/plans/{recovery_plan_id}/report").json()
            before_state = compact_state(before_report)
            output["recovery_before"] = {
                "snapshot": before_snapshot,
                "state": before_state,
            }
            print("\n=== 3. Forced termination after WAL/intent commit ===")
            print("before kill:", json.dumps(before_snapshot, ensure_ascii=False))

            assert workbench_process is not None
            stop_process(workbench_process, force=True)
            workbench_process = None
            runner.join(timeout=5)
            print(
                "workbench killed; background request outcome:",
                holder.get("error", "connection closed"),
            )

            workbench_process = start_process(
                "capacity_workbench.api:app",
                workbench_port,
                env,
                ARTIFACTS / "demo-workbench.log",
            )
            wait_http(f"{workbench_url}/health")

        with clients(workbench_url) as client:
            after_snapshot = client.get(
                f"/plans/{recovery_plan_id}/snapshot"
            ).json()
            after_report = client.get(f"/plans/{recovery_plan_id}/report").json()
            reports = client.get("/recovery-reports").json()["reports"]
            matching_reports = [
                report
                for report in reports
                if recovery_plan_id in report.get("affected_plan_ids", [])
            ]
            recovery_report = matching_reports[-1] if matching_reports else None
            after_state = compact_state(after_report)
            output["recovery_after"] = {
                "snapshot": after_snapshot,
                "state": after_state,
                "report": recovery_report,
            }
            print("after restart:", json.dumps(after_snapshot, ensure_ascii=False))
            print("recovery report:")
            print(json.dumps(recovery_report, ensure_ascii=False, indent=2))
            print("state diff:")
            print(
                json.dumps(
                    {
                        "plan_status": {
                            "before": before_state["plan_status"],
                            "after": after_state["plan_status"],
                        },
                        "round_status": {
                            "before": [
                                row["status"] for row in before_state["rounds"]
                            ],
                            "after": [
                                row["status"] for row in after_state["rounds"]
                            ],
                        },
                        "conclusion": {
                            "before": before_state["conclusion"],
                            "after": after_state["conclusion"],
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            if not recovery_report:
                raise RuntimeError("startup recovery did not produce a report")
            if after_state["plan_status"] != "READY":
                raise RuntimeError("plan was not reset to READY after recovery")
            if any(row["status"] != "ABORTED" for row in after_state["rounds"]):
                raise RuntimeError("interrupted round was not marked ABORTED")

            output["assertions"] = {
                "complete_sequence_found_upper_bound": complete["conclusion"][
                    "outcome"
                ]
                == "SEQUENCE_EXHAUSTED",
                "fallback_found_knee": fallback["conclusion"]["outcome"]
                == "KNEE_FOUND",
                "fallback_capacity_concurrency": fallback["conclusion"]["capacity"][
                    "concurrency"
                ],
                "recovery_before_status": before_state["rounds"][0]["status"],
                "recovery_after_status": after_state["rounds"][0]["status"],
                "recovery_persisted_sample_count": recovery_report["rounds"][0][
                    "persisted_facts"
                ]["sample_count"],
            }
            print("\nassertions:", json.dumps(output["assertions"], ensure_ascii=False))

        output_path = ARTIFACTS / "demo-output.json"
        output_path.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\nfull JSON output: {output_path}")
        return 0
    finally:
        stop_process(workbench_process, force=True)
        stop_process(target_process, force=True)


if __name__ == "__main__":
    raise SystemExit(main())

