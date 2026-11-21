"""Deterministic end-to-end demonstration runnable with ``python scripts/demo.py``."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config_coordinator import Coordinator, ComponentDeclaration, IncrementRequest, Snapshot  # noqa: E402


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_until_ready(url: str, process: subprocess.Popen[str], timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output = process.stdout.read() if process.stdout else ""
            raise RuntimeError(f"endpoint server exited early ({process.returncode}):\n{output}")
        try:
            response = httpx.get(url, timeout=0.3)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError("local endpoint server did not become ready")


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def components_for(endpoint_base: str) -> list[ComponentDeclaration]:
    raw = load_json(ROOT / "samples" / "components.json")["components"]
    text = json.dumps(raw).replace("{endpoint_base}", endpoint_base)
    return [ComponentDeclaration.model_validate(item) for item in json.loads(text)]


def print_impact(title: str, report: dict[str, Any]) -> None:
    scope = report["rebuild_scope"]
    artifact_types = Counter(
        key.split(":", 1)[0] for key in scope.get("invalidated_artifacts", [])
    )
    print(f"\n[{title}] version={report['version']} source={report['source']}")
    print("  changed_config_keys :", report["changed_config_keys"])
    print("  affected_components :", report["affected_components"])
    print("  batches             :", report["batches"])
    print(
        "  rebuild_scope       :",
        {
            "invalidated_artifact_count": scope["invalidated_artifact_count"],
            "invalidated_by_type": dict(sorted(artifact_types.items())),
            "rebuilt_artifact_count": scope["rebuilt_artifact_count"],
            "atomic_transaction": scope["atomic_transaction"],
        },
    )


def print_dispatch(title: str, result: dict[str, Any], *, show_rollback: bool = False) -> None:
    print(f"\n[{title}]")
    print(
        "  outcome:",
        {
            "snapshot_version": result["snapshot_version"],
            "status": result["status"],
            "conclusion": result["conclusion"],
            "reused": result.get("reused", False),
            "affected_components": result.get("affected_components", []),
        },
    )
    component_lines = []
    for item in result.get("components", []):
        component_lines.append(
            {
                "component": item["component_id"],
                "batch": item.get("batch_index"),
                "status": item.get("status"),
                "ok": item.get("ok"),
                "code": item.get("code"),
                "elapsed_ms": item.get("elapsed_ms"),
            }
        )
    print("  component_results:", component_lines)
    if show_rollback:
        print("  rollback_results:", result.get("rollback_results", []))
    trajectory = [
        {
            "component": event["component_id"],
            "version": event["version"],
            "from": event["from_status"],
            "to": event["to_status"],
            "reason": event["reason"],
        }
        for event in result.get("state_events", [])
    ]
    print("  state_trajectory:", trajectory)
    print("  consistency_scores:", result.get("consistency_scores", {}))


def set_modes(endpoint_base: str, modes: list[dict[str, Any]]) -> None:
    response = httpx.post(
        endpoint_base + "/control/modes", json={"modes": modes}, timeout=5.0
    )
    response.raise_for_status()


def main() -> None:
    artifacts = ROOT / "artifacts"
    artifacts.mkdir(exist_ok=True)
    db_path = artifacts / "demo.db"
    for suffix in ("", "-wal", "-shm"):
        try:
            Path(str(db_path) + suffix).unlink()
        except FileNotFoundError:
            pass

    port = free_port()
    endpoint_base = f"http://127.0.0.1:{port}"
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "config_coordinator.endpoint_server",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        wait_until_ready(endpoint_base + "/health", process)
        coordinator = Coordinator(str(db_path))
        components = components_for(endpoint_base)

        print("=" * 78)
        print("End-to-end configuration distribution coordinator demo")
        print(f"SQLite: {db_path}")
        print(f"Local endpoints: {endpoint_base}")
        print("=" * 78)

        snapshot_v1 = Snapshot.model_validate(load_json(ROOT / "samples" / "snapshot-v1.json"))
        full_report = coordinator.submit_snapshot(snapshot_v1, components)
        print_impact("FULL SNAPSHOT COMMIT", full_report)
        full_dispatch = asyncio.run(
            coordinator.dispatch(1, policy="CONTINUE", timeout_seconds=0.8)
        )
        print_dispatch("FULL DISPATCH", full_dispatch)

        incremental = IncrementRequest.model_validate(
            load_json(ROOT / "samples" / "increment-feature-checkout.json")
        )
        increment_report = coordinator.submit_increment(incremental)
        print_impact("SINGLE-KEY INCREMENT + CASCADE", increment_report)
        increment_dispatch = asyncio.run(
            coordinator.dispatch(2, policy="CONTINUE", timeout_seconds=0.8)
        )
        print_dispatch("INCREMENTAL DISPATCH", increment_dispatch)

        failure_increment = IncrementRequest.model_validate(
            load_json(ROOT / "samples" / "increment-push-failure.json")
        )
        failure_report = coordinator.submit_increment(failure_increment)
        print_impact("FAILURE SCENARIO COMMIT", failure_report)
        set_modes(
            endpoint_base,
            [
                {"cluster_id": "east", "component_id": "east-gateway", "mode": "reject"},
                {
                    "cluster_id": "west",
                    "component_id": "west-gateway",
                    "mode": "timeout",
                    "delay_seconds": 1.0,
                },
            ],
        )
        failure_dispatch = asyncio.run(
            coordinator.dispatch(3, policy="ROLLBACK", timeout_seconds=0.20)
        )
        print_dispatch("REJECT + TIMEOUT + ROLLBACK POLICY", failure_dispatch, show_rollback=True)

        # Restore all component behaviours before the recovery phase.
        set_modes(
            endpoint_base,
            [
                {"cluster_id": component.cluster_id, "component_id": component.component_id, "mode": "success"}
                for component in components
            ],
        )

        crash_command = [
            sys.executable,
            "-m",
            "config_coordinator.crash_worker",
            "--db",
            str(db_path),
            "--mode",
            "rebuild",
            "--base-version",
            str(coordinator.current_version()),
            "--config-set",
            json.dumps({"logging.level": "DEBUG"}),
            "--generated-at",
            "2026-11-22T09:20:00Z",
        ]
        crashed = subprocess.run(
            crash_command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        print("\n[FORCED TERMINATION]")
        print("  exit_code:", crashed.returncode)
        print("  worker_output:", crashed.stdout.strip())
        if crashed.stderr.strip():
            print("  worker_stderr:", crashed.stderr.strip())
        if crashed.returncode != 91:
            raise RuntimeError("expected forced-termination exit code 91")
        version_before_recovery = coordinator.current_version()
        recovery_report = coordinator.recover()
        print("\n[RECOVERY REPORT SUMMARY]")
        print(
            json.dumps(
                {
                    "last_complete_version": recovery_report["last_complete_version"],
                    "current_version_after": recovery_report["current_version_after"],
                    "version_before_recovery": version_before_recovery,
                    "pending_before": recovery_report["pending_before"],
                    "aborted_intents": recovery_report["aborted_intents"],
                    "aborted_dispatches": recovery_report["aborted_dispatches"],
                    "recovered_state_transitions": recovery_report["recovered_state_transitions"],
                    "facts_basis": recovery_report["facts_basis"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        recovered_increment = IncrementRequest.model_validate(
            load_json(ROOT / "samples" / "increment-crash.json")
        )
        retried_report = coordinator.submit_increment(recovered_increment)
        print_impact("POST-RECOVERY RETRY COMMIT", retried_report)

        final_summary = {
            "full_dispatch": {
                "version": full_dispatch["snapshot_version"],
                "affected_components": full_dispatch["affected_components"],
                "conclusion": full_dispatch["conclusion"],
            },
            "incremental_cascade": {
                "version": increment_report["version"],
                "changed_config_keys": increment_report["changed_config_keys"],
                "affected_components": increment_report["affected_components"],
                "batches": increment_report["batches"],
                "invalidated_artifact_count": increment_report["rebuild_scope"][
                    "invalidated_artifact_count"
                ],
            },
            "failure_disposition": {
                "version": failure_dispatch["snapshot_version"],
                "conclusion": failure_dispatch["conclusion"],
                "failures": [
                    {"component": item["component_id"], "code": item["code"]}
                    for item in failure_dispatch["components"]
                    if not item["ok"]
                ],
                "rolled_back": [
                    item["component_id"]
                    for item in failure_dispatch.get("rollback_results", [])
                    if item["ok"]
                ],
            },
            "recovery": {
                "crash_exit_code": crashed.returncode,
                "last_complete_version": recovery_report["last_complete_version"],
                "current_version_after": recovery_report["current_version_after"],
                "aborted_intent_count": len(recovery_report["aborted_intents"]),
                "post_recovery_commit_version": retried_report["version"],
            },
            "final_committed_version": coordinator.current_version(),
        }
        print("\n[FINAL SUMMARY]")
        print(json.dumps(final_summary, ensure_ascii=False, indent=2))
    finally:
        stop_process(process)


if __name__ == "__main__":
    main()

