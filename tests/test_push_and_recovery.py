from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from config_coordinator import Coordinator, DomainError

from conftest import ROOT, component, snapshot


def test_push_timeout_is_real_and_closes_socket(
    coordinator: Coordinator, endpoint_base: str
) -> None:
    httpx.post(
        endpoint_base + "/control/modes",
        json={
            "modes": [
                {
                    "cluster_id": "east",
                    "component_id": "slow",
                    "mode": "timeout",
                    "delay_seconds": 0.8,
                }
            ]
        },
        timeout=5,
    ).raise_for_status()
    coordinator.submit_snapshot(
        snapshot(1, {"a": 1}),
        [component("slow", ["a"], endpoint_url=endpoint_base + "/clusters/east/push")],
    )
    started = time.perf_counter()
    result = asyncio.run(coordinator.dispatch(1, timeout_seconds=0.15))
    elapsed = time.perf_counter() - started
    assert elapsed < 0.7
    component_result = result["components"][0]
    assert component_result["code"] == "E_PUSH_TIMEOUT"
    assert component_result["status"] == "FAILED"
    assert component_result["elapsed_ms"] < 700

    time.sleep(0.9)
    stats = httpx.get(endpoint_base + "/control/stats", timeout=5).json()["stats"]
    assert stats["timeout_requests"] >= 1
    assert stats["client_disconnects"] >= 1


def test_connection_failure_is_distinguished(coordinator: Coordinator) -> None:
    coordinator.submit_snapshot(
        snapshot(1, {"a": 1}),
        [component("offline", ["a"], endpoint_url="http://127.0.0.1:1/push")],
    )
    result = asyncio.run(coordinator.dispatch(1, timeout_seconds=0.2))
    assert result["components"][0]["code"] == "E_PUSH_CONNECTION"
    assert result["components"][0]["status"] == "FAILED"


def test_rejection_with_rollback_policy_rolls_back_applied_components(
    coordinator: Coordinator, endpoint_base: str
) -> None:
    httpx.post(
        endpoint_base + "/control/modes",
        json={
            "modes": [
                {
                    "cluster_id": "east",
                    "component_id": "b-bad",
                    "mode": "reject",
                }
            ]
        },
        timeout=5,
    ).raise_for_status()
    components = [
        component(
            "a-good",
            ["a"],
            endpoint_url=endpoint_base + "/clusters/east/push",
            cluster_id="east",
        ),
        component(
            "b-bad",
            ["a"],
            endpoint_url=endpoint_base + "/clusters/east/push",
            cluster_id="east",
        ),
    ]
    coordinator.submit_snapshot(snapshot(1, {"a": 1}), components)
    result = asyncio.run(coordinator.dispatch(1, policy="ROLLBACK", timeout_seconds=0.5))
    assert result["conclusion"] == "ROLLED_BACK"
    assert result["failure_count"] == 1
    failures = {item["component_id"]: item for item in result["components"] if not item["ok"]}
    assert failures["b-bad"]["code"] == "E_PUSH_REJECTED"
    rolled_back = {
        item["component_id"]: item for item in result["rollback_results"] if item["ok"]
    }
    assert set(rolled_back) == {"a-good"}
    final = {item["component_id"]: item["status"] for item in result["final_states"]}
    assert final == {"a-good": "ROLLED_BACK", "b-bad": "FAILED"}


def test_idempotent_dispatch_reuses_result_without_second_push(
    coordinator: Coordinator, endpoint_base: str
) -> None:
    components = [
        component("a", ["a"], endpoint_url=endpoint_base + "/clusters/east/push"),
        component("b", ["a"], endpoint_url=endpoint_base + "/clusters/east/push"),
    ]
    coordinator.submit_snapshot(snapshot(1, {"a": 1}), components)
    first = asyncio.run(coordinator.dispatch(1, timeout_seconds=0.5))
    stats_after_first = httpx.get(endpoint_base + "/control/stats", timeout=5).json()["stats"]
    second = asyncio.run(coordinator.dispatch(1, timeout_seconds=0.5))
    stats_after_second = httpx.get(endpoint_base + "/control/stats", timeout=5).json()["stats"]
    assert first["reused"] is False
    assert second["reused"] is True
    assert second["run_id"] == first["run_id"]
    assert stats_after_second["push_requests"] == stats_after_first["push_requests"]
    assert stats_after_second["applied"] == stats_after_first["applied"]


def test_concurrent_same_snapshot_dispatch_executes_once(
    coordinator: Coordinator, endpoint_base: str
) -> None:
    coordinator.submit_snapshot(
        snapshot(1, {"a": 1}),
        [component("a", ["a"], endpoint_url=endpoint_base + "/clusters/east/push")],
    )

    async def race() -> list[dict]:
        return await asyncio.gather(
            coordinator.dispatch(1, timeout_seconds=0.5),
            coordinator.dispatch(1, timeout_seconds=0.5),
        )

    results = asyncio.run(race())
    assert sorted(item["reused"] for item in results) == [False, True]
    assert results[0]["run_id"] == results[1]["run_id"]
    stats = httpx.get(endpoint_base + "/control/stats", timeout=5).json()["stats"]
    assert stats["push_requests"] == 1
    assert stats["applied"] == 1


def test_conflicting_idempotency_response_is_surfaced(
    coordinator: Coordinator, endpoint_base: str
) -> None:
    httpx.post(
        endpoint_base + "/control/modes",
        json={
            "modes": [
                {"cluster_id": "east", "component_id": "conflict", "mode": "conflict"}
            ]
        },
        timeout=5,
    ).raise_for_status()
    coordinator.submit_snapshot(
        snapshot(1, {"a": 1}),
        [component("conflict", ["a"], endpoint_url=endpoint_base + "/clusters/east/push")],
    )
    result = asyncio.run(coordinator.dispatch(1, timeout_seconds=0.5))
    assert result["components"][0]["code"] == "E_CONFLICTING_PUSH"
    assert result["components"][0]["status"] == "FAILED"


def test_forced_termination_during_cascade_rolls_back_to_last_complete_version(
    coordinator: Coordinator, tmp_path: Path
) -> None:
    coordinator.submit_snapshot(
        snapshot(1, {"a": 1}),
        [component("svc", ["a"], logical_component="svc")],
    )
    db_path = tmp_path / "coordinator.db"
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "config_coordinator.crash_worker",
            "--db",
            str(db_path),
            "--mode",
            "rebuild",
            "--base-version",
            "1",
            "--config-set",
            json.dumps({"a": 2}),
            "--generated-at",
            "2026-02-01T00:00:00Z",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert process.returncode == 91, process.stderr
    assert coordinator.current_version() == 1
    with pytest.raises(DomainError):
        coordinator.get_snapshot(2)
    report = coordinator.recover()
    assert report["last_complete_version"] == 1
    assert report["current_version_after"] == 1
    assert len(report["aborted_intents"]) == 1
    assert report["aborted_intents"][0]["kind"] == "SUBMIT_INCREMENT"

    from config_coordinator import IncrementRequest

    retried = coordinator.submit_increment(
        IncrementRequest(base_version=1, config_set={"a": 2})
    )
    assert retried["version"] == 2
    assert coordinator.current_version() == 2


def test_forced_termination_during_push_aborts_run_and_rolls_back_dispatched(
    coordinator: Coordinator, tmp_path: Path
) -> None:
    coordinator.submit_snapshot(
        snapshot(1, {"a": 1}),
        [component("svc", ["a"], logical_component="svc")],
    )
    db_path = tmp_path / "coordinator.db"
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "config_coordinator.crash_worker",
            "--db",
            str(db_path),
            "--mode",
            "push",
            "--base-version",
            "1",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert process.returncode == 92, process.stderr
    assert coordinator.get_component_state("svc", 1)["state"]["status"] == "DISPATCHED"
    report = coordinator.recover()
    assert len(report["aborted_dispatches"]) == 1
    assert report["aborted_dispatches"][0]["status"] == "ABORTED"
    assert report["recovered_state_transitions"] == [
        {
            "component_id": "svc",
            "snapshot_version": 1,
            "from_status": "DISPATCHED",
            "to_status": "ROLLED_BACK",
            "reason": "RECOVERED_UNCONFIRMED_PUSH",
        }
    ]
    assert coordinator.get_component_state("svc", 1)["state"]["status"] == "ROLLED_BACK"
    dispatch = coordinator.get_dispatch(1)
    assert dispatch["status"] == "ABORTED"
    assert dispatch["result"]["conclusion"] == "ABORTED_PROCESS_TERMINATION"

