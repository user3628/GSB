from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from pdb_engine.service import EngineService


def test_drain_partial_release_and_sequence_comparison(service) -> None:
    result = service.drain_node("worker-a", "drain-partial")

    assert result["status"] == "COMPLETED"
    summary = result["release_summary"]
    assert summary["naive_allowed"] == 1
    assert summary["optimized_preview_allowed"] == 2
    assert summary["actual_allowed"] == 2
    assert summary["optimized_not_less_than_naive"] is True
    assert result["optimization"]["improved_over_naive"] is True
    assert result["optimization"]["constraint_bypass_used"] is False

    assert [item["pod"] for item in result["allowed"]] == [
        "web-a-alpha",
        "web-a-beta",
    ]
    assert [item["target_node"] for item in result["allowed"]] == [
        "worker-b",
        "worker-c",
    ]
    blocked = {item["pod"]: item for item in result["blocked"]}
    assert blocked["web-a-shared"]["code"] == "E_PDB_BLOCKED"
    assert {reason["pdb"] for reason in blocked["web-a-shared"]["reasons"]} == {
        "web-a-budget",
        "web-critical-budget",
    }
    assert all(step["decision"] == "ALLOWED" for step in result["actual_sequence"][:2])
    assert result["actual_sequence"][2]["decision"] == "BLOCKED"

    pdb_state = service.repo.get_pdb_state()
    assert pdb_state["web-a-budget"]["current_available"] == 2
    assert pdb_state["web-critical-budget"]["current_available"] == 2
    assert pdb_state["ledger-full-availability"]["current_available"] == 2


def test_idempotent_replay_does_not_reapply_pdb_decrements(service) -> None:
    first = service.drain_node("worker-a", "same-request")
    second = service.drain_node("worker-a", "same-request")

    assert first["drain_id"] == second["drain_id"]
    assert second["replayed"] is True
    assert second["release_summary"] == first["release_summary"]
    assert len(service.list_drains()) == 1
    assert len(second["steps"]) == 3
    pdb_state = service.repo.get_pdb_state()
    assert pdb_state["web-a-budget"]["current_available"] == 2
    assert pdb_state["web-critical-budget"]["current_available"] == 2


def test_concurrent_duplicate_waiters_reuse_one_execution(tmp_path) -> None:
    engine = EngineService(tmp_path / "concurrent.sqlite3")
    engine.initialize()
    engine.load_sample("samples/scenario.json")

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            executor.map(
                lambda _: engine.drain_node("worker-a", "concurrent-request"),
                range(8),
            )
        )

    assert all(result["drain_id"] == results[0]["drain_id"] for result in results)
    assert sum(1 for result in results if not result["replayed"]) == 1
    assert sum(1 for result in results if result["replayed"]) == 7
    assert len(engine.list_drains()) == 1
    assert len(engine.repo.list_steps(results[0]["drain_id"])) == 3


def test_duplicate_arriving_after_running_status_waits_for_result(service) -> None:
    running = threading.Event()
    release = threading.Event()
    original_update = service.repo.update_drain_status

    def paused_update(drain_id: int, status: str) -> None:
        original_update(drain_id, status)
        if status == "RUNNING":
            running.set()
            release.wait(timeout=5)

    service.repo.update_drain_status = paused_update  # type: ignore[method-assign]
    with ThreadPoolExecutor(max_workers=2) as executor:
        leader = executor.submit(service.drain_node, "worker-a", "running-duplicate")
        assert running.wait(timeout=5)
        duplicate = executor.submit(service.drain_node, "worker-a", "running-duplicate")
        time.sleep(0.05)
        try:
            assert duplicate.done() is False
        finally:
            release.set()
        first = leader.result(timeout=5)
        second = duplicate.result(timeout=5)

    assert second["drain_id"] == first["drain_id"]
    assert second["status"] == "COMPLETED"
    assert second["replayed"] is True
    assert len(service.list_drains()) == 1


def test_visibility_contract_is_reported(service) -> None:
    result = service.drain_node("worker-a", "visibility-request")
    visibility = result["visibility"]
    assert "publish_rule" in visibility
    assert "batch_visibility" in visibility
    assert "idempotency" in visibility
    assert all(step["status"] == "COMMITTED" for step in result["steps"])


def test_different_nodes_commit_concurrently_without_breaking_shared_pdb(
    tmp_path, sample_data
) -> None:
    data = json.loads(json.dumps(sample_data))
    data["nodes"][1]["drain_requested"] = True  # worker-b
    # Make web-b-offsite placeable on worker-c so both drains can contend for
    # the same two web PDB budgets.
    data["workloads"][0]["pods"][3]["tolerations"] = [
        {"key": "dedicated", "operator": "Equal", "value": "ops", "effect": "NoSchedule"}
    ]
    sample_path = tmp_path / "overlap.json"
    sample_path.write_text(json.dumps(data), encoding="utf-8")

    engine = EngineService(tmp_path / "overlap.sqlite3")
    engine.initialize()
    engine.load_sample(str(sample_path))

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(engine.drain_node, node, f"overlap-{node}")
            for node in ("worker-a", "worker-b")
        ]
        results = [future.result() for future in futures]

    assert all(result["status"] == "COMPLETED" for result in results)
    pdb_state = engine.repo.get_pdb_state()
    assert pdb_state["web-a-budget"]["current_available"] >= 2
    assert pdb_state["web-critical-budget"]["current_available"] >= 2
    for drain in results:
        for step in drain["steps"]:
            if step["outcome"] == "ALLOWED":
                for check in step["affected"]:
                    assert check["after_available"] >= check["required_available"]

