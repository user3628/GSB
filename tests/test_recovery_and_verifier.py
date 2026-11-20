import json
from pathlib import Path

import pytest

from config_dispatch.coordinator import Coordinator
from config_dispatch.push_client import PushClient
from config_dispatch.recovery import RecoveryService
from config_dispatch.repository import Repository
from config_dispatch.state_machine import APPLIED, DISPATCHED, PENDING, ROLLED_BACK
from config_dispatch.verifier import verify

SAMPLES = Path(__file__).resolve().parent.parent / "samples"


def _setup_snapshot(coordinator, declarations_raw, snapshot_payload):
    coordinator.register_declarations(declarations_raw)
    coordinator.submit_snapshot(snapshot_payload(1))
    coordinator.ensure_plan(1)
    return 1


def test_recovery_rolls_back_persisted_applied_facts(
    coordinator, declarations_raw, snapshot_payload, success_script, cluster_factory
):
    _setup_snapshot(coordinator, declarations_raw, snapshot_payload)
    cluster_factory(declarations_raw, success_script)
    repo = coordinator.repo
    version = 1
    repo.create_run_intent(version, "idem", "rollback")
    repo.update_run_status(version, "RUNNING")
    plan = repo.get_plan(version)
    first = plan["ordered_components"][0]
    second = plan["ordered_components"][1]
    for component_id in (first, second):
        repo.ensure_component_state(version, component_id, f"apply-v{version}-{component_id}")
    repo.transition_component_state(version, first, DISPATCHED, reason="push_start")
    repo.transition_component_state(version, first, APPLIED, reason="push_succeeded")
    repo.transition_component_state(version, second, DISPATCHED, reason="push_start")
    # 模拟此刻进程被强制终止：first=APPLIED、second=DISPATCHED 已落库

    report = RecoveryService(coordinator).recover()
    assert report["scanned_unfinished_intents"] == 1
    assert report["runs"][0]["final_status"] == "ROLLED_BACK"
    assert repo.get_run(version)["status"] == "ROLLED_BACK"
    assert repo.get_component_state(version, first)["state"] == ROLLED_BACK
    assert repo.get_component_state(version, second)["state"] == ROLLED_BACK
    result = verify(repo).to_dict()
    assert result["ok"] is True


def test_recovery_continue_policy_requeues_pending(
    coordinator, declarations_raw, snapshot_payload
):
    _setup_snapshot(coordinator, declarations_raw, snapshot_payload)
    repo = coordinator.repo
    version = 1
    repo.create_run_intent(version, "idem", "continue")
    repo.update_run_status(version, "RUNNING")
    plan = repo.get_plan(version)
    component_id = plan["ordered_components"][0]
    repo.ensure_component_state(version, component_id, "idem")
    repo.transition_component_state(version, component_id, DISPATCHED, reason="push_start")

    report = RecoveryService(coordinator).recover()
    assert report["runs"][0]["final_status"] == "REQUEUED"
    assert repo.get_component_state(version, component_id)["state"] == PENDING
    assert repo.get_run(version)["status"] == "ABORTED"


def test_verifier_detects_version_state_mismatch(
    coordinator, declarations_raw, snapshot_payload
):
    from config_dispatch.models import Snapshot

    _setup_snapshot(coordinator, declarations_raw, snapshot_payload)
    repo = coordinator.repo
    # 手工构造一条版本号无对应快照的状态记录，模拟版本不匹配
    repo._conn.execute(
        "PRAGMA foreign_keys=OFF"
    )
    repo._conn.execute(
        "INSERT INTO component_states(version, component_id, state, idempotency_key,"
        " error_code, error_message, updated_at) VALUES (999, 'ghost', 'APPLIED', 'k',"
        " NULL, NULL, '2026-11-20T00:00:00+00:00')"
    )
    repo._conn.commit()
    repo._conn.execute("PRAGMA foreign_keys=ON")
    result = verify(repo).to_dict()
    assert result["ok"] is False
    names = {check["name"] for check in result["checks"] if not check["ok"]}
    assert "state_version_binding" in names


def test_verifier_detects_illegal_audit_transition(
    coordinator, declarations_raw, snapshot_payload
):
    _setup_snapshot(coordinator, declarations_raw, snapshot_payload)
    repo = coordinator.repo
    component_id = repo.get_plan(1)["ordered_components"][0]
    repo.ensure_component_state(1, component_id, "k")
    repo._conn.execute("PRAGMA foreign_keys=OFF")
    repo._conn.execute(
        "INSERT INTO state_transition_audit(version, component_id, from_state, to_state,"
        " reason, error_code, created_at) VALUES (1, ?, 'APPLIED', 'DISPATCHED',"
        " 'forged', NULL, '2026-11-20T00:00:00+00:00')",
        (component_id,),
    )
    repo._conn.commit()
    repo._conn.execute("PRAGMA foreign_keys=ON")
    result = verify(repo).to_dict()
    assert result["ok"] is False
    assert any(
        check["name"] == "state_transitions_legal" and not check["ok"]
        for check in result["checks"]
    )


def test_read_only_query_runs_alongside_writer(
    coordinator, declarations_raw, snapshot_payload, success_script, cluster_factory
):
    import threading
    import time

    _setup_snapshot(coordinator, declarations_raw, snapshot_payload)
    cluster = cluster_factory(declarations_raw, success_script)
    observed = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            view = coordinator.repo.read_only_snapshot_view()
            observed.append(len(view["states"]))
            time.sleep(0.005)

    thread = threading.Thread(target=reader)
    thread.start()
    run = coordinator.dispatch(1, failure_policy="continue")
    stop.set()
    thread.join()
    assert run["status"] == "COMPLETED"
    assert observed  # 只读查询在分发期间持续成功
    assert sum(
        count.get("/apply", 0) for count in cluster.request_counts.values()
    ) == len(coordinator.ensure_plan(1)["ordered_components"])


def test_dispatch_intent_persisted_before_execution(
    coordinator, declarations_raw, snapshot_payload
):
    _setup_snapshot(coordinator, declarations_raw, snapshot_payload)
    coordinator.repo.create_run_intent(1, "idem", "rollback")
    unfinished = coordinator.repo.list_unfinished_runs()
    assert [row["version"] for row in unfinished] == [1]


def test_reopen_repository_sees_persisted_facts(tmp_path, declarations_raw, snapshot_payload):
    db_path = str(tmp_path / "reopen.db")
    repo = Coordinator(Repository(db_path), PushClient())
    repo.register_declarations(declarations_raw)
    repo.submit_snapshot(snapshot_payload(1))
    repo.repo.close()

    reopened_repo = Repository(db_path)
    reopened = Coordinator(reopened_repo, PushClient())
    assert reopened.repo.latest_version() == 1
    assert len(reopened.repo.load_declarations()) == len(declarations_raw)
    reopened_repo.close()
