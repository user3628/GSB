from __future__ import annotations

from app.engine import CrashController, RepairEngine
from app.errors import SimulatedCrash
from app.store import Store
from tests.conftest import read_json


def test_forced_stop_after_commit_recovers_budget_tasks_and_reuse(tmp_path, sample_layout) -> None:
    db_path = tmp_path / "crash.db"
    store = Store(db_path)
    controller = CrashController()
    engine = RepairEngine(store, controller)
    engine.recover()
    engine.load_layout(sample_layout)
    controller.arm("after_task_commit", "raise")
    try:
        engine.submit_trace(read_json("recovery_trace.json"))
    except SimulatedCrash:
        pass
    else:  # pragma: no cover
        raise AssertionError("crash injection did not fire")

    building = store.get_round_record(
        store.raw_sql_for_tests("SELECT round_id FROM rounds")[0]["round_id"], include_uncommitted=True
    )
    assert building["state"] == "BUILDING"

    recovered_store = Store(db_path)
    recovered_engine = RepairEngine(recovered_store)
    report = recovered_engine.recover()
    assert report["recovery_required"] is True
    assert report["summary"] == {
        "pending_intents": 0,
        "aborted_rounds": 1,
        "tasks_returned_to_ready": 1,
        "reusable_committed_shards": 1,
    }
    assert report["budget_facts"][0]["committed_bytes"] == 512
    assert report["budget_facts"][0]["released_reserved_bytes_for_uncommitted_tasks"] == 512
    assert report["budget_facts"][0]["remaining_bytes_recomputed"] == 3584
    tasks = [dict(row) for row in recovered_store.raw_sql_for_tests(
        "SELECT task_id, state FROM rebuild_tasks ORDER BY state"
    )]
    assert {task["state"] for task in tasks} == {"READY", "COMMITTED"}
    ledger = [dict(row) for row in recovered_store.raw_sql_for_tests(
        "SELECT state, bytes FROM budget_ledger ORDER BY state"
    )]
    assert {entry["state"] for entry in ledger} == {"COMMITTED", "RELEASED"}

    alpha = next(obj for obj in sample_layout["objects"] if obj["object_id"] == "obj-alpha")
    committed = [row for row in recovered_store.raw_sql_for_tests(
        "SELECT object_id, shard_index, result_digest FROM rebuild_tasks WHERE state='COMMITTED'"
    )][0]
    expected = next(shard["digest"] for shard in alpha["shards"] if shard["shard_index"] == committed["shard_index"])
    reusable = recovered_store.get_reusable_shard(
        sample_layout["layout_version"], committed["object_id"], committed["shard_index"], expected
    )
    assert reusable is not None
    assert reusable["digest"] == committed["result_digest"]

    resumed = recovered_engine.submit_trace(read_json("recovery_trace.json"))
    decision = resumed["decisions"][0]
    assert decision["outcome"] == "SUCCESS"
    assert decision["reused_reconstructed_shards"] == [4]
    assert decision["successfully_rebuilt_shards"] == [2]
    assert resumed["budget"]["consumed_bytes"] == 512
    assert resumed["budget"]["task_count"] == 1


def test_recovery_report_does_not_claim_clean_shutdown(tmp_path, sample_layout) -> None:
    store = Store(tmp_path / "clean.db")
    engine = RepairEngine(store)
    report = engine.recover()
    assert report["clean_shutdown_report"] is False
    assert report["scope"] == "startup_reconciliation"
    assert report["recovery_required"] is False
    assert "不承诺完整退出报告" in report["note"]
