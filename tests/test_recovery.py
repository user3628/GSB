from __future__ import annotations

from chunk_engine.engine import Engine
from chunk_engine.errors import SimulatedCrash
from chunk_engine.snapshot import SnapshotManager


def test_restart_reuses_committed_blocks_and_resumes_pages(engine_factory) -> None:
    engine = engine_factory((1, 2))

    def crash() -> None:
        raise SimulatedCrash("simulated forced termination")

    try:
        engine.create_batch(
            [1, 5000, 10000, 15000, 20000],
            ["id", "region", "score"],
            page_size=2,
            execution_mode="stream",
            batch_id="b_recovery_test",
            crash_after_committed_blocks=2,
            crash_callback=crash,
        )
    except SimulatedCrash:
        pass
    before = engine.db.get_batch("b_recovery_test")
    tasks_before = engine.db.get_tasks("b_recovery_test")
    committed_before = [task["block_no"] for task in tasks_before if task["state"] == "committed"]
    assert before["state"] == "materializing"
    assert len(committed_before) == 2
    assert engine.db.memory_usage()["reserved_bytes"] > 0

    restarted = Engine(engine.data_dir, config=engine.config, snapshot_manager=SnapshotManager())
    report = restarted.recover()
    assert report["pending_intents"] >= 1
    assert len(report["tasks_reused"]) == 2
    assert report["budget_reservations_reclaimed"] == 1
    assert "No clean-shutdown" in report["note"]
    assert restarted.db.get_batch("b_recovery_test")["state"] == "ready"
    assert restarted.db.memory_usage()["reserved_bytes"] == 0

    status = restarted.resume_batch("b_recovery_test")
    assert status["state"] == "materialized"
    assert status["execution"]["blocks_reused"] == 2
    meta, internal = restarted.read_result_rows("b_recovery_test")
    assert meta["row_count"] == 5
    assert [row["key"] for row in internal] == [1, 5000, 10000, 15000, 20000]

    page0 = restarted.get_page("b_recovery_test", 0, "post-restart")
    rows = list(page0["rows"])
    for page_no in range(1, page0["total_pages"]):
        rows.extend(restarted.get_page("b_recovery_test", page_no, f"post-restart-{page_no}")["rows"])
    assert [row["key"] for row in rows] == [1, 5000, 10000, 15000, 20000]
    assert restarted.db.memory_usage()["reserved_bytes"] == 0

    latest = restarted.db.latest_recovery_report()
    assert latest is not None
    assert latest["report"]["basis"].startswith("sqlite_committed_rows")
