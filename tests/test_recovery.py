from __future__ import annotations

from pathlib import Path

from app.service import WorkbenchService


def test_pending_intent_is_aborted_at_startup_and_report_is_persisted(tmp_path: Path):
    db_path = tmp_path / "recovery.sqlite3"
    service = WorkbenchService(db_path)
    intent_id = service._create_intent(
        "RISK_DECISION",
        {
            "batch_id": "B-MISSING",
            "pre_state": {
                "batch_status": "OBSERVING",
                "window_epoch": 7,
            },
        },
    )
    report = service.recover_pending_intents()
    assert report["pending_found"] == 1
    assert report["aborted_count"] == 1
    assert report["forced_exit_report_available"] is False
    assert report["intents"][0]["intent_id"] == intent_id
    assert report["intents"][0]["status_after_scan"] == "ABORTED"
    assert report["recovery_basis"].startswith("SQLite WAL")

    reloaded = WorkbenchService(db_path)
    latest = reloaded.latest_recovery_report()
    assert latest["run_id"] == report["run_id"]
    with reloaded.db.read_snapshot() as connection:
        row = connection.execute(
            "SELECT status, recovery_run_id FROM intents WHERE intent_id=?", (intent_id,)
        ).fetchone()
    assert row["status"] == "ABORTED"
    assert row["recovery_run_id"] == report["run_id"]


def test_startup_scan_with_no_pending_intents_still_records_persisted_report(
    seeded_service,
):
    report = seeded_service.recover_pending_intents()
    assert report["pending_found"] == 0
    assert report["aborted_count"] == 0
    assert report["persisted_fact_counts"]["batches"] == 5
    assert report["caveat"].startswith("恢复报告只描述数据库扫描时已持久化的事实")
