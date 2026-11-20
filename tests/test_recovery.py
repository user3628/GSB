from __future__ import annotations

import sqlite3

from capacity_workbench.db import Store


def test_recovery_aborts_incomplete_sampling_from_persisted_facts(
    db_path, base_plan: dict
) -> None:
    store = Store(db_path)
    store.init_schema()
    plan = store.insert_plan(base_plan)
    store.mark_plan_running(plan.id)
    round_record = store.create_round(
        plan_id=plan.id,
        stage_index=0,
        attempt=1,
        concurrency=1,
        duration_seconds=10,
        origin="sequence",
    )
    store.start_round(round_record.id)
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "INSERT INTO samples(round_id, sequence, latency_ms, outcome, error_kind, occurred_at) VALUES (?, 0, 12.5, 'SUCCESS', NULL, '2026-01-01T00:00:00+00:00')",
            (round_record.id,),
        )

    report = store.recover_incomplete()
    assert report is not None
    item = report["rounds"][0]
    assert item["before_status"] == "SAMPLING"
    assert item["after_status"] == "ABORTED"
    assert item["persisted_facts"]["sample_count"] == 1
    assert item["persisted_facts"]["aggregate_present"] is False
    assert item["persisted_facts"]["judgment_present"] is False
    assert "No complete load-test exit report" in report["limitations"][0]

    detail = store.get_round_detail(round_record.id)
    assert detail["status"] == "ABORTED"
    assert detail["aggregate"] is None
    assert detail["judgment"] is None
    assert store.get_plan(plan.id).status == "READY"
    reports = store.list_recovery_reports()
    assert reports[-1]["report_id"] == report["report_id"]


def test_recovery_restores_complete_facts_without_recomputing(
    db_path, base_plan: dict
) -> None:
    store = Store(db_path)
    store.init_schema()
    plan = store.insert_plan(base_plan)
    round_record = store.create_round(
        plan_id=plan.id,
        stage_index=0,
        attempt=1,
        concurrency=1,
        duration_seconds=1,
        origin="sequence",
    )
    store.start_round(round_record.id)
    store.abort_round(round_record.id, "prepare_terminal_fixture")

    # Move the terminal fixture back to SAMPLING locally to exercise the
    # committed-facts recovery branch. The immutable judgment is not touched.
    with sqlite3.connect(db_path) as connection:
        connection.execute("DROP TRIGGER terminal_round_is_immutable")
        connection.execute(
            "UPDATE rounds SET status='SAMPLING', ended_at=NULL, failure_reason=NULL WHERE id=?",
            (round_record.id,),
        )
        connection.execute(
            "INSERT INTO aggregates VALUES (?, 100, 100, 0, 0, 100.0, 10, 10, 10, 0, 1000, 'nearest_rank', '2026-01-01T00:00:00+00:00')",
            (round_record.id,),
        )
        connection.execute(
            "INSERT INTO judgments VALUES (?, 1, 'PASS', '[]', '{}', 100, 'nearest_rank', 'fixture', '2026-01-01T00:00:00+00:00')",
            (round_record.id,),
        )
        connection.executemany(
            "INSERT INTO samples(round_id, sequence, latency_ms, outcome, error_kind, occurred_at) VALUES (?, ?, 10, 'SUCCESS', NULL, '2026-01-01T00:00:00+00:00')",
            [(round_record.id, index) for index in range(100)],
        )
    recovery_store = Store(db_path)
    report = recovery_store.recover_incomplete()
    assert report is not None
    item = report["rounds"][0]
    assert item["action"] == "restored_from_committed_judgment"
    assert item["after_status"] == "CONVERGED"
    detail = recovery_store.get_round_detail(round_record.id)
    assert detail["judgment"]["passed"] is True
    assert detail["judgment"]["rationale"] == "fixture"


def test_recovery_resets_running_plan_even_without_an_active_round(
    db_path, base_plan: dict
) -> None:
    store = Store(db_path)
    store.init_schema()
    plan = store.insert_plan(base_plan)
    store.mark_plan_running(plan.id)
    report = store.recover_incomplete()
    assert report is not None
    assert report["affected_plan_ids"] == [plan.id]
    assert report["plans"] == [
        {
            "plan_id": plan.id,
            "before_status": "RUNNING",
            "after_status": "READY",
            "persisted_facts": {"sampling_round_count_before": 0},
        }
    ]
    assert store.get_plan(plan.id).status == "READY"

