from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

from gitops_convergence.engine import Engine, EngineConfig

from conftest import ROOT, app, load_sample, snapshot


def test_forced_termination_recovery_uses_persisted_facts(tmp_path: Path) -> None:
    db_path = tmp_path / "crash.db"
    engine = Engine(db_path)
    engine.submit_snapshot(
        snapshot(
            [
                app("a", desired="v2", effective="v1"),
                app("b", desired="v1", effective="v1", dependencies=["a"]),
            ]
        )
    )
    before_plan = engine.preview_plan(1, {"mode": "auto"})
    assert engine.get_app(1, "a")["state"] == "PENDING"

    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(ROOT)
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "gitops_convergence.crash_worker",
            "--db",
            str(db_path),
            "--snapshot",
            "1",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode != 0

    raw = sqlite3.connect(db_path)
    raw.row_factory = sqlite3.Row
    try:
        run = raw.execute(
            "SELECT status FROM convergence_runs WHERE snapshot_version = 1"
        ).fetchone()
        intent = raw.execute(
            "SELECT status FROM intents WHERE kind = 'CONVERGENCE' ORDER BY created_at DESC"
        ).fetchone()
        building_batches = raw.execute(
            "SELECT COUNT(*) AS count FROM batches WHERE status = 'BUILDING'"
        ).fetchone()["count"]
    finally:
        raw.close()
    assert run["status"] == "RUNNING"
    assert intent["status"] == "RUNNING"
    assert building_batches == 1

    restarted = Engine(db_path)
    report = restarted.recover()
    assert report["last_complete_snapshot_version"] == 1
    assert len(report["aborted_intents"]) == 1
    assert len(report["aborted_batch_ids"]) == 1
    assert report["state_diff"]["changed_app_ids"] == []
    assert restarted.get_app(1, "a")["state"] == "PENDING"
    after_plan = restarted.preview_plan(1, {"mode": "auto"})
    assert after_plan["plan_hash"] == before_plan["plan_hash"]
    with restarted.db.transaction(immediate=False) as connection:
        assert connection.execute(
            "SELECT status FROM convergence_runs WHERE snapshot_version = 1"
        ).fetchone()["status"] == "ABORTED"
        assert connection.execute(
            "SELECT COUNT(*) AS count FROM batches WHERE status = 'ACTIVE'"
        ).fetchone()["count"] == 0
        assert connection.execute(
            "SELECT COUNT(*) AS count FROM batches WHERE status = 'ABORTED'"
        ).fetchone()["count"] == 1

    retried = restarted.trigger_convergence(1, {"mode": "auto"})
    assert retried["final_state_counts"] == {"CONVERGED": 2}
    assert retried["execution_count"] == 2


def test_large_sample_full_and_local_paths_converge_identically(tmp_path: Path) -> None:
    sample = load_sample("large_cluster_graph.json")
    engine = Engine(
        tmp_path / "large.db",
        EngineConfig(switch_threshold=1000, max_batch_replicas=512),
    )
    created = engine.submit_snapshot(sample)
    assert created["application_count"] == 3000
    assert created["state_counts"] == {"CONVERGED": 2994, "PENDING": 6}

    exact = engine.preview_plan(1, {"mode": "full"})
    local = engine.preview_plan(1, {"mode": "incremental"})
    assert exact["plan_hash"] == local["plan_hash"]
    assert len(exact["batches"]) == len(local["batches"]) == 1

    single = load_sample("increment_single_app.json")
    increment_result = engine.submit_increment(1, single)
    assert increment_result["affected_app_count"] == 1
    auto = engine.preview_plan(2, {"mode": "auto"})
    full = engine.preview_plan(2, {"mode": "full"})
    local = engine.preview_plan(2, {"mode": "incremental"})
    assert auto["recompute"]["path"] == "INCREMENTAL"
    assert auto["plan_hash"] == full["plan_hash"] == local["plan_hash"]

    rollback = load_sample("rollback_cascade_snapshot.json")
    # The rollback sample bases on the already committed version 2.
    engine.trigger_convergence(2, {"mode": "auto"})
    cascade_result = engine.submit_increment(2, rollback)
    assert cascade_result["rollback_cascade"]["ordered_app_ids"] == [
        "app-2926",
        "app-2998",
        "app-2885",
    ]
    assert cascade_result["state_counts"] == {
        "CONVERGED": 2997,
        "ROLLING_BACK": 3,
    }
    final = engine.trigger_convergence(3, {"mode": "auto"})
    assert final["final_state_counts"] == {"CONVERGED": 3000}
