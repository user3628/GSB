from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from app.config import ROOT
from app.engine import Engine
from app.schemas import BatchRequest


SPEC = {
    "kind": "sliding",
    "width_ms": 1000,
    "step_ms": 500,
    "boundary": "left_closed_right_open",
}


def payload(batch_id: str, events: list[dict], kind="integer", scale=0) -> dict:
    measure = {"kind": kind}
    if kind == "decimal":
        measure["scale"] = scale
    return {
        "batch_id": batch_id,
        "window_spec": SPEC,
        "measure_type": measure,
        "events": events,
        "timestamp_order": "sort",
        "late_policy": "queue",
    }


def ingest(engine: Engine, value: dict) -> dict:
    return engine.ingest_batch(BatchRequest.model_validate(value))


def normalized(engine: Engine) -> list[dict]:
    result = engine.query_windows(include_type_history=True)
    result.pop("state_version", None)
    result.pop("loaded_snapshot_version", None)
    result.pop("query", None)
    return result["windows"]


def run_crashing_worker(db: Path, batch_file: Path | None, mode: str, crash_point: str) -> subprocess.CompletedProcess:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "crash_worker.py"),
        "--db",
        str(db),
        "--mode",
        mode,
        "--crash-point",
        crash_point,
    ]
    if batch_file is not None:
        command.extend(["--batch-file", str(batch_file)])
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    return subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=30)


@pytest.mark.parametrize("crash_point", ["after_intent", "mid_transaction"])
def test_forced_termination_batch_recovery_is_item_for_item_equal(tmp_path: Path, crash_point: str) -> None:
    baseline_db = tmp_path / f"baseline-{crash_point}.db"
    crash_db = tmp_path / f"crash-{crash_point}.db"
    batch_file = tmp_path / f"crash-batch-{crash_point}.json"

    b1 = payload(
        "crash-base-1",
        [{"event_id": "c1", "timestamp_ms": 100, "group_key": "g", "measure": 10}],
    )
    crashed = payload(
        "crash-mid-2",
        [
            {"event_id": "c2", "timestamp_ms": 600, "group_key": "g", "measure": "1.25"},
            {"event_id": "c3", "timestamp_ms": 900, "group_key": "g", "measure": "2.75"},
        ],
        kind="decimal",
        scale=2,
    )
    continuation = payload(
        "crash-after-3",
        [{"event_id": "c4", "timestamp_ms": 1100, "group_key": "g", "measure": 4}],
    )
    batch_file.write_text(json.dumps(crashed))

    baseline = Engine(f"sqlite:///{baseline_db}")
    try:
        ingest(baseline, b1)
        ingest(baseline, continuation)
        baseline_windows = normalized(baseline)
    finally:
        baseline.close()

    first = Engine(f"sqlite:///{crash_db}")
    try:
        ingest(first, b1)
    finally:
        first.close()

    crashed_process = run_crashing_worker(crash_db, batch_file, "ingest", crash_point)
    assert crashed_process.returncode == 86, (crashed_process.stdout, crashed_process.stderr)

    recovered = Engine(f"sqlite:///{crash_db}")
    try:
        report = recovered.last_recovery_report
        assert report is not None
        assert report["status"] == "RECOVERED"
        assert report["pending_intent_count"] == 1
        assert report["aborted_batches"] == [
            {
                "batch_id": "crash-mid-2",
                "request_hash": BatchRequest.model_validate(crashed).request_hash(),
                "reason": "recovered_pending_intent",
            }
        ]
        assert report["durable_facts_only"] is True
        batches = {item["batch_id"]: item for item in recovered.list_batches()["batches"]}
        assert batches["crash-mid-2"]["status"] == "ABORTED"
        assert recovered.query_events(batch_id="crash-mid-2")["event_count"] == 0

        ingest(recovered, continuation)
        assert normalized(recovered) == baseline_windows
        assert recovered.status()["pending_intent_count"] == 0
    finally:
        recovered.close()


@pytest.mark.parametrize("crash_point", ["after_intent", "mid_transaction"])
def test_forced_termination_during_snapshot_commit_leaves_last_snapshot(tmp_path: Path, crash_point: str) -> None:
    db = tmp_path / f"snapshot-crash-{crash_point}.db"
    engine = Engine(f"sqlite:///{db}")
    try:
        ingest(engine, payload("snapshot-base", [{"event_id": "s1", "timestamp_ms": 100, "group_key": "g", "measure": 1}]))
        first_snapshot = engine.create_snapshot()
        assert first_snapshot["version"] == 1
        before_windows = normalized(engine)
    finally:
        engine.close()

    crashed = run_crashing_worker(db, None, "snapshot", crash_point)
    assert crashed.returncode == 86, (crashed.stdout, crashed.stderr)

    recovered = Engine(f"sqlite:///{db}")
    try:
        report = recovered.last_recovery_report
        assert report["status"] == "RECOVERED"
        assert report["pending_intent_count"] == 1
        assert report["latest_complete_snapshot_version"] == 1
        assert [item["version"] for item in recovered.list_snapshots()["snapshots"]] == [1]
        assert normalized(recovered) == before_windows
        assert recovered.status()["state_version"] == 1
        assert any(action["kind"] == "snapshot_create" for action in report["actions"])
    finally:
        recovered.close()
