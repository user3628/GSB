from __future__ import annotations

import concurrent.futures
import os
import subprocess
import sys

from tcp_sim.db import Database
from tcp_sim.service import BatchCommand, SimulationService

from .conftest import ROOT, command


def test_duplicate_batch_is_advanced_once_and_waiters_reuse_result(service, trace):
    cmd = command(
        trace,
        run_id="dup-run",
        batch_id="dup-batch",
        events=trace["events"],
        final_batch=True,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: service.submit_batch(cmd), range(8)))
    digests = {result["state_digest"] for result in results}
    assert len(digests) == 1
    run = service.get_run("dup-run")
    assert run["event_count"] == 26
    snapshot_rows = service.db.fetch_all(
        "SELECT * FROM snapshots WHERE run_id = ? ORDER BY event_index",
        ("dup-run",),
    )
    assert len(snapshot_rows) == 26
    batch_rows = service.db.fetch_all("SELECT * FROM batches WHERE batch_id = ?", ("dup-batch",))
    assert len(batch_rows) == 1
    intents = service.db.fetch_all(
        "SELECT kind, status FROM intent_log WHERE batch_id = ?",
        ("dup-batch",),
    )
    assert all(row["status"] == "COMMITTED" for row in intents)
    assert sum(1 for row in intents if row["kind"] == "BATCH_COMMIT") == 1
    assert sum(1 for row in intents if row["kind"] == "CHECKPOINT_WRITE") == 26


def test_identical_requests_with_provided_config_use_deterministic_digest(service, trace):
    first = service.submit_batch(
        command(
            trace,
            run_id="config-run",
            batch_id="config-1",
            events=trace["events"][:6],
            final_batch=False,
        )
    )
    second = service.submit_batch(
        command(
            trace,
            run_id="config-run",
            batch_id="config-2",
            events=trace["events"][6:12],
            final_batch=False,
            expected_base=first["committed_checkpoint_id"],
            include_config=False,
        )
    )
    assert second["processed_events"] == 6
    assert service.get_run("config-run")["event_count"] == 12


def test_forced_termination_recovery_resumes_from_persisted_checkpoint(tmp_path, trace):
    db_path = tmp_path / "crash.db"
    database = Database(db_path)
    database.initialize()
    service = SimulationService(database)
    full = service.submit_batch(
        command(
            trace,
            run_id="baseline",
            batch_id="baseline-all",
            events=trace["events"],
            final_batch=True,
        )
    )
    prefix_end = trace["suggested_splits"][2]
    prefix = service.submit_batch(
        command(
            trace,
            run_id="crash",
            batch_id="crash-1",
            events=trace["events"][:prefix_end],
            final_batch=False,
        )
    )

    child_code = r"""
import json
from tcp_sim.db import Database
from tcp_sim.service import BatchCommand, SimulationService
from pathlib import Path
import sys
trace = json.loads(Path('samples/fixed_trace.json').read_text(encoding='utf-8'))
db = Database(sys.argv[1]); db.initialize()
service = SimulationService(db)
service.submit_batch(BatchCommand(
    trace_id=trace['trace_id'], run_id='crash', batch_id='crash-2',
    events=trace['events'][18:], final_batch=True,
    expected_base_checkpoint_id=sys.argv[2], config=trace['config'],
))
"""
    env = os.environ.copy()
    env["TCP_SIM_CRASH_BATCH_ID"] = "crash-2"
    env["TCP_SIM_CRASH_AFTER_CHECKPOINTS"] = "5"
    env["PYTHONPATH"] = str(ROOT)
    child = subprocess.run(
        [sys.executable, "-c", child_code, str(db_path), prefix["committed_checkpoint_id"]],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert child.returncode < 0 or child.returncode == 97
    assert "TCP_SIM_FORCED_TERMINATION" in child.stderr

    restarted_db = Database(db_path)
    restarted_db.initialize()
    restarted = SimulationService(restarted_db)
    report = restarted.recover_incomplete()
    assert report["forced_termination_detected"] is True
    assert len(report["aborted_batches"]) == 1
    aborted = report["aborted_batches"][0]
    assert aborted["observed_processed_count"] == 5
    assert aborted["rollback_checkpoint_id"] == prefix["committed_checkpoint_id"]
    assert aborted["rollback_event_index"] == prefix_end - 1

    resumed = restarted.submit_batch(
        command(
            trace,
            run_id="crash",
            batch_id="crash-resume",
            events=trace["events"][aborted["rollback_event_index"] + 1 :],
            final_batch=True,
            expected_base=aborted["rollback_checkpoint_id"],
        )
    )
    assert resumed["processed_events"] == len(trace["events"]) - prefix_end
    compare = restarted.compare_runs("baseline", "crash")
    assert compare["identical"] is True
    assert compare["left_snapshot_digest"] == full["batch_snapshot_digest"]
    assert compare["right_snapshot_digest"] == full["batch_snapshot_digest"]
    assert "No complete graceful-exit report is promised" in report["disclaimer"]


def test_latency_budget_and_intent_chain(service, trace):
    result = service.submit_batch(
        command(
            trace,
            run_id="latency-run",
            batch_id="latency-batch",
            events=trace["events"],
            final_batch=True,
        )
    )
    assert result["latency_ms"] < result["latency_budget_ms"]
    assert result["within_latency_budget"] is True
    intents = service.db.fetch_all(
        "SELECT kind, status FROM intent_log WHERE batch_id = ? ORDER BY intent_id",
        ("latency-batch",),
    )
    kinds = [row["kind"] for row in intents]
    assert kinds[0] == "RUN_INITIALIZE"
    assert "CHECKPOINT_WRITE" in kinds
    assert "SNAPSHOT_COMMIT" in kinds
    assert kinds[-1] == "BATCH_COMMIT"
    assert all(row["status"] == "COMMITTED" for row in intents)


def test_concurrent_different_runs_do_not_share_intents_or_checkpoints(service, trace):
    left_cmd = command(
        trace,
        run_id="parallel-left",
        batch_id="parallel-left-batch",
        events=trace["events"],
        final_batch=True,
    )
    right_cmd = command(
        trace,
        run_id="parallel-right",
        batch_id="parallel-right-batch",
        events=trace["events"],
        final_batch=True,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        left_future = pool.submit(service.submit_batch, left_cmd)
        right_future = pool.submit(service.submit_batch, right_cmd)
        left = left_future.result()
        right = right_future.result()
    assert left["batch_id"] != right["batch_id"]
    assert service.compare_runs("parallel-left", "parallel-right")["identical"] is True
    left_intents = service.db.fetch_all(
        "SELECT run_id, batch_id FROM intent_log WHERE batch_id = ?",
        ("parallel-left-batch",),
    )
    right_intents = service.db.fetch_all(
        "SELECT run_id, batch_id FROM intent_log WHERE batch_id = ?",
        ("parallel-right-batch",),
    )
    assert left_intents and all(row["run_id"] == "parallel-left" for row in left_intents)
    assert right_intents and all(row["run_id"] == "parallel-right" for row in right_intents)
    left_checkpoints = service.db.fetch_all(
        "SELECT run_id, batch_id FROM checkpoints WHERE run_id = ?",
        ("parallel-left",),
    )
    right_checkpoints = service.db.fetch_all(
        "SELECT run_id, batch_id FROM checkpoints WHERE run_id = ?",
        ("parallel-right",),
    )
    assert all(row["run_id"] == "parallel-left" for row in left_checkpoints)
    assert all(row["run_id"] == "parallel-right" for row in right_checkpoints)


def test_large_deterministic_batch_meets_five_second_budget(service, trace):
    events = [
        {"ts_ms": 0, "conn_id": "bulk", "event_type": "send", "payload_bytes": 0},
        {"ts_ms": 10, "conn_id": "bulk", "event_type": "ack", "payload_bytes": 0, "ack_seq": 0},
    ]
    seq = 0
    ts = 20
    for _ in range(999):
        seq += 1000
        events.append(
            {"ts_ms": ts, "conn_id": "bulk", "event_type": "send", "payload_bytes": 1000}
        )
        events.append(
            {
                "ts_ms": ts + 10,
                "conn_id": "bulk",
                "event_type": "ack",
                "payload_bytes": 1000,
                "ack_seq": seq,
            }
        )
        ts += 20

    result = service.submit_batch(
        BatchCommand(
            trace_id="bulk-trace",
            run_id="bulk-run",
            batch_id="bulk-batch",
            events=events,
            final_batch=True,
            config=trace["config"],
        )
    )
    assert result["processed_events"] == 2000
    assert result["within_latency_budget"] is True
    assert result["latency_ms"] < result["latency_budget_ms"]
