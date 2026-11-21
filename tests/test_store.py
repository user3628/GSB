from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from qos_workbench.errors import BusinessError
from qos_workbench.models import Policy, PolicySnapshot
from qos_workbench.store import SnapshotStore


def make_snapshot(snapshot_no: int, queue_ids=("a",), checksum_salt: int = 0) -> PolicySnapshot:
    policies = []
    for index, queue_id in enumerate(queue_ids):
        policies.append(
            Policy(
                queue_id=queue_id,
                cir_bps=1_000 + index + checksum_salt,
                pir_bps=2_000 + index + checksum_salt,
                cbs_bytes=1_000,
                pbs_bytes=2_000,
                priority=index % 8,
                match_rule=f"match-{queue_id}",
                parent_queue_id=None,
                timestamp_us=1_700_000_000_000_000 + snapshot_no,
            )
        )
    return PolicySnapshot(
        format_version=3,
        snapshot_no=snapshot_no,
        generated_at_us=1_700_000_000_000_000 + snapshot_no,
        policies=tuple(sorted(policies, key=lambda policy: policy.queue_id)),
    )


def test_snapshot_replacement_is_atomic_and_never_mixes_generations(tmp_path):
    store = SnapshotStore(tmp_path / "qos.db")
    first = make_snapshot(1, ("a", "b"))
    r1 = store.commit_snapshot(first)
    assert r1.committed and r1.snapshot_seq == 1
    seq1, active1 = store.current_snapshot()
    assert active1 == first

    second = make_snapshot(2, ("c",))
    r2 = store.commit_snapshot(second)
    assert r2.committed and r2.snapshot_seq == 2
    seq2, active2 = store.current_snapshot()
    assert seq2 == 2 and active2 == second
    assert {policy.queue_id for policy in active2.policies} == {"c"}
    assert store.get_snapshot_by_seq(seq1) == first

    same = store.commit_snapshot(second)
    assert same.idempotent is True
    assert same.snapshot_seq == seq2

    with pytest.raises(BusinessError) as caught:
        store.commit_snapshot(make_snapshot(2, ("c",), checksum_salt=10))
    assert caught.value.code == "E_SNAPSHOT_INVALID"

    with pytest.raises(BusinessError) as caught:
        store.commit_snapshot(make_snapshot(1, ("old",)))
    assert caught.value.code == "E_SNAPSHOT_INVALID"
    assert store.current_snapshot()[1] == second


def test_invalid_snapshot_keeps_previous_snapshot_active(tmp_path):
    store = SnapshotStore(tmp_path / "qos.db")
    first = make_snapshot(1)
    store.commit_snapshot(first)
    invalid = PolicySnapshot(
        3,
        2,
        123,
        (
            Policy("a", 1_000, 2_000, 1_000, 2_000, 1, "all", "b", 1),
            Policy("b", 1_000, 2_000, 1_000, 2_000, 1, "all", "a", 1),
        ),
    )
    with pytest.raises(BusinessError) as caught:
        store.commit_snapshot(invalid)
    assert caught.value.code == "E_DEPENDENCY_CYCLE"
    assert store.current_snapshot()[1] == first


def test_committed_intents_and_simulation_batch_are_persisted(tmp_path):
    store = SnapshotStore(tmp_path / "qos.db")
    snapshot = make_snapshot(1)
    store.commit_snapshot(snapshot)
    result = store.run_simulation(
        [{"timestamp_ms": 0, "queue_id": "a", "packet_bytes": 100, "color": "green"}]
    )
    assert result["status"] == "committed"
    batch = store.get_simulation(result["batch_id"])
    assert batch["status"] == "committed"
    assert batch["snapshot_seq"] == 1
    assert batch["result_json"]["packet_count"] == 1
    assert batch["diagnostics_json"]["precision_contract"]["decimal_places"] == 6

    conn = sqlite3.connect(store.db_path)
    try:
        states = conn.execute("SELECT kind, state, COUNT(*) FROM intents GROUP BY kind, state").fetchall()
    finally:
        conn.close()
    assert ("snapshot_commit", "committed", 1) in states
    assert ("simulation", "committed", 1) in states


def test_readers_observe_only_complete_generation_during_concurrent_writes(tmp_path):
    store = SnapshotStore(tmp_path / "qos.db")
    store.commit_snapshot(make_snapshot(1, ("a",)))
    observed: list[tuple[int, str]] = []

    def reader() -> None:
        for _ in range(200):
            seq, snapshot = store.current_snapshot()
            # Recomputing the checksum detects any mixed or partially applied policy set.
            assert snapshot.checksum
            observed.append((seq, snapshot.checksum))

    def writer() -> None:
        for snapshot_no in range(2, 42):
            store.commit_snapshot(make_snapshot(snapshot_no, (f"q{snapshot_no}",)))

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(reader) for _ in range(4)]
        futures.append(pool.submit(writer))
        for future in futures:
            future.result()

    assert observed
    seq, active = store.current_snapshot()
    assert seq == 41
    assert active.snapshot_no == 41
    assert active.checksum == make_snapshot(41, ("q41",)).checksum
    # Every read is one of the known complete commits, never a hybrid.
    known = {make_snapshot(no, ("a",)).checksum if no == 1 else make_snapshot(no, (f"q{no}",)).checksum for no in range(1, 42)}
    assert all(checksum in known for _, checksum in observed)


def test_forced_process_termination_is_recovered_from_persisted_intent(tmp_path):
    db_path = tmp_path / "crash.db"
    store = SnapshotStore(db_path)
    baseline = make_snapshot(1, ("baseline",))
    store.commit_snapshot(baseline)

    child_code = r"""
import os
from qos_workbench.store import SnapshotStore

db = os.environ["CRASH_DB"]
store = SnapshotStore(db, recover_on_start=False)
store.begin_intent(
    kind="simulation",
    snapshot_no=1,
    snapshot_seq=1,
    subject_id="crashed-batch",
    payload_sha256="a" * 64,
    payload={
        "sample_sha256": "a" * 64,
        "snapshot_seq": 1,
        "snapshot_no": 1,
        "request": {"samples": [{"queue_id": "baseline", "packet_bytes": 10}]},
    },
)
conn = store._connect()
conn.execute("BEGIN IMMEDIATE")
conn.execute(
    "INSERT INTO snapshots(snapshot_no, format_version, generated_at_us, checksum, policy_count, created_at_us) "
    "VALUES(99, 3, '1700000000000000', 'uncommitted', 1, 1700000000000000)"
)
seq = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
conn.execute(
    "INSERT INTO snapshot_policies(snapshot_seq, queue_id, cir_bps, pir_bps, cbs_bytes, pbs_bytes, priority, "
    "match_rule, parent_queue_id, timestamp_us) VALUES(?,?,?,?,?,?,?,?,?,?)",
    (seq, "uncommitted", "1", "1", "1", "1", 1, "all", None, "1"),
)
conn.execute(
    "INSERT INTO simulation_batches(batch_id, snapshot_seq, snapshot_no, status, sample_sha256, request_json, "
    "created_at_us) VALUES('uncommitted-running', 1, 1, 'running', 'b', '{}', 1)"
)
os._exit(91)
"""
    completed = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=Path(__file__).resolve().parent.parent,
        env={"CRASH_DB": str(db_path), "PYTHONPATH": str(Path(__file__).resolve().parent.parent)},
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 91, completed.stderr

    recovered = SnapshotStore(db_path)
    report = recovered.recovery_report
    assert report["recovery_performed"] is True
    assert report["started_intents_found"] == 1
    assert report["facts_only"] is True
    assert report["last_complete_snapshot"]["snapshot_no"] == 1
    assert report["aborted_simulation_batches_inserted"] == ["crashed-batch"]
    assert recovered.current_snapshot()[1] == baseline

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        snapshot_numbers = [row[0] for row in conn.execute("SELECT snapshot_no FROM snapshots ORDER BY snapshot_no")]
        batch = conn.execute(
            "SELECT status, result_json, diagnostics_json, error_code FROM simulation_batches "
            "WHERE batch_id='crashed-batch'"
        ).fetchone()
        intent = conn.execute(
            "SELECT state, detail_json FROM intents ORDER BY created_at_us DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert snapshot_numbers == [1]
    assert batch["status"] == "aborted"
    assert batch["result_json"] is None
    assert batch["diagnostics_json"] is None
    assert batch["error_code"] == "E_INTENT_INCOMPLETE"
    assert intent["state"] == "aborted"
    assert json.loads(intent["detail_json"])["recovery_state"] == "aborted"

