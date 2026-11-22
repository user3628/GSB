from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from mqmw.store import SQLiteStore

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "tests" / "crash_helper.py"


def crash(db: Path, mode: str) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.pop("MQMW_FAULT_POINT", None)
    env.pop("MQMW_FAULT_EXIT_CODE", None)
    return subprocess.run(
        [sys.executable, str(HELPER), str(db), mode],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_forced_exit_during_dispatch_recovers_to_pending(tmp_path):
    db = tmp_path / "dispatch_crash.db"
    process = crash(db, "dispatch")
    assert process.returncode == 97, (process.stdout, process.stderr)

    store = SQLiteStore(str(db))
    before = store.get_message("crash-q", "crash-dispatch")
    assert before["status"] == "DISPATCHED"
    assert before["deliveries"][0]["state"] == "DISPATCHED"
    unfinished = [i for i in before["intents"] if i["kind"] == "DISPATCH"]
    assert unfinished[0]["state"] == "APPLIED"

    report = store.recover()
    assert report["unfinished_intents_scanned"] == 1
    assert len(report["dispatch_rollbacks"]) == 1
    rollback = report["dispatch_rollbacks"][0]
    assert rollback["from_state"] == "DISPATCHED"
    assert rollback["to_state"] == "PENDING"
    assert rollback["message_id"] == "crash-dispatch"

    after = store.get_message("crash-q", "crash-dispatch")
    assert after["status"] == "PENDING"
    assert after["active_delivery_id"] is None
    assert after["deliveries"][0]["state"] == "ROLLED_BACK"
    assert after["last_error"] == "E_RECOVERED_UNCOMMITTED_DISPATCH"
    assert report["durable_facts_after_recovery"]["message_status_counts"]["PENDING"] == 1

    # Recovery is idempotent and leaves no unfinished business intent.
    second = store.recover()
    assert second["actions"] == []
    assert store.get_latest_recovery_report()["report_id"] == second["report_id"]

    # Sequence allocation remains gapless after rollback.
    second_enqueue = store.enqueue(
        "crash-q",
        [
            {
                "message_id": "crash-next",
                "body": {"next": True},
                "visibility_timeout_ms": 10_000,
                "max_retries": 1,
            }
        ],
    )
    assert second_enqueue["messages"][0]["seq"] == 2


def test_forced_exit_during_ack_rolls_back_to_unconfirmed(tmp_path):
    db = tmp_path / "ack_crash.db"
    process = crash(db, "ack")
    assert process.returncode == 97, (process.stdout, process.stderr)

    store = SQLiteStore(str(db))
    before = store.get_message("crash-q", "crash-ack")
    assert before["status"] == "ACKED"
    assert before["acks"][0]["state"] == "APPLIED"
    ack_intent = next(i for i in before["intents"] if i["kind"] == "ACK")
    assert ack_intent["state"] == "APPLIED"
    assert store.get_queue_summary("crash-q")["group_offsets"][0]["next_seq"] == 2

    report = store.recover()
    assert len(report["ack_rollbacks"]) == 1
    rollback = report["ack_rollbacks"][0]
    assert rollback["from_state"] == "ACKED"
    assert rollback["to_state"] == "DISPATCHED"

    after = store.get_message("crash-q", "crash-ack")
    assert after["status"] == "DISPATCHED"
    assert after["acks"][0]["state"] == "ROLLED_BACK"
    assert after["active_delivery_id"] == after["deliveries"][0]["delivery_id"]
    assert after["deliveries"][0]["state"] == "DISPATCHED"
    offset = next(
        o for o in store.get_queue_summary("crash-q")["group_offsets"]
        if o["group_name"] == "crash-g"
    )
    assert offset["next_seq"] == 1
    assert report["durable_facts_after_recovery"]["message_status_counts"]["DISPATCHED"] == 1
