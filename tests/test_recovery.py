from __future__ import annotations

from pathlib import Path

from msmq.db import Database
from msmq.recovery import recover_pending_intents
from msmq.service import MQService

from scripts.run_validation import check_forced_termination_recovery


def test_pending_claim_intent_is_rolled_back_from_durable_facts(tmp_path: Path) -> None:
    database = Database(tmp_path / "recovery.sqlite3")
    database.initialize()
    service = MQService(database)
    service.create_queue(name="q", semantics="at_least_once", visibility_timeout_ms=1_000)
    service.enqueue(queue="q", body="body", message_id="m-1", produced_at_ms=4_102_444_800_000)
    database.begin_intent("claim", {"queue": "q", "consumer_id": "c-1"})
    report = recover_pending_intents(database)
    assert report["rolled_back_claims"] == 1
    assert report["evidence"]["runtime_memory_used"] is False
    snapshot = service.snapshot("q")
    assert snapshot["messages"][0]["stored_state"] == "pending"
    assert snapshot["messages"][0]["delivery_count"] == 0


def test_pending_ack_effect_is_repaired_from_intent_evidence(tmp_path: Path) -> None:
    database = Database(tmp_path / "recovery-ack.sqlite3")
    database.initialize()
    service = MQService(database)
    service.create_queue(name="q", semantics="exactly_once", visibility_timeout_ms=1_000)
    service.enqueue(queue="q", body="body", message_id="m-1", produced_at_ms=4_102_444_800_000)
    claimed = service.claim(queue="q", consumer_id="c-1")["message"]
    with database.transaction() as connection:
        row = connection.execute("SELECT * FROM messages WHERE id = 'm-1'").fetchone()
    pre_state = {
        "message_id": "m-1",
        "state": row["state"],
        "lease_owner": row["lease_owner"],
        "lease_token": row["lease_token"],
        "lease_expires_at_us": row["lease_expires_at_us"],
        "delivery_count": row["delivery_count"],
        "last_write_at_us": row["last_write_at_us"],
    }
    intent_id = database.begin_intent(
        "ack",
        {
            "message_id": "m-1",
            "consumer_id": "c-1",
            "lease_token": claimed["lease_token"],
            "pre_state": pre_state,
        },
    )
    with database.transaction() as connection:
        connection.execute(
            "INSERT INTO acks(message_id, consumer_id, acked_at_us, delivery_count, intent_id) VALUES ('m-1', 'c-1', 1, 1, ?)",
            (intent_id,),
        )
        connection.execute("UPDATE messages SET state = 'acked', lease_token = NULL WHERE id = 'm-1'")
    report = recover_pending_intents(database)
    assert report["rolled_back_acks"] == 1
    snapshot = service.snapshot("q")
    assert snapshot["ack_count"] == 0
    assert snapshot["messages"][0]["stored_state"] == "leased"
    assert snapshot["messages"][0]["lease_token"] == claimed["lease_token"]


def test_real_forced_termination_recovery_pipeline() -> None:
    details = check_forced_termination_recovery()
    assert details["claim_crash_exit_code"] == 97
    assert details["ack_crash_exit_code"] == 97
    assert details["rolled_back_claims"] >= 1
    assert details["rolled_back_acks"] >= 1
    assert details["runtime_memory_used"] is False
    assert details["final_ack_count"] == 1

