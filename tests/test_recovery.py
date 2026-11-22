from __future__ import annotations

from hotroute import samples
from hotroute.engine import Engine
from hotroute.errors import BusinessError, E_REQUEST_INTERRUPTED
from hotroute.storage import connect_database, immediate_transaction
from hotroute.utils import canonical_json, utc_now


def _insert_unfinished_intent(db_path: str, snapshot_seq: int) -> str:
    intent_id = "intent-test-started"
    connection = connect_database(db_path)
    try:
        with immediate_transaction(connection):
            connection.execute(
                """
                INSERT INTO intents(
                    intent_id, kind, status, snapshot_seq, base_token,
                    details_json, created_at, completed_at, recovery_note
                ) VALUES(?, 'increment_apply', 'started', ?, NULL, ?, ?, NULL, NULL)
                """,
                (
                    intent_id,
                    snapshot_seq,
                    canonical_json({"test": True}),
                    utc_now(),
                ),
            )
    finally:
        connection.close()
    return intent_id


def test_startup_recovery_aborts_intent_and_interrupts_pending_binding(db_path: str):
    engine = Engine(db_path)
    first = engine.submit_snapshot(samples.initial_snapshot(snapshot_seq=1))
    engine.decide_gray("pending-before-crash", samples.MODEL_ID)
    intent_id = _insert_unfinished_intent(db_path, 2)

    restarted = Engine(db_path)
    report = restarted.get_recovery_report()
    assert report["last_committed_snapshot_seq"] == 1
    assert report["state_version"] == first["state_version"]
    assert any(item["intent_id"] == intent_id for item in report["aborted_intents"])
    assert any(item["request_id"] == "pending-before-crash" for item in report["interrupted_bindings"])
    assert report["details"]["startup_scan_complete"] is True
    assert report["details"]["persisted_facts_only"] is True
    assert report["details"]["exit_report_available"] is False

    state = restarted.get_state()
    assert state["snapshot_seq"] == 1
    assert state["state_version"] == first["state_version"]
    try:
        restarted.infer("pending-before-crash", samples.MODEL_ID, seed=9)
    except BusinessError as exc:
        assert exc.code == E_REQUEST_INTERRUPTED
    else:
        raise AssertionError("interrupted binding was allowed to continue")

    quant = restarted.submit_increment(
        samples.quant_increment(first["state_version"], snapshot_seq=2)
    )
    final_state = restarted.get_state()
    assert quant["snapshot_seq"] == 2
    assert final_state["snapshot_seq"] == 2
    assert {artifact["snapshot_seq"] for artifact in final_state["artifacts"]} == {2}


def test_recovery_report_records_rolled_back_future_artifacts(engine: Engine):
    _ = engine.submit_snapshot(samples.initial_snapshot(snapshot_seq=1))
    connection = connect_database(engine.db_path)
    try:
        with immediate_transaction(connection):
            connection.execute(
                """
                INSERT INTO derived_artifacts(
                    model_id, version, kind, snapshot_seq, generation, payload_json
                ) VALUES('stale-model', 'v9', 'route', 99, 1, '{}')
                """
            )
    finally:
        connection.close()
    restarted = Engine(engine.db_path)
    report = restarted.get_recovery_report()
    assert report["details"]["rolled_back"]["derived_artifacts"] == 1
    assert report["last_committed_snapshot_seq"] == 1

