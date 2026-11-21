from __future__ import annotations

import json
import uuid

from app.config import load_settings
from app.db import Database


def test_startup_recovery_rolls_back_unfinished_reservation(tmp_path, backend_fixture) -> None:
    settings = load_settings(backend_fixture["config_path"])
    database = Database(tmp_path / "recovery.sqlite3")
    database.initialize(settings)
    session_id = "sess-recovery"
    now = "2026-01-01T00:00:00.000+00:00"
    round_id = f"round-{uuid.uuid4().hex[:12]}"
    intent_id = "intent-unfinished"
    reservation_id = "reservation-unfinished"
    with database.transaction() as connection:
        connection.execute(
            """
            INSERT INTO sessions(id, object_key, backend_id, max_parts, total_parts, priority,
                status, version, conflict_policy, metadata_json, completed_parts, submitted_parts,
                created_at, updated_at)
            VALUES(?, 'bucket/recovery.bin', 'test-backend', 2, 2, 1, 'ACTIVE', 5,
                   'reject_active', '{}', 0, 1, ?, ?)
            """,
            (session_id, now, now),
        )
        connection.execute(
            """
            INSERT INTO object_metadata(session_id, object_key, status, manifest_json,
                object_digest, version, updated_at)
            VALUES(?, 'bucket/recovery.bin', 'DRAFT', '[]', NULL, 5, ?)
            """,
            (session_id, now),
        )
        connection.execute(
            """
            INSERT INTO parts(session_id, seq, size_bytes, sha256, arrived_at, status,
                round_id, attempt_count, created_at, updated_at)
            VALUES(?, 1, 512, ?, ?, 'UPLOADING', ?, 1, ?, ?)
            """,
            (session_id, "a" * 64, now, round_id, now, now),
        )
        connection.execute(
            """
            INSERT INTO scheduling_rounds(id, status, time_budget_ms, queue_limit,
                write_timeout_ms, read_timeout_ms, session_filter_json, summary_json, created_at)
            VALUES(?, 'RUNNING', 1000, 2, 1000, 1000, '[]', '{}', ?)
            """,
            (round_id, now),
        )
        connection.execute(
            """
            INSERT INTO schedule_intents(id, round_id, backend_id, session_id, seq, bytes,
                prior_part_status, state, created_at, updated_at)
            VALUES(?, ?, 'test-backend', ?, 1, 512, 'PENDING', 'UPLOADING', ?, ?)
            """,
            (intent_id, round_id, session_id, now, now),
        )
        connection.execute(
            """
            INSERT INTO quota_reservations(id, intent_id, backend_id, session_id, seq, bytes,
                status, created_at, updated_at)
            VALUES(?, ?, 'test-backend', ?, 1, 512, 'RESERVED', ?, ?)
            """,
            (reservation_id, intent_id, session_id, now, now),
        )
        connection.execute(
            "UPDATE backend_quota SET reserved_bytes=512, version=version+1 WHERE backend_id='test-backend'"
        )

    report = database.recover_after_crash()
    assert report["scope"] == "startup_recovery_only"
    assert round_id in report["aborted_rounds"]
    assert report["rolled_back_intents"][0]["intent_id"] == intent_id
    quota = database.backend_quota("test-backend")
    assert quota is not None
    assert quota["reserved_bytes"] == 0
    assert quota["remaining_bytes"] == 30000
    detail = database.read_session_snapshot(session_id)
    assert detail is not None
    assert detail["parts"][0]["status"] == "PENDING"
    assert detail["session"]["version"] == 6
    with database.transaction(immediate=False) as connection:
        round_row = connection.execute(
            "SELECT status, recovered_at FROM scheduling_rounds WHERE id=?", (round_id,)
        ).fetchone()
    assert round_row["status"] == "ABORTED"
    assert round_row["recovered_at"]
    latest = database.list_recovery_reports(1)[0]
    assert "cannot emit a complete graceful-exit report" in latest["report"]["note"]
    assert json.loads(json.dumps(latest))["report_id"] == report["report_id"]
