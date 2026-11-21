from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from app.models import GatewaySettings, utc_now


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    object_key TEXT NOT NULL,
    backend_id TEXT NOT NULL,
    max_parts INTEGER NOT NULL,
    total_parts INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    conflict_policy TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    completed_parts INTEGER NOT NULL DEFAULT 0,
    submitted_parts INTEGER NOT NULL DEFAULT 0,
    object_digest TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sessions_active_object
    ON sessions(object_key) WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS parts (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    arrived_at TEXT NOT NULL,
    status TEXT NOT NULL,
    round_id TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error_code TEXT,
    last_error_context_json TEXT NOT NULL DEFAULT '{}',
    verified_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(session_id, seq)
);

CREATE TABLE IF NOT EXISTS backends (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    capacity_bytes INTEGER NOT NULL,
    max_concurrency INTEGER NOT NULL,
    max_parts_per_session INTEGER NOT NULL,
    estimated_latency_ms INTEGER NOT NULL,
    enabled INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backend_quota (
    backend_id TEXT PRIMARY KEY REFERENCES backends(id),
    capacity_bytes INTEGER NOT NULL,
    committed_bytes INTEGER NOT NULL DEFAULT 0,
    reserved_bytes INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL,
    CHECK (committed_bytes >= 0),
    CHECK (reserved_bytes >= 0),
    CHECK (committed_bytes + reserved_bytes <= capacity_bytes)
);

CREATE TABLE IF NOT EXISTS scheduling_rounds (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    time_budget_ms INTEGER NOT NULL,
    queue_limit INTEGER NOT NULL,
    write_timeout_ms INTEGER NOT NULL,
    read_timeout_ms INTEGER NOT NULL,
    session_filter_json TEXT NOT NULL DEFAULT '[]',
    summary_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    finished_at TEXT,
    recovered_at TEXT
);

CREATE TABLE IF NOT EXISTS schedule_intents (
    id TEXT PRIMARY KEY,
    round_id TEXT NOT NULL REFERENCES scheduling_rounds(id),
    backend_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    prior_part_status TEXT,
    state TEXT NOT NULL,
    error_code TEXT,
    error_context_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(round_id, session_id, seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_schedule_part
    ON schedule_intents(backend_id, session_id, seq)
    WHERE state IN ('PREPARED', 'RESERVED', 'UPLOADING');

CREATE TABLE IF NOT EXISTS quota_reservations (
    id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL UNIQUE REFERENCES schedule_intents(id),
    backend_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS round_tasks (
    id TEXT PRIMARY KEY,
    round_id TEXT NOT NULL REFERENCES scheduling_rounds(id),
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    backend_id TEXT NOT NULL,
    state TEXT NOT NULL,
    business_code TEXT,
    reason TEXT,
    priority INTEGER NOT NULL,
    order_index INTEGER NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(round_id, session_id, seq)
);

CREATE TABLE IF NOT EXISTS object_metadata (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    object_key TEXT NOT NULL,
    status TEXT NOT NULL,
    manifest_json TEXT NOT NULL DEFAULT '[]',
    object_digest TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency (
    request_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    http_status INTEGER,
    response_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id TEXT,
    session_id TEXT,
    event_type TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    scope TEXT NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def initialize(self, settings: GatewaySettings) -> None:
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(SCHEMA)
        finally:
            connection.close()
        now = utc_now()
        with self.transaction() as connection:
            for backend in settings.backends:
                connection.execute(
                    """
                    INSERT INTO backends(id, url, capacity_bytes, max_concurrency,
                                         max_parts_per_session, estimated_latency_ms,
                                         enabled, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        url=excluded.url,
                        capacity_bytes=excluded.capacity_bytes,
                        max_concurrency=excluded.max_concurrency,
                        max_parts_per_session=excluded.max_parts_per_session,
                        estimated_latency_ms=excluded.estimated_latency_ms,
                        enabled=excluded.enabled,
                        updated_at=excluded.updated_at
                    """,
                    (
                        backend.id,
                        backend.url,
                        backend.capacity_bytes,
                        backend.max_concurrency,
                        backend.max_parts_per_session,
                        backend.estimated_latency_ms,
                        int(backend.enabled),
                        now,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO backend_quota(backend_id, capacity_bytes, committed_bytes,
                                              reserved_bytes, version, updated_at)
                    VALUES(?, ?, 0, 0, 1, ?)
                    ON CONFLICT(backend_id) DO UPDATE SET
                        capacity_bytes=excluded.capacity_bytes,
                        updated_at=excluded.updated_at
                    """,
                    (backend.id, backend.capacity_bytes, now),
                )

    def recover_after_crash(self) -> dict[str, Any]:
        """Roll back facts which never completed, preserving committed versions."""
        started_at = utc_now()
        with self.transaction() as connection:
            unfinished_intents = connection.execute(
                """
                SELECT i.*, r.status AS round_status, q.status AS reservation_status,
                       q.bytes AS reservation_bytes
                FROM schedule_intents i
                JOIN scheduling_rounds r ON r.id = i.round_id
                LEFT JOIN quota_reservations q ON q.intent_id = i.id
                WHERE i.state IN ('PREPARED', 'RESERVED', 'UPLOADING')
                   OR (r.status = 'RUNNING' AND i.state NOT IN ('COMMITTED', 'FAILED', 'ROLLED_BACK'))
                ORDER BY i.created_at, i.id
                """
            ).fetchall()
            running_rounds = connection.execute(
                "SELECT id FROM scheduling_rounds WHERE status = 'RUNNING' ORDER BY created_at"
            ).fetchall()
            committed_intents = connection.execute(
                """
                SELECT i.id, i.round_id, i.session_id, i.seq, i.backend_id, i.bytes
                FROM schedule_intents i
                JOIN scheduling_rounds r ON r.id = i.round_id
                WHERE r.status = 'RUNNING' AND i.state = 'COMMITTED'
                ORDER BY i.created_at, i.id
                """
            ).fetchall()

            rolled_back: list[dict[str, Any]] = []
            affected_sessions: set[str] = set()
            now = utc_now()
            for intent in unfinished_intents:
                affected_sessions.add(intent["session_id"])
                if intent["reservation_status"] == "RESERVED":
                    connection.execute(
                        """
                        UPDATE backend_quota
                        SET reserved_bytes = MAX(0, reserved_bytes - ?),
                            version = version + 1,
                            updated_at = ?
                        WHERE backend_id = ?
                        """,
                        (intent["reservation_bytes"], now, intent["backend_id"]),
                    )
                    connection.execute(
                        "UPDATE quota_reservations SET status='RELEASED', updated_at=? WHERE intent_id=?",
                        (now, intent["id"]),
                    )
                prior_status = intent["prior_part_status"] or "PENDING"
                connection.execute(
                    """
                    UPDATE parts
                    SET status=?, round_id=NULL, last_error_code='E_RECOVERED_ABORT',
                        last_error_context_json=?, updated_at=?
                    WHERE session_id=? AND seq=? AND status='UPLOADING'
                    """,
                    (
                        prior_status,
                        json.dumps(
                            {
                                "reason": "startup_recovery_rollback",
                                "round_id": intent["round_id"],
                                "intent_id": intent["id"],
                            },
                            sort_keys=True,
                        ),
                        now,
                        intent["session_id"],
                        intent["seq"],
                    ),
                )
                connection.execute(
                    """
                    UPDATE schedule_intents
                    SET state='ROLLED_BACK', error_code='E_RECOVERED_ABORT',
                        error_context_json=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        json.dumps({"round_id": intent["round_id"]}, sort_keys=True),
                        now,
                        intent["id"],
                    ),
                )
                connection.execute(
                    """
                    UPDATE round_tasks SET state='ROLLED_BACK', business_code='E_RECOVERED_ABORT',
                        reason='startup recovery rolled back unfinished intent', updated_at=?
                    WHERE round_id=? AND session_id=? AND seq=?
                    """,
                    (now, intent["round_id"], intent["session_id"], intent["seq"]),
                )
                rolled_back.append(
                    {
                        "intent_id": intent["id"],
                        "round_id": intent["round_id"],
                        "session_id": intent["session_id"],
                        "seq": intent["seq"],
                        "backend_id": intent["backend_id"],
                        "bytes": intent["bytes"],
                        "prior_part_status": prior_status,
                        "reservation_was": intent["reservation_status"],
                    }
                )

            aborted_rounds: list[str] = []
            for round_row in running_rounds:
                round_id = round_row["id"]
                connection.execute(
                    """
                    UPDATE scheduling_rounds
                    SET status='ABORTED', recovered_at=?,
                        summary_json=?
                    WHERE id=?
                    """,
                    (
                        now,
                        json.dumps(
                            {
                                "recovered": True,
                                "reason": "process_terminated_before_round_commit",
                            },
                            sort_keys=True,
                        ),
                        round_id,
                    ),
                )
                aborted_rounds.append(round_id)

            for session_id in sorted(affected_sessions):
                counts = connection.execute(
                    """
                    SELECT COUNT(*) AS submitted,
                           SUM(CASE WHEN status='VERIFIED' THEN 1 ELSE 0 END) AS verified
                    FROM parts WHERE session_id=?
                    """,
                    (session_id,),
                ).fetchone()
                connection.execute(
                    """
                    UPDATE sessions SET version=version+1, submitted_parts=?,
                        completed_parts=?, updated_at=?
                    WHERE id=?
                    """,
                    (counts["submitted"] or 0, counts["verified"] or 0, now, session_id),
                )

            idempotency_aborted = connection.execute(
                "SELECT request_id FROM idempotency WHERE state='IN_PROGRESS'"
            ).fetchall()
            for row in idempotency_aborted:
                connection.execute(
                    "UPDATE idempotency SET state='ABORTED', updated_at=? WHERE request_id=?",
                    (now, row["request_id"]),
                )

            quotas = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT backend_id, capacity_bytes, committed_bytes, reserved_bytes,
                           capacity_bytes - committed_bytes - reserved_bytes AS remaining_bytes,
                           version
                    FROM backend_quota ORDER BY backend_id
                    """
                ).fetchall()
            ]
            report = {
                "scope": "startup_recovery_only",
                "note": (
                    "Report reflects only facts durably visible after restart. A forcibly "
                    "terminated process cannot emit a complete graceful-exit report."
                ),
                "started_at": started_at,
                "completed_at": now,
                "aborted_rounds": aborted_rounds,
                "rolled_back_intents": rolled_back,
                "committed_intents_preserved": [dict(row) for row in committed_intents],
                "idempotency_requests_aborted": [row["request_id"] for row in idempotency_aborted],
                "quota_after": quotas,
            }
            report_id = f"recovery-{uuid.uuid4().hex[:16]}"
            connection.execute(
                """
                INSERT INTO recovery_reports(id, started_at, completed_at, scope, report_json)
                VALUES(?, ?, ?, ?, ?)
                """,
                (report_id, started_at, now, "startup_recovery_only", json.dumps(report, sort_keys=True)),
            )
            connection.execute(
                """
                INSERT INTO audit_events(round_id, session_id, event_type, detail_json, created_at)
                VALUES(NULL, NULL, 'STARTUP_RECOVERY', ?, ?)
                """,
                (
                    json.dumps(
                        {
                            "report_id": report_id,
                            "rolled_back": len(rolled_back),
                            "aborted_rounds": aborted_rounds,
                        },
                        sort_keys=True,
                    ),
                    now,
                ),
            )
            report["report_id"] = report_id
            return report

    def list_recovery_reports(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.transaction(immediate=False) as connection:
            rows = connection.execute(
                "SELECT id, started_at, completed_at, scope, report_json FROM recovery_reports ORDER BY completed_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            {
                "report_id": row["id"],
                "started_at": row["started_at"],
                "completed_at": row["completed_at"],
                "scope": row["scope"],
                "report": json.loads(row["report_json"]),
            }
            for row in rows
        ]

    def backend_quota(self, backend_id: str) -> dict[str, Any] | None:
        with self.transaction(immediate=False) as connection:
            row = connection.execute(
                """
                SELECT q.backend_id, q.capacity_bytes, q.committed_bytes, q.reserved_bytes,
                       q.capacity_bytes - q.committed_bytes - q.reserved_bytes AS remaining_bytes,
                       q.version
                FROM backend_quota q WHERE q.backend_id=?
                """,
                (backend_id,),
            ).fetchone()
        return dict(row) if row else None

    def read_session_snapshot(self, session_id: str) -> dict[str, Any] | None:
        """Read the whole session projection from one SQLite snapshot."""
        with self.transaction(immediate=False) as connection:
            session = connection.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
            if session is None:
                return None
            parts = connection.execute(
                "SELECT * FROM parts WHERE session_id=? ORDER BY seq", (session_id,)
            ).fetchall()
            metadata = connection.execute(
                "SELECT * FROM object_metadata WHERE session_id=?", (session_id,)
            ).fetchone()
            quota = connection.execute(
                "SELECT * FROM backend_quota WHERE backend_id=?", (session["backend_id"],)
            ).fetchone()
        return {
            "session": dict(session),
            "parts": [dict(row) for row in parts],
            "object_metadata": dict(metadata) if metadata else None,
            "backend_quota": dict(quota) if quota else None,
            "read_version": session["version"],
        }

    def list_audit_events(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.transaction(immediate=False) as connection:
            rows = connection.execute(
                "SELECT * FROM audit_events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(row) for row in rows]

    def get_idempotency(self, request_id: str) -> dict[str, Any] | None:
        with self.transaction(immediate=False) as connection:
            row = connection.execute(
                "SELECT * FROM idempotency WHERE request_id=?", (request_id,)
            ).fetchone()
        return dict(row) if row else None

    def claim_idempotency(
        self, request_id: str, operation: str, request_hash: str
    ) -> tuple[str, dict[str, Any] | None]:
        now = utc_now()
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM idempotency WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None:
                connection.execute(
                    """
                    INSERT INTO idempotency(request_id, operation, request_hash, state, created_at, updated_at)
                    VALUES(?, ?, ?, 'IN_PROGRESS', ?, ?)
                    """,
                    (request_id, operation, request_hash, now, now),
                )
                return "CLAIMED", None
            if row["operation"] != operation or row["request_hash"] != request_hash:
                return "CONFLICT", dict(row)
            if row["state"] == "COMPLETED":
                return "COMPLETED", dict(row)
            if row["state"] == "ABORTED":
                connection.execute(
                    """
                    UPDATE idempotency SET state='IN_PROGRESS', http_status=NULL,
                        response_json=NULL, updated_at=? WHERE request_id=?
                    """,
                    (now, request_id),
                )
                return "CLAIMED", dict(row)
            return "IN_PROGRESS", dict(row)

    def finish_idempotency(
        self, request_id: str, http_status: int, response: dict[str, Any]
    ) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                UPDATE idempotency SET state='COMPLETED', http_status=?, response_json=?,
                    updated_at=? WHERE request_id=?
                """,
                (http_status, json.dumps(response, sort_keys=True), utc_now(), request_id),
            )
