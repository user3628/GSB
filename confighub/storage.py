from __future__ import annotations

import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .util import canonical_json, json_loads, utc_now_iso


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY,
    generated_at TEXT NOT NULL,
    config_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clients (
    client_id TEXT PRIMARY KEY,
    config_keys_json TEXT NOT NULL,
    endpoint_url TEXT NOT NULL,
    timeout_seconds REAL NOT NULL,
    sample_seed INTEGER NOT NULL,
    sample_size INTEGER NOT NULL,
    feature_count INTEGER NOT NULL,
    applied_version INTEGER,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (applied_version) REFERENCES snapshots(version)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    target_version INTEGER NOT NULL UNIQUE,
    state TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    report_json TEXT,
    FOREIGN KEY (target_version) REFERENCES snapshots(version)
);

CREATE TABLE IF NOT EXISTS push_records (
    client_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (client_id, version),
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (version) REFERENCES snapshots(version),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE TABLE IF NOT EXISTS push_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ack_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    batch_id TEXT,
    accepted INTEGER NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    observed_version INTEGER,
    result_digest TEXT,
    result_json TEXT,
    received_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    batch_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    result_digest TEXT NOT NULL,
    result_json TEXT NOT NULL,
    eligible INTEGER NOT NULL,
    exclusion_reason TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (batch_id, client_id, version),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id),
    FOREIGN KEY (client_id) REFERENCES clients(client_id),
    FOREIGN KEY (version) REFERENCES snapshots(version)
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    aggregate_key TEXT NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_push_records_batch ON push_records(batch_id, state);
CREATE INDEX IF NOT EXISTS idx_ack_records_batch ON ack_records(batch_id, accepted);
CREATE INDEX IF NOT EXISTS idx_results_batch ON results(batch_id, eligible);
CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state);
"""


class Storage:
    """SQLite access with one serialized writer and concurrent WAL readers."""

    def __init__(self, db_path: str | Path, *, initialize: bool = True) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self._write_lock = threading.RLock()
        self._initialized = False
        if initialize:
            self.initialize()

    def initialize(self) -> None:
        if self._initialized:
            return
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._write_lock:
            connection = self._connect()
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                connection.execute("PRAGMA foreign_keys=ON")
                connection.executescript(SCHEMA)
                connection.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES('current_version', '0')"
                )
                connection.commit()
            finally:
                connection.close()
        self._initialized = True

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.db_path),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            yield connection
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

    def query_one(self, sql: str, parameters: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            row = connection.execute(sql, parameters).fetchone()
        return dict(row) if row is not None else None

    def query_all(self, sql: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.read_transaction() as connection:
            rows = connection.execute(sql, parameters).fetchall()
        return [dict(row) for row in rows]

    def begin_intent(self, kind: str, aggregate_key: str, payload: dict[str, Any]) -> str:
        intent_id = uuid.uuid4().hex
        now = utc_now_iso()
        with self.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO intents(intent_id, kind, aggregate_key, state, payload_json,
                                    created_at, updated_at)
                VALUES(?, ?, ?, 'STARTED', ?, ?, ?)
                """,
                (intent_id, kind, aggregate_key, canonical_json(payload), now, now),
            )
        return intent_id

    def finish_intent(self, intent_id: str, state: str) -> None:
        with self.write_transaction() as connection:
            connection.execute(
                "UPDATE intents SET state=?, updated_at=? WHERE intent_id=?",
                (state, utc_now_iso(), intent_id),
            )

    def list_started_intents(self) -> list[dict[str, Any]]:
        rows = self.query_all(
            "SELECT * FROM intents WHERE state='STARTED' ORDER BY created_at, intent_id"
        )
        for row in rows:
            row["payload"] = json_loads(row.pop("payload_json"), {})
        return rows

    def save_recovery_report(self, report: dict[str, Any]) -> int:
        with self.write_transaction() as connection:
            cursor = connection.execute(
                "INSERT INTO recovery_reports(report_json, created_at) VALUES(?, ?)",
                (canonical_json(report), utc_now_iso()),
            )
            return int(cursor.lastrowid)

    def latest_recovery_report(self) -> dict[str, Any] | None:
        row = self.query_one(
            "SELECT report_json, created_at FROM recovery_reports ORDER BY id DESC LIMIT 1"
        )
        if row is None:
            return None
        report = json_loads(row["report_json"], {})
        report["persisted_at"] = row["created_at"]
        return report

    def visibility_invariant_violations(self) -> list[dict[str, Any]]:
        """Return committed rows that would expose an unsafe intermediate state."""

        unsafe_acked = self.query_all(
            """
            SELECT p.client_id, p.version, p.state, c.applied_version
            FROM push_records p
            JOIN clients c ON c.client_id=p.client_id
            WHERE p.state='ACKED'
              AND (c.applied_version IS NULL OR c.applied_version < p.version)
            """
        )
        unsafe_applied = self.query_all(
            """
            SELECT p.client_id, p.version, p.state, c.applied_version
            FROM push_records p
            JOIN clients c ON c.client_id=p.client_id
            WHERE p.state='APPLIED'
              AND (c.applied_version IS NULL OR c.applied_version < p.version)
            """
        )
        return unsafe_acked + unsafe_applied

