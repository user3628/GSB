from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .formats import canonical_json
from .timeutil import now_us


SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS queues (
    name TEXT PRIMARY KEY,
    semantics TEXT NOT NULL CHECK (semantics IN ('at_least_once', 'exactly_once')),
    visibility_timeout_ms INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    extensions_json TEXT NOT NULL DEFAULT '{}',
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL REFERENCES queues(name),
    body TEXT NOT NULL,
    content_type TEXT NOT NULL,
    produced_at_us INTEGER NOT NULL,
    expires_at_us INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'leased', 'acked', 'expired')),
    record_version INTEGER NOT NULL,
    last_write_at_us INTEGER NOT NULL,
    delivery_count INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires_at_us INTEGER,
    lease_intent_id TEXT,
    extensions_json TEXT NOT NULL DEFAULT '{}',
    source_format TEXT NOT NULL DEFAULT 'api',
    source_version INTEGER NOT NULL DEFAULT 1,
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_claim
    ON messages(queue_name, state, expires_at_us, lease_expires_at_us, produced_at_us, id);

CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    claimed_at_us INTEGER NOT NULL,
    lease_expires_at_us INTEGER NOT NULL,
    status TEXT NOT NULL,
    intent_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_deliveries_message ON deliveries(message_id, id);
CREATE INDEX IF NOT EXISTS idx_deliveries_intent ON deliveries(intent_id);

CREATE TABLE IF NOT EXISTS acks (
    message_id TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    acked_at_us INTEGER NOT NULL,
    delivery_count INTEGER NOT NULL,
    intent_id TEXT NOT NULL,
    PRIMARY KEY (message_id, consumer_id)
);

CREATE INDEX IF NOT EXISTS idx_acks_intent ON acks(intent_id);

CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state, created_at_us);

CREATE TABLE IF NOT EXISTS queue_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_name TEXT NOT NULL,
    queue_version INTEGER NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at_us INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_queue_snapshots_queue
    ON queue_snapshots(queue_name, queue_version DESC);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at_us INTEGER NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS import_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at_us INTEGER NOT NULL,
    format TEXT NOT NULL,
    format_version INTEGER NOT NULL,
    decision TEXT NOT NULL,
    committed INTEGER NOT NULL,
    report_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_import_reports_created
    ON import_reports(created_at_us DESC);
"""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=15.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(SCHEMA_SQL)
            connection.execute("PRAGMA user_version = 1")
        finally:
            connection.close()

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def begin_intent(self, category: str, payload: dict[str, Any]) -> str:
        intent_id = uuid.uuid4().hex
        timestamp = now_us()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO intents(id, category, state, payload_json, detail_json, created_at_us, updated_at_us)
                VALUES (?, ?, 'pending', ?, '{}', ?, ?)
                """,
                (intent_id, category, canonical_json(payload), timestamp, timestamp),
            )
        return intent_id

    def mark_intent_committed(
        self, connection: sqlite3.Connection, intent_id: str, detail: dict[str, Any] | None = None
    ) -> None:
        timestamp = now_us()
        cursor = connection.execute(
            """
            UPDATE intents SET state = 'committed', detail_json = ?, updated_at_us = ?
            WHERE id = ? AND state = 'pending'
            """,
            (canonical_json(detail or {}), timestamp, intent_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(f"intent {intent_id} was not pending at commit time")

    def abort_intent(self, intent_id: str, error: dict[str, Any]) -> None:
        timestamp = now_us()
        with self.transaction() as connection:
            connection.execute(
                """
                UPDATE intents SET state = 'aborted', detail_json = ?, updated_at_us = ?
                WHERE id = ? AND state = 'pending'
                """,
                (canonical_json(error), timestamp, intent_id),
            )

    def bump_queue_version_and_snapshot(
        self, connection: sqlite3.Connection, queue_name: str, *, timestamp: int | None = None
    ) -> int:
        timestamp = timestamp or now_us()
        cursor = connection.execute(
            """
            UPDATE queues SET version = version + 1, updated_at_us = ?
            WHERE name = ?
            """,
            (timestamp, queue_name),
        )
        if cursor.rowcount != 1:
            raise KeyError(queue_name)
        self.write_queue_snapshot(connection, queue_name, timestamp=timestamp)
        version = connection.execute(
            "SELECT version FROM queues WHERE name = ?", (queue_name,)
        ).fetchone()["version"]
        return int(version)

    def write_queue_snapshot(
        self, connection: sqlite3.Connection, queue_name: str, *, timestamp: int | None = None
    ) -> None:
        timestamp = timestamp or now_us()
        queue = connection.execute("SELECT * FROM queues WHERE name = ?", (queue_name,)).fetchone()
        if queue is None:
            raise KeyError(queue_name)
        rows = connection.execute(
            "SELECT * FROM messages WHERE queue_name = ? ORDER BY id", (queue_name,)
        ).fetchall()
        counts = {"pending": 0, "leased": 0, "acked": 0, "expired": 0}
        canonical_messages: list[dict[str, Any]] = []
        for row in rows:
            effective_state = row["state"]
            if effective_state in {"pending", "leased"} and row["expires_at_us"] <= timestamp:
                effective_state = "expired"
            counts[effective_state] += 1
            canonical_messages.append(
                {
                    "id": row["id"],
                    "body": row["body"],
                    "produced_at_us": row["produced_at_us"],
                    "expires_at_us": row["expires_at_us"],
                    "state": effective_state,
                    "record_version": row["record_version"],
                    "last_write_at_us": row["last_write_at_us"],
                    "delivery_count": row["delivery_count"],
                    "extensions": json.loads(row["extensions_json"]),
                }
            )
        digest_payload = canonical_json(canonical_messages).encode("utf-8")
        snapshot = {
            "queue": queue_name,
            "queue_version": queue["version"],
            "semantics": queue["semantics"],
            "visibility_timeout_ms": queue["visibility_timeout_ms"],
            "counts": counts,
            "message_count": len(rows),
            "message_digest_sha256": hashlib.sha256(digest_payload).hexdigest(),
            "created_at_us": timestamp,
        }
        connection.execute(
            """
            INSERT INTO queue_snapshots(queue_name, queue_version, snapshot_json, created_at_us)
            VALUES (?, ?, ?, ?)
            """,
            (queue_name, queue["version"], canonical_json(snapshot), timestamp),
        )

    def insert_import_report(
        self,
        *,
        timestamp: int,
        format_name: str,
        format_version: int,
        decision: str,
        committed: bool,
        report: dict[str, Any],
    ) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO import_reports(created_at_us, format, format_version, decision, committed, report_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    timestamp,
                    format_name,
                    format_version,
                    decision,
                    1 if committed else 0,
                    canonical_json(report),
                ),
            )

    def latest_recovery_report(self) -> dict[str, Any] | None:
        connection = self.connect()
        try:
            row = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()
            return json.loads(row["report_json"]) if row else None
        finally:
            connection.close()

    def latest_import_report(self) -> dict[str, Any] | None:
        connection = self.connect()
        try:
            row = connection.execute(
                "SELECT report_json FROM import_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()
            return json.loads(row["report_json"]) if row else None
        finally:
            connection.close()
