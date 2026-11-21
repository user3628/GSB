from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS policy_snapshots (
    version INTEGER PRIMARY KEY,
    generated_at_ms INTEGER NOT NULL,
    checksum TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    policy_version INTEGER NOT NULL,
    capacity_units INTEGER NOT NULL,
    rps_units INTEGER NOT NULL,
    burst_units INTEGER NOT NULL,
    weight_units INTEGER NOT NULL,
    parent_id TEXT
);

CREATE TABLE IF NOT EXISTS tenant_states (
    tenant_id TEXT PRIMARY KEY,
    tokens_units INTEGER NOT NULL,
    last_refill_ms INTEGER NOT NULL,
    refill_remainder INTEGER NOT NULL DEFAULT 0,
    direct_used_bytes_units INTEGER NOT NULL DEFAULT 0,
    aggregate_used_bytes_units INTEGER NOT NULL DEFAULT 0,
    direct_used_request_units INTEGER NOT NULL DEFAULT 0,
    aggregate_used_request_units INTEGER NOT NULL DEFAULT 0,
    consecutive_rejections INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    last_reason TEXT,
    suspended_reason TEXT,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS clock_state (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    last_event_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    policy_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0,
    accepted_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    faults_json TEXT NOT NULL DEFAULT '{}',
    summary_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS events (
    batch_id TEXT NOT NULL,
    event_index INTEGER NOT NULL,
    event_time_ms INTEGER NOT NULL,
    tenant_id TEXT NOT NULL,
    meter_mode TEXT NOT NULL,
    io_direction TEXT NOT NULL,
    payload_units INTEGER NOT NULL,
    request_weight_units INTEGER NOT NULL,
    accepted INTEGER NOT NULL,
    code TEXT NOT NULL,
    context_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    PRIMARY KEY (batch_id, event_index)
);

CREATE TABLE IF NOT EXISTS usage_ledger (
    ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    event_index INTEGER NOT NULL,
    account_tenant_id TEXT NOT NULL,
    source_tenant_id TEXT NOT NULL,
    capacity_units INTEGER NOT NULL,
    request_units INTEGER NOT NULL,
    event_time_ms INTEGER NOT NULL,
    policy_version INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_usage_ledger_account
    ON usage_ledger(account_tenant_id, policy_version);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    batch_id TEXT,
    status TEXT NOT NULL,
    before_state_json TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);

CREATE TABLE IF NOT EXISTS simulation_runs (
    run_id TEXT PRIMARY KEY,
    policy_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    label TEXT,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    recovered_at_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS instance_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    at_ms INTEGER NOT NULL,
    detail_json TEXT NOT NULL
);
"""


class Store:
    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            str(self.database_path),
            isolation_level=None,
            check_same_thread=False,
            timeout=10.0,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout=10000")
        with self._lock:
            self._connection.executescript(SCHEMA)

    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            connection = self._connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            yield self._connection

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        """Use one SQLite read snapshot across several SELECT statements."""

        with self._lock:
            connection = self._connection
            connection.execute("BEGIN")
            try:
                yield connection
            finally:
                connection.rollback()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def scalar(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        with self._lock:
            row = self._connection.execute(sql, params).fetchone()
            return None if row is None else row[0]

    def rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._connection.execute(sql, params).fetchall())

    def one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._connection.execute(sql, params).fetchone()

    def current_policy(self) -> sqlite3.Row | None:
        return self.one("SELECT * FROM policy_snapshots ORDER BY version DESC LIMIT 1")

    def current_version(self) -> int:
        version = self.scalar("SELECT MAX(version) FROM policy_snapshots")
        return int(version) if version is not None else 0

    def last_event_ms(self) -> int:
        row = self.one("SELECT last_event_ms FROM clock_state WHERE singleton_id = 1")
        return int(row["last_event_ms"]) if row is not None else 0

    def tenant_rows(self) -> dict[str, sqlite3.Row]:
        return {row["tenant_id"]: row for row in self.rows("SELECT * FROM tenants ORDER BY tenant_id")}

    def state_rows(self) -> dict[str, sqlite3.Row]:
        return {
            row["tenant_id"]: row
            for row in self.rows("SELECT * FROM tenant_states ORDER BY tenant_id")
        }

    def state_snapshot(self, tenant_ids: list[str] | None = None) -> dict[str, Any]:
        if tenant_ids is None:
            states = self.rows("SELECT * FROM tenant_states ORDER BY tenant_id")
        else:
            unique_ids = sorted(set(tenant_ids))
            if not unique_ids:
                states = []
            else:
                placeholders = ",".join("?" for _ in unique_ids)
                states = self.rows(
                    f"SELECT * FROM tenant_states WHERE tenant_id IN ({placeholders}) ORDER BY tenant_id",
                    tuple(unique_ids),
                )
        return {
            "last_event_ms": self.last_event_ms(),
            "states": [dict(row) for row in states],
        }

    def write_instance_event(self, event_type: str, at_ms: int, detail: dict[str, Any]) -> None:
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO instance_events(event_type, at_ms, detail_json) VALUES (?, ?, ?)",
                (event_type, at_ms, json.dumps(detail, sort_keys=True)),
            )
