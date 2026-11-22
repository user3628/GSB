from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .models import NormalizedRegistration


SCHEMA = """
CREATE TABLE IF NOT EXISTS instances (
    service_name TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    weight_permille INTEGER NOT NULL CHECK (weight_permille BETWEEN 1 AND 1000),
    lease_ttl_ms INTEGER NOT NULL CHECK (lease_ttl_ms > 0),
    probe_method TEXT NOT NULL,
    probe_params_json TEXT NOT NULL,
    probe_window_ms INTEGER,
    lease_version INTEGER NOT NULL,
    lease_granted_at_ms INTEGER NOT NULL,
    lease_expires_at_ms INTEGER NOT NULL,
    last_registered_at_ms INTEGER NOT NULL,
    last_heartbeat_at_ms INTEGER NOT NULL,
    health_status TEXT NOT NULL,
    health_reason TEXT NOT NULL,
    record_version INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (service_name, instance_id)
);

CREATE TABLE IF NOT EXISTS registration_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    record_version INTEGER NOT NULL,
    source TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_registration_snapshots_key
    ON registration_snapshots(service_name, instance_id, id DESC);

CREATE TABLE IF NOT EXISTS lease_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    lease_version_before INTEGER,
    lease_version_after INTEGER,
    granted_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS intent_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_type TEXT NOT NULL,
    ref_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('STARTED', 'COMMITTED', 'ABORTED')),
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_intent_status ON intent_log(status, id);

CREATE TABLE IF NOT EXISTS import_batches (
    batch_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('STAGING', 'COMMITTED', 'REJECTED', 'ABORTED')),
    format TEXT,
    version INTEGER,
    kind TEXT,
    report_json TEXT,
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS probe_rounds (
    round_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('PROBING', 'COMMITTED', 'ABORTED')),
    budget_ms INTEGER NOT NULL,
    concurrency INTEGER NOT NULL,
    queue_size INTEGER NOT NULL,
    intent_id INTEGER,
    report_json TEXT,
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS probe_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id TEXT NOT NULL,
    service_name TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    probe_method TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'RUNNING', 'COMPLETED', 'ROLLED_BACK')),
    started_at_ms INTEGER,
    completed_at_ms INTEGER,
    result_json TEXT,
    FOREIGN KEY (round_id) REFERENCES probe_rounds(round_id)
);
CREATE INDEX IF NOT EXISTS idx_probe_attempts_round ON probe_attempts(round_id, state);

CREATE TABLE IF NOT EXISTS probe_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id TEXT NOT NULL,
    service_name TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    probe_method TEXT NOT NULL,
    checked_at_ms INTEGER NOT NULL,
    success INTEGER NOT NULL,
    status TEXT NOT NULL,
    outcome TEXT NOT NULL,
    latency_ms INTEGER NOT NULL,
    error_code TEXT,
    detail_json TEXT NOT NULL,
    weight_before INTEGER NOT NULL,
    weight_after INTEGER NOT NULL,
    lease_version INTEGER NOT NULL,
    health_status TEXT NOT NULL,
    record_version INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    UNIQUE(round_id, service_name, instance_id)
);
CREATE INDEX IF NOT EXISTS idx_probe_history_key
    ON probe_history(service_name, instance_id, id DESC);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
"""


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class InstanceRow:
    service_name: str
    instance_id: str
    endpoint: str
    weight_permille: int
    lease_ttl_ms: int
    probe_method: str
    probe_params: dict[str, Any]
    probe_window_ms: int | None
    lease_version: int
    lease_granted_at_ms: int
    lease_expires_at_ms: int
    last_registered_at_ms: int
    last_heartbeat_at_ms: int
    health_status: str
    health_reason: str
    record_version: int
    updated_at_ms: int

    @property
    def key(self) -> tuple[str, str]:
        return (self.service_name, self.instance_id)

    def to_dict(self, *, observed_at_ms: int | None = None) -> dict[str, Any]:
        now = now_ms() if observed_at_ms is None else observed_at_ms
        expired = now >= self.lease_expires_at_ms
        effective = "EXPIRED" if expired else self.health_status
        effective_reason = "lease expired" if expired else self.health_reason
        return {
            "service_name": self.service_name,
            "instance_id": self.instance_id,
            "endpoint": self.endpoint,
            "weight_permille": self.weight_permille,
            "lease_ttl_ms": self.lease_ttl_ms,
            "probe_method": self.probe_method,
            "probe_params": self.probe_params,
            "probe_window_ms": self.probe_window_ms,
            "lease_version": self.lease_version,
            "lease_granted_at_ms": self.lease_granted_at_ms,
            "lease_expires_at_ms": self.lease_expires_at_ms,
            "lease_remaining_ms": max(0, self.lease_expires_at_ms - now),
            "lease_expired": expired,
            "last_registered_at_ms": self.last_registered_at_ms,
            "last_heartbeat_at_ms": self.last_heartbeat_at_ms,
            "health_status": effective,
            "persisted_health_status": self.health_status,
            "effective_health_status": effective,
            "health_reason": effective_reason,
            "persisted_health_reason": self.health_reason,
            "record_version": self.record_version,
            "updated_at_ms": self.updated_at_ms,
        }

    def as_normalized(self) -> NormalizedRegistration:
        return NormalizedRegistration(
            service_name=self.service_name,
            instance_id=self.instance_id,
            endpoint=self.endpoint,
            weight_permille=self.weight_permille,
            lease_ttl_ms=self.lease_ttl_ms,
            probe_method=self.probe_method,
            probe_params=self.probe_params,
            registered_at_ms=self.last_registered_at_ms,
            lease_version=self.lease_version,
            probe_window_ms=self.probe_window_ms,
            last_heartbeat_at_ms=self.last_heartbeat_at_ms,
        )


def instance_from_row(row: sqlite3.Row) -> InstanceRow:
    return InstanceRow(
        service_name=row["service_name"],
        instance_id=row["instance_id"],
        endpoint=row["endpoint"],
        weight_permille=row["weight_permille"],
        lease_ttl_ms=row["lease_ttl_ms"],
        probe_method=row["probe_method"],
        probe_params=json.loads(row["probe_params_json"]),
        probe_window_ms=row["probe_window_ms"],
        lease_version=row["lease_version"],
        lease_granted_at_ms=row["lease_granted_at_ms"],
        lease_expires_at_ms=row["lease_expires_at_ms"],
        last_registered_at_ms=row["last_registered_at_ms"],
        last_heartbeat_at_ms=row["last_heartbeat_at_ms"],
        health_status=row["health_status"],
        health_reason=row["health_reason"],
        record_version=row["record_version"],
        updated_at_ms=row["updated_at_ms"],
    )


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def initialize(self) -> None:
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(SCHEMA)
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
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.execute("COMMIT")
        finally:
            connection.close()

    def get_instance(
        self,
        service_name: str,
        instance_id: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> InstanceRow | None:
        owns_connection = connection is None
        conn = connection or self.connect()
        try:
            row = conn.execute(
                "SELECT * FROM instances WHERE service_name = ? AND instance_id = ?",
                (service_name, instance_id),
            ).fetchone()
            return instance_from_row(row) if row is not None else None
        finally:
            if owns_connection:
                conn.close()

    def list_instances(
        self,
        *,
        service_name: str | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> list[InstanceRow]:
        owns_connection = connection is None
        conn = connection or self.connect()
        try:
            if service_name is None:
                rows = conn.execute(
                    "SELECT * FROM instances ORDER BY service_name, instance_id"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM instances WHERE service_name = ? ORDER BY instance_id",
                    (service_name,),
                ).fetchall()
            return [instance_from_row(row) for row in rows]
        finally:
            if owns_connection:
                conn.close()

    def write_instance(
        self,
        connection: sqlite3.Connection,
        record: NormalizedRegistration,
        *,
        granted_at_ms: int,
        health_status: str,
        health_reason: str,
        record_version: int,
        source: str,
    ) -> None:
        expires_at_ms = granted_at_ms + record.lease_ttl_ms
        heartbeat = record.last_heartbeat_at_ms if record.last_heartbeat_at_ms is not None else granted_at_ms
        connection.execute(
            """
            INSERT INTO instances (
                service_name, instance_id, endpoint, weight_permille, lease_ttl_ms,
                probe_method, probe_params_json, probe_window_ms, lease_version,
                lease_granted_at_ms, lease_expires_at_ms, last_registered_at_ms,
                last_heartbeat_at_ms, health_status, health_reason, record_version,
                updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(service_name, instance_id) DO UPDATE SET
                endpoint = excluded.endpoint,
                weight_permille = excluded.weight_permille,
                lease_ttl_ms = excluded.lease_ttl_ms,
                probe_method = excluded.probe_method,
                probe_params_json = excluded.probe_params_json,
                probe_window_ms = excluded.probe_window_ms,
                lease_version = excluded.lease_version,
                lease_granted_at_ms = excluded.lease_granted_at_ms,
                lease_expires_at_ms = excluded.lease_expires_at_ms,
                last_registered_at_ms = excluded.last_registered_at_ms,
                last_heartbeat_at_ms = excluded.last_heartbeat_at_ms,
                health_status = excluded.health_status,
                health_reason = excluded.health_reason,
                record_version = excluded.record_version,
                updated_at_ms = excluded.updated_at_ms
            """,
            (
                record.service_name,
                record.instance_id,
                record.endpoint,
                record.weight_permille,
                record.lease_ttl_ms,
                record.probe_method,
                canonical_json(record.probe_params),
                record.probe_window_ms,
                record.lease_version,
                granted_at_ms,
                expires_at_ms,
                record.registered_at_ms,
                heartbeat,
                health_status,
                health_reason,
                record_version,
                granted_at_ms,
            ),
        )
        connection.execute(
            """
            INSERT INTO registration_snapshots (
                service_name, instance_id, snapshot_json, record_version, source, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                record.service_name,
                record.instance_id,
                record.canonical_json(),
                record_version,
                source,
                granted_at_ms,
            ),
        )
        connection.execute(
            """
            INSERT INTO lease_events (
                service_name, instance_id, event_type, lease_version_before,
                lease_version_after, granted_at_ms, expires_at_ms, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.service_name,
                record.instance_id,
                "REGISTER" if source == "register" else source.upper(),
                None,
                record.lease_version,
                granted_at_ms,
                expires_at_ms,
                granted_at_ms,
            ),
        )

    def start_intent(self, intent_type: str, ref_id: str | None, detail: dict[str, Any] | None = None) -> int:
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO intent_log (intent_type, ref_id, status, detail_json, created_at_ms)
                VALUES (?, ?, 'STARTED', ?, ?)
                """,
                (intent_type, ref_id, canonical_json(detail or {}), now_ms()),
            )
            return int(cursor.lastrowid)

    def finish_intent(
        self,
        connection: sqlite3.Connection,
        intent_id: int,
        status: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        if status not in ("COMMITTED", "ABORTED"):
            raise ValueError("intent completion status must be COMMITTED or ABORTED")
        connection.execute(
            "UPDATE intent_log SET status = ?, detail_json = ?, completed_at_ms = ? WHERE id = ?",
            (status, canonical_json(detail or {}), now_ms(), intent_id),
        )

    def abort_intent(self, intent_id: int, detail: dict[str, Any] | None = None) -> None:
        with self.transaction() as connection:
            connection.execute(
                """
                UPDATE intent_log SET status = 'ABORTED', detail_json = ?, completed_at_ms = ?
                WHERE id = ? AND status = 'STARTED'
                """,
                (canonical_json(detail or {}), now_ms(), intent_id),
            )

    def recent_recovery_report(self) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            return json.loads(row["report_json"])
