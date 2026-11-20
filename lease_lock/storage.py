from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from .model import canonical_json, checksum_json


SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resources (
    lock_key TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL DEFAULT '{}',
    revision INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    lock_key TEXT PRIMARY KEY,
    holder TEXT NOT NULL,
    lease_version INTEGER NOT NULL,
    acquired_at_ms INTEGER NOT NULL,
    ttl_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'released')),
    released_at_ms INTEGER,
    release_reason TEXT,
    dependency_expired INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS lease_dependencies (
    lock_key TEXT NOT NULL,
    parent_key TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('strong', 'weak')),
    PRIMARY KEY (lock_key, parent_key)
);

CREATE INDEX IF NOT EXISTS idx_lease_dependencies_parent
    ON lease_dependencies(parent_key);
CREATE INDEX IF NOT EXISTS idx_leases_expiry
    ON leases(state, expires_at_ms);

CREATE TABLE IF NOT EXISTS lock_versions (
    lock_key TEXT PRIMARY KEY,
    last_version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY,
    parent_version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    affected_json TEXT NOT NULL,
    checksum TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    base_version INTEGER,
    target_version INTEGER,
    status TEXT NOT NULL CHECK (status IN ('STARTED', 'COMMITTED', 'ABORTED')),
    payload_json TEXT,
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    pid INTEGER NOT NULL,
    error_code TEXT,
    message TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    generated_at_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.path),
            timeout=15.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 15000")
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    def initialize(self) -> None:
        with closing(self.connect()) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO metadata(key, value) VALUES ('snapshot_version', '0')"
            )
            conn.execute(
                "INSERT OR IGNORE INTO metadata(key, value) VALUES ('clock_high_water_ms', '0')"
            )
            conn.execute("PRAGMA user_version = 1")

    def get_meta(self, conn: sqlite3.Connection, key: str, default: str = "0") -> str:
        row = conn.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row is not None else default

    def set_meta(self, conn: sqlite3.Connection, key: str, value: Any) -> None:
        conn.execute(
            "INSERT INTO metadata(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )

    def insert_snapshot(self, conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO snapshots(version, parent_version, kind, created_at_ms, "
            "affected_json, checksum, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                int(payload["version"]),
                int(payload["parent_version"]),
                str(payload["kind"]),
                int(payload["created_at_ms"]),
                canonical_json(payload.get("affected", [])),
                str(payload["checksum"]),
                canonical_json(payload),
            ),
        )

    def get_snapshot_payload(
        self, conn: sqlite3.Connection, version: int
    ) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT payload_json FROM snapshots WHERE version = ?", (int(version),)
        ).fetchone()
        return _decode_valid_snapshot(row["payload_json"]) if row is not None else None

    def get_latest_snapshot_payload(
        self, conn: sqlite3.Connection
    ) -> dict[str, Any] | None:
        rows = conn.execute(
            "SELECT payload_json FROM snapshots ORDER BY version DESC"
        ).fetchall()
        for row in rows:
            payload = _decode_valid_snapshot(row["payload_json"])
            if payload is not None:
                return payload
        return None

    def insert_intent(
        self,
        conn: sqlite3.Connection,
        *,
        intent_id: str,
        kind: str,
        base_version: int | None,
        target_version: int | None,
        payload: dict[str, Any] | None,
        created_at_ms: int,
        pid: int,
    ) -> None:
        conn.execute(
            "INSERT INTO intents(intent_id, kind, base_version, target_version, status, "
            "payload_json, created_at_ms, pid) VALUES (?, ?, ?, ?, 'STARTED', ?, ?, ?)",
            (
                intent_id,
                kind,
                base_version,
                target_version,
                canonical_json(payload) if payload is not None else None,
                created_at_ms,
                pid,
            ),
        )

    def complete_intent(
        self,
        conn: sqlite3.Connection,
        intent_id: str,
        target_version: int | None,
        completed_at_ms: int,
    ) -> None:
        conn.execute(
            "UPDATE intents SET status = 'COMMITTED', target_version = ?, "
            "completed_at_ms = ? WHERE intent_id = ? AND status = 'STARTED'",
            (target_version, completed_at_ms, intent_id),
        )

    def abort_intent(
        self,
        conn: sqlite3.Connection,
        intent_id: str,
        *,
        completed_at_ms: int,
        error_code: str,
        message: str,
    ) -> None:
        conn.execute(
            "UPDATE intents SET status = 'ABORTED', completed_at_ms = ?, "
            "error_code = ?, message = ? WHERE intent_id = ? AND status = 'STARTED'",
            (completed_at_ms, error_code, message, intent_id),
        )


def _decode_valid_snapshot(raw: str) -> dict[str, Any] | None:
    payload = json.loads(raw)
    expected = payload.get("checksum")
    if not expected:
        return None
    state = {
        "version": payload.get("version"),
        "parent_version": payload.get("parent_version"),
        "kind": payload.get("kind"),
        "created_at_ms": payload.get("created_at_ms"),
        "leases": payload.get("leases", []),
    }
    return payload if checksum_json(state) == expected else None

