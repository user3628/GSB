"""SQLite persistence layer.

Design notes
------------
* Single-writer: every write transaction is serialized through one
  ``threading.Lock`` and executed with ``BEGIN IMMEDIATE`` so concurrent
  rounds never interleave inside a commit boundary.
* Write-ahead intents: mutating workflows first insert a row into
  ``intents`` (status='pending') and commit it, then run the real work and
  mark the intent committed/aborted.  After a hard kill, recovery scans for
  pending intents and rolls quota/state back to the last complete version.
* Readers only ever see fully committed versions (SQLite WAL snapshot
  isolation), which satisfies "queries observe one complete commit version".
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS topologies (
    topology_id   TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    content_hash  TEXT NOT NULL UNIQUE,
    payload       TEXT NOT NULL,
    created_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS plans (
    plan_id       TEXT PRIMARY KEY,
    topology_id   TEXT NOT NULL REFERENCES topologies(topology_id),
    name          TEXT NOT NULL,
    content_hash  TEXT NOT NULL UNIQUE,
    payload       TEXT NOT NULL,
    created_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS rounds (
    round_id      TEXT PRIMARY KEY,
    plan_id       TEXT NOT NULL REFERENCES plans(plan_id),
    round_no      INTEGER NOT NULL,
    status        TEXT NOT NULL,            -- RUNNING | COMPLETED | ABORTED
    started_at    REAL,
    finished_at   REAL,
    time_budget_ms INTEGER NOT NULL,
    time_used_ms  INTEGER NOT NULL DEFAULT 0,
    UNIQUE(plan_id, round_no)
);
CREATE TABLE IF NOT EXISTS injections (
    injection_id  TEXT PRIMARY KEY,
    plan_id       TEXT NOT NULL,
    round_id      TEXT,
    item_key      TEXT NOT NULL,
    service       TEXT NOT NULL,
    fault_domain  TEXT NOT NULL,
    fault_type    TEXT NOT NULL,
    priority      INTEGER NOT NULL,
    duration_rounds INTEGER NOT NULL,
    status        TEXT NOT NULL,            -- PENDING|ADMITTED|RUNNING|DONE|TIMEOUT|SKIPPED|ABORTED
    skip_reason   TEXT,
    detail        TEXT,
    started_at    REAL,
    finished_at   REAL
);
CREATE INDEX IF NOT EXISTS idx_injections_round ON injections(round_id);
CREATE TABLE IF NOT EXISTS domain_quota_ledger (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id      TEXT NOT NULL,
    fault_domain  TEXT NOT NULL,
    service       TEXT NOT NULL,
    injection_id  TEXT NOT NULL,
    acquired_at   REAL NOT NULL,
    released_at   REAL
);
CREATE INDEX IF NOT EXISTS idx_quota_open ON domain_quota_ledger(fault_domain, released_at);
CREATE TABLE IF NOT EXISTS failover_ops (
    op_id         TEXT PRIMARY KEY,
    round_id      TEXT NOT NULL,
    service       TEXT NOT NULL,
    from_domain   TEXT,
    to_domain     TEXT,
    status        TEXT NOT NULL,            -- SUCCESS | DENIED
    deny_reason   TEXT,
    created_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS service_states (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id      TEXT NOT NULL,
    service       TEXT NOT NULL,
    state         TEXT NOT NULL,
    version       INTEGER NOT NULL,
    UNIQUE(round_id, service)
);
CREATE TABLE IF NOT EXISTS intents (
    intent_id     TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,            -- ROUND_RUN | QUOTA_ACQUIRE | FAILOVER | ...
    payload       TEXT NOT NULL,
    status        TEXT NOT NULL,            -- pending | committed | aborted
    created_at    REAL NOT NULL,
    resolved_at   REAL
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           TEXT PRIMARY KEY,
    response      TEXT NOT NULL,
    created_at    REAL NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)

    def close(self):
        with self._lock:
            self._conn.close()

    # -- low-level helpers ---------------------------------------------------
    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def write(self, fn):
        """Run ``fn(conn)`` inside a single serialized IMMEDIATE transaction."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                result = fn(self._conn)
                self._conn.execute("COMMIT")
                return result
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # -- intent log ----------------------------------------------------------
    def write_intent(self, kind: str, payload: dict) -> str:
        """Persist a write-ahead intent in its own commit before real work."""
        intent_id = uuid.uuid4().hex
        def _do(conn):
            conn.execute(
                "INSERT INTO intents(intent_id,kind,payload,status,created_at) VALUES(?,?,?,?,?)",
                (intent_id, kind, json.dumps(payload, sort_keys=True), "pending", time.time()),
            )
        self.write(_do)
        return intent_id

    def resolve_intent(self, intent_id: str, status: str):
        def _do(conn):
            conn.execute(
                "UPDATE intents SET status=?, resolved_at=? WHERE intent_id=?",
                (status, time.time(), intent_id),
            )
        self.write(_do)

    def pending_intents(self) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM intents WHERE status='pending' ORDER BY created_at")

    # -- idempotency ---------------------------------------------------------
    def idempotency_lookup(self, key: str) -> dict | None:
        row = self.query_one("SELECT response FROM idempotency_keys WHERE key=?", (key,))
        return json.loads(row["response"]) if row else None

    def idempotency_store(self, key: str, response: dict):
        def _do(conn):
            conn.execute(
                "INSERT OR IGNORE INTO idempotency_keys(key,response,created_at) VALUES(?,?,?)",
                (key, json.dumps(response, sort_keys=True), time.time()),
            )
        self.write(_do)
