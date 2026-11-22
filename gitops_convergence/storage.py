from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def json_loads(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_headers (
    version INTEGER PRIMARY KEY,
    base_version INTEGER,
    generated_at TEXT NOT NULL,
    checksum TEXT NOT NULL,
    affected_app_ids_json TEXT NOT NULL,
    affected_cluster_ids_json TEXT NOT NULL,
    reevaluation_app_ids_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('COMMITTED', 'ABORTED')),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS clusters (
    version INTEGER NOT NULL,
    cluster_id TEXT NOT NULL,
    replica_quota INTEGER NOT NULL CHECK(replica_quota >= 1),
    PRIMARY KEY (version, cluster_id),
    FOREIGN KEY (version) REFERENCES snapshot_headers(version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS app_configs (
    version INTEGER NOT NULL,
    app_id TEXT NOT NULL,
    cluster_id TEXT NOT NULL,
    desired_version TEXT NOT NULL,
    submitted_effective_version TEXT NOT NULL,
    replica_budget INTEGER NOT NULL CHECK(replica_budget >= 0),
    dependencies_json TEXT NOT NULL,
    PRIMARY KEY (version, app_id),
    FOREIGN KEY (version) REFERENCES snapshot_headers(version) ON DELETE CASCADE,
    FOREIGN KEY (version, cluster_id) REFERENCES clusters(version, cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_app_configs_cluster
    ON app_configs(version, cluster_id);

CREATE TABLE IF NOT EXISTS app_states (
    version INTEGER NOT NULL,
    app_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('PENDING', 'ROLLING_BACK', 'CONVERGED', 'FAILED', 'ABORTED')),
    effective_version TEXT NOT NULL,
    requires_reevaluation INTEGER NOT NULL DEFAULT 0 CHECK(requires_reevaluation IN (0, 1)),
    cascade_id TEXT,
    last_transition_at TEXT NOT NULL,
    PRIMARY KEY (version, app_id),
    FOREIGN KEY (version, app_id) REFERENCES app_configs(version, app_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_app_states_state
    ON app_states(version, state);

CREATE TABLE IF NOT EXISTS convergence_runs (
    snapshot_version INTEGER PRIMARY KEY,
    request_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('RUNNING', 'COMPLETED', 'FAILED', 'ABORTED')),
    execution_count INTEGER NOT NULL DEFAULT 1,
    attempt INTEGER NOT NULL DEFAULT 1,
    result_json TEXT,
    error_json TEXT,
    intent_id TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY (snapshot_version) REFERENCES snapshot_headers(version) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS batches (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL,
    run_attempt INTEGER NOT NULL,
    batch_index INTEGER NOT NULL,
    app_ids_json TEXT NOT NULL,
    cluster_usage_json TEXT NOT NULL,
    total_replicas INTEGER NOT NULL,
    quota_exceeded INTEGER NOT NULL DEFAULT 0 CHECK(quota_exceeded IN (0, 1)),
    status TEXT NOT NULL CHECK(status IN ('BUILDING', 'ACTIVE', 'SUPERSEDED', 'ABORTED')),
    intent_id TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (snapshot_version) REFERENCES snapshot_headers(version) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_batches_version_status
    ON batches(snapshot_version, status, batch_index);

CREATE UNIQUE INDEX IF NOT EXISTS uq_batches_active_version_index
    ON batches(snapshot_version, batch_index)
    WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS rollback_cascades (
    cascade_id TEXT PRIMARY KEY,
    snapshot_version INTEGER NOT NULL,
    trigger_app_ids_json TEXT NOT NULL,
    ordered_app_ids_json TEXT NOT NULL,
    affected_cluster_ids_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('APPLIED', 'ABORTED')),
    created_at TEXT NOT NULL,
    FOREIGN KEY (snapshot_version) REFERENCES snapshot_headers(version) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_rollback_cascades_version
    ON rollback_cascades(snapshot_version, created_at);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('RUNNING', 'COMPLETED', 'ABORTED')),
    payload_json TEXT NOT NULL,
    detail_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_status
    ON intents(status, created_at);
"""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_lock = threading.Lock()
        self._initialized = False
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.path), timeout=30.0, isolation_level=None, check_same_thread=False
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def initialize(self) -> None:
        with self._init_lock:
            if self._initialized:
                return
            connection = self.connect()
            try:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA synchronous = FULL")
                connection.executescript(SCHEMA)
            finally:
                connection.close()
            self._initialized = True

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            else:
                connection.execute("COMMIT")
        finally:
            connection.close()

    def get_meta(self, key: str) -> str | None:
        with self.transaction(immediate=False) as connection:
            row = connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return None if row is None else str(row["value"])

    def set_meta(self, connection: sqlite3.Connection, key: str, value: str) -> None:
        connection.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
