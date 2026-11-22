from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    snapshot_seq INTEGER NOT NULL,
    state_token TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    active_models_json TEXT NOT NULL,
    known_vocabs_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_versions (
    model_id TEXT NOT NULL,
    version TEXT NOT NULL,
    graph_json TEXT NOT NULL,
    graph_signature TEXT NOT NULL,
    source_snapshot_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (model_id, version)
);

CREATE TABLE IF NOT EXISTS known_vocabs (
    vocab_id TEXT PRIMARY KEY,
    snapshot_seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS route_entries (
    model_id TEXT NOT NULL,
    version TEXT NOT NULL,
    snapshot_seq INTEGER NOT NULL,
    active INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    generation INTEGER NOT NULL,
    PRIMARY KEY (model_id, version)
);

CREATE TABLE IF NOT EXISTS derived_artifacts (
    model_id TEXT NOT NULL,
    version TEXT NOT NULL,
    kind TEXT NOT NULL,
    snapshot_seq INTEGER NOT NULL,
    generation INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY (model_id, version, kind)
);

CREATE TABLE IF NOT EXISTS gray_plans (
    model_id TEXT PRIMARY KEY,
    snapshot_seq INTEGER NOT NULL,
    old_version TEXT NOT NULL,
    new_version TEXT NOT NULL,
    ratio_ppm INTEGER NOT NULL,
    ratio_rounding_error TEXT NOT NULL,
    accumulator TEXT NOT NULL,
    compensation TEXT NOT NULL,
    decision_count INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gray_decisions (
    decision_key TEXT PRIMARY KEY,
    state_token TEXT NOT NULL,
    snapshot_seq INTEGER NOT NULL,
    model_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    target_version TEXT NOT NULL,
    version_role TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gray_decisions_state
ON gray_decisions(state_token, model_id, request_id);

CREATE TABLE IF NOT EXISTS bindings (
    request_id TEXT PRIMARY KEY,
    model_id TEXT NOT NULL,
    version TEXT NOT NULL,
    seed INTEGER,
    status TEXT NOT NULL,
    bound_snapshot_seq INTEGER NOT NULL,
    bound_state_token TEXT NOT NULL,
    graph_json TEXT NOT NULL,
    vocab_refs_json TEXT NOT NULL,
    known_vocabs_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    result_json TEXT,
    result_digest TEXT
);

CREATE INDEX IF NOT EXISTS idx_bindings_status ON bindings(status);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    snapshot_seq INTEGER,
    base_token TEXT,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    recovery_note TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);

CREATE TABLE IF NOT EXISTS migrations (
    migration_id TEXT PRIMARY KEY,
    snapshot_seq INTEGER NOT NULL,
    state_token TEXT NOT NULL,
    model_id TEXT NOT NULL,
    old_version TEXT,
    new_version TEXT NOT NULL,
    diff_mask INTEGER NOT NULL,
    diff_names_json TEXT NOT NULL,
    in_place INTEGER NOT NULL,
    mode TEXT NOT NULL,
    manifest_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_migrations_snapshot ON migrations(snapshot_seq);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    last_committed_snapshot_seq INTEGER,
    state_token TEXT,
    aborted_intents_json TEXT NOT NULL,
    interrupted_bindings_json TEXT NOT NULL,
    details_json TEXT NOT NULL,
    startup_scan_complete INTEGER NOT NULL
);
"""


def initialize_database(db_path: str) -> None:
    path = Path(db_path)
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path), timeout=30, isolation_level=None)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        connection.executescript(SCHEMA)
        connection.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES('schema_version', '1')"
        )
    finally:
        connection.close()


def connect_database(db_path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    connection.execute("PRAGMA synchronous=FULL")
    return connection


@contextmanager
def immediate_transaction(connection: sqlite3.Connection) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


@contextmanager
def read_transaction(connection: sqlite3.Connection) -> Iterator[None]:
    """Hold one WAL snapshot across several read statements."""
    connection.execute("BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()

