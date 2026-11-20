from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from app.config import Settings


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS volumes (
    volume_id TEXT PRIMARY KEY,
    block_count INTEGER NOT NULL CHECK (block_count > 0),
    consistency_seq INTEGER NOT NULL CHECK (consistency_seq >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS block_versions (
    volume_id TEXT NOT NULL,
    block_no INTEGER NOT NULL,
    version INTEGER NOT NULL,
    digest TEXT NOT NULL,
    content BLOB NOT NULL,
    written_at TEXT NOT NULL,
    PRIMARY KEY (volume_id, block_no, version),
    FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS current_blocks (
    volume_id TEXT NOT NULL,
    block_no INTEGER NOT NULL,
    block_version INTEGER NOT NULL,
    PRIMARY KEY (volume_id, block_no),
    FOREIGN KEY (volume_id, block_no, block_version)
        REFERENCES block_versions(volume_id, block_no, version)
);

CREATE TABLE IF NOT EXISTS snapshots (
    volume_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    parent_id TEXT,
    mode TEXT NOT NULL CHECK (mode IN ('full', 'incremental')),
    state TEXT NOT NULL,
    consistency_seq INTEGER NOT NULL,
    ref_count INTEGER NOT NULL DEFAULT 0,
    merged_into TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (volume_id, snapshot_id),
    FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE,
    FOREIGN KEY (volume_id, parent_id)
        REFERENCES snapshots(volume_id, snapshot_id)
);

CREATE TABLE IF NOT EXISTS snapshot_refs (
    volume_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    block_no INTEGER NOT NULL,
    block_version INTEGER NOT NULL,
    digest TEXT NOT NULL,
    PRIMARY KEY (volume_id, snapshot_id, block_no),
    FOREIGN KEY (volume_id, snapshot_id)
        REFERENCES snapshots(volume_id, snapshot_id) ON DELETE CASCADE,
    FOREIGN KEY (volume_id, block_no, block_version)
        REFERENCES block_versions(volume_id, block_no, version)
);

CREATE TABLE IF NOT EXISTS snapshot_ref_history (
    history_id TEXT PRIMARY KEY,
    volume_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    block_no INTEGER NOT NULL,
    block_version INTEGER NOT NULL,
    digest TEXT NOT NULL,
    reason TEXT NOT NULL,
    captured_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_events (
    event_id TEXT PRIMARY KEY,
    volume_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    volume_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'COMMITTED', 'ABORTED')),
    payload_json TEXT NOT NULL,
    details_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    volume_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('IN_PROGRESS', 'COMPLETED', 'ABORTED')),
    response_json TEXT,
    error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cow_events (
    event_id TEXT PRIMARY KEY,
    volume_id TEXT NOT NULL,
    block_no INTEGER NOT NULL,
    old_version INTEGER NOT NULL,
    new_version INTEGER NOT NULL,
    old_ref_count INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    volume_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_block_versions_digest
    ON block_versions(volume_id, block_no, digest);
CREATE INDEX IF NOT EXISTS idx_snapshots_parent
    ON snapshots(volume_id, parent_id);
CREATE INDEX IF NOT EXISTS idx_snapshot_refs_version
    ON snapshot_refs(volume_id, block_no, block_version);
CREATE INDEX IF NOT EXISTS idx_intents_state
    ON intents(volume_id, state);
CREATE INDEX IF NOT EXISTS idx_requests_state
    ON requests(state);
"""


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class Database:
    def __init__(self, path: Path):
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(self.path),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    @contextmanager
    def read_view(self) -> Iterator[sqlite3.Connection]:
        """A WAL read transaction exposing one complete committed view."""
        with self.transaction(immediate=False) as conn:
            yield conn

    def initialize(self, settings: Settings, created_at: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA_SQL)
            conn.execute(
                "INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', '1')"
            )
            existing = conn.execute(
                "SELECT block_count FROM volumes WHERE volume_id = ?",
                (settings.volume_id,),
            ).fetchone()
            if existing is not None:
                if int(existing["block_count"]) != settings.block_count:
                    raise RuntimeError(
                        "Configured block count does not match the persisted volume: "
                        f"{settings.block_count} != {existing['block_count']}"
                    )
                return

            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """
                    INSERT INTO volumes(volume_id, block_count, consistency_seq, created_at, updated_at)
                    VALUES (?, ?, 1, ?, ?)
                    """,
                    (settings.volume_id, settings.block_count, created_at, created_at),
                )
                for block_no in range(settings.block_count):
                    content = settings.initial_content(block_no)
                    digest = sha256_bytes(content)
                    conn.execute(
                        """
                        INSERT INTO block_versions(
                            volume_id, block_no, version, digest, content, written_at
                        ) VALUES (?, ?, 1, ?, ?, ?)
                        """,
                        (settings.volume_id, block_no, digest, content, created_at),
                    )
                    conn.execute(
                        """
                        INSERT INTO current_blocks(volume_id, block_no, block_version)
                        VALUES (?, ?, 1)
                        """,
                        (settings.volume_id, block_no),
                    )
                conn.execute("COMMIT")
            except BaseException:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

