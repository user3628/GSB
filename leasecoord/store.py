"""SQLite persistence with WAL readers and one serialized writer per commit."""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator, TypeVar

from .config import CoordinatorConfig

T = TypeVar("T")


SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    priority INTEGER NOT NULL CHECK (priority >= 0),
    lease_ttl_ms INTEGER NOT NULL CHECK (lease_ttl_ms > 0),
    last_heartbeat_ms INTEGER NOT NULL CHECK (last_heartbeat_ms >= 0),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPECT', 'LEADER', 'FOLLOWER')),
    leader_lease_version INTEGER NOT NULL DEFAULT 0 CHECK (leader_lease_version >= 0),
    registered_version INTEGER NOT NULL DEFAULT 1 CHECK (registered_version >= 1),
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0),
    CHECK (status != 'LEADER' OR leader_lease_version > 0)
);

CREATE TABLE IF NOT EXISTS lease (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    holder_id TEXT,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    expires_at_ms INTEGER NOT NULL DEFAULT 0 CHECK (expires_at_ms >= 0),
    term INTEGER NOT NULL DEFAULT 0 CHECK (term >= 0),
    state TEXT NOT NULL CHECK (state IN ('FREE', 'GRANTED', 'EXPIRED')),
    updated_at_ms INTEGER NOT NULL DEFAULT 0 CHECK (updated_at_ms >= 0),
    CHECK (state != 'GRANTED' OR holder_id IS NOT NULL),
    FOREIGN KEY (holder_id) REFERENCES nodes(id)
);

CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY,
    request_id TEXT UNIQUE,
    kind TEXT NOT NULL,
    node_id TEXT,
    expected_lease_version INTEGER,
    fingerprint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'COMMITTED', 'ABORTED')),
    owner_pid INTEGER NOT NULL,
    owner_instance TEXT NOT NULL,
    owner_host TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER,
    result_json TEXT,
    error_json TEXT,
    note TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_status_created
    ON intents(status, created_at_ms);

CREATE TABLE IF NOT EXISTS lease_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at_ms INTEGER NOT NULL,
    operation TEXT NOT NULL,
    old_version INTEGER NOT NULL,
    new_version INTEGER NOT NULL,
    old_state TEXT NOT NULL,
    new_state TEXT NOT NULL,
    holder_id TEXT,
    term INTEGER NOT NULL,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Database:
    """A very small connection-per-operation wrapper around SQLite.

    The intent protocol uses a committed intent row followed by a separate
    ``BEGIN IMMEDIATE`` transaction.  A process crash between the two commits
    leaves an auditable pending intent, while SQLite guarantees that the lease
    mutation itself is all-or-nothing.
    """

    def __init__(self, path: str | Path, config: CoordinatorConfig) -> None:
        self.path = Path(path)
        self.config = config
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.path),
            timeout=self.config.sqlite_busy_timeout_ms / 1000.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {self.config.sqlite_busy_timeout_ms}")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    def initialize(self) -> None:
        conn = sqlite3.connect(
            str(self.path),
            timeout=self.config.sqlite_busy_timeout_ms / 1000.0,
            isolation_level=None,
        )
        try:
            conn.execute(f"PRAGMA busy_timeout = {self.config.sqlite_busy_timeout_ms}")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
            conn.executescript(SCHEMA)
            conn.execute(
                """
                INSERT OR IGNORE INTO lease
                    (singleton, holder_id, version, expires_at_ms, term, state, updated_at_ms)
                VALUES (1, NULL, 0, 0, 0, 'FREE', 0)
                """
            )
        finally:
            conn.close()

    @contextmanager
    def read_connection(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("PRAGMA query_only = ON")
            yield conn
        finally:
            conn.close()

    def read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        with self.read_connection() as conn:
            conn.execute("BEGIN")
            try:
                result = fn(conn)
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
            return result

    def write(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        """Run ``fn`` in a single-writer transaction, retrying lock contention.

        ``fn`` may be re-run after a lock conflict, therefore it must derive all
        decisions from the transaction snapshot and must not mutate process-local
        state until it returns.
        """

        delay = 0.010
        last_error: sqlite3.OperationalError | None = None
        for attempt in range(8):
            conn = self._connect()
            committing = False
            try:
                conn.execute("BEGIN IMMEDIATE")
                result = fn(conn)
                committing = True
                conn.execute("COMMIT")
                return result
            except sqlite3.OperationalError as exc:
                try:
                    if conn.in_transaction:
                        conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                last_error = exc
                message = str(exc).lower()
                if not committing and ("locked" in message or "busy" in message):
                    if attempt < 7:
                        time.sleep(delay)
                        delay = min(delay * 2, 0.25)
                        continue
                raise
            except Exception:
                try:
                    if conn.in_transaction:
                        conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                conn.close()
        if last_error is not None:
            raise last_error
        raise RuntimeError("write transaction failed without an error")


def fetch_lease_row(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM lease WHERE singleton = 1").fetchone()
    if row is None:
        raise RuntimeError("lease row missing")
    return row


def fetch_all_node_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute("SELECT * FROM nodes ORDER BY id ASC"))


def fetch_node_row(conn: sqlite3.Connection, node_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()


def insert_event(
    conn: sqlite3.Connection,
    *,
    at_ms: int,
    operation: str,
    old_version: int,
    new_version: int,
    old_state: str,
    new_state: str,
    holder_id: str | None,
    term: int,
    details_json: str,
) -> None:
    conn.execute(
        """
        INSERT INTO lease_events
            (at_ms, operation, old_version, new_version, old_state, new_state,
             holder_id, term, details_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            at_ms,
            operation,
            old_version,
            new_version,
            old_state,
            new_state,
            holder_id,
            term,
            details_json,
        ),
    )


def assert_lease_invariants(conn: sqlite3.Connection) -> None:
    """Assert the locally enforceable committed-state invariants.

    A GRANTED lease must name its holder, and that holder must be the only node
    marked LEADER with the exact same lease version.  A non-GRANTED lease must
    not leave any node marked LEADER.  This check runs before COMMIT inside the
    same transaction as every lease mutation.
    """

    lease = fetch_lease_row(conn)
    leaders = list(conn.execute("SELECT * FROM nodes WHERE status = 'LEADER'"))
    if lease["state"] == "GRANTED":
        holder = conn.execute(
            "SELECT * FROM nodes WHERE id = ?", (lease["holder_id"],)
        ).fetchone()
        if holder is None:
            raise RuntimeError("GRANTED lease holder is not registered")
        if holder["status"] != "LEADER":
            raise RuntimeError("GRANTED lease holder is not marked LEADER")
        if int(holder["leader_lease_version"]) != int(lease["version"]):
            raise RuntimeError("LEADER node version differs from lease version")
        if len(leaders) != 1 or leaders[0]["id"] != lease["holder_id"]:
            raise RuntimeError("exactly one node may be LEADER")
    elif leaders:
        raise RuntimeError("LEADER node exists while lease is not GRANTED")
