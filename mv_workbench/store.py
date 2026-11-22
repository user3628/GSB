from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import fail
from .util import canonical_json, utc_now


SCHEMA_SQL = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS service_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS commits (
    commit_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    created_at TEXT NOT NULL,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS base_tables (
    name TEXT PRIMARY KEY,
    columns_json TEXT NOT NULL,
    current_version INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS base_snapshots (
    table_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    generated_at TEXT NOT NULL,
    columns_json TEXT NOT NULL,
    null_bitmap_json TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    live_count INTEGER NOT NULL,
    commit_seq INTEGER NOT NULL,
    PRIMARY KEY (table_name, version)
);

CREATE TABLE IF NOT EXISTS base_rows (
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    row_version INTEGER NOT NULL,
    data_json TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    deleted INTEGER NOT NULL,
    updated_commit_seq INTEGER NOT NULL,
    PRIMARY KEY (table_name, row_id)
);

CREATE TABLE IF NOT EXISTS base_row_history (
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    row_version INTEGER NOT NULL,
    data_json TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    deleted INTEGER NOT NULL,
    valid_from_version INTEGER NOT NULL,
    valid_to_version INTEGER,
    commit_seq INTEGER NOT NULL,
    PRIMARY KEY (table_name, row_id, row_version)
);

CREATE TABLE IF NOT EXISTS graph_versions (
    graph_version INTEGER PRIMARY KEY,
    definitions_json TEXT NOT NULL,
    edges_json TEXT NOT NULL,
    layers_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS view_states (
    view_name TEXT PRIMARY KEY,
    graph_version INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('FRESH','STALE','REFRESHING','PARTIAL','INVALID')),
    materialized_version INTEGER,
    state_commit_seq INTEGER NOT NULL,
    gaps_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS view_versions (
    view_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    commit_seq INTEGER NOT NULL,
    graph_version INTEGER NOT NULL,
    mode TEXT NOT NULL,
    affected_groups_json TEXT NOT NULL,
    recomputed_groups INTEGER NOT NULL,
    preserved_groups INTEGER NOT NULL,
    reason TEXT NOT NULL,
    digest TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    group_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (view_name, version)
);

CREATE TABLE IF NOT EXISTS view_group_index (
    view_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    group_key_json TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    digest TEXT NOT NULL,
    PRIMARY KEY (view_name, version, group_key_json)
);

CREATE TABLE IF NOT EXISTS view_rows (
    view_name TEXT NOT NULL,
    version INTEGER NOT NULL,
    group_key_json TEXT NOT NULL,
    row_index INTEGER NOT NULL,
    row_json TEXT NOT NULL,
    PRIMARY KEY (view_name, version, group_key_json, row_index)
);

CREATE TABLE IF NOT EXISTS state_transitions (
    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    view_name TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    gaps_json TEXT NOT NULL,
    commit_seq INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS refresh_requests (
    request_id TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    request_json TEXT NOT NULL,
    status TEXT NOT NULL,
    response_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('RUNNING','COMMITTED','ABORTED')),
    payload_digest TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    commit_seq INTEGER
);

CREATE TABLE IF NOT EXISTS invalidation_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    commit_seq INTEGER NOT NULL,
    trigger_type TEXT NOT NULL,
    trigger_id TEXT NOT NULL,
    view_name TEXT NOT NULL,
    depth INTEGER NOT NULL,
    action TEXT NOT NULL,
    affected_groups_json TEXT NOT NULL,
    recomputed_groups INTEGER,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    status TEXT NOT NULL,
    facts_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_base_history_table_version
    ON base_row_history(table_name, valid_from_version, valid_to_version);
CREATE INDEX IF NOT EXISTS idx_view_rows_lookup
    ON view_rows(view_name, version, group_key_json, row_index);
CREATE INDEX IF NOT EXISTS idx_invalidation_commit
    ON invalidation_events(commit_seq DESC, event_id DESC);
"""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.path),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def initialize_schema(self) -> None:
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(SCHEMA_SQL)
            connection.execute("PRAGMA user_version = 1")
        finally:
            connection.close()

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def remove_files(self) -> None:
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.path) + suffix)
            if candidate.exists():
                candidate.unlink()


def insert_commit(
    connection: sqlite3.Connection, kind: str, details: dict[str, Any]
) -> int:
    cursor = connection.execute(
        "INSERT INTO commits(kind, created_at, details_json) VALUES (?, ?, ?)",
        (kind, utc_now(), canonical_json(details)),
    )
    return int(cursor.lastrowid)


def meta_get(connection: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = connection.execute("SELECT value FROM service_meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def meta_set(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        "INSERT INTO service_meta(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def current_graph_version(connection: sqlite3.Connection) -> int | None:
    row = connection.execute("SELECT MAX(graph_version) AS version FROM graph_versions").fetchone()
    return int(row["version"]) if row and row["version"] is not None else None


def load_graph_row(
    connection: sqlite3.Connection, version: int | None = None
) -> dict[str, Any] | None:
    if version is None:
        version = current_graph_version(connection)
    if version is None:
        return None
    row = connection.execute(
        "SELECT * FROM graph_versions WHERE graph_version = ?", (version,)
    ).fetchone()
    if row is None:
        return None
    return {
        "graph_version": int(row["graph_version"]),
        "definitions": json.loads(row["definitions_json"]),
        "edges": json.loads(row["edges_json"]),
        "layers": json.loads(row["layers_json"]),
        "digest": row["digest"],
        "commit_seq": int(row["commit_seq"]),
        "created_at": row["created_at"],
    }


def load_graph_history(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT graph_version, digest, commit_seq, created_at FROM graph_versions "
        "ORDER BY graph_version"
    ).fetchall()
    return [dict(row) for row in rows]


def table_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute("SELECT name FROM base_tables ORDER BY name").fetchall()
    return {row["name"] for row in rows}


def load_base_table_row(
    connection: sqlite3.Connection, table_name: str
) -> dict[str, Any] | None:
    row = connection.execute("SELECT * FROM base_tables WHERE name = ?", (table_name,)).fetchone()
    if row is None:
        return None
    return {
        "name": row["name"],
        "columns": json.loads(row["columns_json"]),
        "current_version": int(row["current_version"]),
        "created_at": row["created_at"],
    }


def load_base_snapshot(
    connection: sqlite3.Connection, table_name: str, version: int | None = None
) -> dict[str, Any] | None:
    if version is None:
        table = load_base_table_row(connection, table_name)
        if table is None:
            return None
        version = table["current_version"]
    row = connection.execute(
        "SELECT * FROM base_snapshots WHERE table_name = ? AND version = ?",
        (table_name, version),
    ).fetchone()
    if row is None:
        return None
    return {
        "table": row["table_name"],
        "version": int(row["version"]),
        "generated_at": row["generated_at"],
        "columns": json.loads(row["columns_json"]),
        "null_bitmap": json.loads(row["null_bitmap_json"]),
        "content_digest": row["content_digest"],
        "row_count": int(row["row_count"]),
        "live_count": int(row["live_count"]),
        "commit_seq": int(row["commit_seq"]),
    }


def load_base_rows(
    connection: sqlite3.Connection,
    table_name: str,
    *,
    include_deleted: bool = False,
) -> list[dict[str, Any]]:
    if include_deleted:
        rows = connection.execute(
            "SELECT * FROM base_rows WHERE table_name = ? ORDER BY row_id", (table_name,)
        ).fetchall()
    else:
        rows = connection.execute(
            "SELECT * FROM base_rows WHERE table_name = ? AND deleted = 0 ORDER BY row_id",
            (table_name,),
        ).fetchall()
    return [
        {
            "row_id": row["row_id"],
            "row_version": int(row["row_version"]),
            "deleted": bool(row["deleted"]),
            "data": json.loads(row["data_json"]),
            "content_digest": row["content_digest"],
        }
        for row in rows
    ]


def load_view_state(connection: sqlite3.Connection, view_name: str) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT * FROM view_states WHERE view_name = ?", (view_name,)
    ).fetchone()
    if row is None:
        return None
    return {
        "view": row["view_name"],
        "graph_version": int(row["graph_version"]),
        "state": row["state"],
        "materialized_version": (
            int(row["materialized_version"]) if row["materialized_version"] is not None else None
        ),
        "state_commit_seq": int(row["state_commit_seq"]),
        "gaps": json.loads(row["gaps_json"]),
        "updated_at": row["updated_at"],
    }


def load_all_view_states(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    names = connection.execute("SELECT view_name FROM view_states ORDER BY view_name").fetchall()
    return [
        load_view_state(connection, row["view_name"])  # type: ignore[list-item]
        for row in names
    ]


def load_view_version(
    connection: sqlite3.Connection, view_name: str, version: int | None = None
) -> dict[str, Any] | None:
    if version is None:
        state = load_view_state(connection, view_name)
        if state is None or state["materialized_version"] is None:
            return None
        version = int(state["materialized_version"])
    row = connection.execute(
        "SELECT * FROM view_versions WHERE view_name = ? AND version = ?",
        (view_name, version),
    ).fetchone()
    if row is None:
        return None
    return {
        "view": row["view_name"],
        "version": int(row["version"]),
        "commit_seq": int(row["commit_seq"]),
        "graph_version": int(row["graph_version"]),
        "mode": row["mode"],
        "affected_groups": json.loads(row["affected_groups_json"]),
        "recomputed_groups": int(row["recomputed_groups"]),
        "preserved_groups": int(row["preserved_groups"]),
        "reason": row["reason"],
        "digest": row["digest"],
        "row_count": int(row["row_count"]),
        "group_count": int(row["group_count"]),
        "created_at": row["created_at"],
    }


def load_view_groups(
    connection: sqlite3.Connection, view_name: str, version: int
) -> list[dict[str, Any]]:
    group_rows = connection.execute(
        "SELECT group_key_json, row_count, digest FROM view_group_index "
        "WHERE view_name = ? AND version = ? ORDER BY group_key_json",
        (view_name, version),
    ).fetchall()
    groups: list[dict[str, Any]] = []
    for group in group_rows:
        rows = connection.execute(
            "SELECT row_json FROM view_rows WHERE view_name = ? AND version = ? "
            "AND group_key_json = ? ORDER BY row_index",
            (view_name, version, group["group_key_json"]),
        ).fetchall()
        groups.append(
            {
                "key": json.loads(group["group_key_json"]),
                "row_count": int(group["row_count"]),
                "digest": group["digest"],
                "rows": [json.loads(row["row_json"]) for row in rows],
            }
        )
    return groups


def flatten_groups(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for group in groups for row in group["rows"]]


def load_latest_view_rows(
    connection: sqlite3.Connection, view_name: str
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    state = load_view_state(connection, view_name)
    if state is None or state["materialized_version"] is None:
        return state, []
    groups = load_view_groups(connection, view_name, int(state["materialized_version"]))
    return state, flatten_groups(groups)


def max_view_version(connection: sqlite3.Connection, view_name: str) -> int:
    row = connection.execute(
        "SELECT MAX(version) AS version FROM view_versions WHERE view_name = ?", (view_name,)
    ).fetchone()
    return int(row["version"]) if row and row["version"] is not None else 0


def latest_commit_seq(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT MAX(commit_seq) AS value FROM commits").fetchone()
    return int(row["value"]) if row and row["value"] is not None else 0


def insert_state_transition(
    connection: sqlite3.Connection,
    *,
    view_name: str,
    from_state: str,
    to_state: str,
    reason: str,
    gaps: list[dict[str, Any]],
    commit_seq: int | None,
) -> None:
    connection.execute(
        "INSERT INTO state_transitions(view_name, from_state, to_state, reason, gaps_json, "
        "commit_seq, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            view_name,
            from_state,
            to_state,
            reason,
            canonical_json(gaps),
            commit_seq,
            utc_now(),
        ),
    )


def update_view_state(
    connection: sqlite3.Connection,
    *,
    view_name: str,
    graph_version: int,
    state: str,
    materialized_version: int | None,
    gaps: list[dict[str, Any]],
    commit_seq: int,
    reason: str,
    old_state: str,
) -> None:
    connection.execute(
        "UPDATE view_states SET graph_version = ?, state = ?, materialized_version = ?, "
        "state_commit_seq = ?, gaps_json = ?, updated_at = ? WHERE view_name = ?",
        (
            graph_version,
            state,
            materialized_version,
            commit_seq,
            canonical_json(gaps),
            utc_now(),
            view_name,
        ),
    )
    insert_state_transition(
        connection,
        view_name=view_name,
        from_state=old_state,
        to_state=state,
        reason=reason,
        gaps=gaps,
        commit_seq=commit_seq,
    )


def insert_invalidation_event(
    connection: sqlite3.Connection,
    *,
    commit_seq: int,
    trigger_type: str,
    trigger_id: str,
    view_name: str,
    depth: int,
    action: str,
    affected_groups: list[Any],
    recomputed_groups: int | None,
    reason: str,
) -> None:
    connection.execute(
        "INSERT INTO invalidation_events(commit_seq, trigger_type, trigger_id, view_name, "
        "depth, action, affected_groups_json, recomputed_groups, reason, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            commit_seq,
            trigger_type,
            trigger_id,
            view_name,
            depth,
            action,
            canonical_json(affected_groups),
            recomputed_groups,
            reason,
            utc_now(),
        ),
    )


def missing_view(view_name: str) -> None:
    fail(
        "E_NOT_FOUND",
        "Materialized view does not exist.",
        context={"view": view_name},
        status_code=404,
    )
