from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .errors import wb_error
from .policy import CachePolicy, canonical_json


ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "MISS": frozenset({"REVALIDATING"}),
    "REVALIDATING": frozenset({"FRESH", "STALE", "PURGED"}),
    "FRESH": frozenset({"STALE", "REVALIDATING", "PURGED"}),
    "STALE": frozenset({"REVALIDATING", "PURGED"}),
    "PURGED": frozenset(),
}

_SCHEMA = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_snapshots (
    version INTEGER PRIMARY KEY,
    generated_at TEXT NOT NULL,
    nodes_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    submitted_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS current_policy (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL,
    FOREIGN KEY (version) REFERENCES policy_snapshots(version)
);

CREATE TABLE IF NOT EXISTS origin_content (
    path TEXT PRIMARY KEY,
    body BLOB NOT NULL,
    content_type TEXT NOT NULL,
    tags_json TEXT NOT NULL,
    etag TEXT NOT NULL,
    version TEXT NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS node_epochs (
    node_id TEXT PRIMARY KEY,
    epoch INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS cache_entries (
    node_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    key_canonical TEXT NOT NULL,
    canonical_path TEXT NOT NULL,
    canonical_query TEXT NOT NULL,
    vary_json TEXT NOT NULL,
    body BLOB NOT NULL,
    content_type TEXT NOT NULL,
    etag TEXT NOT NULL,
    origin_version TEXT NOT NULL,
    stored_at REAL NOT NULL,
    fresh_until REAL NOT NULL,
    stale_until REAL NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('FRESH', 'STALE', 'REVALIDATING', 'PURGED')),
    policy_version INTEGER NOT NULL,
    tags_json TEXT NOT NULL,
    route_key TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1,
    purge_epoch INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    PRIMARY KEY (node_id, cache_key)
);

CREATE INDEX IF NOT EXISTS idx_cache_entries_path ON cache_entries(node_id, canonical_path);
CREATE INDEX IF NOT EXISTS idx_cache_entries_state ON cache_entries(node_id, state);

CREATE TABLE IF NOT EXISTS route_index (
    node_id TEXT NOT NULL,
    route_key TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    PRIMARY KEY (node_id, route_key, cache_key),
    FOREIGN KEY (node_id, cache_key) REFERENCES cache_entries(node_id, cache_key) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tag_index (
    node_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    PRIMARY KEY (node_id, tag, cache_key),
    FOREIGN KEY (node_id, cache_key) REFERENCES cache_entries(node_id, cache_key) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS entry_state_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    generation INTEGER NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS request_records (
    request_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT,
    http_status INTEGER,
    business_code TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS request_traces (
    trace_id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    cache_key TEXT,
    decision TEXT NOT NULL,
    origin_fetches INTEGER NOT NULL,
    events_json TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_request_traces_request ON request_traces(request_id);

CREATE TABLE IF NOT EXISTS injections (
    injection_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    route TEXT,
    type TEXT NOT NULL,
    params_json TEXT NOT NULL,
    status TEXT NOT NULL,
    business_code TEXT,
    details_json TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_injections_request ON injections(request_id);
CREATE INDEX IF NOT EXISTS idx_injections_node ON injections(node_id);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    request_id TEXT,
    node_id TEXT,
    cache_key TEXT,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    result_json TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);

CREATE TABLE IF NOT EXISTS purge_operations (
    purge_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    path TEXT,
    tag TEXT,
    policy_version INTEGER NOT NULL,
    nodes_json TEXT NOT NULL,
    affected_json TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at REAL NOT NULL,
    committed_at REAL,
    broadcast_window_ms REAL NOT NULL,
    elapsed_ms REAL,
    intent_id TEXT
);

CREATE TABLE IF NOT EXISTS origin_fetches (
    attempt_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    path TEXT NOT NULL,
    status TEXT NOT NULL,
    http_status INTEGER,
    etag TEXT,
    body_length INTEGER,
    error_code TEXT,
    started_at REAL NOT NULL,
    completed_at REAL
);

CREATE INDEX IF NOT EXISTS idx_origin_fetches_request ON origin_fetches(request_id);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    started_at REAL NOT NULL,
    completed_at REAL NOT NULL,
    clean_startup INTEGER NOT NULL,
    intents_scanned INTEGER NOT NULL,
    intents_rolled_back INTEGER NOT NULL,
    details_json TEXT NOT NULL,
    facts_json TEXT NOT NULL
);
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    result = dict(row)
    for key in ("vary_json", "tags_json", "events_json", "params_json", "details_json"):
        if key in result and isinstance(result[key], str):
            result[key.removesuffix("_json")] = json.loads(result[key])
            del result[key]
    return result


class Store:
    """SQLite persistence with one serialized writer and independent read connections."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0, check_same_thread=False, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            yield connection
            connection.execute("ROLLBACK")
        finally:
            connection.close()

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

    def initialize(self) -> None:
        with self._write_lock:
            connection = self._connect()
            try:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA synchronous = FULL")
                connection.executescript(_SCHEMA)
                for node_id in self.list_epochs(connection=connection):
                    pass
            finally:
                connection.close()

    def ensure_node(self, node_id: str) -> None:
        with self.write() as connection:
            connection.execute(
                "INSERT INTO node_epochs(node_id, epoch) VALUES (?, 0) ON CONFLICT(node_id) DO NOTHING",
                (node_id,),
            )

    @staticmethod
    def list_epochs(*, connection: sqlite3.Connection | None = None) -> list[str]:
        if connection is None:
            return []
        return [row["node_id"] for row in connection.execute("SELECT node_id FROM node_epochs")]

    def node_epoch(self, node_id: str) -> int:
        with self.read() as connection:
            row = connection.execute("SELECT epoch FROM node_epochs WHERE node_id = ?", (node_id,)).fetchone()
            return int(row["epoch"]) if row else 0

    def get_current_policy(self) -> CachePolicy | None:
        with self.read() as connection:
            row = connection.execute(
                """
                SELECT p.snapshot_json
                FROM current_policy c
                JOIN policy_snapshots p ON p.version = c.version
                WHERE c.singleton = 1
                """
            ).fetchone()
        if not row:
            return None
        snapshot = json.loads(row["snapshot_json"])
        return CachePolicy.from_snapshot(snapshot, snapshot.get("nodes", []))

    def get_policy(self, version: int) -> CachePolicy | None:
        with self.read() as connection:
            row = connection.execute(
                "SELECT snapshot_json FROM policy_snapshots WHERE version = ?", (version,)
            ).fetchone()
        if not row:
            return None
        snapshot = json.loads(row["snapshot_json"])
        return CachePolicy.from_snapshot(snapshot, snapshot.get("nodes", []))

    def list_policy_snapshots(self) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT version, generated_at, nodes_json, digest, submitted_at FROM policy_snapshots ORDER BY version"
            ).fetchall()
        return [
            {
                "version": row["version"],
                "generated_at": row["generated_at"],
                "nodes": json.loads(row["nodes_json"]),
                "digest": row["digest"],
                "submitted_at": row["submitted_at"],
            }
            for row in rows
        ]

    def submit_policy(self, policy: CachePolicy) -> dict[str, Any]:
        now = time.time()
        with self.write() as connection:
            current = connection.execute("SELECT version FROM current_policy WHERE singleton = 1").fetchone()
            current_version = int(current["version"]) if current else 0
            if policy.version <= current_version:
                raise wb_error(
                    "E_STALE_SNAPSHOT",
                    "Snapshot version must be greater than the currently committed version",
                    current_version=current_version,
                    submitted_version=policy.version,
                )
            old_nodes: set[str] = set()
            if current:
                old_row = connection.execute(
                    "SELECT nodes_json FROM policy_snapshots WHERE version = ?", (current_version,)
                ).fetchone()
                if old_row:
                    old_nodes = set(json.loads(old_row["nodes_json"]))
            connection.execute(
                """
                INSERT INTO policy_snapshots(version, generated_at, nodes_json, digest, snapshot_json, submitted_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    policy.version,
                    policy.generated_at,
                    _json(list(policy.nodes)),
                    policy.digest,
                    _json(policy.as_dict()),
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO current_policy(singleton, version) VALUES (1, ?)
                ON CONFLICT(singleton) DO UPDATE SET version = excluded.version
                """,
                (policy.version,),
            )
            affected_nodes = sorted(old_nodes | set(policy.nodes))
            invalidated = 0
            for node_id in affected_nodes:
                connection.execute(
                    """
                    INSERT INTO node_epochs(node_id, epoch) VALUES (?, 1)
                    ON CONFLICT(node_id) DO UPDATE SET epoch = epoch + 1
                    """,
                    (node_id,),
                )
                rows = connection.execute(
                    "SELECT cache_key, generation, state FROM cache_entries WHERE node_id = ? AND state != 'PURGED'",
                    (node_id,),
                ).fetchall()
                for row in rows:
                    connection.execute(
                        "UPDATE cache_entries SET state = 'PURGED', updated_at = ? WHERE node_id = ? AND cache_key = ?",
                        (now, node_id, row["cache_key"]),
                    )
                    connection.execute(
                        "DELETE FROM route_index WHERE node_id = ? AND cache_key = ?", (node_id, row["cache_key"])
                    )
                    connection.execute(
                        "DELETE FROM tag_index WHERE node_id = ? AND cache_key = ?", (node_id, row["cache_key"])
                    )
                    connection.execute(
                        """
                        INSERT INTO entry_state_events(node_id, cache_key, generation, from_state, to_state, reason, created_at)
                        VALUES (?, ?, ?, ?, 'PURGED', 'POLICY_REPLACED', ?)
                        """,
                        (node_id, row["cache_key"], row["generation"], row["state"], now),
                    )
                    invalidated += 1
            return {
                "version": policy.version,
                "digest": policy.digest,
                "nodes": list(policy.nodes),
                "invalidated_entries": invalidated,
                "previous_version": current_version,
                "committed_at": now,
            }

    def upsert_origin_content(
        self,
        *,
        path: str,
        body: bytes,
        content_type: str,
        tags: Sequence[str],
        etag: str,
        version: str,
    ) -> dict[str, Any]:
        now = time.time()
        with self.write() as connection:
            connection.execute(
                """
                INSERT INTO origin_content(path, body, content_type, tags_json, etag, version, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    body = excluded.body,
                    content_type = excluded.content_type,
                    tags_json = excluded.tags_json,
                    etag = excluded.etag,
                    version = excluded.version,
                    updated_at = excluded.updated_at
                """,
                (path, body, content_type, _json(list(tags)), etag, version, now),
            )
        return {
            "path": path,
            "content_type": content_type,
            "tags": list(tags),
            "etag": etag,
            "version": version,
            "length": len(body),
            "updated_at": now,
        }

    def get_origin_content(self, path: str) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute("SELECT * FROM origin_content WHERE path = ?", (path,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["tags"] = json.loads(result.pop("tags_json"))
        return result

    def list_origin_content(self) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT path, content_type, tags_json, etag, version, length(body) AS length, updated_at FROM origin_content ORDER BY path"
            ).fetchall()
        return [
            {
                "path": row["path"],
                "content_type": row["content_type"],
                "tags": json.loads(row["tags_json"]),
                "etag": row["etag"],
                "version": row["version"],
                "length": row["length"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def claim_request(
        self,
        *,
        request_id: str,
        node_id: str,
        fingerprint: str,
        request_payload: Mapping[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        now = time.time()
        with self.write() as connection:
            row = connection.execute("SELECT * FROM request_records WHERE request_id = ?", (request_id,)).fetchone()
            if row is not None:
                if row["fingerprint"] != fingerprint or row["node_id"] != node_id:
                    raise wb_error(
                        "E_REQUEST_CONFLICT",
                        "request_id was already used with different request content",
                        request_id=request_id,
                    )
                result = dict(row)
                if result.get("response_json"):
                    result["response"] = json.loads(result["response_json"])
                return "EXISTING", result
            connection.execute(
                """
                INSERT INTO request_records(request_id, node_id, fingerprint, status, request_json, created_at, updated_at)
                VALUES (?, ?, ?, 'IN_PROGRESS', ?, ?, ?)
                """,
                (request_id, node_id, fingerprint, _json(request_payload), now, now),
            )
            return "NEW", {"request_id": request_id, "node_id": node_id, "status": "IN_PROGRESS"}

    def complete_request(
        self,
        request_id: str,
        *,
        response: Mapping[str, Any],
        http_status: int,
        business_code: str | None,
        status: str = "COMPLETED",
    ) -> None:
        with self.write() as connection:
            connection.execute(
                """
                UPDATE request_records
                SET status = ?, response_json = ?, http_status = ?, business_code = ?, updated_at = ?
                WHERE request_id = ?
                """,
                (status, _json(response), http_status, business_code, time.time(), request_id),
            )

    def get_request(self, request_id: str) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute("SELECT * FROM request_records WHERE request_id = ?", (request_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["request"] = json.loads(result.pop("request_json"))
        if result.get("response_json"):
            result["response"] = json.loads(result.pop("response_json"))
        else:
            result.pop("response_json", None)
        return result

    def record_trace(
        self,
        *,
        request_id: str,
        node_id: str,
        cache_key: str | None,
        decision: str,
        origin_fetches: int,
        events: Sequence[Mapping[str, Any]],
    ) -> int:
        with self.write() as connection:
            cursor = connection.execute(
                """
                INSERT INTO request_traces(request_id, node_id, cache_key, decision, origin_fetches, events_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (request_id, node_id, cache_key, decision, origin_fetches, _json(list(events)), time.time()),
            )
            return int(cursor.lastrowid)

    def list_traces(self, *, request_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        with self.read() as connection:
            if request_id:
                rows = connection.execute(
                    "SELECT * FROM request_traces WHERE request_id = ? ORDER BY trace_id", (request_id,)
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM request_traces ORDER BY trace_id DESC LIMIT ?", (limit,)
                ).fetchall()
        return [_row_dict(row) or {} for row in rows]

    def get_entry(self, node_id: str, cache_key: str) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM cache_entries WHERE node_id = ? AND cache_key = ?", (node_id, cache_key)
            ).fetchone()
        return _row_dict(row)

    def find_entries(
        self,
        *,
        node_id: str | None = None,
        cache_key: str | None = None,
        path: str | None = None,
        include_purged: bool = True,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if node_id is not None:
            clauses.append("node_id = ?")
            params.append(node_id)
        if cache_key is not None:
            clauses.append("cache_key = ?")
            params.append(cache_key)
        if path is not None:
            clauses.append("canonical_path = ?")
            params.append(path)
        if not include_purged:
            clauses.append("state != 'PURGED'")
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        with self.read() as connection:
            rows = connection.execute(
                f"SELECT * FROM cache_entries{where} ORDER BY node_id, canonical_path, cache_key", params
            ).fetchall()
        return [_row_dict(row) or {} for row in rows]

    def transition_entry(
        self,
        *,
        node_id: str,
        cache_key: str,
        from_state: str,
        to_state: str,
        reason: str,
        now: float | None = None,
    ) -> dict[str, Any]:
        now = time.time() if now is None else now
        with self.write() as connection:
            row = connection.execute(
                "SELECT * FROM cache_entries WHERE node_id = ? AND cache_key = ?", (node_id, cache_key)
            ).fetchone()
            if row is None or row["state"] != from_state:
                current = None if row is None else row["state"]
                raise wb_error(
                    "E_INVALID_TRANSITION",
                    "Cache entry is not in the expected source state",
                    requested_from=from_state,
                    requested_to=to_state,
                    current_state=current,
                )
            if to_state not in ALLOWED_TRANSITIONS.get(from_state, frozenset()):
                raise wb_error(
                    "E_INVALID_TRANSITION",
                    "Cache state transition is not allowed",
                    requested_from=from_state,
                    requested_to=to_state,
                )
            connection.execute(
                "UPDATE cache_entries SET state = ?, updated_at = ? WHERE node_id = ? AND cache_key = ?",
                (to_state, now, node_id, cache_key),
            )
            if to_state == "PURGED":
                connection.execute("DELETE FROM route_index WHERE node_id = ? AND cache_key = ?", (node_id, cache_key))
                connection.execute("DELETE FROM tag_index WHERE node_id = ? AND cache_key = ?", (node_id, cache_key))
            connection.execute(
                """
                INSERT INTO entry_state_events(node_id, cache_key, generation, from_state, to_state, reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (node_id, cache_key, row["generation"], from_state, to_state, reason, now),
            )
            updated = connection.execute(
                "SELECT * FROM cache_entries WHERE node_id = ? AND cache_key = ?", (node_id, cache_key)
            ).fetchone()
        return _row_dict(updated) or {}

    def list_entry_state_events(
        self,
        *,
        node_id: str,
        cache_key: str,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                """
                SELECT * FROM entry_state_events
                WHERE node_id = ? AND cache_key = ?
                ORDER BY event_id DESC LIMIT ?
                """,
                (node_id, cache_key, limit),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def create_intent(
        self,
        *,
        kind: str,
        request_id: str | None,
        node_id: str | None,
        cache_key: str | None,
        payload: Mapping[str, Any],
    ) -> str:
        intent_id = str(uuid.uuid4())
        now = time.time()
        with self.write() as connection:
            connection.execute(
                """
                INSERT INTO intents(intent_id, kind, request_id, node_id, cache_key, payload_json, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'IN_PROGRESS', ?, ?)
                """,
                (intent_id, kind, request_id, node_id, cache_key, _json(payload), now, now),
            )
        return intent_id

    def rollback_intent(self, intent_id: str, *, status: str = "ROLLED_BACK", error: str | None = None) -> None:
        with self.write() as connection:
            connection.execute(
                "UPDATE intents SET status = ?, result_json = ?, updated_at = ? WHERE intent_id = ? AND status = 'IN_PROGRESS'",
                (status, _json({"error": error}) if error else None, time.time(), intent_id),
            )

    def replace_entry(
        self,
        *,
        node_id: str,
        entry: Mapping[str, Any],
        intent_id: str | None,
        expected_epoch: int,
        source_request_id: str,
    ) -> dict[str, Any]:
        now = time.time()
        cache_key = str(entry["cache_key"])
        with self.write() as connection:
            epoch_row = connection.execute("SELECT epoch FROM node_epochs WHERE node_id = ?", (node_id,)).fetchone()
            current_epoch = int(epoch_row["epoch"]) if epoch_row else 0
            if current_epoch != expected_epoch:
                if intent_id:
                    connection.execute(
                        "UPDATE intents SET status = 'ROLLED_BACK', result_json = ?, updated_at = ? WHERE intent_id = ?",
                        (_json({"reason": "NODE_EPOCH_CHANGED", "expected": expected_epoch, "actual": current_epoch}), now, intent_id),
                    )
                raise wb_error(
                    "E_CONFLICT",
                    "Cache write was superseded by a purge or policy replacement",
                    expected_epoch=expected_epoch,
                    current_epoch=current_epoch,
                )
            old = connection.execute(
                "SELECT * FROM cache_entries WHERE node_id = ? AND cache_key = ?", (node_id, cache_key)
            ).fetchone()
            old_dict = dict(old) if old else None
            generation = int(old["generation"]) + 1 if old else 1
            if old is not None:
                connection.execute("DELETE FROM route_index WHERE node_id = ? AND cache_key = ?", (node_id, cache_key))
                connection.execute("DELETE FROM tag_index WHERE node_id = ? AND cache_key = ?", (node_id, cache_key))
                connection.execute("DELETE FROM cache_entries WHERE node_id = ? AND cache_key = ?", (node_id, cache_key))
            connection.execute(
                """
                INSERT INTO cache_entries(
                    node_id, cache_key, key_canonical, canonical_path, canonical_query, vary_json,
                    body, content_type, etag, origin_version, stored_at, fresh_until, stale_until,
                    state, policy_version, tags_json, route_key, generation, purge_epoch, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    node_id,
                    cache_key,
                    entry["key_canonical"],
                    entry["canonical_path"],
                    entry["canonical_query"],
                    _json(entry["vary"]),
                    entry["body"],
                    entry["content_type"],
                    entry["etag"],
                    entry["origin_version"],
                    entry["stored_at"],
                    entry["fresh_until"],
                    entry["stale_until"],
                    "FRESH",
                    entry["policy_version"],
                    _json(entry["tags"]),
                    entry["route_key"],
                    generation,
                    current_epoch,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO route_index(node_id, route_key, cache_key) VALUES (?, ?, ?)",
                (node_id, entry["route_key"], cache_key),
            )
            for tag in entry["tags"]:
                connection.execute(
                    "INSERT OR IGNORE INTO tag_index(node_id, tag, cache_key) VALUES (?, ?, ?)",
                    (node_id, tag, cache_key),
                )
            connection.execute(
                """
                INSERT INTO entry_state_events(node_id, cache_key, generation, from_state, to_state, reason, created_at)
                VALUES (?, ?, ?, 'REVALIDATING', 'FRESH', 'ORIGIN_VERSION_COMMITTED', ?)
                """,
                (node_id, cache_key, generation, now),
            )
            if old is not None:
                connection.execute(
                    """
                    INSERT INTO entry_state_events(node_id, cache_key, generation, from_state, to_state, reason, created_at)
                    VALUES (?, ?, ?, ?, 'PURGED', 'ENTRY_REPLACED', ?)
                    """,
                    (node_id, cache_key, old["generation"], old["state"], now),
                )
            result = {
                "node_id": node_id,
                "cache_key": cache_key,
                "generation": generation,
                "old_generation": old_dict["generation"] if old_dict else None,
                "old_etag": old_dict["etag"] if old_dict else None,
                "new_etag": entry["etag"],
                "version_changed": bool(old_dict and old_dict["etag"] != entry["etag"]),
                "source_request_id": source_request_id,
            }
            if intent_id:
                connection.execute(
                    "UPDATE intents SET status = 'COMMITTED', result_json = ?, updated_at = ? WHERE intent_id = ?",
                    (_json(result), now, intent_id),
                )
        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM cache_entries WHERE node_id = ? AND cache_key = ?", (node_id, cache_key)
            ).fetchone()
        result["entry"] = _row_dict(row) or {}
        return result

    def begin_origin_fetch(self, *, attempt_id: str, request_id: str, node_id: str, path: str) -> None:
        with self.write() as connection:
            connection.execute(
                """
                INSERT INTO origin_fetches(attempt_id, request_id, node_id, path, status, started_at)
                VALUES (?, ?, ?, ?, 'IN_PROGRESS', ?)
                """,
                (attempt_id, request_id, node_id, path, time.time()),
            )

    def finish_origin_fetch(
        self,
        attempt_id: str,
        *,
        status: str,
        http_status: int | None = None,
        etag: str | None = None,
        body_length: int | None = None,
        error_code: str | None = None,
    ) -> None:
        with self.write() as connection:
            connection.execute(
                """
                UPDATE origin_fetches
                SET status = ?, http_status = ?, etag = ?, body_length = ?, error_code = ?, completed_at = ?
                WHERE attempt_id = ?
                """,
                (status, http_status, etag, body_length, error_code, time.time(), attempt_id),
            )

    def record_injection(
        self,
        *,
        injection_id: str,
        request_id: str,
        node_id: str,
        route: str | None,
        injection_type: str,
        params: Mapping[str, Any],
        status: str,
        business_code: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        with self.write() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO injections(
                    injection_id, request_id, node_id, route, type, params_json, status,
                    business_code, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    injection_id,
                    request_id,
                    node_id,
                    route,
                    injection_type,
                    _json(dict(params)),
                    status,
                    business_code,
                    _json(dict(details or {})),
                    time.time(),
                ),
            )

    def list_injections(
        self,
        *,
        request_id: str | None = None,
        node_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if request_id:
            clauses.append("request_id = ?")
            params.append(request_id)
        if node_id:
            clauses.append("node_id = ?")
            params.append(node_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(limit)
        with self.read() as connection:
            rows = connection.execute(
                f"SELECT * FROM injections{where} ORDER BY created_at DESC LIMIT ?", params
            ).fetchall()
        return [_row_dict(row) or {} for row in rows]

    def update_injection_status(
        self,
        injection_ids: Sequence[str],
        *,
        status: str,
        business_code: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        if not injection_ids:
            return
        with self.write() as connection:
            for injection_id in injection_ids:
                connection.execute(
                    "UPDATE injections SET status = ?, business_code = ?, details_json = ? WHERE injection_id = ?",
                    (status, business_code, _json(dict(details or {})), injection_id),
                )

    def commit_purge(
        self,
        *,
        purge_id: str,
        intent_id: str,
        request_id: str,
        nodes: Sequence[str],
        canonical_path: str | None,
        tag: str | None,
        policy_version: int,
        broadcast_window_ms: float,
        started_at: float,
    ) -> dict[str, Any]:
        now = time.time()
        with self.write() as connection:
            affected: dict[str, list[str]] = {node: [] for node in nodes}
            transitions: list[tuple[str, str, str, int, str]] = []
            for node_id in nodes:
                clauses = ["node_id = ?", "state != 'PURGED'"]
                params: list[Any] = [node_id]
                if canonical_path is not None:
                    clauses.append("(canonical_path = ? OR canonical_path LIKE ?)")
                    params.extend([canonical_path, canonical_path.rstrip("/") + "/%"])
                if tag is not None:
                    clauses.append("cache_key IN (SELECT cache_key FROM tag_index WHERE node_id = ? AND tag = ?)")
                    params.extend([node_id, tag])
                rows = connection.execute(
                    f"SELECT cache_key, generation, state FROM cache_entries WHERE {' AND '.join(clauses)}",
                    params,
                ).fetchall()
                for row in rows:
                    connection.execute(
                        "UPDATE cache_entries SET state = 'PURGED', purge_epoch = purge_epoch + 1, updated_at = ? "
                        "WHERE node_id = ? AND cache_key = ?",
                        (now, node_id, row["cache_key"]),
                    )
                    connection.execute(
                        "DELETE FROM route_index WHERE node_id = ? AND cache_key = ?", (node_id, row["cache_key"])
                    )
                    connection.execute(
                        "DELETE FROM tag_index WHERE node_id = ? AND cache_key = ?", (node_id, row["cache_key"])
                    )
                    transitions.append((node_id, row["cache_key"], row["state"], row["generation"], "PURGE_BROADCAST"))
                    affected[node_id].append(row["cache_key"])
                connection.execute(
                    """
                    INSERT INTO node_epochs(node_id, epoch) VALUES (?, 1)
                    ON CONFLICT(node_id) DO UPDATE SET epoch = epoch + 1
                    """,
                    (node_id,),
                )
            for node_id, cache_key, from_state, generation, reason in transitions:
                connection.execute(
                    """
                    INSERT INTO entry_state_events(node_id, cache_key, generation, from_state, to_state, reason, created_at)
                    VALUES (?, ?, ?, ?, 'PURGED', ?, ?)
                    """,
                    (node_id, cache_key, generation, from_state, reason, now),
                )
            elapsed_ms = (now - started_at) * 1000.0
            if elapsed_ms > broadcast_window_ms:
                # The atomic transaction is deliberately not considered committed
                # when the declared propagation window is violated.
                raise wb_error(
                    "E_PURGE_BROADCAST_FAIL",
                    "Purge propagation exceeded the policy window",
                    elapsed_ms=elapsed_ms,
                    propagation_window_ms=broadcast_window_ms,
                )
            connection.execute(
                """
                INSERT INTO purge_operations(
                    purge_id, request_id, path, tag, policy_version, nodes_json, affected_json,
                    status, started_at, committed_at, broadcast_window_ms, elapsed_ms, intent_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'COMMITTED', ?, ?, ?, ?, ?)
                """,
                (
                    purge_id,
                    request_id,
                    canonical_path,
                    tag,
                    policy_version,
                    _json(list(nodes)),
                    _json(affected),
                    started_at,
                    now,
                    broadcast_window_ms,
                    elapsed_ms,
                    intent_id,
                ),
            )
            result = {
                "purge_id": purge_id,
                "nodes": list(nodes),
                "path": canonical_path,
                "tag": tag,
                "affected": affected,
                "affected_count": sum(len(keys) for keys in affected.values()),
                "atomic": True,
                "committed_at": now,
                "elapsed_ms": elapsed_ms,
                "propagation_window_ms": broadcast_window_ms,
            }
            connection.execute(
                "UPDATE intents SET status = 'COMMITTED', result_json = ?, updated_at = ? WHERE intent_id = ?",
                (_json(result), now, intent_id),
            )
        return result

    def record_purge_failure(
        self,
        *,
        purge_id: str,
        intent_id: str,
        request_id: str,
        nodes: Sequence[str],
        canonical_path: str | None,
        tag: str | None,
        policy_version: int,
        broadcast_window_ms: float,
        started_at: float,
        error_code: str,
        details: Mapping[str, Any],
    ) -> None:
        now = time.time()
        with self.write() as connection:
            connection.execute(
                """
                INSERT INTO purge_operations(
                    purge_id, request_id, path, tag, policy_version, nodes_json, affected_json,
                    status, started_at, committed_at, broadcast_window_ms, elapsed_ms, intent_id
                ) VALUES (?, ?, ?, ?, ?, ?, '{}', 'ROLLED_BACK', ?, NULL, ?, ?, ?)
                """,
                (
                    purge_id,
                    request_id,
                    canonical_path,
                    tag,
                    policy_version,
                    _json(list(nodes)),
                    started_at,
                    broadcast_window_ms,
                    (now - started_at) * 1000.0,
                    intent_id,
                ),
            )
            connection.execute(
                "UPDATE intents SET status = 'ROLLED_BACK', result_json = ?, updated_at = ? WHERE intent_id = ?",
                (_json({"business_code": error_code, "details": dict(details)}), now, intent_id),
            )

    def list_purges(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT * FROM purge_operations ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["nodes"] = json.loads(item.pop("nodes_json"))
            item["affected"] = json.loads(item.pop("affected_json"))
            results.append(item)
        return results

    def get_state_summary(self, nodes: Sequence[str]) -> dict[str, Any]:
        with self.read() as connection:
            policy_row = connection.execute(
                """
                SELECT p.version, p.digest, p.generated_at, p.nodes_json, p.snapshot_json, p.submitted_at
                FROM current_policy c JOIN policy_snapshots p ON p.version = c.version WHERE c.singleton = 1
                """
            ).fetchone()
            node_summaries: dict[str, Any] = {}
            for node_id in nodes:
                state_rows = connection.execute(
                    "SELECT state, COUNT(*) AS count FROM cache_entries WHERE node_id = ? GROUP BY state", (node_id,)
                ).fetchall()
                counts = {row["state"]: row["count"] for row in state_rows}
                index_rows = connection.execute(
                    "SELECT (SELECT COUNT(*) FROM route_index WHERE node_id = ?) AS routes, "
                    "(SELECT COUNT(*) FROM tag_index WHERE node_id = ?) AS tags",
                    (node_id, node_id),
                ).fetchone()
                origin_count = connection.execute(
                    "SELECT COUNT(*) AS count FROM origin_fetches WHERE node_id = ?", (node_id,)
                ).fetchone()["count"]
                epoch_row = connection.execute(
                    "SELECT epoch FROM node_epochs WHERE node_id = ?", (node_id,)
                ).fetchone()
                node_summaries[node_id] = {
                    "entries_by_state": counts,
                    "route_index_count": index_rows["routes"],
                    "tag_index_count": index_rows["tags"],
                    "origin_fetch_attempts": origin_count,
                    "epoch": int(epoch_row["epoch"]) if epoch_row else 0,
                }
            intent_rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM intents GROUP BY status"
            ).fetchall()
            request_rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM request_records GROUP BY status"
            ).fetchall()
            origin_rows = connection.execute("SELECT path, etag, version, length(body) AS length FROM origin_content ORDER BY path")
            origin_content = [dict(row) for row in origin_rows.fetchall()]
            latest_recovery = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY completed_at DESC LIMIT 1"
            ).fetchone()
        policy_summary = None
        if policy_row:
            snapshot = json.loads(policy_row["snapshot_json"])
            policy_summary = {
                "version": policy_row["version"],
                "digest": policy_row["digest"],
                "generated_at": policy_row["generated_at"],
                "nodes": json.loads(policy_row["nodes_json"]),
                "ttl_seconds": snapshot.get("ttl_seconds"),
                "stale_while_revalidate_seconds": snapshot.get("stale_while_revalidate_seconds"),
                "vary": snapshot.get("vary"),
                "purge": snapshot.get("purge"),
                "submitted_at": policy_row["submitted_at"],
            }
        recovery_summary = None
        if latest_recovery:
            recovery_summary = {
                "report_id": latest_recovery["report_id"],
                "completed_at": latest_recovery["completed_at"],
                "clean_startup": bool(latest_recovery["clean_startup"]),
                "intents_rolled_back": latest_recovery["intents_rolled_back"],
            }
        return {
            "policy": policy_summary,
            "nodes": node_summaries,
            "intents": {row["status"]: row["count"] for row in intent_rows},
            "requests": {row["status"]: row["count"] for row in request_rows},
            "origin_content": origin_content,
            "latest_recovery": recovery_summary,
        }

    def _facts(self, connection: sqlite3.Connection) -> dict[str, Any]:
        entries = connection.execute(
            "SELECT node_id, cache_key, etag, origin_version, state, generation, policy_version, updated_at "
            "FROM cache_entries ORDER BY node_id, cache_key"
        ).fetchall()
        state_counts: dict[str, dict[str, int]] = {}
        complete_entries: list[dict[str, Any]] = []
        for row in entries:
            node_counts = state_counts.setdefault(row["node_id"], {})
            node_counts[row["state"]] = node_counts.get(row["state"], 0) + 1
            complete_entries.append(dict(row))
        policy = connection.execute(
            "SELECT c.version, p.digest FROM current_policy c JOIN policy_snapshots p ON p.version = c.version WHERE c.singleton = 1"
        ).fetchone()
        last_requests = connection.execute(
            "SELECT request_id, node_id, status, business_code, updated_at FROM request_records "
            "WHERE status = 'COMPLETED' ORDER BY updated_at DESC LIMIT 20"
        ).fetchall()
        recent_purges = connection.execute(
            "SELECT purge_id, status, affected_json, committed_at FROM purge_operations "
            "WHERE status = 'COMMITTED' ORDER BY committed_at DESC LIMIT 20"
        ).fetchall()
        origin = connection.execute(
            "SELECT path, etag, version, length(body) AS length, updated_at FROM origin_content ORDER BY path"
        ).fetchall()
        return {
            "policy": dict(policy) if policy else None,
            "entries_by_node_and_state": state_counts,
            "complete_cache_entries": complete_entries,
            "last_completed_requests": [dict(row) for row in last_requests],
            "recent_committed_purges": [
                {
                    **dict(row),
                    "affected": json.loads(row["affected_json"]),
                    "affected_json": None,
                }
                for row in recent_purges
            ],
            "origin_content_versions": [dict(row) for row in origin],
            "observed_at": time.time(),
        }

    def recover_on_startup(self) -> dict[str, Any]:
        started_at = time.time()
        report_id = str(uuid.uuid4())
        with self.write() as connection:
            pre_facts = self._facts(connection)
            intent_rows = connection.execute("SELECT * FROM intents WHERE status = 'IN_PROGRESS'").fetchall()
            rolled_back: list[dict[str, Any]] = []
            for row in intent_rows:
                connection.execute(
                    "UPDATE intents SET status = 'ROLLED_BACK', result_json = ?, updated_at = ? WHERE intent_id = ?",
                    (_json({"reason": "PROCESS_TERMINATED_BEFORE_COMMIT", "recovery_report_id": report_id}), time.time(), row["intent_id"]),
                )
                rolled_back.append(
                    {
                        "intent_id": row["intent_id"],
                        "kind": row["kind"],
                        "request_id": row["request_id"],
                        "node_id": row["node_id"],
                        "cache_key": row["cache_key"],
                        "payload": json.loads(row["payload_json"]),
                        "resolution": "ROLLED_BACK; no uncommitted cache mutation retained",
                    }
                )
            connection.execute(
                "UPDATE request_records SET status = 'INCOMPLETE', business_code = 'E_REQUEST_INCOMPLETE', updated_at = ? "
                "WHERE status = 'IN_PROGRESS'",
                (time.time(),),
            )
            connection.execute(
                "UPDATE purge_operations SET status = 'ROLLED_BACK' WHERE status = 'IN_PROGRESS'"
            )
            interrupted_fetches = connection.execute(
                "UPDATE origin_fetches SET status = 'INTERRUPTED', error_code = 'E_PROCESS_TERMINATED', completed_at = ? "
                "WHERE status = 'IN_PROGRESS'",
                (time.time(),),
            ).rowcount
            now = time.time()
            revalidating = connection.execute("SELECT * FROM cache_entries WHERE state = 'REVALIDATING'").fetchall()
            normalized_states = 0
            for row in revalidating:
                restored = "FRESH" if now < row["fresh_until"] else "STALE"
                connection.execute(
                    "UPDATE cache_entries SET state = ?, updated_at = ? WHERE node_id = ? AND cache_key = ?",
                    (restored, now, row["node_id"], row["cache_key"]),
                )
                connection.execute(
                    """
                    INSERT INTO entry_state_events(node_id, cache_key, generation, from_state, to_state, reason, created_at)
                    VALUES (?, ?, ?, 'REVALIDATING', ?, 'RECOVERY_LAST_COMPLETE_VERSION', ?)
                    """,
                    (row["node_id"], row["cache_key"], row["generation"], restored, now),
                )
                normalized_states += 1
            post_facts = self._facts(connection)
            clean = not intent_rows and normalized_states == 0
            details = {
                "recovered_intents": rolled_back,
                "interrupted_origin_fetches": int(interrupted_fetches),
                "normalized_revalidating_entries": normalized_states,
                "basis": "SQLite persisted rows only; report generated after process restart",
                "exit_report_available_at_termination": False,
                "note": "A forcibly terminated process cannot emit a final exit report; this report is produced by the next startup.",
            }
            completed_at = time.time()
            connection.execute(
                """
                INSERT INTO recovery_reports(
                    report_id, started_at, completed_at, clean_startup, intents_scanned,
                    intents_rolled_back, details_json, facts_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report_id,
                    started_at,
                    completed_at,
                    1 if clean else 0,
                    len(intent_rows),
                    len(rolled_back),
                    _json(details),
                    _json({"pre_recovery": pre_facts, "post_recovery": post_facts}),
                ),
            )
        return {
            "report_id": report_id,
            "started_at": started_at,
            "completed_at": completed_at,
            "clean_startup": clean,
            "intents_scanned": len(intent_rows),
            "intents_rolled_back": len(rolled_back),
            "recovered_intents": rolled_back,
            "normalized_revalidating_entries": normalized_states,
            "interrupted_origin_fetches": int(interrupted_fetches),
            "facts": {"pre_recovery": pre_facts, "post_recovery": post_facts},
            "basis": details["basis"],
            "exit_report_available_at_termination": False,
            "note": details["note"],
        }

    def get_recovery_reports(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY completed_at DESC LIMIT ?", (limit,)
            ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["clean_startup"] = bool(item["clean_startup"])
            details = json.loads(item.pop("details_json"))
            item["details"] = details
            for key in (
                "recovered_intents",
                "interrupted_origin_fetches",
                "normalized_revalidating_entries",
                "basis",
                "exit_report_available_at_termination",
                "note",
            ):
                if key in details:
                    item[key] = details[key]
            item["facts"] = json.loads(item.pop("facts_json"))
            results.append(item)
        return results

    def latest_recovery_report(self) -> dict[str, Any] | None:
        reports = self.get_recovery_reports(limit=1)
        return reports[0] if reports else None

    def list_origin_fetches(self, *, request_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        with self.read() as connection:
            if request_id:
                rows = connection.execute(
                    "SELECT * FROM origin_fetches WHERE request_id = ? ORDER BY started_at", (request_id,)
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM origin_fetches ORDER BY started_at DESC LIMIT ?", (limit,)
                ).fetchall()
        return [dict(row) for row in rows]

