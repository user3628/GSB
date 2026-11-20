from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .models import RouteState
from .util import canonical_json, utc_now


TERMINAL_INTENT_STATES = {"COMMITTED", "FAILED", "ABORTED"}


class MetadataStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            else:
                connection.execute("COMMIT")

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    version INTEGER PRIMARY KEY,
                    checksum TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    document_json TEXT NOT NULL,
                    committed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS active_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    version INTEGER NOT NULL,
                    checksum TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS intents (
                    intent_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    request_id TEXT,
                    routing_version INTEGER,
                    state TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state);
                CREATE INDEX IF NOT EXISTS idx_intents_request ON intents(request_id);

                CREATE TABLE IF NOT EXISTS cache_manifests (
                    routing_version INTEGER NOT NULL,
                    logical_table TEXT NOT NULL,
                    cache_version INTEGER NOT NULL,
                    source_version INTEGER NOT NULL,
                    rebuilt INTEGER NOT NULL,
                    cache_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (routing_version, logical_table)
                );

                CREATE TABLE IF NOT EXISTS query_batches (
                    request_id TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    routing_version INTEGER,
                    status TEXT NOT NULL,
                    result_json TEXT,
                    error_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_query_batches_status ON query_batches(status);

                CREATE TABLE IF NOT EXISTS service_runs (
                    run_id TEXT PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    graceful_stop_at TEXT,
                    detected_crash INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS recovery_reports (
                    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    generated_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                """
            )

    @staticmethod
    def new_intent_id(prefix: str) -> str:
        return f"{prefix}:{uuid.uuid4().hex}"

    def create_intent(
        self,
        *,
        intent_id: str,
        kind: str,
        detail: dict[str, Any],
        request_id: str | None = None,
        routing_version: int | None = None,
        state: str = "PREPARED",
    ) -> dict[str, Any]:
        now = utc_now()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO intents
                    (intent_id, kind, request_id, routing_version, state,
                     detail_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent_id,
                    kind,
                    request_id,
                    routing_version,
                    state,
                    canonical_json(detail),
                    now,
                    now,
                ),
            )
        return {
            "intent_id": intent_id,
            "kind": kind,
            "request_id": request_id,
            "routing_version": routing_version,
            "state": state,
            "created_at": now,
        }

    def commit_snapshot(
        self,
        *,
        route_state: RouteState,
        intent_id: str,
        cache_intent_id: str | None = None,
    ) -> None:
        now = utc_now()
        snapshot = route_state.snapshot
        document_json = canonical_json(snapshot.document)
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO snapshots
                    (version, checksum, generated_at, document_json, committed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (snapshot.version, snapshot.checksum, snapshot.generated_at, document_json, now),
            )
            connection.execute(
                """
                INSERT INTO active_state (singleton, version, checksum, updated_at)
                VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    version = excluded.version,
                    checksum = excluded.checksum,
                    updated_at = excluded.updated_at
                """,
                (snapshot.version, snapshot.checksum, now),
            )
            for table_name, cache in sorted(route_state.caches.items()):
                connection.execute(
                    """
                    INSERT INTO cache_manifests
                        (routing_version, logical_table, cache_version, source_version,
                         rebuilt, cache_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        route_state.version,
                        table_name,
                        cache.effective_version,
                        cache.source_version,
                        int(cache.rebuilt),
                        canonical_json(cache.as_dict()),
                        now,
                    ),
                )
            updated = connection.execute(
                """
                UPDATE intents
                   SET state = 'COMMITTED', updated_at = ?
                 WHERE intent_id = ? AND state = 'PREPARED'
                """,
                (now, intent_id),
            ).rowcount
            if updated != 1:
                raise RuntimeError(f"snapshot intent {intent_id} was not in PREPARED state")
            if cache_intent_id is not None:
                cache_updated = connection.execute(
                    """
                    UPDATE intents
                       SET state = 'COMMITTED', updated_at = ?
                     WHERE intent_id = ? AND state = 'PREPARED'
                    """,
                    (now, cache_intent_id),
                ).rowcount
                if cache_updated != 1:
                    raise RuntimeError(
                        f"cache intent {cache_intent_id} was not in PREPARED state"
                    )

    def record_cache_rebuild(
        self,
        *,
        route_state: RouteState,
        intent_id: str,
    ) -> None:
        now = utc_now()
        with self.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE intents
                   SET state = 'COMMITTED', updated_at = ?
                 WHERE intent_id = ? AND state = 'PREPARED'
                """,
                (now, intent_id),
            ).rowcount
            if updated != 1:
                raise RuntimeError(f"cache intent {intent_id} was not in PREPARED state")

    def complete_intent(self, intent_id: str, state: str = "COMMITTED") -> None:
        with self.transaction() as connection:
            updated = connection.execute(
                "UPDATE intents SET state = ?, updated_at = ? WHERE intent_id = ?",
                (state, utc_now(), intent_id),
            ).rowcount
            if updated != 1:
                raise RuntimeError(f"intent {intent_id} does not exist")

    def get_active_row(self) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT version, checksum, updated_at FROM active_state WHERE singleton = 1"
            ).fetchone()
        return dict(row) if row else None

    def get_snapshot_row(self, version: int) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT version, checksum, generated_at, document_json, committed_at
                  FROM snapshots WHERE version = ?
                """,
                (version,),
            ).fetchone()
        return dict(row) if row else None

    def list_snapshot_rows(self) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT version, checksum, generated_at, document_json, committed_at
                  FROM snapshots ORDER BY version DESC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def set_active_version(self, version: int, checksum: str) -> None:
        now = utc_now()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO active_state (singleton, version, checksum, updated_at)
                VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    version = excluded.version,
                    checksum = excluded.checksum,
                    updated_at = excluded.updated_at
                """,
                (version, checksum, now),
            )

    def clear_active_state(self) -> None:
        with self.transaction() as connection:
            connection.execute("DELETE FROM active_state WHERE singleton = 1")

    def begin_run(self, *, run_id: str, pid: int) -> dict[str, Any]:
        started_at = utc_now()
        with self.transaction() as connection:
            previous_rows = connection.execute(
                """
                SELECT run_id, pid, started_at, graceful_stop_at, detected_crash
                  FROM service_runs ORDER BY started_at DESC
                """
            ).fetchall()
            previous = dict(previous_rows[0]) if previous_rows else None
            unclean = [
                dict(row)
                for row in previous_rows
                if row["graceful_stop_at"] is None and not row["detected_crash"]
            ]
            connection.execute(
                "UPDATE service_runs SET detected_crash = 1 WHERE graceful_stop_at IS NULL"
            )
            connection.execute(
                """
                INSERT INTO service_runs (run_id, pid, started_at, graceful_stop_at, detected_crash)
                VALUES (?, ?, ?, NULL, 0)
                """,
                (run_id, pid, started_at),
            )
        return {
            "run_id": run_id,
            "started_at": started_at,
            "previous_run": previous,
            "previous_unclean_run_count": len(unclean),
        }

    def mark_graceful_stop(self, run_id: str) -> None:
        with self.transaction() as connection:
            connection.execute(
                "UPDATE service_runs SET graceful_stop_at = ? WHERE run_id = ?",
                (utc_now(), run_id),
            )

    def create_query_batch(
        self,
        *,
        request_id: str,
        fingerprint: str,
        routing_version: int | None,
        intent_id: str,
        request_detail: dict[str, Any],
    ) -> None:
        now = utc_now()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO query_batches
                    (request_id, fingerprint, routing_version, status,
                     result_json, error_json, created_at, updated_at)
                VALUES (?, ?, ?, 'RUNNING', NULL, NULL, ?, ?)
                """,
                (request_id, fingerprint, routing_version, now, now),
            )
            connection.execute(
                """
                INSERT INTO intents
                    (intent_id, kind, request_id, routing_version, state,
                     detail_json, created_at, updated_at)
                VALUES (?, 'QUERY_BATCH', ?, ?, 'RUNNING', ?, ?, ?)
                """,
                (
                    intent_id,
                    request_id,
                    routing_version,
                    canonical_json(request_detail),
                    now,
                    now,
                ),
            )

    def finalize_query_batch(
        self,
        *,
        request_id: str,
        status: str,
        result: dict[str, Any] | None = None,
        error_payload: dict[str, Any] | None = None,
        intent_id: str,
    ) -> None:
        if status not in {"SUCCEEDED", "FAILED", "ABORTED"}:
            raise ValueError(f"invalid query batch status: {status}")
        now = utc_now()
        intent_state = "COMMITTED" if status == "SUCCEEDED" else status
        with self.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE query_batches
                   SET status = ?, result_json = ?, error_json = ?, updated_at = ?
                 WHERE request_id = ?
                """,
                (
                    status,
                    canonical_json(result) if result is not None else None,
                    canonical_json(error_payload) if error_payload is not None else None,
                    now,
                    request_id,
                ),
            ).rowcount
            if updated != 1:
                raise RuntimeError(f"query batch {request_id} does not exist")
            connection.execute(
                "UPDATE intents SET state = ?, updated_at = ? WHERE intent_id = ?",
                (intent_state, now, intent_id),
            )

    def get_query_batch(self, request_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM query_batches WHERE request_id = ?", (request_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_unfinished_intents(self) -> list[dict[str, Any]]:
        placeholders = ", ".join("?" for _ in TERMINAL_INTENT_STATES)
        with self.connect() as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM intents
                 WHERE state NOT IN ({placeholders})
                 ORDER BY created_at, intent_id
                """,
                tuple(sorted(TERMINAL_INTENT_STATES)),
            ).fetchall()
        return [dict(row) for row in rows]

    def abort_running_queries(self, reason: str) -> list[dict[str, Any]]:
        now = utc_now()
        aborted: list[dict[str, Any]] = []
        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM query_batches WHERE status = 'RUNNING' ORDER BY created_at, request_id"
            ).fetchall()
            for row in rows:
                error_payload = {
                    "code": "E_EXECUTION_ABORTED",
                    "message": "query batch was not completed before process termination",
                    "context": {
                        "request_id": row["request_id"],
                        "routing_version": row["routing_version"],
                        "recovered": True,
                        "reason": reason,
                    },
                }
                connection.execute(
                    """
                    UPDATE query_batches
                       SET status = 'ABORTED', error_json = ?, updated_at = ?
                     WHERE request_id = ? AND status = 'RUNNING'
                    """,
                    (canonical_json(error_payload), now, row["request_id"]),
                )
                connection.execute(
                    """
                    UPDATE intents
                       SET state = 'ABORTED', updated_at = ?
                     WHERE request_id = ? AND kind = 'QUERY_BATCH' AND state = 'RUNNING'
                    """,
                    (now, row["request_id"]),
                )
                aborted.append(
                    {
                        "request_id": row["request_id"],
                        "routing_version": row["routing_version"],
                        "created_at": row["created_at"],
                        "error": error_payload,
                    }
                )
        return aborted

    def abort_other_unfinished_intents(self, reason: str) -> list[dict[str, Any]]:
        now = utc_now()
        aborted: list[dict[str, Any]] = []
        with self.transaction() as connection:
            placeholders = ", ".join("?" for _ in TERMINAL_INTENT_STATES)
            rows = connection.execute(
                f"""
                SELECT * FROM intents
                 WHERE kind != 'QUERY_BATCH' AND state NOT IN ({placeholders})
                 ORDER BY created_at, intent_id
                """,
                tuple(sorted(TERMINAL_INTENT_STATES)),
            ).fetchall()
            for row in rows:
                connection.execute(
                    "UPDATE intents SET state = 'ABORTED', updated_at = ? WHERE intent_id = ?",
                    (now, row["intent_id"]),
                )
                aborted.append(
                    {
                        "intent_id": row["intent_id"],
                        "kind": row["kind"],
                        "request_id": row["request_id"],
                        "routing_version": row["routing_version"],
                        "prior_state": row["state"],
                        "reason": reason,
                    }
                )
        return aborted

    def insert_recovery_report(self, payload: dict[str, Any]) -> int:
        generated_at = utc_now()
        with self.transaction() as connection:
            cursor = connection.execute(
                "INSERT INTO recovery_reports (generated_at, payload_json) VALUES (?, ?)",
                (generated_at, canonical_json(payload)),
            )
            return int(cursor.lastrowid)

    def latest_recovery_report(self) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT report_id, generated_at, payload_json FROM recovery_reports ORDER BY report_id DESC LIMIT 1"
            ).fetchone()
        if not row:
            return None
        payload = json.loads(row["payload_json"])
        payload["report_id"] = row["report_id"]
        payload["persisted_at"] = row["generated_at"]
        return payload

    def counts(self) -> dict[str, int]:
        with self.connect() as connection:
            return {
                "snapshots": int(connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]),
                "intents": int(connection.execute("SELECT COUNT(*) FROM intents").fetchone()[0]),
                "query_batches": int(
                    connection.execute("SELECT COUNT(*) FROM query_batches").fetchone()[0]
                ),
                "running_query_batches": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM query_batches WHERE status = 'RUNNING'"
                    ).fetchone()[0]
                ),
                "recovery_reports": int(
                    connection.execute("SELECT COUNT(*) FROM recovery_reports").fetchone()[0]
                ),
            }

    def list_query_batches(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT request_id, fingerprint, routing_version, status, created_at, updated_at
                  FROM query_batches ORDER BY created_at DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_cache_manifests(self, routing_version: int) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM cache_manifests
                 WHERE routing_version = ? ORDER BY logical_table
                """,
                (routing_version,),
            ).fetchall()
        return [dict(row) for row in rows]

