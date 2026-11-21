"""SQLite persistence with intent logging and atomic publication boundaries."""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .codec import canonical_json, utc_now_iso
from .models import ComponentDeclaration


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    version INTEGER PRIMARY KEY,
    base_version INTEGER NOT NULL,
    generated_at TEXT NOT NULL,
    configs_json TEXT NOT NULL,
    components_json TEXT NOT NULL,
    checksum TEXT NOT NULL,
    source TEXT NOT NULL,
    changed_config_keys_json TEXT NOT NULL,
    changed_component_ids_json TEXT NOT NULL,
    affected_components_json TEXT NOT NULL,
    rebuild_scope_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS derived_artifacts (
    version INTEGER NOT NULL,
    artifact_key TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    value_json TEXT NOT NULL,
    refs_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    invalidated INTEGER NOT NULL,
    PRIMARY KEY (version, artifact_key)
);

CREATE TABLE IF NOT EXISTS component_states (
    version INTEGER NOT NULL,
    component_id TEXT NOT NULL,
    status TEXT NOT NULL,
    desired_digest TEXT NOT NULL,
    last_applied_digest TEXT,
    last_applied_version INTEGER,
    endpoint_url TEXT NOT NULL,
    request_id TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (version, component_id)
);

CREATE TABLE IF NOT EXISTS state_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL,
    component_id TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dispatch_runs (
    version INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    policy TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    result_json TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS intents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    snapshot_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_events_version_component
    ON state_events(version, component_id, id);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, id);
CREATE INDEX IF NOT EXISTS idx_artifacts_type ON derived_artifacts(version, artifact_type);
"""


class Store:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(SCHEMA)
            connection.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('current_version', '0')"
            )
        finally:
            connection.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def create_intent(self, kind: str, snapshot_version: int, payload: dict[str, Any]) -> str:
        intent_id = uuid.uuid4().hex
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO intents(intent_id, kind, snapshot_version, status, payload_json, created_at)
                VALUES(?, ?, ?, 'PENDING', ?, ?)
                """,
                (intent_id, kind, snapshot_version, canonical_json(payload), utc_now_iso()),
            )
        return intent_id

    @staticmethod
    def complete_intent(connection: sqlite3.Connection, intent_id: str) -> None:
        connection.execute(
            "UPDATE intents SET status='COMPLETE', finished_at=? WHERE intent_id=?",
            (utc_now_iso(), intent_id),
        )

    @staticmethod
    def abort_intent_in_transaction(
        connection: sqlite3.Connection, intent_id: str, error: str
    ) -> None:
        connection.execute(
            "UPDATE intents SET status='ABORTED', finished_at=?, error=? WHERE intent_id=?",
            (utc_now_iso(), error[:1000], intent_id),
        )

    def abort_intent(self, intent_id: str, error: str) -> None:
        try:
            with self.transaction() as connection:
                connection.execute(
                    """
                    UPDATE intents
                    SET status='ABORTED', finished_at=?, error=?
                    WHERE intent_id=? AND status='PENDING'
                    """,
                    (utc_now_iso(), error[:1000], intent_id),
                )
        except sqlite3.Error:
            # Recovery can repair a pending intent later.
            pass

    @contextmanager
    def intent_scope(
        self, kind: str, snapshot_version: int, payload: dict[str, Any]
    ) -> Iterator[tuple[sqlite3.Connection, str]]:
        intent_id = self.create_intent(kind, snapshot_version, payload)
        try:
            with self.transaction() as connection:
                yield connection, intent_id
                self.complete_intent(connection, intent_id)
        except BaseException as exc:
            self.abort_intent(intent_id, f"{type(exc).__name__}: {exc}")
            raise

    @staticmethod
    def current_version(connection: sqlite3.Connection) -> int:
        row = connection.execute("SELECT value FROM meta WHERE key='current_version'").fetchone()
        return int(row["value"]) if row else 0

    @staticmethod
    def set_current_version(connection: sqlite3.Connection, version: int) -> None:
        connection.execute(
            "INSERT INTO meta(key, value) VALUES('current_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(version),),
        )

    @staticmethod
    def insert_snapshot(connection: sqlite3.Connection, record: dict[str, Any]) -> None:
        connection.execute(
            """
            INSERT INTO snapshots(
                version, base_version, generated_at, configs_json, components_json,
                checksum, source, changed_config_keys_json, changed_component_ids_json,
                affected_components_json, rebuild_scope_json, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["version"],
                record["base_version"],
                record["generated_at"],
                canonical_json(record["configs"]),
                canonical_json(record["components"]),
                record["checksum"],
                record["source"],
                canonical_json(record["changed_config_keys"]),
                canonical_json(record["changed_component_ids"]),
                canonical_json(record["affected_components"]),
                canonical_json(record["rebuild_scope"]),
                record.get("created_at") or utc_now_iso(),
            ),
        )

    @staticmethod
    def get_snapshot_row(
        connection: sqlite3.Connection, version: int
    ) -> sqlite3.Row | None:
        return connection.execute("SELECT * FROM snapshots WHERE version=?", (version,)).fetchone()

    @staticmethod
    def snapshot_from_row(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "version": row["version"],
            "base_version": row["base_version"],
            "generated_at": row["generated_at"],
            "configs": json.loads(row["configs_json"]),
            "components": [
                ComponentDeclaration.model_validate(item)
                for item in json.loads(row["components_json"])
            ],
            "checksum": row["checksum"],
            "source": row["source"],
            "changed_config_keys": json.loads(row["changed_config_keys_json"]),
            "changed_component_ids": json.loads(row["changed_component_ids_json"]),
            "affected_components": json.loads(row["affected_components_json"]),
            "rebuild_scope": json.loads(row["rebuild_scope_json"]),
            "created_at": row["created_at"],
        }

    @classmethod
    def load_snapshot(cls, connection: sqlite3.Connection, version: int) -> dict[str, Any] | None:
        row = cls.get_snapshot_row(connection, version)
        return cls.snapshot_from_row(row) if row else None

    @classmethod
    def load_current_snapshot(cls, connection: sqlite3.Connection) -> dict[str, Any] | None:
        version = cls.current_version(connection)
        return cls.load_snapshot(connection, version) if version else None

    @staticmethod
    def write_artifacts(
        connection: sqlite3.Connection, version: int, artifacts: list[Any]
    ) -> None:
        connection.execute("DELETE FROM derived_artifacts WHERE version=?", (version,))
        connection.executemany(
            """
            INSERT INTO derived_artifacts(
                version, artifact_key, artifact_type, artifact_id,
                value_json, refs_json, digest, invalidated
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    version,
                    artifact.artifact_key,
                    artifact.artifact_type,
                    artifact.artifact_id,
                    canonical_json(artifact.value),
                    canonical_json(artifact.refs),
                    artifact.digest,
                    1 if artifact.invalidated else 0,
                )
                for artifact in artifacts
            ],
        )

    @staticmethod
    def load_artifacts(connection: sqlite3.Connection, version: int) -> list[dict[str, Any]]:
        rows = connection.execute(
            "SELECT * FROM derived_artifacts WHERE version=? ORDER BY artifact_key",
            (version,),
        ).fetchall()
        return [
            {
                "artifact_key": row["artifact_key"],
                "artifact_type": row["artifact_type"],
                "artifact_id": row["artifact_id"],
                "value": json.loads(row["value_json"]),
                "refs": json.loads(row["refs_json"]),
                "digest": row["digest"],
                "invalidated": bool(row["invalidated"]),
            }
            for row in rows
        ]

    @staticmethod
    def write_component_states(
        connection: sqlite3.Connection, version: int, states: list[dict[str, Any]]
    ) -> None:
        connection.execute("DELETE FROM component_states WHERE version=?", (version,))
        connection.executemany(
            """
            INSERT INTO component_states(
                version, component_id, status, desired_digest, last_applied_digest,
                last_applied_version, endpoint_url, request_id, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    version,
                    state["component_id"],
                    state["status"],
                    state["desired_digest"],
                    state.get("last_applied_digest"),
                    state.get("last_applied_version"),
                    state["endpoint_url"],
                    state.get("request_id"),
                    state.get("updated_at") or utc_now_iso(),
                )
                for state in states
            ],
        )

    @staticmethod
    def load_component_states(
        connection: sqlite3.Connection, version: int
    ) -> dict[str, dict[str, Any]]:
        rows = connection.execute(
            "SELECT * FROM component_states WHERE version=? ORDER BY component_id",
            (version,),
        ).fetchall()
        return {
            row["component_id"]: {
                "component_id": row["component_id"],
                "status": row["status"],
                "desired_digest": row["desired_digest"],
                "last_applied_digest": row["last_applied_digest"],
                "last_applied_version": row["last_applied_version"],
                "endpoint_url": row["endpoint_url"],
                "request_id": row["request_id"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        }

    @staticmethod
    def update_component_state(
        connection: sqlite3.Connection,
        version: int,
        component_id: str,
        *,
        status: str,
        last_applied_digest: str | None = None,
        last_applied_version: int | None = None,
        request_id: str | None = None,
    ) -> None:
        connection.execute(
            """
            UPDATE component_states
            SET status=?, last_applied_digest=?, last_applied_version=?,
                request_id=?, updated_at=?
            WHERE version=? AND component_id=?
            """,
            (
                status,
                last_applied_digest,
                last_applied_version,
                request_id,
                utc_now_iso(),
                version,
                component_id,
            ),
        )

    @staticmethod
    def append_event(
        connection: sqlite3.Connection,
        *,
        version: int,
        component_id: str,
        from_status: str | None,
        to_status: str,
        reason: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO state_events(
                version, component_id, from_status, to_status, reason, created_at
            ) VALUES(?, ?, ?, ?, ?, ?)
            """,
            (version, component_id, from_status, to_status, reason, utc_now_iso()),
        )

    @staticmethod
    def load_events(
        connection: sqlite3.Connection,
        *,
        version: int | None = None,
        component_id: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if version is not None:
            clauses.append("version=?")
            params.append(version)
        if component_id is not None:
            clauses.append("component_id=?")
            params.append(component_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = connection.execute(
            f"SELECT * FROM state_events {where} ORDER BY id", params
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def get_dispatch_run(
        connection: sqlite3.Connection, version: int
    ) -> sqlite3.Row | None:
        return connection.execute("SELECT * FROM dispatch_runs WHERE version=?", (version,)).fetchone()

    @staticmethod
    def insert_dispatch_run(
        connection: sqlite3.Connection,
        *,
        version: int,
        run_id: str,
        policy: str,
        plan: dict[str, Any],
    ) -> None:
        connection.execute(
            """
            INSERT INTO dispatch_runs(
                version, run_id, status, policy, plan_json, result_json, started_at
            ) VALUES(?, ?, 'RUNNING', ?, ?, NULL, ?)
            """,
            (version, run_id, policy, canonical_json(plan), utc_now_iso()),
        )

    @staticmethod
    def finalize_dispatch_run(
        connection: sqlite3.Connection,
        version: int,
        *,
        status: str,
        result: dict[str, Any],
        error: str | None = None,
    ) -> None:
        connection.execute(
            """
            UPDATE dispatch_runs
            SET status=?, result_json=?, finished_at=?, error=?
            WHERE version=?
            """,
            (status, canonical_json(result), utc_now_iso(), error, version),
        )

    @staticmethod
    def dispatch_result(row: sqlite3.Row) -> dict[str, Any] | None:
        return json.loads(row["result_json"]) if row["result_json"] else None

    @staticmethod
    def pending_intents(connection: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = connection.execute(
            "SELECT * FROM intents WHERE status='PENDING' ORDER BY id"
        ).fetchall()
        return [
            {
                "intent_id": row["intent_id"],
                "kind": row["kind"],
                "snapshot_version": row["snapshot_version"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

