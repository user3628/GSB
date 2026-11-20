from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence

from .models import Policy, normalize_policy, now_ms


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def policies_snapshot_json(policies: Sequence[Policy]) -> str:
    return canonical_json([policy.to_internal_dict() for policy in sorted(policies, key=lambda item: item.slave_id)])


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()

    @contextmanager
    def reader(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        self.initialize_connection()
        with self.transaction() as connection:
            statements = [
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS policies (
                    slave_id TEXT PRIMARY KEY,
                    replication_group TEXT NOT NULL,
                    delay_threshold_ms INTEGER NOT NULL,
                    tolerable_delay_ms INTEGER NOT NULL,
                    expected_version TEXT NOT NULL,
                    degradation_action TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    forward_compatible INTEGER NOT NULL,
                    policy_version INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS dataset_versions (
                    version INTEGER PRIMARY KEY,
                    reason TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    snapshot_json TEXT NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS observations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    slave_id TEXT NOT NULL,
                    sampled_at_ms INTEGER NOT NULL,
                    binlog_gap INTEGER NOT NULL,
                    delay_ms INTEGER NOT NULL,
                    reported_version TEXT NOT NULL,
                    ingested_at_ms INTEGER NOT NULL
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_observations_batch_slave_time ON observations(batch_id, slave_id, sampled_at_ms)",
                "CREATE INDEX IF NOT EXISTS idx_observations_slave_time ON observations(slave_id, sampled_at_ms)",
                """
                CREATE TABLE IF NOT EXISTS states (
                    slave_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    last_sampled_at_ms INTEGER,
                    last_observation_json TEXT,
                    effective_action TEXT NOT NULL,
                    trigger_json TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS state_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slave_id TEXT NOT NULL,
                    from_state TEXT NOT NULL,
                    to_state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    batch_id TEXT,
                    created_at_ms INTEGER NOT NULL,
                    details_json TEXT NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS recalc_runs (
                    recalc_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS adjudications (
                    recalc_id TEXT NOT NULL,
                    slave_id TEXT NOT NULL,
                    effective_action TEXT NOT NULL,
                    note TEXT NOT NULL,
                    edited_at_ms INTEGER NOT NULL,
                    base_dataset_version INTEGER,
                    PRIMARY KEY (recalc_id, slave_id),
                    FOREIGN KEY (recalc_id) REFERENCES recalc_runs(recalc_id) ON DELETE CASCADE
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS intents (
                    intent_id TEXT PRIMARY KEY,
                    batch_id TEXT,
                    operation TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    finished_at_ms INTEGER,
                    details_json TEXT NOT NULL,
                    error_json TEXT
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status)",
                """
                CREATE TABLE IF NOT EXISTS applied_batches (
                    batch_id TEXT PRIMARY KEY,
                    operation TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    error_json TEXT,
                    created_at_ms INTEGER NOT NULL,
                    finished_at_ms INTEGER NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS observation_batches (
                    batch_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS recovery_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_ms INTEGER NOT NULL,
                    report_json TEXT NOT NULL
                )
                """
            ]
            for statement in statements:
                connection.execute(statement)

            defaults = {
                "schema_version": "1",
                "dataset_version": "1",
                "state_version": "1",
            }
            for key, value in defaults.items():
                connection.execute("INSERT OR IGNORE INTO meta(key, value) VALUES (?, ?)", (key, value))

            has_snapshot = connection.execute("SELECT 1 FROM dataset_versions LIMIT 1").fetchone()
            if not has_snapshot:
                created = now_ms()
                connection.execute(
                    "INSERT INTO dataset_versions(version, reason, created_at_ms, snapshot_json) VALUES (?, ?, ?, ?)",
                    (1, "INITIAL_EMPTY_DATASET", created, "[]"),
                )

    @staticmethod
    def meta_value(connection: sqlite3.Connection, key: str) -> str | None:
        row = connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row["value"])

    @staticmethod
    def set_meta(connection: sqlite3.Connection, key: str, value: Any) -> None:
        connection.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )

    @classmethod
    def dataset_version(cls, connection: sqlite3.Connection) -> int:
        value = cls.meta_value(connection, "dataset_version")
        return int(value) if value is not None else 1

    @classmethod
    def state_version(cls, connection: sqlite3.Connection) -> int:
        value = cls.meta_value(connection, "state_version")
        return int(value) if value is not None else 1

    @staticmethod
    def row_to_policy(row: sqlite3.Row | dict[str, Any]) -> Policy:
        data = dict(row)
        data["forward_compatible"] = bool(data["forward_compatible"])
        return normalize_policy(data)

    @classmethod
    def read_policies(cls, connection: sqlite3.Connection) -> list[Policy]:
        rows = connection.execute("SELECT * FROM policies ORDER BY slave_id").fetchall()
        return [cls.row_to_policy(row) for row in rows]

    @classmethod
    def upsert_policy(cls, connection: sqlite3.Connection, policy: Policy) -> None:
        connection.execute(
            """
            INSERT INTO policies(
                slave_id, replication_group, delay_threshold_ms, tolerable_delay_ms,
                expected_version, degradation_action, priority, forward_compatible,
                policy_version, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(slave_id) DO UPDATE SET
                replication_group = excluded.replication_group,
                delay_threshold_ms = excluded.delay_threshold_ms,
                tolerable_delay_ms = excluded.tolerable_delay_ms,
                expected_version = excluded.expected_version,
                degradation_action = excluded.degradation_action,
                priority = excluded.priority,
                forward_compatible = excluded.forward_compatible,
                policy_version = excluded.policy_version,
                updated_at_ms = excluded.updated_at_ms
            """,
            (
                policy.slave_id,
                policy.replication_group,
                policy.delay_threshold_ms,
                policy.tolerable_delay_ms,
                policy.expected_version,
                policy.degradation_action,
                policy.priority,
                1 if policy.forward_compatible else 0,
                policy.policy_version,
                policy.updated_at_ms,
            ),
        )

    @classmethod
    def capture_dataset_version(
        cls, connection: sqlite3.Connection, *, reason: str, increment: bool = True
    ) -> int:
        current = cls.dataset_version(connection)
        version = current + 1 if increment else current
        policies = cls.read_policies(connection)
        connection.execute(
            "INSERT INTO dataset_versions(version, reason, created_at_ms, snapshot_json) VALUES (?, ?, ?, ?)",
            (version, reason, now_ms(), policies_snapshot_json(policies)),
        )
        cls.set_meta(connection, "dataset_version", version)
        return version

    @classmethod
    def current_policy_snapshot(cls, connection: sqlite3.Connection) -> str:
        return policies_snapshot_json(cls.read_policies(connection))

    @classmethod
    def current_policy_storage_snapshot(cls, connection: sqlite3.Connection) -> str:
        """Canonical snapshot that can be read even when persisted rows fail domain validation."""
        rows = connection.execute("SELECT * FROM policies ORDER BY slave_id").fetchall()
        records = []
        for row in rows:
            data = dict(row)
            data["forward_compatible"] = bool(data["forward_compatible"])
            records.append(data)
        return canonical_json(records)

    def initialize_connection(self) -> None:
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
        finally:
            connection.close()

    @classmethod
    def get_batch_row(cls, connection: sqlite3.Connection, batch_id: str) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM applied_batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()

    def get_batch_result(self, batch_id: str) -> dict[str, Any] | None:
        if not batch_id:
            return None
        connection = self.connect()
        try:
            row = connection.execute(
                "SELECT * FROM applied_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            return None
        result = json.loads(row["result_json"])
        if isinstance(result, dict):
            result = dict(result)
            result["deduplicated"] = True
            result["batchId"] = batch_id
        return result

