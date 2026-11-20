"""SQLite persistence, intent protocol, and startup recovery."""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

from .config import GuardConfig


SCHEMA_VERSION = "1"

SCHEMA = r"""
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS routes (
    route TEXT PRIMARY KEY,
    watermark_ms INTEGER NOT NULL,
    current_composite_version INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS route_runtime (
    route TEXT PRIMARY KEY,
    current_concurrency INTEGER NOT NULL,
    budget_watermark TEXT NOT NULL,
    target_latency_us INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('COMMITTED', 'ABORTED')),
    event_count INTEGER NOT NULL,
    route_summary_json TEXT NOT NULL,
    result_json TEXT,
    abort_reason TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY (batch_id, sequence)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_index INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    route TEXT NOT NULL,
    success INTEGER NOT NULL CHECK (success IN (0, 1)),
    latency_us INTEGER NOT NULL CHECK (latency_us >= 0),
    UNIQUE (batch_id, sequence, event_index)
);

CREATE INDEX IF NOT EXISTS idx_events_route_time
    ON events(route, timestamp_ms, batch_id, sequence, event_index);

CREATE TABLE IF NOT EXISTS window_candidates (
    route TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    first_seen_at TEXT NOT NULL,
    PRIMARY KEY (route, start_ms)
);

CREATE TABLE IF NOT EXISTS window_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    version INTEGER NOT NULL,
    closed INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    success_count INTEGER NOT NULL,
    failure_count INTEGER NOT NULL,
    consecutive_failures INTEGER NOT NULL,
    sample_digest TEXT NOT NULL,
    stats_json TEXT NOT NULL,
    samples_json TEXT NOT NULL,
    source_batch_id TEXT NOT NULL,
    source_sequence INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (route, start_ms, end_ms, version)
);

CREATE INDEX IF NOT EXISTS idx_window_versions_route_start
    ON window_versions(route, start_ms, version);

CREATE TABLE IF NOT EXISTS window_heads (
    route TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    current_version_id INTEGER NOT NULL,
    PRIMARY KEY (route, start_ms)
);

CREATE TABLE IF NOT EXISTS breaker_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route TEXT NOT NULL,
    version INTEGER NOT NULL,
    window_version_id INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    opened_at_ms INTEGER,
    next_probe_at_ms INTEGER,
    half_open_since_ms INTEGER,
    last_reason TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (route, version)
);

CREATE TABLE IF NOT EXISTS breaker_heads (
    route TEXT PRIMARY KEY,
    current_version_id INTEGER NOT NULL,
    state TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS breaker_transitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    composite_version INTEGER,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_breaker_transitions_route
    ON breaker_transitions(route, id);

CREATE TABLE IF NOT EXISTS quota_versions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route TEXT NOT NULL,
    version INTEGER NOT NULL,
    window_version_id INTEGER NOT NULL,
    breaker_version_id INTEGER NOT NULL,
    previous_quota INTEGER,
    final_quota INTEGER NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (route, version)
);

CREATE TABLE IF NOT EXISTS quota_heads (
    route TEXT PRIMARY KEY,
    current_version_id INTEGER NOT NULL,
    final_quota INTEGER NOT NULL,
    window_version_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS composite_snapshots (
    route TEXT NOT NULL,
    version INTEGER NOT NULL,
    window_version_id INTEGER NOT NULL,
    quota_version_id INTEGER NOT NULL,
    breaker_version_id INTEGER NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY (route, version)
);

CREATE TABLE IF NOT EXISTS composite_heads (
    route TEXT PRIMARY KEY,
    current_version INTEGER NOT NULL,
    window_version_id INTEGER NOT NULL,
    quota_version_id INTEGER NOT NULL,
    breaker_version_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS decision_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    route TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT NOT NULL,
    composite_version INTEGER,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    target_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PREPARED', 'COMMITTED', 'ABORTED_REJECTED', 'ABORTED_RECOVERED')),
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state, created_at);

CREATE TABLE IF NOT EXISTS recovery_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class Storage:
    def __init__(self, database: str | Path, config: GuardConfig) -> None:
        self.database = str(Path(database).resolve())
        self.config = config
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def read_connection(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.read_connection() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(SCHEMA)
            connection.execute(
                "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (SCHEMA_VERSION,),
            )

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def prepared_intent(
        self,
        kind: str,
        target_key: str,
        payload: dict[str, Any],
    ) -> str:
        intent_id = uuid.uuid4().hex
        created_at = utc_now()
        with self.transaction() as connection:
            connection.execute(
                "INSERT INTO intents(id, kind, target_key, payload_json, state, created_at) "
                "VALUES(?, ?, ?, ?, 'PREPARED', ?)",
                (
                    intent_id,
                    kind,
                    target_key,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    created_at,
                ),
            )
        return intent_id

    @staticmethod
    def complete_intent(connection: sqlite3.Connection, intent_id: str) -> None:
        connection.execute(
            "UPDATE intents SET state='COMMITTED', completed_at=? WHERE id=? AND state='PREPARED'",
            (utc_now(), intent_id),
        )

    def abort_intent(self, intent_id: str, *, recovered: bool = False) -> None:
        state = "ABORTED_RECOVERED" if recovered else "ABORTED_REJECTED"
        with self.transaction() as connection:
            connection.execute(
                "UPDATE intents SET state=?, completed_at=? WHERE id=? AND state='PREPARED'",
                (state, utc_now(), intent_id),
            )

    def quick_check(self) -> str:
        with self.read_connection() as connection:
            row = connection.execute("PRAGMA quick_check").fetchone()
            return str(row[0]) if row else "unknown"

    def latest_recovery_report(self) -> dict[str, Any] | None:
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT id, started_at, finished_at, report_json FROM recovery_runs "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            report = json.loads(row["report_json"])
            report["run_id"] = row["id"]
            return report

    def recovery_by_id(self, run_id: int) -> dict[str, Any] | None:
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT id, started_at, finished_at, report_json FROM recovery_runs WHERE id=?",
                (run_id,),
            ).fetchone()
            if row is None:
                return None
            report = json.loads(row["report_json"])
            report["run_id"] = row["id"]
            return report

    def recover(self) -> dict[str, Any]:
        """Recover PREPARED intents and repair version heads.

        A normal committed mutation and its COMMITTED intent are in one SQLite
        transaction. Therefore finding PREPARED means the mutation did not
        commit. The recovery pass also verifies all head pointers against
        immutable version tables before recording the report.
        """

        started_at = utc_now()
        before_versions = self._current_composite_versions()
        aborted_batches: list[dict[str, Any]] = []
        aborted_intents: list[dict[str, Any]] = []

        with self.transaction() as connection:
            pending = connection.execute(
                "SELECT * FROM intents WHERE state='PREPARED' ORDER BY created_at, id"
            ).fetchall()
            for row in pending:
                payload = json.loads(row["payload_json"])
                resolution: dict[str, Any] = {
                    "intent_id": row["id"],
                    "kind": row["kind"],
                    "target_key": row["target_key"],
                    "state_before": row["state"],
                    "state_after": "ABORTED_RECOVERED",
                    "resolution": "mutation transaction did not commit; previous complete versions preserved",
                }
                if row["kind"] == "INGEST":
                    batch_id = str(payload.get("batch_id"))
                    sequence = int(payload.get("sequence", 0))
                    existing = connection.execute(
                        "SELECT status FROM batches WHERE batch_id=? AND sequence=?",
                        (batch_id, sequence),
                    ).fetchone()
                    if existing is None:
                        created_at = utc_now()
                        connection.execute(
                            "INSERT INTO batches(batch_id, sequence, content_hash, status, event_count, "
                            "route_summary_json, result_json, abort_reason, created_at, completed_at) "
                            "VALUES(?, ?, ?, 'ABORTED', ?, ?, NULL, ?, ?, ?)",
                            (
                                batch_id,
                                sequence,
                                str(payload.get("content_hash", "")),
                                int(payload.get("event_count", 0)),
                                json.dumps(
                                    payload.get("route_summary", {}),
                                    ensure_ascii=False,
                                    sort_keys=True,
                                ),
                                "RECOVERED_INCOMPLETE_INTENT",
                                created_at,
                                created_at,
                            ),
                        )
                        aborted_batches.append(
                            {
                                "batch_id": batch_id,
                                "sequence": sequence,
                                "status": "ABORTED",
                                "abort_reason": "RECOVERED_INCOMPLETE_INTENT",
                            }
                        )
                        resolution["batch_marked_aborted"] = True
                    else:
                        resolution["existing_batch_status"] = existing["status"]
                        resolution["batch_marked_aborted"] = False
                connection.execute(
                    "UPDATE intents SET state='ABORTED_RECOVERED', completed_at=? WHERE id=?",
                    (utc_now(), row["id"]),
                )
                aborted_intents.append(resolution)

            head_repairs = self._repair_heads(connection)
            after_versions = self._current_composite_versions(connection)
            pending_after = int(
                connection.execute("SELECT COUNT(*) FROM intents WHERE state='PREPARED'").fetchone()[0]
            )
            quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])

            report = {
                "report_version": 1,
                "database": self.database,
                "started_at": started_at,
                "finished_at": utc_now(),
                "incomplete_intents_found": len(aborted_intents),
                "aborted_intents": aborted_intents,
                "aborted_batches": aborted_batches,
                "integrity": {
                    "sqlite_quick_check": quick_check,
                    "head_repairs": head_repairs,
                    "last_complete_composite_versions": after_versions,
                    "previous_complete_versions_preserved": before_versions == after_versions,
                    "pending_intents_after_recovery": pending_after,
                },
                "policy": {
                    "intent_protocol": (
                        "PREPARED intent commits before the mutation; mutation and COMMITTED intent "
                        "share one SQLite transaction."
                    ),
                    "forced_termination": (
                        "SQLite rolls back an uncommitted mutation. Recovery marks the batch ABORTED "
                        "and points heads at immutable committed versions only."
                    ),
                    "exit_report_guarantee": False,
                    "exit_report_note": (
                        "A forcefully terminated process does not emit an exit report. This report "
                        "contains only facts persisted after restart."
                    ),
                },
            }
            report_json = json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            connection.execute(
                "INSERT INTO recovery_runs(started_at, finished_at, report_json) VALUES(?, ?, ?)",
                (report["started_at"], report["finished_at"], report_json),
            )
            report["run_id"] = int(
                connection.execute("SELECT last_insert_rowid()").fetchone()[0]
            )
        return report

    def _current_composite_versions(self, connection: sqlite3.Connection | None = None) -> dict[str, int]:
        own = connection is None
        conn = connection or self.connect()
        try:
            rows = conn.execute(
                "SELECT route, current_version FROM composite_heads ORDER BY route"
            ).fetchall()
            return {str(row["route"]): int(row["current_version"]) for row in rows}
        finally:
            if own:
                conn.close()

    def _repair_heads(self, connection: sqlite3.Connection) -> list[dict[str, Any]]:
        repairs: list[dict[str, Any]] = []

        window_rows = connection.execute(
            "SELECT route, start_ms, end_ms, current_version_id FROM window_heads"
        ).fetchall()
        for head in window_rows:
            latest = connection.execute(
                "SELECT id FROM window_versions WHERE route=? AND start_ms=? AND end_ms=? "
                "ORDER BY version DESC LIMIT 1",
                (head["route"], head["start_ms"], head["end_ms"]),
            ).fetchone()
            if latest is not None and int(latest["id"]) != int(head["current_version_id"]):
                repairs.append(
                    {
                        "head": "window_heads",
                        "route": head["route"],
                        "start_ms": head["start_ms"],
                        "from": head["current_version_id"],
                        "to": latest["id"],
                    }
                )
                connection.execute(
                    "UPDATE window_heads SET current_version_id=? WHERE route=? AND start_ms=?",
                    (latest["id"], head["route"], head["start_ms"]),
                )

        for head in connection.execute("SELECT * FROM breaker_heads").fetchall():
            latest = connection.execute(
                "SELECT id, state FROM breaker_versions WHERE route=? ORDER BY version DESC LIMIT 1",
                (head["route"],),
            ).fetchone()
            if latest is not None and (
                int(latest["id"]) != int(head["current_version_id"]) or latest["state"] != head["state"]
            ):
                repairs.append(
                    {
                        "head": "breaker_heads",
                        "route": head["route"],
                        "from": head["current_version_id"],
                        "to": latest["id"],
                    }
                )
                connection.execute(
                    "UPDATE breaker_heads SET current_version_id=?, state=? WHERE route=?",
                    (latest["id"], latest["state"], head["route"]),
                )

        for head in connection.execute("SELECT * FROM quota_heads").fetchall():
            latest = connection.execute(
                "SELECT id, final_quota, window_version_id FROM quota_versions "
                "WHERE route=? ORDER BY version DESC LIMIT 1",
                (head["route"],),
            ).fetchone()
            if latest is not None and (
                int(latest["id"]) != int(head["current_version_id"])
                or int(latest["final_quota"]) != int(head["final_quota"])
            ):
                repairs.append(
                    {
                        "head": "quota_heads",
                        "route": head["route"],
                        "from": head["current_version_id"],
                        "to": latest["id"],
                    }
                )
                connection.execute(
                    "UPDATE quota_heads SET current_version_id=?, final_quota=?, window_version_id=? "
                    "WHERE route=?",
                    (latest["id"], latest["final_quota"], latest["window_version_id"], head["route"]),
                )

        for head in connection.execute("SELECT * FROM composite_heads").fetchall():
            latest = connection.execute(
                "SELECT * FROM composite_snapshots WHERE route=? ORDER BY version DESC LIMIT 1",
                (head["route"],),
            ).fetchone()
            if latest is not None and (
                int(latest["version"]) != int(head["current_version"])
                or int(latest["window_version_id"]) != int(head["window_version_id"])
                or int(latest["quota_version_id"]) != int(head["quota_version_id"])
                or int(latest["breaker_version_id"]) != int(head["breaker_version_id"])
            ):
                repairs.append(
                    {
                        "head": "composite_heads",
                        "route": head["route"],
                        "from": head["current_version"],
                        "to": latest["version"],
                    }
                )
                connection.execute(
                    "UPDATE composite_heads SET current_version=?, window_version_id=?, "
                    "quota_version_id=?, breaker_version_id=? WHERE route=?",
                    (
                        latest["version"],
                        latest["window_version_id"],
                        latest["quota_version_id"],
                        latest["breaker_version_id"],
                        head["route"],
                    ),
                )
        return repairs


def row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return None if row is None else {key: row[key] for key in row.keys()}


def fetch_all_dicts(connection: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    return [{key: row[key] for key in row.keys()} for row in connection.execute(sql, params)]

