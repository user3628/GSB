"""SQLite persistence with write-ahead intent records and crash recovery."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass
class RecoveryReport:
    last_complete_version: int | None
    complete_snapshot_count: int
    pending_intents_seen: int
    actions: tuple[dict[str, Any], ...]
    previous_process_state: str
    previous_shutdown: dict[str, Any] | None
    source: str = "sqlite-durable-facts"

    def public(self) -> dict[str, Any]:
        return {
            "last_complete_version": self.last_complete_version,
            "complete_snapshot_count": self.complete_snapshot_count,
            "pending_intents_seen": self.pending_intents_seen,
            "actions": list(self.actions),
            "previous_process_state": self.previous_process_state,
            "previous_exit_clean": self.previous_process_state == "stopped",
            "previous_shutdown": self.previous_shutdown,
            "source": self.source,
        }


class Store:
    """Small durable store. The configuration update lock lives in Runtime."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = str(Path(path))
        parent = Path(self.path).parent
        parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    @contextmanager
    def _connection(self):
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        with self._lock, self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    version INTEGER PRIMARY KEY,
                    payload TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS intents (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    base_version INTEGER,
                    target_version INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event TEXT NOT NULL,
                    version INTEGER,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    @staticmethod
    def _set_meta(connection: sqlite3.Connection, key: str, value: str) -> None:
        connection.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    @staticmethod
    def _get_meta(connection: sqlite3.Connection, key: str) -> str | None:
        row = connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row["value"])

    def _history(self, connection: sqlite3.Connection, event: str, version: int | None, details: dict[str, Any]) -> None:
        connection.execute(
            "INSERT INTO history(event, version, details, created_at) VALUES(?, ?, ?, ?)",
            (event, version, json.dumps(details, ensure_ascii=False, sort_keys=True), _now()),
        )

    def recover(self) -> RecoveryReport:
        """Reconcile durable facts after a potentially abrupt process exit."""
        with self._lock, self._connection() as connection:
            previous_process_state = self._get_meta(connection, "process_state") or "absent"
            raw_shutdown = self._get_meta(connection, "last_shutdown")
            previous_shutdown = json.loads(raw_shutdown) if raw_shutdown else None
            complete_rows = connection.execute(
                "SELECT version FROM snapshots WHERE complete = 1 ORDER BY version"
            ).fetchall()
            complete_versions = [int(row["version"]) for row in complete_rows]
            last_complete = max(complete_versions) if complete_versions else None
            pending_rows = connection.execute(
                "SELECT * FROM intents WHERE state = 'prepared' ORDER BY created_at"
            ).fetchall()
            actions: list[dict[str, Any]] = []
            connection.execute("BEGIN IMMEDIATE")
            try:
                for row in pending_rows:
                    target = int(row["target_version"])
                    snapshot = connection.execute(
                        "SELECT complete FROM snapshots WHERE version = ?", (target,)
                    ).fetchone()
                    if snapshot is not None and int(snapshot["complete"]) == 1:
                        action = "commit_already_durable"
                        state = "committed"
                    else:
                        action = "rolled_back_to_last_complete_snapshot"
                        state = "aborted"
                    connection.execute(
                        "UPDATE intents SET state = ?, updated_at = ? WHERE id = ?",
                        (state, _now(), row["id"]),
                    )
                    record = {
                        "intent_id": row["id"],
                        "kind": row["kind"],
                        "base_version": row["base_version"],
                        "target_version": target,
                        "action": action,
                        "retained_version": last_complete,
                    }
                    actions.append(record)
                    self._history(connection, "intent_recovered", target, record)

                # Incomplete snapshot rows are physically impossible to use and are removed.
                incomplete = connection.execute(
                    "SELECT version FROM snapshots WHERE complete = 0 ORDER BY version"
                ).fetchall()
                for row in incomplete:
                    record = {"version": int(row["version"]), "action": "removed_incomplete_snapshot"}
                    actions.append(record)
                    self._history(connection, "incomplete_snapshot_removed", int(row["version"]), record)
                connection.execute("DELETE FROM snapshots WHERE complete = 0")

                meta_current = self._get_meta(connection, "current_version")
                repaired = False
                if last_complete is None:
                    connection.execute("DELETE FROM meta WHERE key = 'current_version'")
                    repaired = meta_current is not None
                elif meta_current != str(last_complete):
                    self._set_meta(connection, "current_version", str(last_complete))
                    repaired = True
                if repaired:
                    record = {"from": meta_current, "to": last_complete, "action": "meta_reconciled"}
                    actions.append(record)
                    self._history(connection, "current_version_reconciled", last_complete, record)

                if previous_process_state == "running":
                    record = {"action": "detected_unclean_previous_exit"}
                    actions.append(record)
                    self._history(connection, "unclean_exit_detected", last_complete, record)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            return RecoveryReport(
                last_complete_version=last_complete,
                complete_snapshot_count=len(complete_versions),
                pending_intents_seen=len(pending_rows),
                actions=tuple(actions),
                previous_process_state=previous_process_state,
                previous_shutdown=previous_shutdown,
            )

    def begin_process(self) -> str:
        with self._lock, self._connection() as connection:
            previous = self._get_meta(connection, "process_state") or "absent"
            self._set_meta(connection, "process_state", "running")
            self._history(connection, "process_started", None, {"previous_state": previous})
            return previous

    def finish_process(
        self,
        *,
        mode: str,
        completed_in_flight: int,
        cancelled_in_flight: int,
        detail: str,
    ) -> dict[str, Any]:
        shutdown = {
            "mode": mode,
            "completed_in_flight": completed_in_flight,
            "cancelled_in_flight": cancelled_in_flight,
            "detail": detail,
            "at": _now(),
        }
        with self._lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._set_meta(connection, "process_state", "stopped")
                self._set_meta(
                    connection, "last_shutdown", json.dumps(shutdown, ensure_ascii=False, sort_keys=True)
                )
                self._history(connection, "process_stopped", None, shutdown)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return shutdown

    def latest_complete_version(self) -> int | None:
        with self._lock, self._connection() as connection:
            row = connection.execute("SELECT MAX(version) AS version FROM snapshots WHERE complete = 1").fetchone()
            return None if row is None or row["version"] is None else int(row["version"])

    def load_complete(self, version: int | None = None) -> tuple[int, dict[str, Any]] | None:
        with self._lock, self._connection() as connection:
            if version is None:
                row = connection.execute(
                    "SELECT version, payload FROM snapshots WHERE complete = 1 ORDER BY version DESC LIMIT 1"
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT version, payload FROM snapshots WHERE complete = 1 AND version = ?", (version,)
                ).fetchone()
            if row is None:
                return None
            return int(row["version"]), json.loads(row["payload"])

    def begin_intent(
        self,
        *,
        kind: str,
        base_version: int | None,
        target_version: int,
        payload: dict[str, Any],
        detail: dict[str, Any],
    ) -> str:
        intent_id = uuid.uuid4().hex
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        now = _now()
        with self._lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "INSERT INTO intents(id, kind, base_version, target_version, payload, state, created_at, updated_at) "
                    "VALUES(?, ?, ?, ?, ?, 'prepared', ?, ?)",
                    (intent_id, kind, base_version, target_version, serialized, now, now),
                )
                self._history(
                    connection,
                    "intent_prepared",
                    target_version,
                    {"intent_id": intent_id, "kind": kind, "base_version": base_version, **detail},
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return intent_id

    def persist_complete(
        self,
        *,
        intent_id: str,
        version: int,
        payload: dict[str, Any],
        payload_sha256: str,
        detail: dict[str, Any],
    ) -> None:
        serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self._lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "INSERT INTO snapshots(version, payload, payload_sha256, complete, created_at) "
                    "VALUES(?, ?, ?, 1, ?)",
                    (version, serialized, payload_sha256, _now()),
                )
                self._set_meta(connection, "current_version", str(version))
                self._history(
                    connection,
                    "snapshot_committed",
                    version,
                    {"intent_id": intent_id, "payload_sha256": payload_sha256, **detail},
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def mark_intent_committed(self, intent_id: str) -> None:
        with self._lock, self._connection() as connection:
            connection.execute(
                "UPDATE intents SET state = 'committed', updated_at = ? WHERE id = ?",
                (_now(), intent_id),
            )

    def abort_intent(self, intent_id: str, reason: str) -> None:
        with self._lock, self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    "UPDATE intents SET state = 'aborted', updated_at = ? WHERE id = ?",
                    (_now(), intent_id),
                )
                self._history(connection, "intent_aborted", None, {"intent_id": intent_id, "reason": reason})
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def history(self, limit: int = 100) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 1000))
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                "SELECT id, event, version, details, created_at FROM history ORDER BY id DESC LIMIT ?",
                (safe_limit,),
            ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "event": row["event"],
                "version": row["version"],
                "details": json.loads(row["details"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def snapshot_versions(self, limit: int = 100) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 1000))
        with self._lock, self._connection() as connection:
            rows = connection.execute(
                "SELECT version, payload_sha256, created_at FROM snapshots WHERE complete = 1 "
                "ORDER BY version DESC LIMIT ?",
                (safe_limit,),
            ).fetchall()
        return [
            {
                "version": int(row["version"]),
                "payload_sha256": row["payload_sha256"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def current_process_state(self) -> str:
        with self._lock, self._connection() as connection:
            return self._get_meta(connection, "process_state") or "absent"

    def last_shutdown(self) -> dict[str, Any] | None:
        with self._lock, self._connection() as connection:
            raw = self._get_meta(connection, "last_shutdown")
        return json.loads(raw) if raw else None

