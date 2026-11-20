from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from app.errors import BusinessError


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class Storage:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    spec_name TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    intent_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    result_json TEXT,
                    error_json TEXT,
                    recovered INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS recovery_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trigger TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    scanned_count INTEGER NOT NULL DEFAULT 0,
                    recovered_count INTEGER NOT NULL DEFAULT 0,
                    aborted_count INTEGER NOT NULL DEFAULT 0,
                    notes TEXT
                );

                CREATE TABLE IF NOT EXISTS recovery_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    batch_id TEXT NOT NULL,
                    detected_at TEXT NOT NULL,
                    action TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES recovery_runs(id)
                );

                CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status);
                CREATE INDEX IF NOT EXISTS idx_recovery_events_run ON recovery_events(run_id);
                """
            )

    def begin_intent(
        self,
        *,
        batch_id: str,
        spec_name: str,
        spec_json: str,
        timestamp: str,
    ) -> None:
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO batches (
                        batch_id, status, spec_name, spec_json, intent_at, updated_at
                    ) VALUES (?, 'intent', ?, ?, ?, ?)
                    """,
                    (batch_id, spec_name, spec_json, timestamp, timestamp),
                )
        except sqlite3.IntegrityError as exc:
            raise BusinessError(
                "E_BATCH_EXISTS",
                f"batch already exists: {batch_id}",
                status_code=409,
                details={"batchId": batch_id},
            ) from exc

    def finalize(
        self,
        *,
        batch_id: str,
        result: dict[str, Any],
        timestamp: str,
        recovered: bool = False,
    ) -> None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE batches
                SET status = 'completed', updated_at = ?, completed_at = ?, result_json = ?,
                    error_json = NULL, recovered = ?
                WHERE batch_id = ? AND status IN ('intent', 'recovering')
                """,
                (timestamp, timestamp, canonical_json(result), 1 if recovered else 0, batch_id),
            )
            if cursor.rowcount != 1:
                raise BusinessError(
                    "E_BATCH_STATE_CONFLICT",
                    f"batch is not in an intent state: {batch_id}",
                    status_code=409,
                    details={"batchId": batch_id},
                )

    def abort(self, *, batch_id: str, error: dict[str, Any], timestamp: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE batches
                SET status = 'aborted', updated_at = ?, completed_at = ?, error_json = ?
                WHERE batch_id = ? AND status IN ('intent', 'recovering')
                """,
                (timestamp, timestamp, canonical_json(error), batch_id),
            )

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
        return dict(row) if row else None

    def list_incomplete(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM batches
                WHERE status IN ('intent', 'recovering')
                ORDER BY intent_at, batch_id
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_recovering(self, *, batch_id: str, timestamp: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE batches SET status = 'recovering', updated_at = ?
                WHERE batch_id = ? AND status IN ('intent', 'recovering')
                """,
                (timestamp, batch_id),
            )
            return cursor.rowcount == 1

    def begin_recovery_run(self, *, trigger: str, timestamp: str) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT INTO recovery_runs (trigger, started_at) VALUES (?, ?)",
                (trigger, timestamp),
            )
            return int(cursor.lastrowid)

    def finish_recovery_run(
        self,
        *,
        run_id: int,
        timestamp: str,
        scanned_count: int,
        recovered_count: int,
        aborted_count: int,
        notes: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE recovery_runs
                SET completed_at = ?, scanned_count = ?, recovered_count = ?, aborted_count = ?, notes = ?
                WHERE id = ?
                """,
                (timestamp, scanned_count, recovered_count, aborted_count, notes, run_id),
            )

    def add_recovery_event(
        self,
        *,
        run_id: int,
        batch_id: str,
        timestamp: str,
        action: str,
        details: dict[str, Any],
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO recovery_events (run_id, batch_id, detected_at, action, details_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, batch_id, timestamp, action, canonical_json(details)),
            )

    def recovery_report_rows(self) -> dict[str, Any]:
        with self._connect() as connection:
            runs = [dict(row) for row in connection.execute("SELECT * FROM recovery_runs ORDER BY id").fetchall()]
            events = [
                dict(row)
                for row in connection.execute("SELECT * FROM recovery_events ORDER BY id").fetchall()
            ]
        return {"runs": runs, "events": events}
