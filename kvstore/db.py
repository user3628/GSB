from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any


class IntentDB:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.path, timeout=30.0, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA busy_timeout=30000")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS intents (
                    tx_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    segment_id TEXT,
                    snapshot_seq INTEGER,
                    payload_json TEXT NOT NULL,
                    detail_json TEXT NOT NULL DEFAULT '{}',
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
                CREATE INDEX IF NOT EXISTS idx_intents_batch ON intents(batch_id);

                CREATE TABLE IF NOT EXISTS import_reports (
                    batch_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    format_name TEXT,
                    report_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS recovery_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    generated_at_ms INTEGER NOT NULL,
                    report_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS batch_claims (
                    batch_key TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    def begin_intent(
        self,
        *,
        tx_id: str,
        batch_id: str,
        kind: str,
        segment_id: str,
        snapshot_seq: int | None,
        payload: dict[str, Any],
        now_ms: int,
    ) -> None:
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO intents(
                    tx_id, batch_id, kind, status, segment_id, snapshot_seq,
                    payload_json, detail_json, created_at_ms, updated_at_ms
                ) VALUES (?, ?, ?, 'prepared', ?, ?, ?, '{}', ?, ?)
                """,
                (
                    tx_id,
                    batch_id,
                    kind,
                    segment_id,
                    snapshot_seq,
                    payload_json,
                    now_ms,
                    now_ms,
                ),
            )

    def update_intent(
        self,
        tx_id: str,
        status: str,
        *,
        detail: dict[str, Any] | None = None,
        now_ms: int,
    ) -> None:
        detail_json = json.dumps(detail or {}, sort_keys=True, separators=(",", ":"))
        with self._lock:
            self._conn.execute(
                """
                UPDATE intents
                   SET status = ?, detail_json = ?, updated_at_ms = ?
                 WHERE tx_id = ?
                """,
                (status, detail_json, now_ms, tx_id),
            )

    def list_intents(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM intents ORDER BY created_at_ms, tx_id"
            ).fetchall()
        return [self._row_to_intent(row) for row in rows]

    def get_intent(self, tx_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM intents WHERE tx_id = ?", (tx_id,)
            ).fetchone()
        return None if row is None else self._row_to_intent(row)

    @staticmethod
    def _row_to_intent(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "tx_id": row["tx_id"],
            "batch_id": row["batch_id"],
            "kind": row["kind"],
            "status": row["status"],
            "segment_id": row["segment_id"],
            "snapshot_seq": row["snapshot_seq"],
            "payload": json.loads(row["payload_json"]),
            "detail": json.loads(row["detail_json"]),
            "created_at_ms": row["created_at_ms"],
            "updated_at_ms": row["updated_at_ms"],
        }

    def claim_batch(self, batch_key: str, owner: str, now_ms: int) -> bool:
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO batch_claims(
                    batch_key, owner, status, created_at_ms, updated_at_ms
                ) VALUES (?, ?, 'active', ?, ?)
                """,
                (batch_key, owner, now_ms, now_ms),
            )
            return cursor.rowcount == 1

    def finish_batch(self, batch_key: str, status: str, now_ms: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE batch_claims SET status=?, updated_at_ms=? WHERE batch_key=?",
                (status, now_ms, batch_key),
            )

    def save_import_report(
        self,
        *,
        batch_id: str,
        status: str,
        format_name: str | None,
        report: dict[str, Any],
        now_ms: int,
    ) -> None:
        report_json = json.dumps(report, sort_keys=True, separators=(",", ":"))
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO import_reports(batch_id, status, format_name, report_json, created_at_ms)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(batch_id) DO UPDATE SET
                    status=excluded.status,
                    format_name=excluded.format_name,
                    report_json=excluded.report_json,
                    created_at_ms=excluded.created_at_ms
                """,
                (batch_id, status, format_name, report_json, now_ms),
            )

    def get_import_report(self, batch_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM import_reports WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        if row is None:
            return None
        return {
            "batch_id": row["batch_id"],
            "status": row["status"],
            "format": row["format_name"],
            "created_at_ms": row["created_at_ms"],
            "report": json.loads(row["report_json"]),
        }

    def save_recovery_report(self, report: dict[str, Any], now_ms: int) -> None:
        report_json = json.dumps(report, sort_keys=True, separators=(",", ":"))
        with self._lock:
            self._conn.execute(
                "INSERT INTO recovery_reports(generated_at_ms, report_json) VALUES (?, ?)",
                (now_ms, report_json),
            )

    def latest_recovery_report(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM recovery_reports ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "generated_at_ms": row["generated_at_ms"],
            "report": json.loads(row["report_json"]),
        }

    def set_meta(self, key: str, value: Any) -> None:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO meta(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                (key, raw),
            )

    def get_meta(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return default

    def close(self) -> None:
        with self._lock:
            self._conn.close()

