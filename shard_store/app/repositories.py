"""Thin data-access layer. All SQL lives here; services hold business logic."""
from __future__ import annotations

import json
import sqlite3
from typing import Any

from app.utils import new_intent_id, utc_now_iso


class SessionRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create(
        self,
        session_id: str,
        object_key: str,
        min_part_size: int,
        max_part_size: int,
        expected_total_size: int | None,
        merge_strategy: str,
        conflict_policy: str,
    ) -> None:
        now = utc_now_iso()
        self.conn.execute(
            """INSERT INTO upload_sessions
               (session_id, object_key, status, min_part_size, max_part_size,
                expected_total_size, merge_strategy, conflict_policy,
                part_count, total_size, created_at, updated_at)
               VALUES (?, ?, 'open', ?, ?, ?, ?, ?, 0, 0, ?, ?)""",
            (session_id, object_key, min_part_size, max_part_size,
             expected_total_size, merge_strategy, conflict_policy, now, now),
        )

    def get(self, session_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM upload_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()

    def list_open_for_key(self, object_key: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT * FROM upload_sessions
               WHERE object_key = ? AND status IN ('open','merging')
               ORDER BY created_at""",
            (object_key,),
        ).fetchall()

    def update_status(self, session_id: str, status: str, intent_id: str | None = None) -> None:
        self.conn.execute(
            """UPDATE upload_sessions
               SET status = ?, updated_at = ?, last_intent_id = COALESCE(?, last_intent_id)
               WHERE session_id = ?""",
            (status, utc_now_iso(), intent_id, session_id),
        )

    def refresh_counters(self, session_id: str) -> None:
        # Recomputed inside the same write transaction as the part row, so a
        # reader always sees part rows and counters from one commit.
        self.conn.execute(
            """UPDATE upload_sessions
               SET part_count = (SELECT COUNT(*) FROM parts WHERE session_id = ?),
                   total_size = (SELECT COALESCE(SUM(size_bytes),0) FROM parts
                                 WHERE session_id = ?),
                   updated_at = ?
               WHERE session_id = ?""",
            (session_id, session_id, utc_now_iso(), session_id),
        )

    def list_all(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM upload_sessions ORDER BY created_at"
        ).fetchall()


class PartRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get(self, session_id: str, part_number: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM parts WHERE session_id = ? AND part_number = ?",
            (session_id, part_number),
        ).fetchone()

    def add(
        self, session_id: str, part_number: int, size_bytes: int,
        sha256: str, content: bytes,
    ) -> None:
        now = utc_now_iso()
        self.conn.execute(
            """INSERT INTO parts (session_id, part_number, size_bytes, sha256, uploaded_at)
               VALUES (?, ?, ?, ?, ?)""",
            (session_id, part_number, size_bytes, sha256, now),
        )
        self.conn.execute(
            """INSERT INTO part_bodies (session_id, part_number, content)
               VALUES (?, ?, ?)""",
            (session_id, part_number, content),
        )

    def list_for_session(self, session_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT part_number, size_bytes, sha256, uploaded_at FROM parts "
            "WHERE session_id = ? ORDER BY part_number",
            (session_id,),
        ).fetchall()

    def get_body(self, session_id: str, part_number: int) -> bytes | None:
        row = self.conn.execute(
            "SELECT content FROM part_bodies WHERE session_id = ? AND part_number = ?",
            (session_id, part_number),
        ).fetchone()
        return None if row is None else bytes(row["content"])


class ObjectRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def next_version(self, object_key: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 AS next_version "
            "FROM object_versions WHERE object_key = ?",
            (object_key,),
        ).fetchone()
        return int(row["next_version"])

    def put_version(
        self, object_key: str, version: int, total_size: int, part_count: int,
        object_sha256: str, etag: str, session_id: str,
    ) -> None:
        now = utc_now_iso()
        self.conn.execute(
            """INSERT INTO object_versions
               (object_key, version, total_size, part_count, object_sha256,
                etag, created_at, session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (object_key, version, total_size, part_count, object_sha256,
             etag, now, session_id),
        )
        self.conn.execute(
            """INSERT INTO objects_current (object_key, current_version, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(object_key) DO UPDATE SET
                   current_version = excluded.current_version,
                   updated_at = excluded.updated_at""",
            (object_key, version, now),
        )

    def get_current(self, object_key: str) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT v.* FROM objects_current c
               JOIN object_versions v
                 ON v.object_key = c.object_key AND v.version = c.current_version
               WHERE c.object_key = ?""",
            (object_key,),
        ).fetchone()

    def list_versions(self, object_key: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM object_versions WHERE object_key = ? ORDER BY version",
            (object_key,),
        ).fetchall()

    def get_version(self, object_key: str, version: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM object_versions WHERE object_key = ? AND version = ?",
            (object_key, version),
        ).fetchone()


class IntentRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def add_pending(self, session_id: str, object_key: str, strategy: str,
                    detail: dict[str, Any] | None = None) -> str:
        intent_id = new_intent_id()
        self.conn.execute(
            """INSERT INTO merge_intents
               (intent_id, session_id, object_key, strategy, state,
                created_at, detail)
               VALUES (?, ?, ?, ?, 'pending', ?, ?)""",
            (intent_id, session_id, object_key, strategy, utc_now_iso(),
             json.dumps(detail or {}, sort_keys=True)),
        )
        return intent_id

    def mark(self, intent_id: str, state: str, detail: dict[str, Any] | None = None) -> None:
        self.conn.execute(
            """UPDATE merge_intents
               SET state = ?, committed_at = CASE WHEN ? = 'committed' THEN ? ELSE committed_at END,
                   detail = ?
               WHERE intent_id = ?""",
            (state, state, utc_now_iso(),
             json.dumps(detail or {}, sort_keys=True), intent_id),
        )

    def list_pending(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM merge_intents WHERE state = 'pending' ORDER BY created_at"
        ).fetchall()

    def get(self, intent_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM merge_intents WHERE intent_id = ?", (intent_id,)
        ).fetchone()

    def save_recovery_report(self, report_id: str, scanned: int, recovered: int,
                             aborted: int, detail: dict[str, Any]) -> None:
        self.conn.execute(
            """INSERT INTO recovery_reports
               (report_id, created_at, scanned, recovered, aborted, detail_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (report_id, utc_now_iso(), scanned, recovered, aborted,
             json.dumps(detail, sort_keys=True)),
        )

    def list_recovery_reports(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM recovery_reports ORDER BY created_at"
        ).fetchall()
