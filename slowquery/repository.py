from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from .database import Database
from .errors import BusinessError, invalid_sample, invalid_transition, session_not_found
from .state_machine import require_transition
from .util import Clock, canonical_json


def _decode_json(value: str | None, default: Any) -> Any:
    if value is None:
        return default
    return json.loads(value)


def _decode_session(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    data = dict(row)
    data["is_slow"] = None if data["is_slow"] is None else bool(data["is_slow"])
    data["analysis"] = _decode_json(data.pop("analysis_json", None), None)
    return data


def _decode_suggestion(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    data["setup_sql"] = _decode_json(data.pop("setup_sql_json", None), [])
    data["cause_codes"] = _decode_json(data.pop("cause_codes_json", None), [])
    return data


def _decode_verification(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    data = dict(row)
    data["original_cost"] = _decode_json(data.pop("original_cost_json", None), {})
    data["rewritten_cost"] = _decode_json(data.pop("rewritten_cost_json", None), {})
    data["detail"] = _decode_json(data.pop("detail_json", None), {})
    return data


class Repository:
    def __init__(self, database: Database, clock: Clock | None = None) -> None:
        self.db = database
        self.clock = clock or Clock()

    # ------------------------------------------------------------------ intents
    def start_intent(
        self,
        *,
        kind: str,
        session_id: str | None,
        request: dict[str, Any],
        checkpoint: str = "INTENT_RECORDED",
    ) -> str:
        intent_id = "int_" + uuid.uuid4().hex
        now = self.clock.now()
        with self.db.write_transaction() as conn:
            conn.execute(
                """
                INSERT INTO operation_intents(
                    intent_id, session_id, kind, status, checkpoint,
                    request_json, result_json, created_at, updated_at
                ) VALUES (?, ?, ?, 'PENDING', ?, ?, NULL, ?, ?)
                """,
                (intent_id, session_id, kind, checkpoint, canonical_json(request), now, now),
            )
        return intent_id

    def update_intent(
        self,
        conn: sqlite3.Connection,
        intent_id: str,
        *,
        status: str,
        checkpoint: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        cursor = conn.execute(
            """
            UPDATE operation_intents
            SET status = ?, checkpoint = ?, result_json = ?, updated_at = ?
            WHERE intent_id = ?
            """,
            (
                status,
                checkpoint,
                None if result is None else canonical_json(result),
                self.clock.now(),
                intent_id,
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(f"operation intent {intent_id!r} was not found")

    def get_intent(self, intent_id: str) -> dict[str, Any] | None:
        with self.db.read_transaction() as conn:
            row = conn.execute(
                "SELECT * FROM operation_intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
        if row is None:
            return None
        data = dict(row)
        data["request"] = _decode_json(data.pop("request_json"), {})
        data["result"] = _decode_json(data.pop("result_json", None), None)
        return data

    def pending_intents(self) -> list[dict[str, Any]]:
        with self.db.read_transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM operation_intents WHERE status = 'PENDING' "
                "ORDER BY created_at, intent_id"
            ).fetchall()
        result = []
        for row in rows:
            data = dict(row)
            data["request"] = _decode_json(data.pop("request_json"), {})
            data["result"] = _decode_json(data.pop("result_json", None), None)
            result.append(data)
        return result

    def list_intents(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.db.read_transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM operation_intents ORDER BY created_at DESC, intent_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def count_completed_intents(self, kind: str, session_id: str) -> int:
        with self.db.read_transaction() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM operation_intents "
                "WHERE kind = ? AND session_id = ? AND status = 'COMPLETED'",
                (kind, session_id),
            ).fetchone()
        return int(row["count"])

    # ------------------------------------------------------------------ samples
    def insert_samples(
        self,
        conn: sqlite3.Connection,
        samples: list[dict[str, Any]],
        *,
        batch_id: str,
    ) -> dict[str, int]:
        inserted = 0
        unchanged = 0
        now = self.clock.now()
        for sample in samples:
            existing = conn.execute(
                "SELECT payload_sha256 FROM samples WHERE sample_id = ?", (sample["sample_id"],)
            ).fetchone()
            if existing is not None:
                if existing["payload_sha256"] == sample["payload_sha256"]:
                    unchanged += 1
                    continue
                raise invalid_sample(
                    "A sample with the same sample_id already exists with different content.",
                    sample_id=sample["sample_id"],
                )
            conn.execute(
                """
                INSERT INTO samples(
                    sample_id, sql, duration_ms, scanned_rows, lock_wait_ms,
                    occurred_at, payload_sha256, normalized_json, batch_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sample["sample_id"],
                    sample["sql"],
                    sample["duration_ms"],
                    sample["scanned_rows"],
                    sample["lock_wait_ms"],
                    sample["occurred_at"],
                    sample["payload_sha256"],
                    canonical_json(sample),
                    batch_id,
                    now,
                ),
            )
            inserted += 1
        return {"inserted": inserted, "unchanged": unchanged}

    def get_sample(self, sample_id: str) -> dict[str, Any] | None:
        with self.db.read_transaction() as conn:
            row = conn.execute("SELECT * FROM samples WHERE sample_id = ?", (sample_id,)).fetchone()
        if row is None:
            return None
        data = dict(row)
        data["normalized"] = _decode_json(data.pop("normalized_json"), {})
        return data

    def list_samples(self) -> list[dict[str, Any]]:
        with self.db.read_transaction() as conn:
            rows = conn.execute("SELECT * FROM samples ORDER BY sample_id").fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------ snapshots
    def insert_snapshot(self, conn: sqlite3.Connection, snapshot: dict[str, Any]) -> None:
        now = self.clock.now()
        conn.execute(
            """
            INSERT INTO snapshots(
                version, generated_at, digest, summary_json, payload_json, loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot["version"],
                snapshot["generated_at"],
                snapshot["digest"],
                canonical_json(snapshot["summary"]),
                canonical_json(snapshot),
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO snapshot_pointer(singleton, active_version, updated_at)
            VALUES (1, ?, ?)
            ON CONFLICT(singleton) DO UPDATE SET
                active_version = excluded.active_version,
                updated_at = excluded.updated_at
            """,
            (snapshot["version"], now),
        )

    def active_snapshot_version(self, conn: sqlite3.Connection | None = None) -> int | None:
        if conn is not None:
            row = conn.execute(
                "SELECT active_version FROM snapshot_pointer WHERE singleton = 1"
            ).fetchone()
        else:
            with self.db.read_transaction() as read_conn:
                row = read_conn.execute(
                    "SELECT active_version FROM snapshot_pointer WHERE singleton = 1"
                ).fetchone()
        return None if row is None else int(row["active_version"])

    def get_snapshot(
        self, version: int, conn: sqlite3.Connection | None = None
    ) -> dict[str, Any] | None:
        def load(read_conn: sqlite3.Connection):
            return read_conn.execute(
                "SELECT * FROM snapshots WHERE version = ?", (version,)
            ).fetchone()

        row = load(conn) if conn is not None else None
        if conn is None:
            with self.db.read_transaction() as read_conn:
                row = load(read_conn)
        if row is None:
            return None
        payload = _decode_json(row["payload_json"], {})
        payload["loaded_at"] = row["loaded_at"]
        return payload

    def get_active_snapshot(self, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
        version = self.active_snapshot_version(conn)
        if version is None:
            return None
        return self.get_snapshot(version, conn)

    def snapshot_exists(self, version: int, conn: sqlite3.Connection | None = None) -> bool:
        if conn is not None:
            row = conn.execute("SELECT 1 FROM snapshots WHERE version = ?", (version,)).fetchone()
        else:
            with self.db.read_transaction() as read_conn:
                row = read_conn.execute(
                    "SELECT 1 FROM snapshots WHERE version = ?", (version,)
                ).fetchone()
        return row is not None

    # ------------------------------------------------------------------ sessions
    def create_session(
        self,
        conn: sqlite3.Connection,
        *,
        session_id: str,
        sample_id: str,
    ) -> tuple[dict[str, Any], bool]:
        existing_row = conn.execute(
            "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        if existing_row is not None:
            existing = _decode_session(existing_row)
            if existing["sample_id"] != sample_id:
                raise BusinessError(
                    "E_SESSION_CONFLICT",
                    "The session_id is already bound to a different sample.",
                    {"session_id": session_id, "existing_sample_id": existing["sample_id"]},
                )
            return existing, False
        now = self.clock.now()
        conn.execute(
            """
            INSERT INTO sessions(
                session_id, sample_id, status, snapshot_version, is_slow,
                analysis_json, last_error_code, last_error_message, revision,
                created_at, updated_at
            ) VALUES (?, ?, 'CREATED', NULL, NULL, NULL, NULL, NULL, 0, ?, ?)
            """,
            (session_id, sample_id, now, now),
        )
        conn.execute(
            """
            INSERT INTO session_events(
                event_id, session_id, from_status, to_status, reason, intent_id, created_at
            ) VALUES (?, ?, NULL, 'CREATED', 'SESSION_CREATED', NULL, ?)
            """,
            ("evt_" + uuid.uuid4().hex, session_id, now),
        )
        row = conn.execute(
            "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        return _decode_session(row), True

    def get_session(
        self, session_id: str, conn: sqlite3.Connection | None = None
    ) -> dict[str, Any] | None:
        def load(read_conn: sqlite3.Connection):
            return read_conn.execute(
                "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone()

        row = load(conn) if conn is not None else None
        if conn is None:
            with self.db.read_transaction() as read_conn:
                row = load(read_conn)
        return _decode_session(row)

    def require_session(
        self, session_id: str, conn: sqlite3.Connection | None = None
    ) -> dict[str, Any]:
        session = self.get_session(session_id, conn)
        if session is None:
            raise session_not_found(session_id)
        return session

    def transition_session(
        self,
        conn: sqlite3.Connection,
        *,
        session_id: str,
        target: str,
        expected: str | None = None,
        updates: dict[str, Any] | None = None,
        reason: str = "STATE_TRANSITION",
        intent_id: str | None = None,
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        if row is None:
            raise session_not_found(session_id)
        current = row["status"]
        if expected is not None and current != expected:
            raise invalid_transition(
                "The session state changed before this operation could commit.",
                session_id=session_id,
                current_status=current,
                expected_status=expected,
            )
        require_transition(current, target, session_id=session_id)
        allowed = {
            "snapshot_version",
            "is_slow",
            "analysis_json",
            "last_error_code",
            "last_error_message",
        }
        updates = updates or {}
        unknown = sorted(set(updates) - allowed)
        if unknown:
            raise ValueError(f"unsupported session updates: {', '.join(unknown)}")
        assignments = ["status = ?", "updated_at = ?", "revision = revision + 1"]
        values: list[Any] = [str(target), self.clock.now()]
        for key, value in updates.items():
            assignments.append(f"{key} = ?")
            values.append(value)
        values.extend([session_id, current])
        try:
            cursor = conn.execute(
                f"UPDATE sessions SET {', '.join(assignments)} "
                "WHERE session_id = ? AND status = ?",
                values,
            )
        except sqlite3.IntegrityError as exc:
            if "E_INVALID_TRANSITION" in str(exc):
                raise invalid_transition(
                    f"Illegal diagnosis-session transition: {current} -> {target}.",
                    session_id=session_id,
                    current_status=current,
                    target_status=str(target),
                ) from exc
            raise
        if cursor.rowcount != 1:
            latest = conn.execute(
                "SELECT status FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            raise invalid_transition(
                "Concurrent update prevented the diagnosis-session transition.",
                session_id=session_id,
                current_status=None if latest is None else latest["status"],
                target_status=str(target),
            )
        conn.execute(
            """
            INSERT INTO session_events(
                event_id, session_id, from_status, to_status, reason, intent_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "evt_" + uuid.uuid4().hex,
                session_id,
                current,
                str(target),
                reason,
                intent_id,
                self.clock.now(),
            ),
        )
        return self.require_session(session_id, conn)

    def get_session_and_suggestions(self, session_id: str) -> dict[str, Any]:
        with self.db.read_transaction() as conn:
            session = self.require_session(session_id, conn)
            rows = conn.execute(
                "SELECT * FROM suggestions WHERE session_id = ? ORDER BY created_at, rowid",
                (session_id,),
            ).fetchall()
            verification_rows = conn.execute(
                "SELECT * FROM verification_records WHERE session_id = ? "
                "ORDER BY created_at, rowid",
                (session_id,),
            ).fetchall()
            event_rows = conn.execute(
                "SELECT * FROM session_events WHERE session_id = ? ORDER BY created_at, rowid",
                (session_id,),
            ).fetchall()
        session["suggestions"] = [_decode_suggestion(row) for row in rows]
        session["verifications"] = [_decode_verification(row) for row in verification_rows]
        session["trace"] = [dict(row) for row in event_rows]
        return session

    def get_session_trace(self, session_id: str) -> list[dict[str, Any]]:
        with self.db.read_transaction() as conn:
            if conn.execute(
                "SELECT 1 FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone() is None:
                raise session_not_found(session_id)
            rows = conn.execute(
                "SELECT * FROM session_events WHERE session_id = ? ORDER BY created_at, rowid",
                (session_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------ suggestions
    def insert_suggestions(
        self, conn: sqlite3.Connection, suggestions: list[dict[str, Any]]
    ) -> None:
        now = self.clock.now()
        for suggestion in suggestions:
            conn.execute(
                """
                INSERT INTO suggestions(
                    suggestion_id, session_id, rule_id, kind, title,
                    original_sql, rewritten_sql, setup_sql_json, expected_benefit,
                    snapshot_version, cause_codes_json, verification_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PROPOSED', ?)
                """,
                (
                    suggestion["suggestion_id"],
                    suggestion["session_id"],
                    suggestion["rule_id"],
                    suggestion["kind"],
                    suggestion["title"],
                    suggestion["original_sql"],
                    suggestion["rewritten_sql"],
                    canonical_json(suggestion["setup_sql"]),
                    suggestion["expected_benefit"],
                    suggestion["snapshot_version"],
                    canonical_json(suggestion["cause_codes"]),
                    now,
                ),
            )

    def get_suggestions(self, session_id: str) -> list[dict[str, Any]]:
        with self.db.read_transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM suggestions WHERE session_id = ? ORDER BY created_at, rowid",
                (session_id,),
            ).fetchall()
        return [_decode_suggestion(row) for row in rows]

    def get_suggestion(self, session_id: str, suggestion_id: str) -> dict[str, Any] | None:
        with self.db.read_transaction() as conn:
            row = conn.execute(
                "SELECT * FROM suggestions WHERE session_id = ? AND suggestion_id = ?",
                (session_id, suggestion_id),
            ).fetchone()
        return None if row is None else _decode_suggestion(row)

    def set_suggestion_verification_status(
        self, conn: sqlite3.Connection, suggestion_id: str, status: str
    ) -> None:
        cursor = conn.execute(
            "UPDATE suggestions SET verification_status = ? WHERE suggestion_id = ?",
            (status, suggestion_id),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(f"suggestion {suggestion_id!r} was not found")

    # ------------------------------------------------------------------ verification
    def insert_verification(
        self, conn: sqlite3.Connection, record: dict[str, Any]
    ) -> dict[str, Any]:
        conn.execute(
            """
            INSERT INTO verification_records(
                verification_id, session_id, suggestion_id, outcome,
                original_cost_json, rewritten_cost_json,
                original_result_digest, rewritten_result_digest,
                original_row_count, rewritten_row_count,
                error_code, detail_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["verification_id"],
                record["session_id"],
                record["suggestion_id"],
                record["outcome"],
                canonical_json(record.get("original_cost", {})),
                canonical_json(record.get("rewritten_cost", {})),
                record.get("original_result_digest"),
                record.get("rewritten_result_digest"),
                record.get("original_row_count"),
                record.get("rewritten_row_count"),
                record.get("error_code"),
                canonical_json(record.get("details", {})),
                self.clock.now(),
            ),
        )
        row = conn.execute(
            "SELECT * FROM verification_records WHERE verification_id = ?",
            (record["verification_id"],),
        ).fetchone()
        return _decode_verification(row)

    def get_verification_for_suggestion(self, suggestion_id: str) -> dict[str, Any] | None:
        with self.db.read_transaction() as conn:
            row = conn.execute(
                "SELECT * FROM verification_records WHERE suggestion_id = ?", (suggestion_id,)
            ).fetchone()
        return None if row is None else _decode_verification(row)

    def list_verifications(self, session_id: str) -> list[dict[str, Any]]:
        with self.db.read_transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM verification_records WHERE session_id = ? "
                "ORDER BY created_at, rowid",
                (session_id,),
            ).fetchall()
        return [_decode_verification(row) for row in rows]

    # ------------------------------------------------------------------ recovery
    def store_recovery_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        report_id = "rec_" + uuid.uuid4().hex
        generated_at = payload.get("generated_at") or self.clock.now()
        with self.db.write_transaction() as conn:
            conn.execute(
                "INSERT INTO recovery_reports(report_id, generated_at, payload_json) "
                "VALUES (?, ?, ?)",
                (report_id, generated_at, canonical_json(payload)),
            )
        return {"report_id": report_id, "generated_at": generated_at, "payload": payload}

    def latest_recovery_report(self) -> dict[str, Any] | None:
        with self.db.read_transaction() as conn:
            row = conn.execute(
                "SELECT * FROM recovery_reports ORDER BY generated_at DESC, rowid DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return {
            "report_id": row["report_id"],
            "generated_at": row["generated_at"],
            "payload": _decode_json(row["payload_json"], {}),
        }

    def recovery_facts(self) -> dict[str, Any]:
        with self.db.read_transaction() as conn:
            sessions = [
                dict(row)
                for row in conn.execute(
                    "SELECT session_id, sample_id, status, snapshot_version, is_slow, "
                    "last_error_code, revision, updated_at FROM sessions "
                    "ORDER BY created_at, session_id"
                ).fetchall()
            ]
            events = [
                dict(row)
                for row in conn.execute(
                    "SELECT event_id, session_id, from_status, to_status, reason, intent_id, "
                    "created_at FROM session_events ORDER BY created_at, rowid"
                ).fetchall()
            ]
            intents = [
                dict(row)
                for row in conn.execute(
                    "SELECT intent_id, session_id, kind, status, checkpoint, created_at, updated_at "
                    "FROM operation_intents ORDER BY created_at, intent_id"
                ).fetchall()
            ]
            verifications = [
                dict(row)
                for row in conn.execute(
                    "SELECT verification_id, session_id, suggestion_id, outcome, error_code, created_at "
                    "FROM verification_records ORDER BY created_at, verification_id"
                ).fetchall()
            ]
            suggestions = [
                dict(row)
                for row in conn.execute(
                    "SELECT suggestion_id, session_id, rule_id, kind, snapshot_version, "
                    "verification_status, created_at FROM suggestions "
                    "ORDER BY created_at, rowid"
                ).fetchall()
            ]
            suggested_without_suggestions = conn.execute(
                "SELECT COUNT(*) AS count FROM sessions s WHERE s.status = 'SUGGESTED' "
                "AND s.is_slow = 1 AND NOT EXISTS "
                "(SELECT 1 FROM suggestions g WHERE g.session_id = s.session_id)"
            ).fetchone()["count"]
            verified_without_record = conn.execute(
                "SELECT COUNT(*) AS count FROM sessions s WHERE s.status = 'VERIFIED' "
                "AND NOT EXISTS (SELECT 1 FROM verification_records v "
                "WHERE v.session_id = s.session_id AND v.outcome = 'VERIFIED')"
            ).fetchone()["count"]
            pending_intents = conn.execute(
                "SELECT COUNT(*) AS count FROM operation_intents WHERE status = 'PENDING'"
            ).fetchone()["count"]
        return {
            "sessions": sessions,
            "session_events": events,
            "intents": intents,
            "suggestions": suggestions,
            "verifications": verifications,
            "invariants": {
                "suggested_without_suggestions": int(suggested_without_suggestions),
                "verified_without_verification_record": int(verified_without_record),
                "pending_intents_remaining": int(pending_intents),
            },
        }
