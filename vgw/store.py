from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .snapshot import binding_endpoint
from .models import SnapshotDocument


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class Store:
    """SQLite persistence with one serialized writer and immutable read commits."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self._initialize()

    def close(self) -> None:
        try:
            self.conn.close()
        except sqlite3.Error:
            pass

    @contextmanager
    def _tx(self) -> Iterator[None]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _initialize(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                version INTEGER PRIMARY KEY,
                digest TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                body_json TEXT NOT NULL,
                applied_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                last_acked_seq INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS leases (
                session_id TEXT PRIMARY KEY,
                channel_id INTEGER,
                sent_unacked_seq INTEGER,
                recent_activity_at TEXT NOT NULL,
                status TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS frames (
                frame_id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                payload_hash TEXT NOT NULL,
                payload_len INTEGER NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                snapshot_version INTEGER,
                channel_id INTEGER,
                egress TEXT,
                result_json TEXT,
                error_code TEXT,
                error_context_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_frames_session_seq
                ON frames(session_id, sequence);
            CREATE INDEX IF NOT EXISTS idx_frames_status ON frames(status);
            CREATE TABLE IF NOT EXISTS intents (
                intent_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                frame_id TEXT,
                session_id TEXT,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                resolved_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
            CREATE TABLE IF NOT EXISTS stats (
                session_id TEXT PRIMARY KEY,
                snapshot_version INTEGER,
                egress TEXT,
                channel_id INTEGER,
                frames_acked INTEGER NOT NULL DEFAULT 0,
                bytes_acked INTEGER NOT NULL DEFAULT 0,
                invalidations INTEGER NOT NULL DEFAULT 0,
                rebuilt_at TEXT
            );
            CREATE TABLE IF NOT EXISTS trace_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                at TEXT NOT NULL,
                kind TEXT NOT NULL,
                frame_id TEXT,
                session_id TEXT,
                channel_id INTEGER,
                sequence INTEGER,
                snapshot_version INTEGER,
                egress TEXT,
                detail_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trace_session ON trace_events(session_id);
            CREATE TABLE IF NOT EXISTS recovery_reports (
                report_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                report_json TEXT NOT NULL
            );
            """
        )

    # ----------------------------- snapshots -----------------------------
    def latest_snapshot(self) -> tuple[int, dict[str, Any]] | None:
        row = self.conn.execute(
            "SELECT version, body_json FROM snapshots ORDER BY version DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return int(row["version"]), json.loads(row["body_json"])

    def snapshot_version(self) -> int | None:
        row = self.conn.execute("SELECT MAX(version) AS version FROM snapshots").fetchone()
        value = row["version"] if row is not None else None
        return int(value) if value is not None else None

    def commit_snapshot(
        self,
        snapshot: SnapshotDocument,
        body: dict[str, Any],
        *,
        affected_sessions: tuple[str, ...],
        trace_kind: str,
        trace_detail: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        now = utc_now()
        with self._tx():
            self.conn.execute(
                "INSERT INTO snapshots(version, digest, generated_at, body_json, applied_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    snapshot.version,
                    snapshot.digest,
                    snapshot.generated_at,
                    json_text(body),
                    now,
                ),
            )
            for session_id in snapshot.bindings:
                self.conn.execute(
                    "INSERT OR IGNORE INTO sessions(session_id, last_acked_seq, updated_at) "
                    "VALUES (?, 0, ?)",
                    (session_id, now),
                )
                self.conn.execute(
                    "INSERT OR IGNORE INTO leases(session_id, channel_id, sent_unacked_seq, "
                    "recent_activity_at, status) VALUES (?, NULL, NULL, ?, 'idle')",
                    (session_id, now),
                )
            self._rebuild_stats_tx(snapshot, affected_sessions, now)
            self._trace_tx(
                kind=trace_kind,
                frame_id=None,
                session_id=None,
                channel_id=None,
                sequence=None,
                snapshot_version=snapshot.version,
                egress=None,
                detail=trace_detail,
                at=now,
            )
        return self.load_stats()

    def active_snapshot_model(self) -> SnapshotDocument | None:
        latest = self.latest_snapshot()
        if latest is None:
            return None
        return SnapshotDocument.model_validate(latest[1])

    def _rebuild_stats_tx(
        self,
        snapshot: SnapshotDocument,
        affected_sessions: tuple[str, ...],
        now: str,
    ) -> None:
        for session_id in affected_sessions:
            binding = snapshot.bindings.get(session_id)
            if binding is None:
                self.conn.execute("DELETE FROM stats WHERE session_id = ?", (session_id,))
                continue
            old = self.conn.execute(
                "SELECT invalidations FROM stats WHERE session_id = ?", (session_id,)
            ).fetchone()
            invalidations = int(old["invalidations"]) + 1 if old is not None else 1
            aggregate = self.conn.execute(
                "SELECT COUNT(*) AS frames_acked, COALESCE(SUM(payload_len), 0) AS bytes_acked "
                "FROM frames WHERE session_id = ? AND status = 'acked'",
                (session_id,),
            ).fetchone()
            egress = snapshot.egress_endpoints[binding.egress]
            self.conn.execute(
                """
                INSERT INTO stats(
                    session_id, snapshot_version, egress, channel_id,
                    frames_acked, bytes_acked, invalidations, rebuilt_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    snapshot_version = excluded.snapshot_version,
                    egress = excluded.egress,
                    channel_id = excluded.channel_id,
                    frames_acked = excluded.frames_acked,
                    bytes_acked = excluded.bytes_acked,
                    invalidations = excluded.invalidations,
                    rebuilt_at = excluded.rebuilt_at
                """,
                (
                    session_id,
                    snapshot.version,
                    egress,
                    binding.channel,
                    int(aggregate["frames_acked"]),
                    int(aggregate["bytes_acked"]),
                    invalidations,
                    now,
                ),
            )

    # ------------------------------- frames ------------------------------
    def commit_queued_frame(
        self,
        *,
        frame_id: str,
        session_id: str,
        sequence: int,
        payload_hash: str,
        payload_len: int,
        snapshot_version: int,
    ) -> None:
        now = utc_now()
        with self._tx():
            self.conn.execute(
                """
                INSERT INTO frames(
                    frame_id, session_id, sequence, payload_hash, payload_len,
                    status, attempts, snapshot_version, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'queued', 0, ?, ?, ?)
                """,
                (
                    frame_id,
                    session_id,
                    sequence,
                    payload_hash,
                    payload_len,
                    snapshot_version,
                    now,
                    now,
                ),
            )
            self._trace_tx(
                kind="frame_queued",
                frame_id=frame_id,
                session_id=session_id,
                channel_id=None,
                sequence=sequence,
                snapshot_version=snapshot_version,
                egress=None,
                detail={"payload_bytes": payload_len},
                at=now,
            )

    def commit_dispatch(
        self,
        *,
        intent_id: str,
        frame_id: str,
        session_id: str,
        sequence: int,
        channel_id: int,
        egress: str,
        snapshot_version: int,
        payload_len: int,
    ) -> None:
        now = utc_now()
        intent = {
            "frame_id": frame_id,
            "session_id": session_id,
            "sequence": sequence,
            "channel_id": channel_id,
            "egress": egress,
            "snapshot_version": snapshot_version,
        }
        with self._tx():
            self.conn.execute(
                "INSERT INTO intents(intent_id, kind, frame_id, session_id, status, "
                "payload_json, created_at) VALUES (?, 'frame_dispatch', ?, ?, 'pending', ?, ?)",
                (intent_id, frame_id, session_id, json_text(intent), now),
            )
            cursor = self.conn.execute(
                """
                UPDATE frames SET
                    status = 'dispatched', attempts = attempts + 1,
                    snapshot_version = ?, channel_id = ?, egress = ?, updated_at = ?
                WHERE frame_id = ? AND status = 'queued'
                """,
                (snapshot_version, channel_id, egress, now, frame_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"frame {frame_id} was not in queued state")
            self.conn.execute(
                """
                INSERT INTO leases(session_id, channel_id, sent_unacked_seq, recent_activity_at, status)
                VALUES (?, ?, ?, ?, 'active')
                ON CONFLICT(session_id) DO UPDATE SET
                    channel_id = excluded.channel_id,
                    sent_unacked_seq = excluded.sent_unacked_seq,
                    recent_activity_at = excluded.recent_activity_at,
                    status = 'active'
                """,
                (session_id, channel_id, sequence, now),
            )
            self._trace_tx(
                kind="channel_acquired",
                frame_id=frame_id,
                session_id=session_id,
                channel_id=channel_id,
                sequence=sequence,
                snapshot_version=snapshot_version,
                egress=egress,
                detail={"intent_id": intent_id, "payload_bytes": payload_len},
                at=now,
            )

    def commit_success(
        self,
        *,
        intent_id: str,
        frame_id: str,
        session_id: str,
        sequence: int,
        channel_id: int,
        egress: str,
        snapshot_version: int,
        payload_len: int,
        result: dict[str, Any],
    ) -> None:
        now = utc_now()
        with self._tx():
            self.conn.execute(
                "UPDATE intents SET status = 'committed', resolved_at = ? WHERE intent_id = ?",
                (now, intent_id),
            )
            self.conn.execute(
                """
                UPDATE frames SET status = 'acked', result_json = ?, error_code = NULL,
                    error_context_json = NULL, updated_at = ?
                WHERE frame_id = ?
                """,
                (json_text(result), now, frame_id),
            )
            self._release_lease_tx(session_id, now, "idle")
            self.conn.execute(
                """
                INSERT INTO stats(
                    session_id, snapshot_version, egress, channel_id,
                    frames_acked, bytes_acked, invalidations, rebuilt_at
                ) VALUES (?, ?, ?, ?, 1, ?, 0, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    snapshot_version = excluded.snapshot_version,
                    egress = excluded.egress,
                    channel_id = excluded.channel_id,
                    frames_acked = stats.frames_acked + 1,
                    bytes_acked = stats.bytes_acked + excluded.bytes_acked,
                    rebuilt_at = excluded.rebuilt_at
                """,
                (session_id, snapshot_version, egress, channel_id, payload_len, now),
            )
            self.conn.execute(
                """
                INSERT INTO sessions(session_id, last_acked_seq, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    last_acked_seq = MAX(sessions.last_acked_seq, excluded.last_acked_seq),
                    updated_at = excluded.updated_at
                """,
                (session_id, sequence, now),
            )
            self._trace_tx(
                kind="frame_acked",
                frame_id=frame_id,
                session_id=session_id,
                channel_id=channel_id,
                sequence=sequence,
                snapshot_version=snapshot_version,
                egress=egress,
                detail={"intent_id": intent_id, "payload_bytes": payload_len},
                at=now,
            )
            self._trace_tx(
                kind="channel_released",
                frame_id=frame_id,
                session_id=session_id,
                channel_id=channel_id,
                sequence=sequence,
                snapshot_version=snapshot_version,
                egress=egress,
                detail={"reason": "ack"},
                at=now,
            )

    def commit_failure(
        self,
        *,
        intent_id: str,
        frame_id: str,
        session_id: str,
        sequence: int,
        channel_id: int | None,
        egress: str | None,
        snapshot_version: int | None,
        error_code: str,
        error_context: dict[str, Any],
        abort_queued: list[dict[str, Any]],
    ) -> None:
        now = utc_now()
        with self._tx():
            self.conn.execute(
                "UPDATE intents SET status = 'failed', resolved_at = ? WHERE intent_id = ?",
                (now, intent_id),
            )
            self.conn.execute(
                """
                UPDATE frames SET status = 'failed', error_code = ?,
                    error_context_json = ?, updated_at = ? WHERE frame_id = ?
                """,
                (error_code, json_text(error_context), now, frame_id),
            )
            self._release_lease_tx(session_id, now, "idle")
            self._trace_tx(
                kind="frame_failed",
                frame_id=frame_id,
                session_id=session_id,
                channel_id=channel_id,
                sequence=sequence,
                snapshot_version=snapshot_version,
                egress=egress,
                detail={"error_code": error_code, "context": error_context},
                at=now,
            )
            if channel_id is not None:
                self._trace_tx(
                    kind="channel_released",
                    frame_id=frame_id,
                    session_id=session_id,
                    channel_id=channel_id,
                    sequence=sequence,
                    snapshot_version=snapshot_version,
                    egress=egress,
                    detail={"reason": error_code},
                    at=now,
                )
            for item in abort_queued:
                self.conn.execute(
                    "UPDATE frames SET status = 'aborted', error_code = ?, "
                    "error_context_json = ?, updated_at = ? WHERE frame_id = ?",
                    (
                        "E_FRAME_INVALID",
                        json_text(
                            {
                                "reason": "prior_frame_in_session_failed",
                                "prior_frame_id": frame_id,
                                "prior_sequence": sequence,
                            }
                        ),
                        now,
                        item["frame_id"],
                    ),
                )
                self._trace_tx(
                    kind="frame_aborted",
                    frame_id=item["frame_id"],
                    session_id=session_id,
                    channel_id=None,
                    sequence=item["sequence"],
                    snapshot_version=snapshot_version,
                    egress=None,
                    detail={
                        "reason": "prior_frame_in_session_failed",
                        "prior_frame_id": frame_id,
                    },
                    at=now,
                )

    def commit_queued_abort(
        self,
        *,
        frame_id: str,
        session_id: str,
        sequence: int,
        reason: str,
        error_code: str = "E_FRAME_INVALID",
    ) -> None:
        now = utc_now()
        with self._tx():
            self.conn.execute(
                "UPDATE frames SET status = 'aborted', error_code = ?, "
                "error_context_json = ?, updated_at = ? WHERE frame_id = ?",
                (error_code, json_text({"reason": reason}), now, frame_id),
            )
            self._trace_tx(
                kind="frame_aborted",
                frame_id=frame_id,
                session_id=session_id,
                channel_id=None,
                sequence=sequence,
                snapshot_version=None,
                egress=None,
                detail={"reason": reason},
                at=now,
            )

    def add_trace(
        self,
        *,
        kind: str,
        frame_id: str | None = None,
        session_id: str | None = None,
        channel_id: int | None = None,
        sequence: int | None = None,
        snapshot_version: int | None = None,
        egress: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        with self._tx():
            self._trace_tx(
                kind=kind,
                frame_id=frame_id,
                session_id=session_id,
                channel_id=channel_id,
                sequence=sequence,
                snapshot_version=snapshot_version,
                egress=egress,
                detail=detail or {},
                at=utc_now(),
            )

    def _trace_tx(
        self,
        *,
        kind: str,
        at: str,
        frame_id: str | None,
        session_id: str | None,
        channel_id: int | None,
        sequence: int | None,
        snapshot_version: int | None,
        egress: str | None,
        detail: dict[str, Any],
    ) -> None:
        self.conn.execute(
            "INSERT INTO trace_events(at, kind, frame_id, session_id, channel_id, "
            "sequence, snapshot_version, egress, detail_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                at,
                kind,
                frame_id,
                session_id,
                channel_id,
                sequence,
                snapshot_version,
                egress,
                json_text(detail),
            ),
        )

    def _release_lease_tx(self, session_id: str, now: str, status: str) -> None:
        self.conn.execute(
            "UPDATE leases SET channel_id = NULL, sent_unacked_seq = NULL, "
            "recent_activity_at = ?, status = ? WHERE session_id = ?",
            (now, status, session_id),
        )

    # ------------------------------- reads -------------------------------
    def load_sessions(self) -> dict[str, dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM sessions").fetchall()
        return {
            row["session_id"]: {
                "last_acked_seq": int(row["last_acked_seq"]),
                "updated_at": row["updated_at"],
            }
            for row in rows
        }

    def load_leases(self) -> dict[str, dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM leases ORDER BY session_id").fetchall()
        return {
            row["session_id"]: {
                "session_id": row["session_id"],
                "channel_id": row["channel_id"],
                "sent_unacked_seq": row["sent_unacked_seq"],
                "recent_activity_at": row["recent_activity_at"],
                "status": row["status"],
            }
            for row in rows
        }

    def load_stats(self) -> dict[str, dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM stats ORDER BY session_id").fetchall()
        return {
            row["session_id"]: {
                "session_id": row["session_id"],
                "snapshot_version": row["snapshot_version"],
                "egress": row["egress"],
                "channel_id": row["channel_id"],
                "frames_acked": int(row["frames_acked"]),
                "bytes_acked": int(row["bytes_acked"]),
                "invalidations": int(row["invalidations"]),
                "rebuilt_at": row["rebuilt_at"],
            }
            for row in rows
        }

    def get_frame(self, frame_id: str) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT * FROM frames WHERE frame_id = ?", (frame_id,)).fetchone()
        if row is None:
            return None
        data = dict(row)
        data["result"] = json.loads(data["result_json"]) if data["result_json"] else None
        data["error_context"] = (
            json.loads(data["error_context_json"]) if data["error_context_json"] else None
        )
        data.pop("result_json", None)
        data.pop("error_context_json", None)
        return data

    def list_frames(self, limit: int = 200, session_id: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM frames"
        params: list[Any] = []
        if session_id:
            sql += " WHERE session_id = ?"
            params.append(session_id)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            data["result"] = json.loads(data.pop("result_json")) if data["result_json"] else None
            data["error_context"] = (
                json.loads(data.pop("error_context_json"))
                if data["error_context_json"]
                else None
            )
            result.append(data)
        return result

    def list_traces(self, limit: int = 300, session_id: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM trace_events"
        params: list[Any] = []
        if session_id:
            sql += " WHERE session_id = ?"
            params.append(session_id)
        sql += " ORDER BY event_id DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in reversed(rows):
            data = dict(row)
            data["detail"] = json.loads(data.pop("detail_json"))
            result.append(data)
        return result

    def list_intents(self, limit: int = 100, status: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM intents"
        params: list[Any] = []
        if status:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            data = dict(row)
            data["payload"] = json.loads(data.pop("payload_json"))
            result.append(data)
        return result

    def latest_recovery(self) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT report_json FROM recovery_reports ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        return json.loads(row["report_json"]) if row is not None else None

    # ------------------------------ recovery -----------------------------
    def recover_incomplete_intents(self) -> dict[str, Any]:
        report_id = str(uuid.uuid4())
        created_at = utc_now()
        latest = self.latest_snapshot()
        snapshot = SnapshotDocument.model_validate(latest[1]) if latest else None
        lease_before = self.load_leases()
        pending_intents = self.conn.execute(
            "SELECT * FROM intents WHERE status = 'pending' ORDER BY created_at"
        ).fetchall()
        queued_rows = self.conn.execute(
            "SELECT * FROM frames WHERE status IN ('queued', 'dispatched') "
            "ORDER BY created_at"
        ).fetchall()
        intent_ids_with_pending_frame = {
            row["frame_id"] for row in pending_intents if row["frame_id"] is not None
        }
        pending_actions: list[dict[str, Any]] = []
        retransmit: list[dict[str, Any]] = []
        aborted: list[dict[str, Any]] = []
        now = utc_now()
        with self._tx():
            for row in pending_intents:
                payload = json.loads(row["payload_json"])
                frame_id = row["frame_id"]
                frame = self.conn.execute(
                    "SELECT * FROM frames WHERE frame_id = ?", (frame_id,)
                ).fetchone()
                action: dict[str, Any]
                if row["kind"] != "frame_dispatch":
                    action = {
                        "intent_id": row["intent_id"],
                        "kind": row["kind"],
                        "frame_id": frame_id,
                        "outcome": "rolled_back",
                        "basis": "intent_was_not_committed",
                    }
                elif frame is None:
                    action = {
                        "intent_id": row["intent_id"],
                        "kind": row["kind"],
                        "frame_id": frame_id,
                        "outcome": "aborted",
                        "reason": "missing_frame_fact",
                        "basis": "persisted_intent_only",
                    }
                else:
                    current_binding_ok = False
                    if snapshot is not None:
                        binding = snapshot.bindings.get(payload.get("session_id"))
                        if binding is not None:
                            _host, _port, current_egress = binding_endpoint(
                                snapshot, payload["session_id"]
                            )
                            current_binding_ok = (
                                current_egress == payload.get("egress")
                                and binding.channel == payload.get("channel_id")
                            )
                    if frame["status"] == "dispatched" and current_binding_ok:
                        outcome = "retransmit_required"
                        self.conn.execute(
                            "UPDATE frames SET status = 'retransmit_required', "
                            "error_code = 'E_RECOVERY_RETRANSMIT_REQUIRED', "
                            "error_context_json = ?, updated_at = ? WHERE frame_id = ?",
                            (
                                json_text(
                                    {
                                        "basis": "persisted_pending_intent_and_sent_unacked_lease",
                                        "intent_id": row["intent_id"],
                                    }
                                ),
                                now,
                                frame_id,
                            ),
                        )
                        action = {
                            "intent_id": row["intent_id"],
                            "kind": row["kind"],
                            "frame_id": frame_id,
                            "session_id": payload.get("session_id"),
                            "sequence": payload.get("sequence"),
                            "outcome": outcome,
                            "basis": "persisted_dispatch_intent",
                        }
                        retransmit.append(action)
                    else:
                        reason = (
                            "binding_superseded"
                            if not current_binding_ok
                            else f"frame_state_{frame['status']}"
                        )
                        self.conn.execute(
                            "UPDATE frames SET status = 'aborted', error_code = ?, "
                            "error_context_json = ?, updated_at = ? WHERE frame_id = ?",
                            (
                                "E_RECOVERY_ABORTED",
                                json_text({"basis": "persisted_facts", "reason": reason}),
                                now,
                                frame_id,
                            ),
                        )
                        action = {
                            "intent_id": row["intent_id"],
                            "kind": row["kind"],
                            "frame_id": frame_id,
                            "session_id": payload.get("session_id"),
                            "sequence": payload.get("sequence"),
                            "outcome": "aborted",
                            "reason": reason,
                            "basis": "persisted_dispatch_intent",
                        }
                        aborted.append(action)
                if row["kind"] == "frame_dispatch":
                    new_status = (
                        "recovered_retransmit"
                        if action["outcome"] == "retransmit_required"
                        else "recovered_aborted"
                    )
                else:
                    new_status = "recovered_rolled_back"
                self.conn.execute(
                    "UPDATE intents SET status = ?, resolved_at = ? WHERE intent_id = ?",
                    (new_status, now, row["intent_id"]),
                )
                pending_actions.append(action)

            for row in queued_rows:
                frame_id = row["frame_id"]
                if row["status"] == "dispatched" and frame_id in intent_ids_with_pending_frame:
                    continue
                if row["status"] == "dispatched":
                    reason = "dispatched_without_pending_intent"
                else:
                    reason = "process_restarted_before_dispatch"
                self.conn.execute(
                    "UPDATE frames SET status = 'aborted', error_code = 'E_RECOVERY_ABORTED', "
                    "error_context_json = ?, updated_at = ? WHERE frame_id = ?",
                    (json_text({"basis": "persisted_facts", "reason": reason}), now, frame_id),
                )
                item = {
                    "frame_id": frame_id,
                    "session_id": row["session_id"],
                    "sequence": int(row["sequence"]),
                    "outcome": "aborted",
                    "reason": reason,
                    "basis": "persisted_frame_state",
                }
                aborted.append(item)
                self._trace_tx(
                    kind="recovery_frame_aborted",
                    frame_id=frame_id,
                    session_id=row["session_id"],
                    channel_id=None,
                    sequence=int(row["sequence"]),
                    snapshot_version=row["snapshot_version"],
                    egress=row["egress"],
                    detail={"reason": reason},
                    at=now,
                )

            self.conn.execute(
                "UPDATE leases SET channel_id = NULL, sent_unacked_seq = NULL, "
                "status = CASE WHEN status = 'active' THEN 'recovered_idle' ELSE status END, "
                "recent_activity_at = CASE WHEN status = 'active' THEN ? "
                "ELSE recent_activity_at END",
                (now,),
            )
            self._trace_tx(
                kind="recovery_completed",
                frame_id=None,
                session_id=None,
                channel_id=None,
                sequence=None,
                snapshot_version=snapshot.version if snapshot else None,
                egress=None,
                detail={
                    "retransmit_required": len(retransmit),
                    "aborted": len(aborted),
                    "pending_intents": len(pending_intents),
                },
                at=now,
            )
            lease_after = self.load_leases()
            lease_differences = [
                {
                    "session_id": session_id,
                    "before": lease_before.get(session_id),
                    "after": lease_after.get(session_id),
                }
                for session_id in sorted(set(lease_before) | set(lease_after))
                if lease_before.get(session_id) != lease_after.get(session_id)
            ]
            report = {
                "report_id": report_id,
                "created_at": created_at,
                "source": "crash_recovery_scan",
                "facts_only": True,
                "graceful_exit_report_available": False,
                "snapshot_version": snapshot.version if snapshot else None,
                "scanned_pending_intents": len(pending_intents),
                "scanned_queued_or_dispatched_frames": len(queued_rows),
                "found_work": bool(pending_intents or queued_rows or any(
                    item.get("channel_id") is not None or item.get("sent_unacked_seq") is not None
                    for item in lease_before.values()
                )),
                "pending_actions": pending_actions,
                "retransmit_required": retransmit,
                "aborted": aborted,
                "lease_differences": lease_differences,
                "lease_diff_count": len(lease_differences),
                "unchanged_lease_count": len(lease_after) - len(lease_differences),
                "statement": (
                    "Recovery is derived only from committed SQLite intent, frame, and lease rows. "
                    "The record does not claim that a killed process produced a graceful exit report."
                ),
            }
            self.conn.execute(
                "INSERT INTO recovery_reports(report_id, created_at, report_json) VALUES (?, ?, ?)",
                (report_id, created_at, json_text(report)),
            )
            self._trace_tx(
                kind="recovery_report_written",
                frame_id=None,
                session_id=None,
                channel_id=None,
                sequence=None,
                snapshot_version=snapshot.version if snapshot else None,
                egress=None,
                detail={"report_id": report_id, "facts_only": True},
                at=now,
            )
        return report

