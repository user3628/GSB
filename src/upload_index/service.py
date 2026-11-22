from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import BusinessError
from .state import ContentRecord, IndexState, PartRecord, SessionState
from .validation import (
    MAX_I64,
    PartInput,
    parse_int,
    parse_manifest,
    parse_object_key,
    parse_part,
    parse_parts,
    parse_policy,
    parse_session_id,
    parse_version,
    validate_unique_indexes,
)


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    object_key TEXT NOT NULL,
    generation INTEGER NOT NULL,
    lower_bound INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'completed', 'cancelled')),
    conflict_policy TEXT NOT NULL,
    duplicate_commits INTEGER NOT NULL DEFAULT 0,
    created_version INTEGER NOT NULL,
    updated_version INTEGER NOT NULL,
    completed_version INTEGER,
    cancelled_version INTEGER,
    UNIQUE (object_key, generation)
);

CREATE TABLE IF NOT EXISTS parts (
    session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    arrival_ms INTEGER NOT NULL,
    first_version INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, seq)
);

CREATE INDEX IF NOT EXISTS parts_sha_idx ON parts(sha256);

CREATE TABLE IF NOT EXISTS content_blobs (
    sha256 TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    ref_count INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'pending_gc')),
    first_version INTEGER NOT NULL,
    gc_version INTEGER
);

CREATE TABLE IF NOT EXISTS objects (
    object_key TEXT PRIMARY KEY,
    total_size INTEGER NOT NULL,
    part_count INTEGER NOT NULL,
    object_sha256 TEXT NOT NULL,
    generation INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    merged_version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS version_events (
    version INTEGER PRIMARY KEY,
    operation TEXT NOT NULL,
    session_id TEXT,
    object_key TEXT,
    created_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS version_events_op_idx ON version_events(operation, version);

CREATE TABLE IF NOT EXISTS index_segments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    start_version INTEGER NOT NULL,
    end_version INTEGER NOT NULL,
    events_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'staging')),
    merge_intent_id TEXT,
    created_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS index_segments_session_idx
    ON index_segments(session_id, status, start_version);

CREATE TABLE IF NOT EXISTS recovery_intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'complete', 'aborted')),
    session_id TEXT,
    object_key TEXT,
    details_json TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    result_json TEXT
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    created_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS version_checkpoints (
    version INTEGER PRIMARY KEY,
    created_ms INTEGER NOT NULL,
    state_json TEXT NOT NULL
);
"""


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _object_digest(parts: list[sqlite3.Row]) -> str:
    digest = hashlib.sha256()
    digest.update(b"LOCAL-OBJECT-V1\0")
    for part in parts:
        digest.update(f"{part['seq']}:{part['size']}:{part['sha256']}\n".encode("ascii"))
    return digest.hexdigest()


class UploadIndexService:
    """Transactional segmented-upload index backed by SQLite.

    The in-memory indexes are updated incrementally after each committed transaction.
    Versioned reads never inspect mutable current state; they replay committed events
    from the latest retained checkpoint.
    """

    def __init__(self, db_path: str | os.PathLike[str], *, enable_crash_drill: bool = False) -> None:
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self.enable_crash_drill = enable_crash_drill
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(
            self.db_path,
            timeout=15.0,
            isolation_level=None,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = FULL")
        self.conn.execute("PRAGMA busy_timeout = 15000")
        self.state = IndexState()
        self.current_version = 0
        self.retention_floor = 0
        self.query_leases: dict[str, dict[str, Any]] = {}
        self._initialized = False

    # ------------------------------------------------------------------ lifecycle

    def initialize(self) -> None:
        with self.lock:
            if self._initialized:
                return
            self.conn.executescript(SCHEMA)
            self.conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES ('current_version', '0')")
            self.conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES ('retention_floor', '0')")

            previous_clean_row = self.conn.execute(
                "SELECT value FROM meta WHERE key = 'clean_shutdown'"
            ).fetchone()
            first_initialization = previous_clean_row is None
            previous_clean = True if first_initialization else previous_clean_row["value"] == "1"
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES ('clean_shutdown', '0') "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
            )

            recovered = self._recover_pending_intents(previous_clean=previous_clean)
            self.current_version = int(
                self.conn.execute("SELECT value FROM meta WHERE key = 'current_version'").fetchone()[0]
            )
            self.retention_floor = int(
                self.conn.execute("SELECT value FROM meta WHERE key = 'retention_floor'").fetchone()[0]
            )
            self._load_current_state()
            self._persist_recovery_report(
                previous_clean=previous_clean,
                forced_termination_detected=not previous_clean,
                recovered=recovered,
            )
            self._initialized = True

    def shutdown(self) -> None:
        with self.lock:
            self.conn.execute(
                "INSERT INTO meta(key, value) VALUES ('clean_shutdown', '1') "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
            )

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    # ------------------------------------------------------------------ helpers

    @contextmanager
    def _write_tx(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        else:
            self.conn.execute("COMMIT")

    def _meta_get(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row["value"])

    def _meta_set(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def _next_version(self, conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT value FROM meta WHERE key = 'current_version'").fetchone()
        version = int(row["value"]) + 1
        conn.execute("UPDATE meta SET value = ? WHERE key = 'current_version'", (str(version),))
        return version

    def _new_event(
        self,
        version: int,
        operation: str,
        *,
        session_id: str | None,
        object_key: str | None,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "version": version,
            "operation": operation,
            "session_id": session_id,
            "object_key": object_key,
            "created_ms": _now_ms(),
            "payload": payload,
        }

    def _persist_session_event(self, conn: sqlite3.Connection, event: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO version_events(version, operation, session_id, object_key, created_ms, payload_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                event["version"],
                event["operation"],
                event["session_id"],
                event["object_key"],
                event["created_ms"],
                _json(event["payload"]),
            ),
        )
        conn.execute(
            "INSERT INTO index_segments(session_id, start_version, end_version, events_json, status, created_ms) "
            "VALUES (?, ?, ?, ?, 'active', ?)",
            (
                event["session_id"],
                event["version"],
                event["version"],
                _json([event]),
                event["created_ms"],
            ),
        )

    def _persist_global_event(self, conn: sqlite3.Connection, event: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO version_events(version, operation, session_id, object_key, created_ms, payload_json) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                event["version"],
                event["operation"],
                event["session_id"],
                event["object_key"],
                event["created_ms"],
                _json(event["payload"]),
            ),
        )

    def _apply_events_to_memory(self, events: list[dict[str, Any]]) -> None:
        try:
            for event in events:
                self.state.apply_event(event)
        except BaseException:
            # A committed database version must remain authoritative under all circumstances.
            self._load_current_state()
            raise

    def _load_current_state(self) -> None:
        state = IndexState()
        for row in self.conn.execute("SELECT * FROM sessions"):
            state.sessions[str(row["session_id"])] = SessionState(
                session_id=str(row["session_id"]),
                object_key=str(row["object_key"]),
                generation=int(row["generation"]),
                lower_bound=int(row["lower_bound"]),
                status=str(row["status"]),
                conflict_policy=str(row["conflict_policy"]),
                created_version=int(row["created_version"]),
                updated_version=int(row["updated_version"]),
                duplicate_commits=int(row["duplicate_commits"]),
                completed_version=(
                    None if row["completed_version"] is None else int(row["completed_version"])
                ),
                cancelled_version=(
                    None if row["cancelled_version"] is None else int(row["cancelled_version"])
                ),
            )

        for row in self.conn.execute("SELECT * FROM parts ORDER BY session_id, seq"):
            session = state.sessions.get(str(row["session_id"]))
            if session is None:
                continue
            part = PartRecord(
                seq=int(row["seq"]),
                size=int(row["size"]),
                sha256=str(row["sha256"]),
                arrival_ms=int(row["arrival_ms"]),
                first_version=int(row["first_version"]),
                duplicate_count=int(row["duplicate_count"]),
            )
            session.parts[part.seq] = part
        for session in state.sessions.values():
            session.rebuild_intervals()

        for row in self.conn.execute("SELECT * FROM content_blobs"):
            state.dedup[str(row["sha256"])] = ContentRecord(
                sha256=str(row["sha256"]),
                size=int(row["size"]),
                ref_count=int(row["ref_count"]),
                state=str(row["state"]),
                first_version=int(row["first_version"]),
                gc_version=None if row["gc_version"] is None else int(row["gc_version"]),
            )

        for row in self.conn.execute("SELECT * FROM objects"):
            obj = {
                "object_key": str(row["object_key"]),
                "total_size": int(row["total_size"]),
                "part_count": int(row["part_count"]),
                "object_sha256": str(row["object_sha256"]),
                "generation": int(row["generation"]),
                "session_id": str(row["session_id"]),
                "merged_version": int(row["merged_version"]),
            }
            state.objects[obj["object_key"]] = obj
        self.state = state

    # ------------------------------------------------------------------ recovery

    def _recover_pending_intents(self, *, previous_clean: bool) -> dict[str, Any]:
        staging_count = int(
            self.conn.execute(
                "SELECT COUNT(*) FROM index_segments WHERE status = 'staging'"
            ).fetchone()[0]
        )
        self.conn.execute("DELETE FROM index_segments WHERE status = 'staging'")

        pending_rows = list(
            self.conn.execute(
                "SELECT * FROM recovery_intents WHERE status = 'pending' ORDER BY created_ms, intent_id"
            )
        )
        recovered: list[dict[str, Any]] = []
        now = _now_ms()
        for row in pending_rows:
            details = json.loads(row["details_json"])
            decision = "aborted_rolled_back"
            evidence: dict[str, Any] = {}
            if row["kind"] == "batch":
                batch_id = details.get("batch_id")
                if batch_id:
                    for event in self.conn.execute(
                        "SELECT version, payload_json FROM version_events "
                        "WHERE operation IN ('part_committed', 'part_duplicate') ORDER BY version"
                    ):
                        payload = json.loads(event["payload_json"])
                        if payload.get("batch_id") == batch_id:
                            decision = "committed_verified"
                            evidence["batch_version"] = int(event["version"])
                            break
            recovered.append(
                {
                    "intent_id": row["intent_id"],
                    "kind": row["kind"],
                    "session_id": row["session_id"],
                    "decision": decision,
                    "evidence": evidence,
                }
            )
            self.conn.execute(
                "UPDATE recovery_intents SET status = 'aborted', updated_ms = ?, result_json = ? "
                "WHERE intent_id = ?",
                (now, _json({"recovery": decision, **evidence}), row["intent_id"]),
            )

        return {
            "previous_clean_shutdown": previous_clean,
            "forced_termination_detected": not previous_clean,
            "pending_intents_found": len(pending_rows),
            "recovered_intents": recovered,
            "staging_segments_discarded": staging_count,
            "recovery_source": "sqlite_persisted_facts_only",
        }

    def _persist_recovery_report(
        self,
        *,
        previous_clean: bool,
        forced_termination_detected: bool,
        recovered: dict[str, Any],
    ) -> None:
        counts = {
            "sessions": int(self.conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]),
            "parts": int(self.conn.execute("SELECT COUNT(*) FROM parts").fetchone()[0]),
            "content_records": int(
                self.conn.execute("SELECT COUNT(*) FROM content_blobs").fetchone()[0]
            ),
            "objects": int(self.conn.execute("SELECT COUNT(*) FROM objects").fetchone()[0]),
            "index_segments": int(
                self.conn.execute(
                    "SELECT COUNT(*) FROM index_segments WHERE status = 'active'"
                ).fetchone()[0]
            ),
        }
        report = {
            "report_id": str(uuid.uuid4()),
            "created_ms": _now_ms(),
            "source": "sqlite_persisted_facts_only",
            "previous_clean_shutdown": previous_clean,
            "forced_termination_detected": forced_termination_detected,
            "clean_exit_report_available": previous_clean,
            "last_complete_version": self.current_version,
            "retention_floor": self.retention_floor,
            "counts": counts,
            **recovered,
            "note": (
                "启动恢复报告仅由已提交的 SQLite 事实生成；强制终止发生时不存在完整退出报告。"
                "未完成事务由 SQLite 回滚，未完成意图在此审计并标记为 aborted。"
            ),
        }
        self.conn.execute(
            "INSERT INTO recovery_reports(report_id, created_ms, report_json) VALUES (?, ?, ?)",
            (report["report_id"], report["created_ms"], _json(report)),
        )

    def get_recovery_report(self) -> dict[str, Any]:
        with self.lock:
            row = self.conn.execute(
                "SELECT report_json FROM recovery_reports ORDER BY created_ms DESC, report_id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                raise BusinessError("E_RECOVERY_NOT_FOUND", "no recovery report is available")
            return json.loads(row["report_json"])

    # ------------------------------------------------------------------ sessions

    def create_session(self, payload: dict[str, Any]) -> dict[str, Any]:
        object_key = parse_object_key(payload.get("object_key"))
        session_id = parse_session_id(payload.get("session_id")) or str(uuid.uuid4())
        lower_bound = parse_int(payload.get("lower_bound", 1), "lower_bound", minimum=1)
        conflict_policy = parse_policy(payload.get("conflict_policy"))

        with self.lock:
            with self._write_tx() as conn:
                existing = conn.execute(
                    "SELECT session_id FROM sessions WHERE session_id = ?", (session_id,)
                ).fetchone()
                if existing is not None:
                    raise BusinessError(
                        "E_SESSION_EXISTS", "session_id already exists", details={"session_id": session_id}
                    )

                generation_rows = conn.execute(
                    "SELECT generation, status FROM sessions WHERE object_key = ?", (object_key,)
                ).fetchall()
                current_generation = max((int(row["generation"]) for row in generation_rows), default=0)
                active_sessions = [
                    row for row in generation_rows if str(row["status"]) == "active"
                ]
                if conflict_policy == "reject" and active_sessions:
                    raise BusinessError(
                        "E_SESSION_CONFLICT",
                        "an active session already exists for this object key",
                        details={
                            "object_key": object_key,
                            "current_generation": current_generation,
                            "conflict_policy": conflict_policy,
                        },
                    )

                generation = current_generation + 1
                version = self._next_version(conn)
                event = self._new_event(
                    version,
                    "session_created",
                    session_id=session_id,
                    object_key=object_key,
                    payload={
                        "session_id": session_id,
                        "object_key": object_key,
                        "generation": generation,
                        "lower_bound": lower_bound,
                        "conflict_policy": conflict_policy,
                    },
                )
                conn.execute(
                    "INSERT INTO sessions(session_id, object_key, generation, lower_bound, status, "
                    "conflict_policy, duplicate_commits, created_version, updated_version) "
                    "VALUES (?, ?, ?, ?, 'active', ?, 0, ?, ?)",
                    (
                        session_id,
                        object_key,
                        generation,
                        lower_bound,
                        conflict_policy,
                        version,
                        version,
                    ),
                )
                self._persist_session_event(conn, event)

            self.current_version = version
            self._apply_events_to_memory([event])
            return {
                "session_id": session_id,
                "object_key": object_key,
                "generation": generation,
                "lower_bound": lower_bound,
                "conflict_policy": conflict_policy,
                "status": "active",
                "created_version": version,
            }

    def get_session_basic(self, session_id: str) -> dict[str, Any]:
        with self.lock:
            session = self.state.sessions.get(session_id)
            if session is None:
                raise BusinessError(
                    "E_SESSION_NOT_FOUND", "session does not exist", details={"session_id": session_id}
                )
            return session.to_dict()

    # ------------------------------------------------------------------ part submission

    def submit_part(self, session_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        part = parse_part(payload)
        with self.lock:
            with self._write_tx() as conn:
                events, final_version = self._process_parts_tx(
                    conn, session_id, [part], batch_id=None
                )
            self.current_version = final_version
            self._apply_events_to_memory(events)
            return self._submission_response(session_id, events, batch_id=None)

    def submit_batch(
        self, session_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        raw_parts = payload.get("parts")
        parts = parse_parts(raw_parts)
        validate_unique_indexes(parts)
        batch_id = payload.get("batch_id") or str(uuid.uuid4())
        if not isinstance(batch_id, str) or not batch_id or len(batch_id) > 256:
            raise BusinessError("E_REQUEST_INVALID", "batch_id must be a non-empty string")

        with self.lock:
            intent_id = self._create_intent(
                kind="batch",
                session_id=session_id,
                object_key=None,
                details={
                    "batch_id": batch_id,
                    "part_count": len(parts),
                    "part_indexes": [part.seq for part in parts],
                    "payload_sha256": hashlib.sha256(
                        _json([part.as_dict() for part in parts]).encode("utf-8")
                    ).hexdigest(),
                },
            )
            try:
                with self._write_tx() as conn:
                    events, final_version = self._process_parts_tx(
                        conn, session_id, parts, batch_id=batch_id
                    )
                    self._finish_intent_tx(
                        conn,
                        intent_id,
                        {"decision": "complete", "last_version": final_version},
                    )
            except BaseException:
                self._abort_intent(intent_id, "transaction_rolled_back")
                raise
            self.current_version = final_version
            self._apply_events_to_memory(events)
            return self._submission_response(session_id, events, batch_id=batch_id)

    def delete_part(self, session_id: str, seq: int) -> dict[str, Any]:
        seq = parse_int(seq, "seq", minimum=1)
        with self.lock:
            with self._write_tx() as conn:
                session = conn.execute(
                    "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
                ).fetchone()
                if session is None:
                    raise BusinessError(
                        "E_SESSION_NOT_FOUND",
                        "session does not exist",
                        details={"session_id": session_id},
                    )
                if session["status"] != "active":
                    raise BusinessError(
                        "E_SESSION_NOT_ACTIVE",
                        "parts can only be deleted from an active session",
                        details={"session_id": session_id, "status": session["status"]},
                    )
                existing = conn.execute(
                    "SELECT * FROM parts WHERE session_id = ? AND seq = ?", (session_id, seq)
                ).fetchone()
                if existing is None:
                    raise BusinessError(
                        "E_PART_MISSING",
                        "part does not exist",
                        details={"session_id": session_id, "seq": seq},
                    )
                version = self._next_version(conn)
                content = conn.execute(
                    "SELECT * FROM content_blobs WHERE sha256 = ?", (existing["sha256"],)
                ).fetchone()
                if content is None or int(content["ref_count"]) <= 0:
                    raise BusinessError(
                        "E_INTERNAL",
                        "content reference count is inconsistent",
                        http_status=500,
                        details={"sha256": str(existing["sha256"])},
                    )
                next_count = int(content["ref_count"]) - 1
                pending = next_count == 0
                if pending:
                    conn.execute(
                        "UPDATE content_blobs SET ref_count = 0, state = 'pending_gc', gc_version = ? "
                        "WHERE sha256 = ?",
                        (version, existing["sha256"]),
                    )
                else:
                    conn.execute(
                        "UPDATE content_blobs SET ref_count = ? WHERE sha256 = ?",
                        (next_count, existing["sha256"]),
                    )
                conn.execute(
                    "DELETE FROM parts WHERE session_id = ? AND seq = ?", (session_id, seq)
                )
                conn.execute(
                    "UPDATE sessions SET updated_version = ? WHERE session_id = ?",
                    (version, session_id),
                )
                event = self._new_event(
                    version,
                    "part_deleted",
                    session_id=session_id,
                    object_key=str(session["object_key"]),
                    payload={
                        "session_id": session_id,
                        "seq": seq,
                        "sha256": str(existing["sha256"]),
                        "size": int(existing["size"]),
                        "content_ref_count": next_count,
                        "pending_gc": pending,
                    },
                )
                self._persist_session_event(conn, event)

            self.current_version = version
            self._apply_events_to_memory([event])
            pending_hashes = [str(existing["sha256"])] if pending else []
            return {
                "session_id": session_id,
                "seq": seq,
                "sha256": str(existing["sha256"]),
                "deleted_version": version,
                "content_ref_count": next_count,
                "pending_gc": pending_hashes,
                "current_version": version,
            }

    def _submission_response(
        self, session_id: str, events: list[dict[str, Any]], *, batch_id: str | None
    ) -> dict[str, Any]:
        duplicate_count = sum(1 for event in events if event["operation"] == "part_duplicate")
        first_sha = None
        for event in events:
            if event["operation"] == "part_committed":
                first_sha = event["payload"]["part"]["sha256"]
                break
        return {
            "session_id": session_id,
            "batch_id": batch_id,
            "versions": [int(event["version"]) for event in events],
            "current_version": self.current_version,
            "committed": len(events) - duplicate_count,
            "duplicates": duplicate_count,
            "first_sha256": first_sha,
        }

    def _process_parts_tx(
        self,
        conn: sqlite3.Connection,
        session_id: str,
        parts: list[PartInput],
        *,
        batch_id: str | None,
    ) -> tuple[list[dict[str, Any]], int]:
        session = conn.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
        if session is None:
            raise BusinessError(
                "E_SESSION_NOT_FOUND", "session does not exist", details={"session_id": session_id}
            )
        if session["status"] != "active":
            raise BusinessError(
                "E_SESSION_NOT_ACTIVE",
                "parts can only be submitted to an active session",
                details={"session_id": session_id, "status": session["status"]},
            )
        lower_bound = int(session["lower_bound"])
        object_key = str(session["object_key"])
        events: list[dict[str, Any]] = []
        duplicate_commits = int(session["duplicate_commits"])
        last_version = self.current_version

        for part in parts:
            if part.seq < lower_bound:
                raise BusinessError(
                    "E_PART_OUT_OF_RANGE",
                    "part seq is below the session lower bound",
                    details={"seq": part.seq, "lower_bound": lower_bound},
                )
            existing = conn.execute(
                "SELECT * FROM parts WHERE session_id = ? AND seq = ?", (session_id, part.seq)
            ).fetchone()
            version = self._next_version(conn)
            last_version = version

            if existing is not None:
                if str(existing["sha256"]) != part.sha256:
                    raise BusinessError(
                        "E_PART_MISMATCH",
                        "the same seq was already committed with a different digest",
                        details={
                            "session_id": session_id,
                            "seq": part.seq,
                            "stored_sha256": str(existing["sha256"]),
                            "submitted_sha256": part.sha256,
                        },
                    )
                new_duplicate_count = int(existing["duplicate_count"]) + 1
                duplicate_commits += 1
                conn.execute(
                    "UPDATE parts SET duplicate_count = ? WHERE session_id = ? AND seq = ?",
                    (new_duplicate_count, session_id, part.seq),
                )
                conn.execute(
                    "UPDATE sessions SET duplicate_commits = ?, updated_version = ? WHERE session_id = ?",
                    (duplicate_commits, version, session_id),
                )
                event = self._new_event(
                    version,
                    "part_duplicate",
                    session_id=session_id,
                    object_key=object_key,
                    payload={
                        "session_id": session_id,
                        "seq": part.seq,
                        "sha256": part.sha256,
                        "duplicate_count": new_duplicate_count,
                        "session_duplicate_commits": duplicate_commits,
                        "batch_id": batch_id,
                    },
                )
            else:
                content = conn.execute(
                    "SELECT * FROM content_blobs WHERE sha256 = ?", (part.sha256,)
                ).fetchone()
                if content is not None:
                    if int(content["size"]) != part.size:
                        raise BusinessError(
                            "E_PART_MISMATCH",
                            "digest already exists with a different byte size",
                            details={
                                "sha256": part.sha256,
                                "stored_size": int(content["size"]),
                                "submitted_size": part.size,
                            },
                        )
                    conn.execute(
                        "UPDATE content_blobs SET ref_count = ref_count + 1, state = 'active', "
                        "gc_version = NULL WHERE sha256 = ?",
                        (part.sha256,),
                    )
                else:
                    conn.execute(
                        "INSERT INTO content_blobs(sha256, size, ref_count, state, first_version, gc_version) "
                        "VALUES (?, ?, 1, 'active', ?, NULL)",
                        (part.sha256, part.size, version),
                    )
                conn.execute(
                    "INSERT INTO parts(session_id, seq, size, sha256, arrival_ms, first_version, duplicate_count) "
                    "VALUES (?, ?, ?, ?, ?, ?, 0)",
                    (session_id, part.seq, part.size, part.sha256, part.arrival_ms, version),
                )
                conn.execute(
                    "UPDATE sessions SET updated_version = ? WHERE session_id = ?",
                    (version, session_id),
                )
                event = self._new_event(
                    version,
                    "part_committed",
                    session_id=session_id,
                    object_key=object_key,
                    payload={
                        "session_id": session_id,
                        "part": part.as_dict(),
                        "batch_id": batch_id,
                    },
                )
            self._persist_session_event(conn, event)
            events.append(event)

        return events, last_version

    # ------------------------------------------------------------------ cancel / merge / gc

    def cancel_session(self, session_id: str) -> dict[str, Any]:
        with self.lock:
            with self._write_tx() as conn:
                session = conn.execute(
                    "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
                ).fetchone()
                if session is None:
                    raise BusinessError(
                        "E_SESSION_NOT_FOUND",
                        "session does not exist",
                        details={"session_id": session_id},
                    )
                if session["status"] == "cancelled":
                    return {
                        "session_id": session_id,
                        "status": "cancelled",
                        "already_cancelled": True,
                        "current_version": self.current_version,
                        "pending_gc": [],
                    }
                if session["status"] != "active":
                    raise BusinessError(
                        "E_SESSION_NOT_ACTIVE",
                        "only an active session can be cancelled",
                        details={"session_id": session_id, "status": session["status"]},
                    )

                parts = list(
                    conn.execute(
                        "SELECT * FROM parts WHERE session_id = ? ORDER BY seq", (session_id,)
                    )
                )
                version = self._next_version(conn)
                pending_gc: list[str] = []
                for part in parts:
                    content = conn.execute(
                        "SELECT * FROM content_blobs WHERE sha256 = ?", (part["sha256"],)
                    ).fetchone()
                    if content is None or int(content["ref_count"]) <= 0:
                        raise BusinessError(
                            "E_INTERNAL",
                            "content reference count is inconsistent",
                            http_status=500,
                            details={"sha256": part["sha256"]},
                        )
                    next_count = int(content["ref_count"]) - 1
                    if next_count == 0:
                        conn.execute(
                            "UPDATE content_blobs SET ref_count = 0, state = 'pending_gc', gc_version = ? "
                            "WHERE sha256 = ?",
                            (version, part["sha256"]),
                        )
                        pending_gc.append(str(part["sha256"]))
                    else:
                        conn.execute(
                            "UPDATE content_blobs SET ref_count = ? WHERE sha256 = ?",
                            (next_count, part["sha256"]),
                        )
                conn.execute("DELETE FROM parts WHERE session_id = ?", (session_id,))
                conn.execute(
                    "UPDATE sessions SET status = 'cancelled', cancelled_version = ?, updated_version = ? "
                    "WHERE session_id = ?",
                    (version, version, session_id),
                )
                decremented = [
                    {"seq": int(part["seq"]), "sha256": str(part["sha256"]), "size": int(part["size"])}
                    for part in parts
                ]
                event = self._new_event(
                    version,
                    "session_cancelled",
                    session_id=session_id,
                    object_key=str(session["object_key"]),
                    payload={
                        "session_id": session_id,
                        "parts": decremented,
                        "pending_gc": sorted(set(pending_gc)),
                    },
                )
                self._persist_session_event(conn, event)

            self.current_version = version
            self._apply_events_to_memory([event])
            return {
                "session_id": session_id,
                "status": "cancelled",
                "cancelled_version": version,
                "removed_parts": len(parts),
                "pending_gc": sorted(set(pending_gc)),
                "current_version": version,
            }

    def merge_session(self, session_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        generation = parse_int(payload.get("generation"), "generation", minimum=1)
        declared_total_size = payload.get("total_size")
        total_size = (
            None
            if declared_total_size is None
            else parse_int(declared_total_size, "total_size", minimum=0)
        )
        manifest = parse_manifest(payload.get("parts"))
        if manifest is not None:
            validate_unique_indexes(manifest)

        with self.lock:
            with self._write_tx() as conn:
                session = conn.execute(
                    "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
                ).fetchone()
                if session is None:
                    raise BusinessError(
                        "E_SESSION_NOT_FOUND",
                        "session does not exist",
                        details={"session_id": session_id},
                    )
                current_generation = int(
                    conn.execute(
                        "SELECT COALESCE(MAX(generation), 0) FROM sessions WHERE object_key = ?",
                        (session["object_key"],),
                    ).fetchone()[0]
                )
                if generation != int(session["generation"]) or generation != current_generation:
                    raise BusinessError(
                        "E_SESSION_STALE",
                        "merge generation does not match the current object-key generation",
                        details={
                            "requested_generation": generation,
                            "session_generation": int(session["generation"]),
                            "current_generation": current_generation,
                        },
                    )
                object_exists = conn.execute(
                    "SELECT 1 FROM objects WHERE object_key = ?", (session["object_key"],)
                ).fetchone()
                if object_exists is not None:
                    raise BusinessError(
                        "E_OBJECT_EXISTS",
                        "the object key has already been merged once",
                        details={"object_key": str(session["object_key"])},
                    )
                if session["status"] != "active":
                    raise BusinessError(
                        "E_SESSION_NOT_ACTIVE",
                        "only an active session can be merged",
                        details={"session_id": session_id, "status": session["status"]},
                    )

                parts = list(
                    conn.execute(
                        "SELECT * FROM parts WHERE session_id = ? ORDER BY seq", (session_id,)
                    )
                )
                if not parts:
                    raise BusinessError(
                        "E_PART_MISSING",
                        "session has no committed parts",
                        details={"session_id": session_id, "missing": []},
                    )
                lower_bound = int(session["lower_bound"])
                expected = list(range(lower_bound, int(parts[-1]["seq"]) + 1))
                actual = [int(part["seq"]) for part in parts]
                missing = [seq for seq in expected if seq not in set(actual)]
                if missing:
                    raise BusinessError(
                        "E_PART_MISSING",
                        "part sequence has gaps and cannot be merged",
                        details={"missing": missing, "lower_bound": lower_bound},
                    )

                if manifest is not None:
                    manifest_by_seq = {part.seq: part for part in manifest}
                    missing_manifest = [seq for seq in actual if seq not in manifest_by_seq]
                    unexpected_manifest = [seq for seq in manifest_by_seq if seq not in set(actual)]
                    if missing_manifest or unexpected_manifest or len(manifest) != len(parts):
                        raise BusinessError(
                            "E_PART_MISSING",
                            "merge manifest does not exactly match stored parts",
                            details={
                                "missing": missing_manifest,
                                "unexpected": unexpected_manifest,
                                "manifest_count": len(manifest),
                                "stored_count": len(parts),
                            },
                        )
                    for stored in parts:
                        wanted = manifest_by_seq[int(stored["seq"])]
                        if wanted.sha256 != str(stored["sha256"]):
                            raise BusinessError(
                                "E_PART_MISMATCH",
                                "merge manifest digest does not match the stored part",
                                details={
                                    "seq": int(stored["seq"]),
                                    "stored_sha256": str(stored["sha256"]),
                                    "manifest_sha256": wanted.sha256,
                                },
                            )
                        if wanted.size != int(stored["size"]):
                            raise BusinessError(
                                "E_PART_MISMATCH",
                                "merge manifest size does not match the stored part",
                                details={
                                    "seq": int(stored["seq"]),
                                    "stored_size": int(stored["size"]),
                                    "manifest_size": wanted.size,
                                },
                            )

                stored_total = sum(int(part["size"]) for part in parts)
                if stored_total > MAX_I64:
                    raise BusinessError(
                        "E_PART_INVALID", "object total size overflows signed 64-bit range"
                    )
                if total_size is not None and stored_total != total_size:
                    raise BusinessError(
                        "E_SIZE_MISMATCH",
                        "declared total_size does not equal the sum of stored parts",
                        details={"declared_total_size": total_size, "stored_total_size": stored_total},
                    )

                object_sha256 = _object_digest(parts)
                version = self._next_version(conn)
                obj = {
                    "object_key": str(session["object_key"]),
                    "total_size": stored_total,
                    "part_count": len(parts),
                    "object_sha256": object_sha256,
                    "generation": generation,
                    "session_id": session_id,
                    "merged_version": version,
                }
                conn.execute(
                    "INSERT INTO objects(object_key, total_size, part_count, object_sha256, generation, "
                    "session_id, merged_version) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        obj["object_key"],
                        obj["total_size"],
                        obj["part_count"],
                        obj["object_sha256"],
                        obj["generation"],
                        obj["session_id"],
                        obj["merged_version"],
                    ),
                )
                conn.execute(
                    "UPDATE sessions SET status = 'completed', completed_version = ?, updated_version = ? "
                    "WHERE session_id = ?",
                    (version, version, session_id),
                )
                event = self._new_event(
                    version,
                    "session_merged",
                    session_id=session_id,
                    object_key=obj["object_key"],
                    payload={"session_id": session_id, "object": obj},
                )
                self._persist_session_event(conn, event)

            self.current_version = version
            self._apply_events_to_memory([event])
            return {**obj, "merged": True}

    def purge_pending_content(self) -> dict[str, Any]:
        with self.lock:
            with self._write_tx() as conn:
                rows = list(
                    conn.execute(
                        "SELECT * FROM content_blobs WHERE state = 'pending_gc' AND ref_count = 0 "
                        "ORDER BY sha256"
                    )
                )
                if not rows:
                    return {
                        "purged": [],
                        "purged_count": 0,
                        "current_version": self.current_version,
                    }
                version = self._next_version(conn)
                hashes = [str(row["sha256"]) for row in rows]
                conn.execute(
                    "DELETE FROM content_blobs WHERE state = 'pending_gc' AND ref_count = 0"
                )
                event = self._new_event(
                    version,
                    "content_purged",
                    session_id=None,
                    object_key=None,
                    payload={
                        "sha256": hashes,
                        "records": [
                            {"sha256": str(row["sha256"]), "size": int(row["size"])} for row in rows
                        ],
                    },
                )
                self._persist_global_event(conn, event)

            self.current_version = version
            self._apply_events_to_memory([event])
            return {
                "purged": hashes,
                "purged_count": len(hashes),
                "version": version,
                "current_version": version,
            }

    # ------------------------------------------------------------------ versioned reads

    def _validate_version(self, version: int) -> None:
        if version < self.retention_floor:
            raise BusinessError(
                "E_VERSION_RETIRED",
                "the requested index version has been retired",
                details={"version": version, "retention_floor": self.retention_floor},
            )
        if version > self.current_version:
            raise BusinessError(
                "E_VERSION_NOT_FOUND",
                "the requested index version is newer than the current version",
                details={"version": version, "current_version": self.current_version},
            )

    def _replay_state(self, target_version: int) -> IndexState:
        self._validate_version(target_version)
        checkpoint = self.conn.execute(
            "SELECT version, state_json FROM version_checkpoints WHERE version <= ? "
            "ORDER BY version DESC LIMIT 1",
            (target_version,),
        ).fetchone()
        if checkpoint is None:
            state = IndexState()
            base_version = 0
        else:
            state = IndexState.from_dict(json.loads(checkpoint["state_json"]))
            base_version = int(checkpoint["version"])
        if target_version == base_version:
            return state

        events: list[dict[str, Any]] = []
        for segment in self.conn.execute(
            "SELECT events_json FROM index_segments WHERE status = 'active' ORDER BY start_version, id"
        ):
            for event in json.loads(segment["events_json"]):
                version = int(event["version"])
                if base_version < version <= target_version:
                    events.append(event)
        for row in self.conn.execute(
            "SELECT * FROM version_events WHERE operation = 'content_purged' "
            "AND version > ? AND version <= ? ORDER BY version",
            (base_version, target_version),
        ):
            events.append(
                {
                    "version": int(row["version"]),
                    "operation": str(row["operation"]),
                    "session_id": row["session_id"],
                    "object_key": row["object_key"],
                    "created_ms": int(row["created_ms"]),
                    "payload": json.loads(row["payload_json"]),
                }
            )
        events.sort(key=lambda event: int(event["version"]))
        for event in events:
            state.apply_event(event)
        return state

    def get_session_view(self, session_id: str, version: int | None = None) -> dict[str, Any]:
        with self.lock:
            target = self.current_version if version is None else parse_version(version)
            state = self._replay_state(target)
            session = state.sessions.get(session_id)
            if session is None:
                raise BusinessError(
                    "E_SESSION_NOT_FOUND",
                    "session does not exist at the requested version",
                    details={"session_id": session_id, "version": target},
                )
            parts = [session.parts[seq].to_dict() for seq in sorted(session.parts)]
            session_hashes = sorted({part["sha256"] for part in parts})
            return {
                "version": target,
                "session": {
                    "session_id": session.session_id,
                    "object_key": session.object_key,
                    "generation": session.generation,
                    "lower_bound": session.lower_bound,
                    "status": session.status,
                    "conflict_policy": session.conflict_policy,
                    "created_version": session.created_version,
                    "updated_version": session.updated_version,
                    "completed_version": session.completed_version,
                    "cancelled_version": session.cancelled_version,
                    "duplicate_commits": session.duplicate_commits,
                },
                "parts": parts,
                "index": {
                    "contiguous_ranges": [list(interval) for interval in session.intervals],
                    "interval_count": len(session.intervals),
                    "missing_ranges": self._missing_ranges(session),
                    "has_gaps": bool(self._missing_ranges(session)),
                    "dedup_refs": [
                        state.dedup[sha].to_dict() for sha in session_hashes if sha in state.dedup
                    ],
                    "generation_index": state.generation_index(session.object_key),
                    "object": state.objects.get(session.object_key),
                    "active_content_records": sum(
                        1 for content in state.dedup.values() if content.state == "active"
                    ),
                    "pending_gc_records": sum(
                        1 for content in state.dedup.values() if content.state == "pending_gc"
                    ),
                    "removed_content_records": len(state.removed),
                },
            }

    @staticmethod
    def _missing_ranges(session: SessionState) -> list[list[int]]:
        if not session.parts:
            return []
        maximum = max(session.parts)
        missing: list[list[int]] = []
        cursor = session.lower_bound
        for start, end in session.intervals:
            if end < cursor:
                continue
            if start > cursor:
                missing.append([cursor, start - 1])
            cursor = max(cursor, end + 1)
        if cursor <= maximum:
            missing.append([cursor, maximum])
        return missing

    def get_object_view(self, object_key: str, version: int | None = None) -> dict[str, Any]:
        with self.lock:
            target = self.current_version if version is None else parse_version(version)
            state = self._replay_state(target)
            generation = state.generation_index(object_key)
            obj = state.objects.get(object_key)
            if not generation["sessions"] and obj is None:
                raise BusinessError(
                    "E_OBJECT_NOT_FOUND",
                    "no session or object exists for the object key at the requested version",
                    details={"object_key": object_key, "version": target},
                )
            return {
                "version": target,
                "object_key": object_key,
                "object": obj,
                "generation_index": generation,
            }

    def get_content_view(self, sha256: str, version: int | None = None) -> dict[str, Any]:
        with self.lock:
            normalized = sha256.lower()
            if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
                raise BusinessError(
                    "E_REQUEST_INVALID", "sha256 must be exactly 64 hexadecimal characters"
                )
            target = self.current_version if version is None else parse_version(version)
            state = self._replay_state(target)
            content = state.dedup.get(normalized) or state.removed.get(normalized)
            if content is None:
                raise BusinessError(
                    "E_CONTENT_NOT_FOUND",
                    "content digest does not exist at the requested version",
                    details={"sha256": normalized, "version": target},
                )
            physical = self.conn.execute(
                "SELECT 1 FROM content_blobs WHERE sha256 = ?", (normalized,)
            ).fetchone()
            return {
                "version": target,
                "content": content.to_dict(),
                "physical_record_exists_now": physical is not None,
            }

    def get_current_version(self) -> dict[str, Any]:
        with self.lock:
            return {
                "current_version": self.current_version,
                "retention_floor": self.retention_floor,
            }

    def get_version_trace(self, *, since: int = 0, limit: int = 200) -> dict[str, Any]:
        with self.lock:
            limit = max(1, min(int(limit), 2000))
            rows = list(
                self.conn.execute(
                    "SELECT * FROM version_events WHERE version >= ? ORDER BY version LIMIT ?",
                    (since, limit),
                )
            )
            events = [
                {
                    "version": int(row["version"]),
                    "operation": str(row["operation"]),
                    "session_id": row["session_id"],
                    "object_key": row["object_key"],
                    "created_ms": int(row["created_ms"]),
                    "payload": json.loads(row["payload_json"]),
                }
                for row in rows
            ]
            return {
                "current_version": self.current_version,
                "retention_floor": self.retention_floor,
                "events": events,
            }

    # ------------------------------------------------------------------ leases and retirement

    def acquire_query_lease(self, version: int, ttl_ms: int = 300_000) -> dict[str, Any]:
        with self.lock:
            self._prune_expired_leases()
            parse_version(version)
            self._validate_version(version)
            ttl_ms = max(1000, min(int(ttl_ms), 86_400_000))
            lease_id = str(uuid.uuid4())
            expires_ms = _now_ms() + ttl_ms
            self.query_leases[lease_id] = {"version": version, "expires_ms": expires_ms}
            return {"lease_id": lease_id, "version": version, "expires_ms": expires_ms}

    def release_query_lease(self, lease_id: str) -> dict[str, Any]:
        with self.lock:
            self._prune_expired_leases()
            lease = self.query_leases.pop(lease_id, None)
            if lease is None:
                raise BusinessError(
                    "E_LEASE_NOT_FOUND",
                    "active query lease does not exist or has expired",
                    details={"lease_id": lease_id},
                )
            return {"lease_id": lease_id, "released": True, "version": lease["version"]}

    def _prune_expired_leases(self) -> None:
        now = _now_ms()
        expired = [lease_id for lease_id, lease in self.query_leases.items() if lease["expires_ms"] <= now]
        for lease_id in expired:
            self.query_leases.pop(lease_id, None)

    def _min_active_query_version(self) -> int | None:
        self._prune_expired_leases()
        if not self.query_leases:
            return None
        return min(int(lease["version"]) for lease in self.query_leases.values())

    def retire_versions(self, *, min_active_version: int | None = None) -> dict[str, Any]:
        with self.lock:
            if min_active_version is not None:
                candidate = parse_version(min_active_version)
            else:
                candidate = self.current_version
            active_min = self._min_active_query_version()
            if active_min is not None:
                candidate = min(candidate, active_min)
            if candidate <= self.retention_floor:
                return {
                    "retired": False,
                    "retention_floor": self.retention_floor,
                    "current_version": self.current_version,
                    "active_query_min_version": active_min,
                }
            if candidate > self.current_version:
                raise BusinessError(
                    "E_VERSION_NOT_FOUND",
                    "cannot retain from a version newer than the current version",
                    details={"requested_floor": candidate, "current_version": self.current_version},
                )
            checkpoint_version = candidate - 1
            checkpoint_state = self._replay_state(checkpoint_version)
            checkpoint_json = _json(checkpoint_state.to_dict())
            now = _now_ms()
            with self._write_tx() as conn:
                purged_events = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM version_events WHERE version < ?", (candidate,)
                    ).fetchone()[0]
                )
                conn.execute("DELETE FROM version_events WHERE version < ?", (candidate,))
                conn.execute("DELETE FROM version_checkpoints")
                conn.execute(
                    "INSERT INTO version_checkpoints(version, created_ms, state_json) VALUES (?, ?, ?)",
                    (checkpoint_version, now, checkpoint_json),
                )

                trimmed_segments = 0
                removed_segments = 0
                for segment in list(conn.execute("SELECT * FROM index_segments WHERE status = 'active'")):
                    events = json.loads(segment["events_json"])
                    retained = [event for event in events if int(event["version"]) >= candidate]
                    if len(retained) == len(events):
                        continue
                    trimmed_segments += 1
                    if not retained:
                        conn.execute("DELETE FROM index_segments WHERE id = ?", (segment["id"],))
                        removed_segments += 1
                    else:
                        retained.sort(key=lambda event: int(event["version"]))
                        conn.execute(
                            "UPDATE index_segments SET start_version = ?, end_version = ?, events_json = ? "
                            "WHERE id = ?",
                            (
                                int(retained[0]["version"]),
                                int(retained[-1]["version"]),
                                _json(retained),
                                segment["id"],
                            ),
                        )
                self._meta_set("retention_floor", str(candidate))
            self.retention_floor = candidate
            return {
                "retired": True,
                "retention_floor": candidate,
                "checkpoint_version": checkpoint_version,
                "purged_version_events": purged_events,
                "trimmed_segments": trimmed_segments,
                "removed_empty_segments": removed_segments,
                "active_query_min_version": active_min,
                "current_version": self.current_version,
                "note": "versions below retention_floor now return E_VERSION_RETIRED",
            }

    # ------------------------------------------------------------------ index segments

    def get_session_segments(self, session_id: str) -> dict[str, Any]:
        with self.lock:
            rows = list(
                self.conn.execute(
                    "SELECT id, start_version, end_version, events_json, status FROM index_segments "
                    "WHERE session_id = ? ORDER BY start_version, id",
                    (session_id,),
                )
            )
            segments = []
            for row in rows:
                events = json.loads(row["events_json"])
                segments.append(
                    {
                        "segment_id": int(row["id"]),
                        "start_version": int(row["start_version"]),
                        "end_version": int(row["end_version"]),
                        "event_count": len(events),
                        "status": str(row["status"]),
                    }
                )
            return {
                "session_id": session_id,
                "segment_count": len(segments),
                "segments": segments,
            }

    def compact_session(self, session_id: str) -> dict[str, Any]:
        with self.lock:
            rows = list(
                self.conn.execute(
                    "SELECT * FROM index_segments WHERE session_id = ? AND status = 'active' "
                    "ORDER BY start_version, id",
                    (session_id,),
                )
            )
            if len(rows) < 2:
                return {
                    "session_id": session_id,
                    "compacted": False,
                    "reason": "fewer than two active index segments",
                    "segment_count_before": len(rows),
                    "segment_count_after": len(rows),
                }

            segment_ids = [int(row["id"]) for row in rows]
            events: list[dict[str, Any]] = []
            for row in rows:
                events.extend(json.loads(row["events_json"]))
            events.sort(key=lambda event: int(event["version"]))
            start_version = min(int(row["start_version"]) for row in rows)
            end_version = max(int(row["end_version"]) for row in rows)
            intent_id = self._create_intent(
                kind="compact",
                session_id=session_id,
                object_key=None,
                details={
                    "segment_ids": segment_ids,
                    "start_version": start_version,
                    "end_version": end_version,
                    "event_count": len(events),
                },
            )
            try:
                with self._write_tx() as conn:
                    refreshed = list(
                        conn.execute(
                            "SELECT id FROM index_segments WHERE session_id = ? AND status = 'active' "
                            "AND id IN ({}) ORDER BY id".format(",".join("?" for _ in segment_ids)),
                            (session_id, *segment_ids),
                        )
                    )
                    if len(refreshed) != len(segment_ids):
                        raise BusinessError(
                            "E_INTERNAL",
                            "index segments changed before compaction",
                            http_status=500,
                        )
                    cursor = conn.execute(
                        "INSERT INTO index_segments(session_id, start_version, end_version, events_json, "
                        "status, merge_intent_id, created_ms) VALUES (?, ?, ?, ?, 'active', ?, ?)",
                        (session_id, start_version, end_version, _json(events), intent_id, _now_ms()),
                    )
                    merged_segment_id = int(cursor.lastrowid)
                    conn.executemany(
                        "DELETE FROM index_segments WHERE id = ?", [(sid,) for sid in segment_ids]
                    )
                    self._finish_intent_tx(
                        conn,
                        intent_id,
                        {"decision": "compacted", "merged_segment_id": merged_segment_id},
                    )
            except BaseException:
                self._abort_intent(intent_id, "compaction_transaction_rolled_back")
                raise
            return {
                "session_id": session_id,
                "compacted": True,
                "segment_count_before": len(rows),
                "segment_count_after": len(rows) - len(segment_ids) + 1,
                "merged_segment_id": merged_segment_id,
                "event_count": len(events),
                "start_version": start_version,
                "end_version": end_version,
                "intent_id": intent_id,
            }

    def compact_all(self) -> dict[str, Any]:
        with self.lock:
            session_ids = [
                str(row["session_id"])
                for row in self.conn.execute(
                    "SELECT DISTINCT session_id FROM index_segments WHERE status = 'active' "
                    "ORDER BY session_id"
                )
            ]
            results = [self.compact_session(session_id) for session_id in session_ids]
            return {
                "compacted_sessions": sum(1 for result in results if result.get("compacted")),
                "results": results,
            }

    # ------------------------------------------------------------------ intents / crash drill

    def _create_intent(
        self,
        *,
        kind: str,
        session_id: str | None,
        object_key: str | None,
        details: dict[str, Any],
    ) -> str:
        intent_id = str(uuid.uuid4())
        now = _now_ms()
        with self._write_tx() as conn:
            conn.execute(
                "INSERT INTO recovery_intents(intent_id, kind, status, session_id, object_key, "
                "details_json, created_ms, updated_ms, result_json) VALUES (?, ?, 'pending', ?, ?, ?, ?, ?, NULL)",
                (intent_id, kind, session_id, object_key, _json(details), now, now),
            )
        return intent_id

    def _finish_intent_tx(
        self, conn: sqlite3.Connection, intent_id: str, result: dict[str, Any]
    ) -> None:
        conn.execute(
            "UPDATE recovery_intents SET status = 'complete', updated_ms = ?, result_json = ? "
            "WHERE intent_id = ? AND status = 'pending'",
            (_now_ms(), _json(result), intent_id),
        )

    def _abort_intent(self, intent_id: str, reason: str) -> None:
        with self._write_tx() as conn:
            conn.execute(
                "UPDATE recovery_intents SET status = 'aborted', updated_ms = ?, result_json = ? "
                "WHERE intent_id = ? AND status = 'pending'",
                (_now_ms(), _json({"decision": reason}), intent_id),
            )

    @staticmethod
    def _force_kill_process() -> None:
        if os.name == "nt":
            os._exit(137)
        os.kill(os.getpid(), signal.SIGKILL)

    def crash_drill(self, payload: dict[str, Any]) -> None:
        if not self.enable_crash_drill:
            raise BusinessError(
                "E_CRASH_DRILL_DISABLED",
                "crash drill endpoint is disabled; set UPLOAD_INDEX_ENABLE_CRASH_DRILL=1",
            )
        kind = payload.get("kind")
        crash_point = payload.get("crash_point", "after_partial_writes")
        if crash_point not in {"after_intent", "after_partial_writes"}:
            raise BusinessError("E_REQUEST_INVALID", "crash_point must be after_intent or after_partial_writes")

        if kind == "batch":
            session_id = parse_session_id(payload.get("session_id"), required=True)
            assert session_id is not None
            parts = parse_parts(payload.get("parts"))
            validate_unique_indexes(parts)
            batch_id = str(payload.get("batch_id") or f"crash-{uuid.uuid4()}")
            with self.lock:
                intent_id = self._create_intent(
                    kind="batch",
                    session_id=session_id,
                    object_key=None,
                    details={
                        "batch_id": batch_id,
                        "part_count": len(parts),
                        "part_indexes": [part.seq for part in parts],
                        "crash_point": crash_point,
                        "drill": True,
                    },
                )
                if crash_point == "after_intent":
                    self._force_kill_process()
                    return
                self.conn.execute("BEGIN IMMEDIATE")
                self._process_parts_tx(self.conn, session_id, parts, batch_id=batch_id)
                self._force_kill_process()
            return

        if kind == "compact":
            session_id = parse_session_id(payload.get("session_id"), required=True)
            assert session_id is not None
            with self.lock:
                rows = list(
                    self.conn.execute(
                        "SELECT * FROM index_segments WHERE session_id = ? AND status = 'active' "
                        "ORDER BY start_version, id",
                        (session_id,),
                    )
                )
                if len(rows) < 2:
                    raise BusinessError(
                        "E_REQUEST_INVALID",
                        "crash drill compaction needs at least two active index segments",
                        details={"session_id": session_id, "segment_count": len(rows)},
                    )
                segment_ids = [int(row["id"]) for row in rows]
                events: list[dict[str, Any]] = []
                for row in rows:
                    events.extend(json.loads(row["events_json"]))
                events.sort(key=lambda event: int(event["version"]))
                intent_id = self._create_intent(
                    kind="compact",
                    session_id=session_id,
                    object_key=None,
                    details={
                        "segment_ids": segment_ids,
                        "event_count": len(events),
                        "crash_point": crash_point,
                        "drill": True,
                    },
                )
                if crash_point == "after_intent":
                    self._force_kill_process()
                    return
                self.conn.execute("BEGIN IMMEDIATE")
                self.conn.execute(
                    "INSERT INTO index_segments(session_id, start_version, end_version, events_json, "
                    "status, merge_intent_id, created_ms) VALUES (?, ?, ?, ?, 'staging', ?, ?)",
                    (
                        session_id,
                        min(int(row["start_version"]) for row in rows),
                        max(int(row["end_version"]) for row in rows),
                        _json(events),
                        intent_id,
                        _now_ms(),
                    ),
                )
                self.conn.executemany(
                    "DELETE FROM index_segments WHERE id = ?", [(sid,) for sid in segment_ids]
                )
                self._force_kill_process()
            return

        raise BusinessError("E_REQUEST_INVALID", "kind must be batch or compact")

    # ------------------------------------------------------------------ diagnostics

    def current_snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "current_version": self.current_version,
                "retention_floor": self.retention_floor,
                "sessions": {
                    sid: session.to_dict() for sid, session in self.state.sessions.items()
                },
                "dedup": {
                    sha: content.to_dict() for sha, content in self.state.dedup.items()
                },
                "removed": {
                    sha: content.to_dict() for sha, content in self.state.removed.items()
                },
                "objects": self.state.objects,
            }

