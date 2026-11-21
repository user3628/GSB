"""SQLite persistence with write-ahead intent logging and crash recovery."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone

from .errors import Codes, WorkbenchError

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    chain_id TEXT NOT NULL,
    params_json TEXT NOT NULL,
    current_stage TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS stage_states (
    session_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    version INTEGER NOT NULL,
    status TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (session_id, stage)
);
CREATE TABLE IF NOT EXISTS diagnostics (
    session_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS intent_log (
    intent_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    intent_type TEXT NOT NULL,
    target_stage TEXT,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
);
"""


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


class Store:
    def __init__(self, db_path: str):
        self.db_path = db_path
        directory = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(directory, exist_ok=True)
        self._writer_lock = threading.RLock()
        self._connect()

    def _connect(self) -> None:
        self.conn = sqlite3.connect(
            self.db_path, check_same_thread=False, isolation_level=None
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(SCHEMA)

    @staticmethod
    def open_reader(db_path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        return conn

    def _write_intent(self, session_id: str, intent_type: str,
                      target_stage: str | None, payload: dict) -> str:
        intent_id = str(uuid.uuid4())
        now = _now_ms()
        self.conn.execute(
            "INSERT INTO intent_log (intent_id, session_id, intent_type, "
            "target_stage, payload_json, status, created_at_ms) "
            "VALUES (?, ?, ?, ?, ?, 'PENDING', ?)",
            (
                intent_id,
                session_id,
                intent_type,
                target_stage,
                json.dumps(payload, sort_keys=True),
                now,
            ),
        )
        return intent_id

    def _complete_intent(self, intent_id: str) -> None:
        self.conn.execute(
            "UPDATE intent_log SET status='COMPLETED', completed_at_ms=? "
            "WHERE intent_id=?",
            (_now_ms(), intent_id),
        )

    def create_session(self, session_id: str, chain_id: str,
                       params: dict, stage: str = "INIT") -> None:
        with self._writer_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                intent_id = self._write_intent(
                    session_id, "CREATE_SESSION", stage,
                    {"chain_id": chain_id, "params": params},
                )
                now = _now_ms()
                existing = self.conn.execute(
                    "SELECT params_json, chain_id FROM sessions WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                if existing is not None:
                    self.conn.execute("ROLLBACK")
                    raise WorkbenchError(
                        Codes.CONFLICTING_SESSION,
                        f"session {session_id} already exists",
                        {"session_id": session_id},
                    )
                self.conn.execute(
                    "INSERT INTO sessions (session_id, chain_id, params_json, "
                    "current_stage, status, version, created_at_ms, updated_at_ms) "
                    "VALUES (?, ?, ?, ?, 'ACTIVE', 0, ?, ?)",
                    (
                        session_id,
                        chain_id,
                        json.dumps(params, sort_keys=True),
                        stage,
                        now,
                        now,
                    ),
                )
                self._complete_intent(intent_id)
                self.conn.execute("COMMIT")
            except WorkbenchError:
                raise
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def append_stage(self, session_id: str, stage: str, status: str,
                     detail: dict) -> str:
        """Persist one complete stage boundary under a single committed tx.

        The PENDING intent is written and committed first, then the stage
        mutation runs in its own transaction. A forced termination between
        the two leaves a recoverable PENDING intent with no partial stage.
        """
        with self._writer_lock:
            intent_id = self._write_intent(
                session_id, "ADVANCE_STAGE", stage, detail
            )
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                row = self.conn.execute(
                    "SELECT current_stage, version, status FROM sessions "
                    "WHERE session_id=?",
                    (session_id,),
                ).fetchone()
                if row is None:
                    self.conn.execute("ROLLBACK")
                    raise WorkbenchError(
                        Codes.SAMPLE_NOT_FOUND,
                        f"unknown session {session_id}",
                    )
                now = _now_ms()
                self.conn.execute(
                    "INSERT INTO stage_states (session_id, stage, version, "
                    "status, detail_json, created_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        session_id,
                        stage,
                        row["version"] + 1,
                        status,
                        json.dumps(detail, sort_keys=True),
                        now,
                    ),
                )
                self.conn.execute(
                    "UPDATE sessions SET current_stage=?, version=version+1, "
                    "updated_at_ms=? WHERE session_id=?",
                    (stage, now, session_id),
                )
                self._complete_intent(intent_id)
                self.conn.execute("COMMIT")
                return intent_id
            except WorkbenchError:
                raise
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def save_diagnostic(self, session_id: str, version: int, payload: dict) -> None:
        with self._writer_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                intent_id = self._write_intent(
                    session_id, "SAVE_DIAGNOSTIC", None, payload
                )
                self.conn.execute(
                    "INSERT INTO diagnostics (session_id, version, payload_json, "
                    "updated_at_ms) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(session_id) DO UPDATE SET version=excluded.version, "
                    "payload_json=excluded.payload_json, updated_at_ms=excluded.updated_at_ms",
                    (
                        session_id,
                        version,
                        json.dumps(payload, sort_keys=True, default=str),
                        _now_ms(),
                    ),
                )
                self._complete_intent(intent_id)
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def mark_status(self, session_id: str, status: str) -> None:
        with self._writer_lock:
            self.conn.execute(
                "UPDATE sessions SET status=?, updated_at_ms=? WHERE session_id=?",
                (status, _now_ms(), session_id),
            )

    def get_session(self, session_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_stages(self, session_id: str) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM stage_states WHERE session_id=? ORDER BY version",
            (session_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_diagnostic(self, session_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM diagnostics WHERE session_id=?", (session_id,)
        ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["payload"] = json.loads(payload.pop("payload_json"))
        return payload

    def get_pending_intents(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM intent_log WHERE status='PENDING' ORDER BY created_at_ms"
        ).fetchall()
        return [dict(row) for row in rows]

    def recover_pending(self) -> dict:
        """Scan PENDING intents after a forced termination.

        A PENDING ADVANCE_STAGE whose stage never reached stage_states means
        the process died before the boundary commit: roll the session back to
        its last complete stage. A PENDING create/diagnostic intent aborts the
        session. Only already-persisted facts are used in the report.
        """
        pending = self.get_pending_intents()
        report = {"recovered_at_ms": _now_ms(), "actions": []}
        for intent in pending:
            session_id = intent["session_id"]
            session = self.get_session(session_id)
            action = {
                "intent_id": intent["intent_id"],
                "intent_type": intent["intent_type"],
                "session_id": session_id,
            }
            if session is None:
                self._abort_intent(intent["intent_id"], session_id, action, report)
                continue
            if intent["intent_type"] == "ADVANCE_STAGE":
                stage_row = self.conn.execute(
                    "SELECT 1 FROM stage_states WHERE session_id=? AND stage=?",
                    (session_id, intent["target_stage"]),
                ).fetchone()
                if stage_row is None:
                    stages = self.get_stages(session_id)
                    last_stage = stages[-1]["stage"] if stages else "INIT"
                    self.conn.execute(
                        "UPDATE sessions SET status='ABORTED', current_stage=?, "
                        "updated_at_ms=? WHERE session_id=?",
                        (last_stage, _now_ms(), session_id),
                    )
                    self.conn.execute(
                        "UPDATE intent_log SET status='ABORTED', completed_at_ms=? "
                        "WHERE intent_id=?",
                        (_now_ms(), intent["intent_id"]),
                    )
                    action.update(
                        decision="ABORTED_AT_LAST_COMPLETE_BOUNDARY",
                        last_complete_stage=last_stage,
                    )
                else:
                    self._complete_intent(intent["intent_id"])
                    action.update(decision="INTENT_ALREADY_MATERIALIZED")
            else:
                self._abort_intent(intent["intent_id"], session_id, action, report)
            report["actions"].append(action)
        return report

    def _abort_intent(self, intent_id: str, session_id: str,
                      action: dict, report: dict) -> None:
        self.conn.execute(
            "UPDATE sessions SET status='ABORTED', updated_at_ms=? "
            "WHERE session_id=?",
            (_now_ms(), session_id),
        )
        self.conn.execute(
            "UPDATE intent_log SET status='ABORTED', completed_at_ms=? "
            "WHERE intent_id=?",
            (_now_ms(), intent_id),
        )
        action.update(decision="ABORTED_UNFINISHED_INTENT")
        report["actions"].append(action)

    def list_sessions(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT session_id, chain_id, current_stage, status, version "
            "FROM sessions ORDER BY created_at_ms"
        ).fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self.conn.close()
