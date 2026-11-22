"""Intent-based durability, fault injection and forced-kill recovery."""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import time
import uuid

from . import cascade
from .config import Settings

FAULT_META_KEY = "fault_mode"
VALID_FAULTS = ("crash_after_intent", "fail_rebuild")


def set_fault(conn: sqlite3.Connection, mode: str | None) -> None:
    if mode is None:
        conn.execute("DELETE FROM meta WHERE key=?", (FAULT_META_KEY,))
        return
    if mode not in VALID_FAULTS:
        raise ValueError(f"unknown fault mode {mode!r}; valid: {VALID_FAULTS}")
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (FAULT_META_KEY, mode),
    )


def consume_fault(conn: sqlite3.Connection) -> str | None:
    """Read and clear a one-shot fault (only a triggered crash would survive)."""
    row = conn.execute("SELECT value FROM meta WHERE key=?", (FAULT_META_KEY,)).fetchone()
    mode = row["value"] if row else None
    if mode == "fail_rebuild":
        conn.execute("DELETE FROM meta WHERE key=?", (FAULT_META_KEY,))
    return mode


def get_fault(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (FAULT_META_KEY,)).fetchone()
    return row["value"] if row else None


def write_intent(conn, kind: str, commit_id: str | None, detail: dict) -> str:
    intent_id = f"intent_{uuid.uuid4().hex}"
    conn.execute(
        "INSERT INTO intents(intent_id, kind, commit_id, detail_json, status, created_at_ms) "
        "VALUES(?, ?, ?, ?, 'pending', ?)",
        (intent_id, kind, commit_id, json.dumps(detail, sort_keys=True), int(time.time() * 1000)),
    )
    if commit_id is not None:
        conn.execute(
            "INSERT OR IGNORE INTO commits(commit_id, kind, payload_hash, status, created_at_ms) "
            "VALUES(?, ?, ?, 'inflight', ?)",
            (commit_id, kind, detail.get("payload_hash", ""), int(time.time() * 1000)),
        )
    return intent_id


def complete_intent(conn, intent_id: str, status: str = "done") -> None:
    conn.execute(
        "UPDATE intents SET status=?, finished_at_ms=? WHERE intent_id=?",
        (status, int(time.time() * 1000), intent_id),
    )
    row = conn.execute("SELECT commit_id FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
    if row and row["commit_id"]:
        conn.execute(
            "UPDATE commits SET status=? WHERE commit_id=? AND status='inflight'",
            ("committed" if status == "done" else "aborted", row["commit_id"]),
        )


def maybe_crash(mode: str | None) -> None:
    """Simulate SIGKILL: hard terminate without flushing buffered Python state.

    Uses SIGKILL against the current process rather than ``os._exit`` so the
    termination semantics match an external ``kill -9`` and the exit status is
    the conventional 137 (128 + 9).
    """
    if mode == "crash_after_intent":
        os.kill(os.getpid(), signal.SIGKILL)


def run_recovery(conn: sqlite3.Connection, settings: Settings) -> dict:
    """Scan unfinished intents, abort them, and regenerate derived artifacts.

    Only durable facts are reported.  A process killed *before* its transaction
    committed leaves no partial rows (SQLite atomicity); a process killed after
    commit but before the intent was marked done is reconciled here.
    """
    # A one-shot crash fault must never survive the restart that consumes it;
    # otherwise the recovering process would re-arm and kill itself again.
    if get_fault(conn) == "crash_after_intent":
        set_fault(conn, None)
    at_ms = int(time.time() * 1000)
    pending = conn.execute(
        "SELECT intent_id, kind, commit_id, detail_json FROM intents WHERE status='pending' "
        "ORDER BY id"
    ).fetchall()
    actions = []
    for row in pending:
        conn.execute(
            "UPDATE intents SET status='aborted', finished_at_ms=? WHERE intent_id=?",
            (at_ms, row["intent_id"]),
        )
        if row["commit_id"]:
            conn.execute(
                "UPDATE commits SET status='aborted' WHERE commit_id=? AND status='inflight'",
                (row["commit_id"],),
            )
        action = {"intent_id": row["intent_id"], "kind": row["kind"],
                  "commit_id": row["commit_id"], "action": "aborted_unfinished_intent"}
        conn.execute(
            "INSERT INTO recovery_events(at_ms, intent_id, action, detail_json) VALUES(?, ?, ?, ?)",
            (at_ms, row["intent_id"], action["action"], row["detail_json"]),
        )
        actions.append(action)

    rebuild_summary = cascade.full_rebuild(conn, settings)
    report = {
        "recovered_at_ms": at_ms,
        "pending_intents_found": len(pending),
        "actions": actions,
        "derived_rebuild": rebuild_summary,
        "note": "report contains durable facts only; a hard-killed process cannot emit an exit report",
    }
    conn.execute(
        "INSERT INTO recovery_events(at_ms, intent_id, action, detail_json) VALUES(?, NULL, ?, ?)",
        (at_ms, "derived_rebuild", json.dumps(rebuild_summary, sort_keys=True)),
    )
    return report
