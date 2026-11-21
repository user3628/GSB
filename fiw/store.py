"""SQLite persistence with write-ahead intent records.

Every mutating operation first commits an intent row (its own transaction),
then performs the real work inside a second transaction that also marks the
intent COMMITTED. If the process dies between the two, the intent stays
PENDING and the work transaction is rolled back by SQLite, so recovery only
ever sees the last fully committed version.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from dataclasses import asdict
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS plans (
    plan_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rounds (
    plan_id TEXT NOT NULL,
    round_no INTEGER NOT NULL,
    budget_json TEXT NOT NULL,
    trajectory_json TEXT NOT NULL,
    observations_json TEXT NOT NULL,
    skipped_json TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY (plan_id, round_no)
);
CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    plan_id TEXT,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    resolved_at TEXT
);
CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

INTENT_PENDING = "PENDING"
INTENT_COMMITTED = "COMMITTED"
INTENT_FAILED = "FAILED"
INTENT_INTERRUPTED = "INTERRUPTED"

INTENT_LOAD_PLAN = "LOAD_PLAN"
INTENT_STATE_TRANSITION = "STATE_TRANSITION"
INTENT_ROUND_EXEC = "ROUND_EXEC"


def _now():
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False,
                                     isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(SCHEMA)

    def close(self):
        with self._lock:
            self._conn.close()

    def begin_intent(self, kind, plan_id, payload):
        """Commit the intent record in its own transaction (write-ahead)."""
        intent_id = uuid.uuid4().hex
        with self._lock:
            self._conn.execute(
                "INSERT INTO intents (intent_id, kind, plan_id, payload_json, status,"
                " created_at) VALUES (?,?,?,?,?,?)",
                (intent_id, kind, plan_id, json.dumps(payload, sort_keys=True),
                 INTENT_PENDING, _now()))
        return intent_id

    def resolve_intent(self, intent_id, status):
        with self._lock:
            self._conn.execute(
                "UPDATE intents SET status=?, resolved_at=? WHERE intent_id=?",
                (status, _now(), intent_id))

    def pending_intents(self):
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM intents WHERE status=? ORDER BY created_at",
                (INTENT_PENDING,)).fetchall()
        return [dict(r) for r in rows]

    def save_plan(self, catalog, plan, intent_id):
        payload = {"plan": asdict(plan), "catalog": asdict(catalog)}
        now = _now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute("DELETE FROM plans WHERE plan_id=?", (plan.plan_id,))
                self._conn.execute("DELETE FROM rounds WHERE plan_id=?", (plan.plan_id,))
                self._conn.execute(
                    "INSERT INTO plans (plan_id, version, status, payload_json,"
                    " created_at, updated_at) VALUES (?,?,?,?,?,?)",
                    (plan.plan_id, plan.version, "PLANNED",
                     json.dumps(payload, sort_keys=True), now, now))
                self._conn.execute(
                    "UPDATE intents SET status=?, resolved_at=? WHERE intent_id=?",
                    (INTENT_COMMITTED, _now(), intent_id))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def get_plan(self, plan_id):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM plans WHERE plan_id=?", (plan_id,)).fetchone()
        return dict(row) if row else None

    def list_plans(self):
        with self._lock:
            rows = self._conn.execute(
                "SELECT plan_id, version, status, updated_at FROM plans"
                " ORDER BY plan_id").fetchall()
        return [dict(r) for r in rows]

    def set_plan_status(self, plan_id, status, intent_id=None):
        """State transition committed together with its intent resolution."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "UPDATE plans SET status=?, updated_at=? WHERE plan_id=?",
                    (status, _now(), plan_id))
                if intent_id is not None:
                    self._conn.execute(
                        "UPDATE intents SET status=?, resolved_at=? WHERE intent_id=?",
                        (INTENT_COMMITTED, _now(), intent_id))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def commit_round(self, plan_id, round_no, budget, trajectory, observations,
                     skipped, intent_id, final_status=None):
        """Round conclusions, budget usage and the intent commit atomically."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO rounds (plan_id, round_no, budget_json,"
                    " trajectory_json, observations_json, skipped_json,"
                    " committed_at) VALUES (?,?,?,?,?,?,?)",
                    (plan_id, round_no,
                     json.dumps(budget, sort_keys=True),
                     json.dumps(trajectory, sort_keys=True),
                     json.dumps(observations, sort_keys=True),
                     json.dumps(skipped, sort_keys=True), _now()))
                if final_status is not None:
                    self._conn.execute(
                        "UPDATE plans SET status=?, updated_at=? WHERE plan_id=?",
                        (final_status, _now(), plan_id))
                self._conn.execute(
                    "UPDATE intents SET status=?, resolved_at=? WHERE intent_id=?",
                    (INTENT_COMMITTED, _now(), intent_id))
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def get_round(self, plan_id, round_no):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM rounds WHERE plan_id=? AND round_no=?",
                (plan_id, round_no)).fetchone()
        if not row:
            return None
        data = dict(row)
        for key in ("budget_json", "trajectory_json", "observations_json", "skipped_json"):
            data[key[:-5]] = json.loads(data.pop(key))
        return data

    def last_committed_round(self, plan_id):
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(round_no) AS last FROM rounds WHERE plan_id=?",
                (plan_id,)).fetchone()
        return row["last"] if row and row["last"] is not None else 0

    def save_recovery_report(self, report):
        report_id = uuid.uuid4().hex
        with self._lock:
            self._conn.execute(
                "INSERT INTO recovery_reports (report_id, report_json, created_at)"
                " VALUES (?,?,?)",
                (report_id, json.dumps(report, sort_keys=True), _now()))
        return report_id

    def latest_recovery_report(self):
        with self._lock:
            row = self._conn.execute(
                "SELECT report_json FROM recovery_reports ORDER BY created_at DESC"
                " LIMIT 1").fetchone()
        return json.loads(row["report_json"]) if row else None
