"""Persistent checkpoint store backed by SQLite.

Every stage publish is one SQLite transaction guarded by a cross-process
file lock; the caller must present the sequence number it based its work
on. Only a strictly consecutive sequence is accepted, otherwise the
publish is rejected with E_STALE_CHECKPOINT and no state is overwritten.
"""

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .errors import E_STALE_CHECKPOINT, PipelineError
from .locking import FileLock

SCHEMA = """
CREATE TABLE IF NOT EXISTS checkpoints (
    seq INTEGER PRIMARY KEY,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    input_versions TEXT NOT NULL,
    artifact_digest TEXT NOT NULL,
    artifact_path TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stage_state (
    stage TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    seq INTEGER,
    detail TEXT
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL,
    stage TEXT NOT NULL,
    detail TEXT
);
"""


def _utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class CheckpointStore:
    def __init__(self, workspace):
        self.workspace = str(workspace)
        self.state_dir = os.path.join(self.workspace, "state")
        self.db_path = os.path.join(self.state_dir, "checkpoints.db")
        self.lock_path = os.path.join(self.state_dir, "publish.lock")
        os.makedirs(self.state_dir, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(SCHEMA)

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _session(self):
        """Transaction scope that also closes the connection."""
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def current_seq(self):
        with self._session() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS seq FROM checkpoints"
            ).fetchone()
            return int(row["seq"])

    def latest_checkpoint(self, stage):
        with self._session() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints WHERE stage=? ORDER BY seq DESC "
                "LIMIT 1", (stage,)).fetchone()
            return self._decode(row) if row else None

    def checkpoints(self):
        with self._session() as conn:
            rows = conn.execute(
                "SELECT * FROM checkpoints ORDER BY seq").fetchall()
            return [self._decode(row) for row in rows]

    def stage_states(self):
        with self._session() as conn:
            rows = conn.execute("SELECT * FROM stage_state").fetchall()
            out = {}
            for row in rows:
                out[row["stage"]] = {
                    "state": row["state"],
                    "seq": row["seq"],
                    "detail": json.loads(row["detail"]) if row["detail"] else {},
                }
            return out

    def events(self, limit=50):
        with self._session() as conn:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?",
                (limit,)).fetchall()
            return [dict(row) for row in rows]

    @staticmethod
    def _decode(row):
        return {
            "seq": row["seq"],
            "stage": row["stage"],
            "status": row["status"],
            "input_versions": json.loads(row["input_versions"]),
            "artifact_digest": row["artifact_digest"],
            "artifact_path": row["artifact_path"],
            "created_at": row["created_at"],
        }

    def publish(self, stage, input_versions, artifact_digest,
                artifact_path, expected_seq):
        """Atomically publish a stage checkpoint.

        Raises PipelineError(E_STALE_CHECKPOINT) when the store has moved
        past `expected_seq` (concurrent or duplicate publish).
        """
        with FileLock(self.lock_path):
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS seq FROM checkpoints"
                ).fetchone()
                current = int(row["seq"])
                if expected_seq != current:
                    conn.execute("ROLLBACK")
                    raise PipelineError(
                        E_STALE_CHECKPOINT,
                        "stale checkpoint for stage '%s': expected next seq "
                        "%d but store is already at seq %d; another worker "
                        "published first, no state was overwritten" % (
                            stage, expected_seq + 1, current))
                new_seq = current + 1
                now = _utcnow()
                conn.execute(
                    "INSERT INTO checkpoints (seq, stage, status, "
                    "input_versions, artifact_digest, artifact_path, "
                    "created_at) VALUES (?,?,?,?,?,?,?)",
                    (new_seq, stage, "completed",
                     json.dumps(input_versions, sort_keys=True),
                     artifact_digest, artifact_path, now))
                conn.execute(
                    "INSERT INTO stage_state (stage, state, seq, detail) "
                    "VALUES (?,?,?,?) ON CONFLICT(stage) DO UPDATE SET "
                    "state=excluded.state, seq=excluded.seq, "
                    "detail=excluded.detail",
                    (stage, "completed", new_seq, json.dumps({
                        "artifact_digest": artifact_digest,
                        "input_versions": input_versions,
                    }, sort_keys=True)))
                conn.execute(
                    "INSERT INTO events (ts, kind, stage, detail) "
                    "VALUES (?,?,?,?)",
                    (now, "publish", stage, json.dumps({
                        "seq": new_seq,
                        "artifact_digest": artifact_digest,
                    }, sort_keys=True)))
                conn.execute("COMMIT")
                return new_seq
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                conn.close()

    def set_stage_state(self, stage, state, seq=None, detail=None):
        with FileLock(self.lock_path):
            with self._session() as conn:
                conn.execute(
                    "INSERT INTO stage_state (stage, state, seq, detail) "
                    "VALUES (?,?,?,?) ON CONFLICT(stage) DO UPDATE SET "
                    "state=excluded.state, seq=excluded.seq, "
                    "detail=excluded.detail",
                    (stage, state, seq,
                     json.dumps(detail or {}, sort_keys=True)))

    def mark_invalidated(self, stage, reason):
        with FileLock(self.lock_path):
            with self._session() as conn:
                existing = conn.execute(
                    "SELECT state, detail FROM stage_state WHERE stage=?",
                    (stage,)).fetchone()
                if existing and existing["state"] == "invalidated":
                    old = json.loads(existing["detail"] or "{}")
                    if old.get("reason") == reason:
                        return
                conn.execute(
                    "INSERT INTO stage_state (stage, state, seq, detail) "
                    "VALUES (?,?,?,?) ON CONFLICT(stage) DO UPDATE SET "
                    "state=excluded.state, seq=excluded.seq, "
                    "detail=excluded.detail",
                    (stage, "invalidated", None,
                     json.dumps({"reason": reason}, sort_keys=True)))
                conn.execute(
                    "INSERT INTO events (ts, kind, stage, detail) "
                    "VALUES (?,?,?,?)",
                    (_utcnow(), "invalidate", stage,
                     json.dumps({"reason": reason}, sort_keys=True)))
