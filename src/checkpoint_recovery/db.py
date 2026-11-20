from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Sequence

from .errors import ErrorCode, ServiceError
from .models import CheckpointState, ParsedCheckpointRequest, utc_now
from .state_machine import apply_transition


SCHEMA = """
CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    round_number INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('WRITING', 'COMMITTED', 'INVALID', 'PRUNED')),
    state_reason TEXT NOT NULL,
    scheduler_position INTEGER NOT NULL,
    data_cursor TEXT NOT NULL,
    manifest_path TEXT,
    manifest_sha256 TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    committed_at TEXT,
    invalidated_at TEXT,
    pruned_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_state_round
    ON checkpoints(state, round_number DESC, committed_at DESC);

CREATE TABLE IF NOT EXISTS checkpoint_shards (
    checkpoint_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('params', 'optimizer')),
    ordinal INTEGER NOT NULL,
    logical_name TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    verified_at TEXT,
    PRIMARY KEY (checkpoint_id, kind, ordinal)
);

CREATE TABLE IF NOT EXISTS transitions (
    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_id TEXT NOT NULL,
    request_id TEXT,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    details_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_transitions_checkpoint
    ON transitions(checkpoint_id, transition_id);

CREATE TABLE IF NOT EXISTS intent_log (
    intent_id TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    checkpoint_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'DONE', 'ABORTED')),
    created_at TEXT NOT NULL,
    completed_at TEXT,
    details_json TEXT NOT NULL DEFAULT '{}',
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_intent_state ON intent_log(state, created_at);

CREATE TABLE IF NOT EXISTS write_requests (
    request_id TEXT PRIMARY KEY,
    checkpoint_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'SUCCESS', 'ERROR')),
    created_at TEXT NOT NULL,
    completed_at TEXT,
    result_json TEXT,
    error_json TEXT
);

CREATE TABLE IF NOT EXISTS recovery_runs (
    recovery_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    details_json TEXT NOT NULL
);
"""


class Database:
    """Short-lived SQLite connections; WAL allows concurrent readers."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
            conn.executescript(SCHEMA)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    @contextmanager
    def reader(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def create_writing_checkpoint(
        self,
        checkpoint_id: str,
        parsed: ParsedCheckpointRequest,
        max_writing: int,
    ) -> None:
        now = utc_now()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT checkpoint_id FROM write_requests WHERE request_id = ?",
                (parsed.request_id,),
            ).fetchone()
            if existing is not None:
                raise ServiceError(
                    ErrorCode.INVALID_REQUEST,
                    "request_id already exists",
                    {"request_id": parsed.request_id, "checkpoint_id": existing["checkpoint_id"]},
                )
            writing = conn.execute(
                "SELECT COUNT(*) AS n FROM checkpoints WHERE state = 'WRITING'"
            ).fetchone()["n"]
            if writing >= max_writing:
                raise ServiceError(
                    ErrorCode.WRITER_BUSY,
                    "the configured maximum number of WRITING checkpoints is already reached",
                    {"max_writing": max_writing, "writing": writing},
                )
            conn.execute(
                "INSERT INTO checkpoints("
                "checkpoint_id, request_id, round_number, state, state_reason, scheduler_position, "
                "data_cursor, created_at, updated_at"
                ") VALUES (?, ?, ?, 'WRITING', ?, ?, ?, ?, ?)",
                (
                    checkpoint_id,
                    parsed.request_id,
                    parsed.round_number,
                    "write_accepted_by_writer",
                    parsed.scheduler_position,
                    json.dumps(parsed.data_cursor, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO transitions(checkpoint_id, request_id, from_state, to_state, reason, occurred_at, details_json) "
                "VALUES (?, ?, NULL, 'WRITING', ?, ?, '{}')",
                (checkpoint_id, parsed.request_id, "write_accepted_by_writer", now),
            )
            conn.execute(
                "INSERT INTO write_requests(request_id, checkpoint_id, state, created_at) VALUES (?, ?, 'PENDING', ?)",
                (parsed.request_id, checkpoint_id, now),
            )

    def get_checkpoint_row(self, checkpoint_id: str) -> sqlite3.Row | None:
        with self.reader() as conn:
            return conn.execute(
                "SELECT * FROM checkpoints WHERE checkpoint_id = ?", (checkpoint_id,)
            ).fetchone()

    def get_checkpoint_by_request(self, request_id: str) -> sqlite3.Row | None:
        with self.reader() as conn:
            return conn.execute(
                "SELECT * FROM checkpoints WHERE request_id = ?", (request_id,)
            ).fetchone()

    def get_write_request(self, request_id: str) -> sqlite3.Row | None:
        with self.reader() as conn:
            return conn.execute(
                "SELECT * FROM write_requests WHERE request_id = ?", (request_id,)
            ).fetchone()

    def list_checkpoints(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        sql = "SELECT * FROM checkpoints"
        params: list[Any] = []
        if status is not None:
            sql += " WHERE state = ?"
            params.append(status)
        sql += " ORDER BY round_number DESC, created_at DESC LIMIT ?"
        params.append(limit)
        with self.reader() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._checkpoint_to_dict(conn, row, include_shards=False) for row in rows]

    def list_committed(self) -> list[dict[str, Any]]:
        with self.reader() as conn:
            rows = conn.execute(
                "SELECT * FROM checkpoints WHERE state = 'COMMITTED' "
                "ORDER BY round_number DESC, committed_at DESC, created_at DESC"
            ).fetchall()
            return [self._checkpoint_to_dict(conn, row, include_shards=True) for row in rows]

    def ids_by_state(self, state: CheckpointState) -> list[str]:
        with self.reader() as conn:
            rows = conn.execute(
                "SELECT checkpoint_id FROM checkpoints WHERE state = ?", (state.value,)
            ).fetchall()
            return [row["checkpoint_id"] for row in rows]

    def get_checkpoint(self, checkpoint_id: str) -> dict[str, Any] | None:
        with self.reader() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints WHERE checkpoint_id = ?", (checkpoint_id,)
            ).fetchone()
            if row is None:
                return None
            return self._checkpoint_to_dict(conn, row, include_shards=True)

    def _checkpoint_to_dict(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        include_shards: bool,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "checkpoint_id": row["checkpoint_id"],
            "request_id": row["request_id"],
            "round": row["round_number"],
            "status": row["state"],
            "state_reason": row["state_reason"],
            "scheduler_position": row["scheduler_position"],
            "data_cursor": json.loads(row["data_cursor"]),
            "manifest_path": row["manifest_path"],
            "manifest_sha256": row["manifest_sha256"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "committed_at": row["committed_at"],
            "invalidated_at": row["invalidated_at"],
            "pruned_at": row["pruned_at"],
        }
        shards = conn.execute(
            "SELECT kind, ordinal, logical_name, relative_path, size_bytes, sha256, verified_at "
            "FROM checkpoint_shards WHERE checkpoint_id = ? ORDER BY kind, ordinal",
            (row["checkpoint_id"],),
        ).fetchall()
        result["param_shard_count"] = sum(1 for shard in shards if shard["kind"] == "params")
        result["optimizer_shard_count"] = sum(1 for shard in shards if shard["kind"] == "optimizer")
        if include_shards:
            result["shards"] = [dict(shard) for shard in shards]
        request = conn.execute(
            "SELECT state, completed_at, result_json, error_json FROM write_requests WHERE request_id = ?",
            (row["request_id"],),
        ).fetchone()
        if request is not None:
            write_request: dict[str, Any] = {
                "state": request["state"],
                "completed_at": request["completed_at"],
            }
            if request["result_json"]:
                write_request["result"] = json.loads(request["result_json"])
            if request["error_json"]:
                write_request["error"] = json.loads(request["error_json"])
            result["write_request"] = write_request
        return result

    def transitions_for(self, checkpoint_id: str) -> list[dict[str, Any]]:
        with self.reader() as conn:
            rows = conn.execute(
                "SELECT transition_id, checkpoint_id, request_id, from_state, to_state, reason, "
                "occurred_at, details_json FROM transitions WHERE checkpoint_id = ? ORDER BY transition_id",
                (checkpoint_id,),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["details"] = json.loads(item.pop("details_json"))
                result.append(item)
            return result

    def transition(self, checkpoint_id: str, target: CheckpointState, reason: str, details: dict[str, Any] | None = None) -> None:
        with self.transaction() as conn:
            apply_transition(conn, checkpoint_id, target, reason, details)

    def complete_commit(
        self,
        checkpoint_id: str,
        manifest_path: str,
        manifest_sha256: str,
        shards: Sequence[dict[str, Any]],
        result: dict[str, Any],
    ) -> None:
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT state, request_id FROM checkpoints WHERE checkpoint_id = ?", (checkpoint_id,)
            ).fetchone()
            if row is None:
                raise ServiceError(ErrorCode.CHECKPOINT_NOT_FOUND, "checkpoint disappeared", {"checkpoint_id": checkpoint_id})
            conn.execute("DELETE FROM checkpoint_shards WHERE checkpoint_id = ?", (checkpoint_id,))
            conn.executemany(
                "INSERT INTO checkpoint_shards("
                "checkpoint_id, kind, ordinal, logical_name, relative_path, size_bytes, sha256, verified_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        checkpoint_id,
                        shard["kind"],
                        shard["ordinal"],
                        shard["logical_name"],
                        shard["relative_path"],
                        shard["size_bytes"],
                        shard["sha256"],
                        now,
                    )
                    for shard in shards
                ],
            )
            conn.execute(
                "UPDATE checkpoints SET manifest_path = ?, manifest_sha256 = ?, updated_at = ? "
                "WHERE checkpoint_id = ?",
                (manifest_path, manifest_sha256, now, checkpoint_id),
            )
            apply_transition(
                conn,
                checkpoint_id,
                CheckpointState.COMMITTED,
                "all_temporary_shards_and_manifest_verified",
                {"manifest_path": manifest_path, "manifest_sha256": manifest_sha256},
            )
            conn.execute(
                "UPDATE write_requests SET state = 'SUCCESS', completed_at = ?, result_json = ?, error_json = NULL "
                "WHERE request_id = ?",
                (now, json.dumps(result, ensure_ascii=False, sort_keys=True), row["request_id"]),
            )

    def fail_write(
        self,
        checkpoint_id: str,
        request_id: str,
        error: ServiceError,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        now = utc_now()
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT state, request_id FROM checkpoints WHERE checkpoint_id = ?", (checkpoint_id,)
            ).fetchone()
            if row is not None and row["state"] == CheckpointState.WRITING.value:
                apply_transition(conn, checkpoint_id, CheckpointState.INVALID, reason, details)
            conn.execute(
                "UPDATE write_requests SET state = 'ERROR', completed_at = ?, error_json = ?, result_json = NULL "
                "WHERE request_id = ?",
                (now, json.dumps(error.to_dict(), ensure_ascii=False, sort_keys=True), request_id),
            )

    def create_intent(self, intent_id: str, checkpoint_id: str, details: dict[str, Any]) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO intent_log(intent_id, operation, checkpoint_id, state, created_at, details_json) "
                "VALUES (?, 'PRUNE', ?, 'PENDING', ?, ?)",
                (intent_id, checkpoint_id, utc_now(), json.dumps(details, ensure_ascii=False, sort_keys=True)),
            )

    def finish_intent(self, intent_id: str, state: str, error: str | None = None) -> None:
        with self.transaction() as conn:
            completed_at = None if state == "PENDING" else utc_now()
            conn.execute(
                "UPDATE intent_log SET state = ?, completed_at = ?, error = ? WHERE intent_id = ?",
                (state, completed_at, error, intent_id),
            )

    def pending_intents(self) -> list[dict[str, Any]]:
        with self.reader() as conn:
            rows = conn.execute(
                "SELECT * FROM intent_log WHERE state = 'PENDING' ORDER BY created_at"
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["details"] = json.loads(item.pop("details_json"))
                result.append(item)
            return result

    def all_intent_ids(self) -> set[str]:
        with self.reader() as conn:
            rows = conn.execute("SELECT intent_id FROM intent_log").fetchall()
            return {row["intent_id"] for row in rows}

    def clear_pruned_metadata(self, checkpoint_id: str) -> None:
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT state FROM checkpoints WHERE checkpoint_id = ?", (checkpoint_id,)
            ).fetchone()
            if row is None:
                return
            if row["state"] != CheckpointState.PRUNED.value:
                raise ServiceError(
                    ErrorCode.PRUNE_CONFLICT,
                    "checkpoint is not logically pruned; refusing to remove metadata",
                    {"checkpoint_id": checkpoint_id, "state": row["state"]},
                )
            conn.execute(
                "UPDATE checkpoints SET manifest_path = NULL, manifest_sha256 = NULL, updated_at = ? "
                "WHERE checkpoint_id = ?",
                (utc_now(), checkpoint_id),
            )
            conn.execute("DELETE FROM checkpoint_shards WHERE checkpoint_id = ?", (checkpoint_id,))

    def pending_write_requests(self) -> list[sqlite3.Row]:
        with self.reader() as conn:
            return conn.execute(
                "SELECT request_id, checkpoint_id FROM write_requests WHERE state = 'PENDING'"
            ).fetchall()

    def mark_shards_verified(self, checkpoint_id: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE checkpoint_shards SET verified_at = ? WHERE checkpoint_id = ?",
                (utc_now(), checkpoint_id),
            )

    def add_recovery_run(self, recovery_id: str, started_at: str, details: dict[str, Any]) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO recovery_runs(recovery_id, started_at, finished_at, details_json) VALUES (?, ?, ?, ?)",
                (recovery_id, started_at, utc_now(), json.dumps(details, ensure_ascii=False, sort_keys=True)),
            )

    def latest_recovery_run(self) -> dict[str, Any] | None:
        with self.reader() as conn:
            row = conn.execute(
                "SELECT * FROM recovery_runs ORDER BY finished_at DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            item = dict(row)
            item["details"] = json.loads(item.pop("details_json"))
            return item

