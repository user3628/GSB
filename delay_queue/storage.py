from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .errors import (
    E_IDEMPOTENCY_CONFLICT,
    E_INVALID_TRANSITION,
    E_NOT_FOUND,
    E_PROCESS_INTERRUPTED,
    BusinessError,
)
from .model import (
    ALLOWED_TRANSITIONS,
    TaskSpec,
    TaskState,
    can_transition,
    canonical_json,
    retry_backoff_ms,
)


SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL,
    due_at_ms INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    max_retries INTEGER NOT NULL,
    remaining_retries INTEGER NOT NULL,
    resource_requirement INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at_ms INTEGER,
    input_fingerprint TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    last_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_queue_state
    ON tasks(queue_name, state, priority, due_at_ms, task_id);

CREATE TABLE IF NOT EXISTS task_transitions (
    transition_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    metadata_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transitions_task
    ON task_transitions(task_id, occurred_at_ms, transition_id);

CREATE TABLE IF NOT EXISTS rounds (
    round_id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_config_json TEXT NOT NULL,
    effective_config_json TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    result_json TEXT,
    error_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_rounds_queue ON rounds(queue_name, started_at_ms);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    round_id TEXT,
    task_id TEXT,
    queue_name TEXT NOT NULL,
    attempt_id TEXT,
    payload_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    closed_at_ms INTEGER,
    resolution TEXT
);
CREATE INDEX IF NOT EXISTS idx_intents_open ON intents(state, created_at_ms);

CREATE TABLE IF NOT EXISTS dispatch_attempts (
    attempt_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    endpoint TEXT NOT NULL,
    outcome TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    http_status INTEGER,
    duration_ms INTEGER,
    error_code TEXT,
    error_detail TEXT,
    UNIQUE(round_id, task_id)
);
CREATE INDEX IF NOT EXISTS idx_attempts_round ON dispatch_attempts(round_id, started_at_ms);
CREATE INDEX IF NOT EXISTS idx_attempts_task ON dispatch_attempts(task_id, attempt_no);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    queue_name TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    report_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recovery_created ON recovery_reports(created_at_ms);
"""


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def _stable_id(prefix: str, *parts: Any) -> str:
    raw = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}{hashlib.sha256(raw).hexdigest()[:24]}"


class Storage:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path), timeout=30.0, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def initialize(self) -> None:
        with self._write_lock:
            conn = self._connect()
            try:
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA synchronous = FULL")
                conn.executescript(SCHEMA)
            finally:
                conn.close()

    @contextmanager
    def _write_tx(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                conn.close()

    @contextmanager
    def _read_tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            yield conn
        finally:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            conn.close()

    @staticmethod
    def _task_dict(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        try:
            value["payload"] = json.loads(value.pop("payload_json"))
        except (KeyError, json.JSONDecodeError):
            value["payload"] = {}
        return value

    @staticmethod
    def _insert_transition(
        conn: sqlite3.Connection,
        task_id: str,
        queue_name: str,
        from_state: str | None,
        to_state: str,
        reason: str,
        at_ms: int,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        transition_id = _stable_id(
            "tr-", task_id, from_state, to_state, at_ms, uuid.uuid4().hex
        )
        conn.execute(
            """
            INSERT INTO task_transitions(
                transition_id, task_id, queue_name, from_state, to_state,
                reason, occurred_at_ms, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                transition_id,
                task_id,
                queue_name,
                from_state,
                to_state,
                reason,
                at_ms,
                canonical_json(metadata or {}),
            ),
        )
        return transition_id

    def insert_tasks(
        self, specs: list[TaskSpec], batch_duplicate_ids: list[str] | None = None
    ) -> dict[str, Any]:
        inserted: list[str] = []
        duplicate_ids = list(batch_duplicate_ids or [])
        at = now_ms()
        with self._write_tx() as conn:
            for spec in specs:
                existing = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (spec.task_id,)
                ).fetchone()
                if existing is not None:
                    if existing["input_fingerprint"] != spec.input_fingerprint:
                        raise BusinessError(
                            E_IDEMPOTENCY_CONFLICT,
                            "task id already exists with different immutable content",
                            {
                                "task_id": spec.task_id,
                                "existing_queue": existing["queue_name"],
                                "submitted_queue": spec.queue_name,
                            },
                            409,
                        )
                    duplicate_ids.append(spec.task_id)
                    continue

                conn.execute(
                    """
                    INSERT INTO tasks(
                        task_id, queue_name, due_at_ms, priority, max_retries,
                        remaining_retries, resource_requirement, payload_json,
                        endpoint, state, attempt_count, next_attempt_at_ms,
                        input_fingerprint, created_at_ms, updated_at_ms, last_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
                    """,
                    (
                        spec.task_id,
                        spec.queue_name,
                        spec.due_at_ms,
                        spec.priority,
                        spec.remaining_retries,
                        spec.remaining_retries,
                        spec.resource_requirement,
                        canonical_json(spec.payload),
                        spec.endpoint,
                        TaskState.PENDING.value,
                        spec.due_at_ms,
                        spec.input_fingerprint,
                        at,
                        at,
                        "TASK_ACCEPTED",
                    ),
                )
                self._insert_transition(
                    conn,
                    spec.task_id,
                    spec.queue_name,
                    None,
                    TaskState.PENDING.value,
                    "TASK_ACCEPTED",
                    at,
                    {"due_at_ms": spec.due_at_ms, "priority": spec.priority},
                )
                inserted.append(spec.task_id)

            task_ids = inserted + duplicate_ids
            rows: list[dict[str, Any]] = []
            if task_ids:
                placeholders = ",".join("?" for _ in task_ids)
                rows = [
                    self._task_dict(row)
                    for row in conn.execute(
                        f"SELECT * FROM tasks WHERE task_id IN ({placeholders}) "
                        "ORDER BY created_at_ms, task_id",
                        tuple(task_ids),
                    ).fetchall()
                ]
        return {
            "inserted": inserted,
            "duplicates": duplicate_ids,
            "tasks": rows,
        }

    def get_task(self, queue_name: str, task_id: str) -> dict[str, Any] | None:
        with self._read_tx() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ? AND queue_name = ?",
                (task_id, queue_name),
            ).fetchone()
            return self._task_dict(row) if row else None

    def manual_transition(
        self, queue_name: str, task_id: str, to_state: str, reason: str
    ) -> dict[str, Any]:
        with self._write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ? AND queue_name = ?",
                (task_id, queue_name),
            ).fetchone()
            if row is None:
                raise BusinessError(
                    E_NOT_FOUND,
                    "task not found",
                    {"queue": queue_name, "task_id": task_id},
                    404,
                )
            from_state = row["state"]
            if to_state not in ALLOWED_TRANSITIONS.get(from_state, frozenset()):
                raise BusinessError(
                    E_INVALID_TRANSITION,
                    f"transition {from_state} -> {to_state} is not allowed",
                    {
                        "task_id": task_id,
                        "queue": queue_name,
                        "from_state": from_state,
                        "to_state": to_state,
                    },
                    409,
                )
            if to_state in {TaskState.DISPATCHED.value, TaskState.RETRY_WAITING.value}:
                raise BusinessError(
                    E_INVALID_TRANSITION,
                    "DISPATCHED and RETRY_WAITING are scheduler-owned states",
                    {
                        "task_id": task_id,
                        "from_state": from_state,
                        "to_state": to_state,
                    },
                    409,
                )
            if from_state == TaskState.DISPATCHED.value:
                raise BusinessError(
                    E_INVALID_TRANSITION,
                    "manual transitions out of DISPATCHED are not permitted",
                    {"task_id": task_id, "from_state": from_state, "to_state": to_state},
                    409,
                )
            at = now_ms()
            conn.execute(
                "UPDATE tasks SET state = ?, updated_at_ms = ?, last_reason = ? "
                "WHERE task_id = ?",
                (to_state, at, reason, task_id),
            )
            self._insert_transition(
                conn,
                task_id,
                queue_name,
                from_state,
                to_state,
                reason,
                at,
                {"manual": True, "requested_state": to_state},
            )
            updated = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        return self._task_dict(updated)

    def peek_schedulable(self, queue_name: str, at_ms: int) -> list[dict[str, Any]]:
        with self._read_tx() as conn:
            rows = conn.execute(
                """
                SELECT * FROM tasks
                WHERE queue_name = ? AND (
                    (state = ? AND due_at_ms <= ?)
                    OR (state = ? AND next_attempt_at_ms IS NOT NULL AND next_attempt_at_ms <= ?)
                    OR state = ?
                )
                ORDER BY priority ASC, due_at_ms ASC, task_id ASC
                """,
                (
                    queue_name,
                    TaskState.PENDING.value,
                    at_ms,
                    TaskState.RETRY_WAITING.value,
                    at_ms,
                    TaskState.READY.value,
                ),
            ).fetchall()
            return [self._task_dict(row) for row in rows]

    def promote_due_tasks(
        self, queue_name: str, task_ids: list[str], at_ms: int
    ) -> list[dict[str, Any]]:
        if not task_ids:
            return []
        promoted: list[dict[str, Any]] = []
        with self._write_tx() as conn:
            for task_id in task_ids:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ? AND queue_name = ?",
                    (task_id, queue_name),
                ).fetchone()
                if row is None:
                    raise BusinessError(
                        E_NOT_FOUND,
                        "task disappeared while scheduling",
                        {"queue": queue_name, "task_id": task_id},
                        404,
                    )
                state = row["state"]
                if state == TaskState.READY.value:
                    promoted.append(self._task_dict(row))
                    continue
                if state == TaskState.PENDING.value and row["due_at_ms"] <= at_ms:
                    reason = "DUE_TIME_REACHED"
                elif (
                    state == TaskState.RETRY_WAITING.value
                    and row["next_attempt_at_ms"] is not None
                    and row["next_attempt_at_ms"] <= at_ms
                ):
                    reason = "RETRY_BACKOFF_ELAPSED"
                else:
                    continue
                conn.execute(
                    "UPDATE tasks SET state = ?, updated_at_ms = ?, last_reason = ? "
                    "WHERE task_id = ?",
                    (TaskState.READY.value, at_ms, reason, task_id),
                )
                self._insert_transition(
                    conn,
                    task_id,
                    queue_name,
                    state,
                    TaskState.READY.value,
                    reason,
                    at_ms,
                    {"next_attempt_at_ms": row["next_attempt_at_ms"]},
                )
                updated = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
                ).fetchone()
                promoted.append(self._task_dict(updated))
        return promoted

    def prepare_attempts(
        self, queue_name: str, round_id: str, task_ids: list[str], at_ms: int
    ) -> list[dict[str, Any]]:
        attempts: list[dict[str, Any]] = []
        with self._write_tx() as conn:
            for task_id in task_ids:
                task = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ? AND queue_name = ?",
                    (task_id, queue_name),
                ).fetchone()
                if task is None:
                    raise BusinessError(
                        E_NOT_FOUND,
                        "task not found while preparing dispatch",
                        {"queue": queue_name, "task_id": task_id},
                        404,
                    )
                if task["state"] != TaskState.READY.value:
                    raise BusinessError(
                        E_INVALID_TRANSITION,
                        f"cannot dispatch task from state {task['state']}",
                        {
                            "task_id": task_id,
                            "queue": queue_name,
                            "state": task["state"],
                        },
                        409,
                    )
                attempt_no = int(task["attempt_count"]) + 1
                attempt_id = _stable_id(
                    "att-", queue_name, round_id, task_id, attempt_no
                )
                intent_id = _stable_id(
                    "int-", queue_name, round_id, task_id, attempt_no
                )
                intent_payload = {
                    "action": "DISPATCH",
                    "queue": queue_name,
                    "round_id": round_id,
                    "task_id": task_id,
                    "attempt_id": attempt_id,
                    "attempt_no": attempt_no,
                    "endpoint": task["endpoint"],
                    "prepared_at_ms": at_ms,
                }
                conn.execute(
                    """
                    INSERT INTO intents(
                        intent_id, kind, state, round_id, task_id, queue_name,
                        attempt_id, payload_json, created_at_ms
                    ) VALUES (?, 'DISPATCH', 'OPEN', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        intent_id,
                        round_id,
                        task_id,
                        queue_name,
                        attempt_id,
                        canonical_json(intent_payload),
                        at_ms,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO dispatch_attempts(
                        attempt_id, intent_id, round_id, task_id, queue_name,
                        attempt_no, endpoint, outcome, started_at_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'IN_FLIGHT', ?)
                    """,
                    (
                        attempt_id,
                        intent_id,
                        round_id,
                        task_id,
                        queue_name,
                        attempt_no,
                        task["endpoint"],
                        at_ms,
                    ),
                )
                conn.execute(
                    """
                    UPDATE tasks
                    SET state = ?, attempt_count = attempt_count + 1,
                        updated_at_ms = ?, last_reason = ?
                    WHERE task_id = ?
                    """,
                    (
                        TaskState.DISPATCHED.value,
                        at_ms,
                        "DISPATCH_INTENT_COMMITTED",
                        task_id,
                    ),
                )
                self._insert_transition(
                    conn,
                    task_id,
                    queue_name,
                    TaskState.READY.value,
                    TaskState.DISPATCHED.value,
                    "DISPATCH_INTENT_COMMITTED",
                    at_ms,
                    {
                        "round_id": round_id,
                        "attempt_id": attempt_id,
                        "intent_id": intent_id,
                        "attempt_no": attempt_no,
                    },
                )
                attempts.append(
                    {
                        "attempt_id": attempt_id,
                        "intent_id": intent_id,
                        "round_id": round_id,
                        "task_id": task_id,
                        "queue_name": queue_name,
                        "attempt_no": attempt_no,
                        "endpoint": task["endpoint"],
                        "payload": json.loads(task["payload_json"]),
                        "due_at_ms": task["due_at_ms"],
                        "priority": task["priority"],
                        "started_at_ms": at_ms,
                    }
                )
        return attempts

    def finalize_attempt(
        self,
        attempt_id: str,
        outcome: str,
        http_status: int | None,
        duration_ms: int | None,
        error_code: str | None,
        error_detail: str | None,
        finished_at_ms: int,
        retry_backoff_base_ms: int,
    ) -> dict[str, Any]:
        with self._write_tx() as conn:
            row = conn.execute(
                """
                SELECT a.*, t.state AS task_state, t.remaining_retries,
                       t.queue_name AS task_queue
                FROM dispatch_attempts a JOIN tasks t ON t.task_id = a.task_id
                WHERE a.attempt_id = ?
                """,
                (attempt_id,),
            ).fetchone()
            if row is None:
                raise BusinessError(
                    E_NOT_FOUND,
                    "dispatch attempt not found",
                    {"attempt_id": attempt_id},
                    404,
                )
            if row["outcome"] != "IN_FLIGHT":
                return dict(row)
            if row["task_state"] != TaskState.DISPATCHED.value:
                raise BusinessError(
                    E_INVALID_TRANSITION,
                    "attempt belongs to a task that is no longer DISPATCHED",
                    {
                        "attempt_id": attempt_id,
                        "task_id": row["task_id"],
                        "task_state": row["task_state"],
                        "attempt_outcome": row["outcome"],
                    },
                    409,
                )

            conn.execute(
                """
                UPDATE dispatch_attempts
                SET outcome = ?, finished_at_ms = ?, http_status = ?,
                    duration_ms = ?, error_code = ?, error_detail = ?
                WHERE attempt_id = ?
                """,
                (
                    outcome,
                    finished_at_ms,
                    http_status,
                    duration_ms,
                    error_code,
                    error_detail,
                    attempt_id,
                ),
            )

            queue_name = row["task_queue"]
            task_id = row["task_id"]
            attempt_no = int(row["attempt_no"])
            metadata: dict[str, Any] = {
                "round_id": row["round_id"],
                "attempt_id": attempt_id,
                "attempt_no": attempt_no,
                "dispatch_outcome": outcome,
                "http_status": http_status,
                "duration_ms": duration_ms,
                "error_code": error_code,
            }

            if outcome == "ABORTED":
                to_state = TaskState.READY.value
                reason = "ATTEMPT_ABORTED_BEFORE_SEND"
                conn.execute(
                    "UPDATE tasks SET state = ?, next_attempt_at_ms = ?, "
                    "updated_at_ms = ?, last_reason = ? WHERE task_id = ?",
                    (to_state, finished_at_ms, finished_at_ms, reason, task_id),
                )
                resolution = "PRE_SEND_ABORT"
            elif outcome == "SUCCEEDED":
                to_state = TaskState.COMPLETED.value
                reason = "DISPATCH_SUCCEEDED"
                conn.execute(
                    "UPDATE tasks SET state = ?, next_attempt_at_ms = NULL, "
                    "updated_at_ms = ?, last_reason = ? WHERE task_id = ?",
                    (to_state, finished_at_ms, reason, task_id),
                )
                resolution = "DISPATCH_SUCCEEDED"
            else:
                remaining = int(row["remaining_retries"])
                if remaining > 0:
                    new_remaining = remaining - 1
                    backoff = retry_backoff_ms(retry_backoff_base_ms, attempt_no)
                    next_attempt = finished_at_ms + backoff
                    to_state = TaskState.RETRY_WAITING.value
                    reason = "DISPATCH_FAILED_RETRY_SCHEDULED"
                    metadata["backoff_ms"] = backoff
                    metadata["next_attempt_at_ms"] = next_attempt
                    metadata["remaining_retries_before"] = remaining
                    metadata["remaining_retries_after"] = new_remaining
                    conn.execute(
                        """
                        UPDATE tasks SET state = ?, remaining_retries = ?,
                            next_attempt_at_ms = ?, updated_at_ms = ?, last_reason = ?
                        WHERE task_id = ?
                        """,
                        (
                            to_state,
                            new_remaining,
                            next_attempt,
                            finished_at_ms,
                            reason,
                            task_id,
                        ),
                    )
                    resolution = "RETRY_SCHEDULED"
                else:
                    to_state = TaskState.DEAD_LETTERED.value
                    reason = "DISPATCH_FAILED_RETRIES_EXHAUSTED"
                    conn.execute(
                        "UPDATE tasks SET state = ?, next_attempt_at_ms = NULL, "
                        "updated_at_ms = ?, last_reason = ? WHERE task_id = ?",
                        (to_state, finished_at_ms, reason, task_id),
                    )
                    resolution = "DEAD_LETTERED"

            if not can_transition(TaskState.DISPATCHED.value, to_state):
                raise RuntimeError(f"internal invalid transition to {to_state}")
            self._insert_transition(
                conn,
                task_id,
                queue_name,
                TaskState.DISPATCHED.value,
                to_state,
                reason,
                finished_at_ms,
                metadata,
            )
            conn.execute(
                "UPDATE intents SET state = 'CLOSED', closed_at_ms = ?, resolution = ? "
                "WHERE intent_id = ?",
                (finished_at_ms, resolution, row["intent_id"]),
            )
            task_after = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        return {
            "attempt_id": attempt_id,
            "task_id": task_id,
            "queue": queue_name,
            "round_id": row["round_id"],
            "attempt_no": attempt_no,
            "outcome": outcome,
            "http_status": http_status,
            "duration_ms": duration_ms,
            "error_code": error_code,
            "error_detail": error_detail,
            "finished_at_ms": finished_at_ms,
            "state_after": to_state,
            "transition_reason": reason,
            "remaining_retries_after": task_after["remaining_retries"],
            "next_attempt_at_ms": task_after["next_attempt_at_ms"],
        }

    def claim_round(
        self,
        round_id: str,
        queue_name: str,
        fingerprint: str,
        requested_config: dict[str, Any],
        effective_config: dict[str, Any],
        started_at_ms: int,
    ) -> tuple[dict[str, Any], bool]:
        with self._write_tx() as conn:
            existing = conn.execute(
                "SELECT * FROM rounds WHERE round_id = ?", (round_id,)
            ).fetchone()
            if existing is not None:
                return self._round_dict(existing), False
            conn.execute(
                """
                INSERT INTO rounds(
                    round_id, queue_name, fingerprint, status,
                    requested_config_json, effective_config_json, started_at_ms
                ) VALUES (?, ?, ?, 'RUNNING', ?, ?, ?)
                """,
                (
                    round_id,
                    queue_name,
                    fingerprint,
                    canonical_json(requested_config),
                    canonical_json(effective_config),
                    started_at_ms,
                ),
            )
            return self._round_dict(
                conn.execute(
                    "SELECT * FROM rounds WHERE round_id = ?", (round_id,)
                ).fetchone()
            ), True

    @staticmethod
    def _round_dict(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        for key in ("requested_config", "effective_config", "result", "error"):
            source = f"{key}_json"
            raw = value.pop(source, None)
            value[key] = json.loads(raw) if raw else None
        return value

    def finish_round(
        self,
        round_id: str,
        status: str,
        result: dict[str, Any] | None,
        error: BusinessError | None,
        finished_at_ms: int,
    ) -> dict[str, Any]:
        with self._write_tx() as conn:
            conn.execute(
                """
                UPDATE rounds SET status = ?, finished_at_ms = ?,
                    result_json = ?, error_json = ?
                WHERE round_id = ?
                """,
                (
                    status,
                    finished_at_ms,
                    canonical_json(result) if result is not None else None,
                    canonical_json(error.as_dict()) if error is not None else None,
                    round_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM rounds WHERE round_id = ?", (round_id,)
            ).fetchone()
            if row is None:
                raise BusinessError(
                    E_NOT_FOUND, "round not found", {"round_id": round_id}, 404
                )
            return self._round_dict(row)

    def get_round(self, round_id: str) -> dict[str, Any] | None:
        with self._read_tx() as conn:
            row = conn.execute(
                "SELECT * FROM rounds WHERE round_id = ?", (round_id,)
            ).fetchone()
            return self._round_dict(row) if row else None

    def queue_snapshot(self, queue_name: str, limit: int = 500) -> dict[str, Any]:
        with self._read_tx() as conn:
            counts = {state.value: 0 for state in TaskState}
            for row in conn.execute(
                "SELECT state, COUNT(*) AS n FROM tasks WHERE queue_name = ? GROUP BY state",
                (queue_name,),
            ).fetchall():
                counts[row["state"]] = int(row["n"])
            tasks = []
            for row in conn.execute(
                """
                SELECT * FROM tasks WHERE queue_name = ?
                ORDER BY priority ASC, due_at_ms ASC, task_id ASC LIMIT ?
                """,
                (queue_name, limit),
            ).fetchall():
                task = self._task_dict(row)
                latest = conn.execute(
                    """
                    SELECT attempt_id, round_id, attempt_no, outcome, started_at_ms,
                           finished_at_ms, http_status, duration_ms, error_code
                    FROM dispatch_attempts WHERE task_id = ?
                    ORDER BY attempt_no DESC LIMIT 1
                    """,
                    (task["task_id"],),
                ).fetchone()
                task["latest_attempt"] = dict(latest) if latest else None
                tasks.append(task)
            dispatching_without_attempt = conn.execute(
                """
                SELECT COUNT(*) AS n FROM tasks t
                WHERE t.queue_name = ? AND t.state = 'DISPATCHED'
                  AND NOT EXISTS (
                      SELECT 1 FROM dispatch_attempts a
                      WHERE a.task_id = t.task_id AND a.outcome = 'IN_FLIGHT'
                  )
                """,
                (queue_name,),
            ).fetchone()["n"]
            open_intents = conn.execute(
                "SELECT COUNT(*) AS n FROM intents WHERE queue_name = ? AND state = 'OPEN'",
                (queue_name,),
            ).fetchone()["n"]
            recent_round = conn.execute(
                "SELECT round_id, status, started_at_ms, finished_at_ms FROM rounds "
                "WHERE queue_name = ? ORDER BY started_at_ms DESC LIMIT 1",
                (queue_name,),
            ).fetchone()
        return {
            "queue": queue_name,
            "state_counts": counts,
            "total_tasks": sum(counts.values()),
            "open_intents": int(open_intents),
            "dispatching_without_attempt": int(dispatching_without_attempt),
            "recent_round": dict(recent_round) if recent_round else None,
            "tasks": tasks,
        }

    def task_trajectory(self, queue_name: str, task_id: str) -> dict[str, Any]:
        with self._read_tx() as conn:
            task_row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ? AND queue_name = ?",
                (task_id, queue_name),
            ).fetchone()
            if task_row is None:
                raise BusinessError(
                    E_NOT_FOUND,
                    "task not found",
                    {"queue": queue_name, "task_id": task_id},
                    404,
                )
            task = self._task_dict(task_row)
            transitions = [
                {
                    **dict(row),
                    "metadata": json.loads(row["metadata_json"]),
                }
                for row in conn.execute(
                    "SELECT * FROM task_transitions WHERE task_id = ? "
                    "ORDER BY occurred_at_ms, transition_id",
                    (task_id,),
                ).fetchall()
            ]
            for item in transitions:
                item.pop("metadata_json", None)
            attempts = []
            for row in conn.execute(
                "SELECT * FROM dispatch_attempts WHERE task_id = ? "
                "ORDER BY attempt_no, started_at_ms",
                (task_id,),
            ).fetchall():
                attempts.append(dict(row))
            intents = []
            for row in conn.execute(
                "SELECT * FROM intents WHERE task_id = ? ORDER BY created_at_ms, intent_id",
                (task_id,),
            ).fetchall():
                intent = dict(row)
                intent["payload"] = json.loads(intent.pop("payload_json"))
                intents.append(intent)
        return {
            "queue": queue_name,
            "task": task,
            "transitions": transitions,
            "attempts": attempts,
            "intents": intents,
        }

    def verify_consistency(self, round_id: str | None = None) -> dict[str, Any]:
        with self._read_tx() as conn:
            params: tuple[Any, ...] = ()
            round_filter = ""
            if round_id is not None:
                round_filter = " WHERE r.round_id = ?"
                params = (round_id,)
            violations = []
            for row in conn.execute(
                f"""
                SELECT t.task_id, t.queue_name, t.state
                FROM tasks t
                JOIN rounds r ON r.queue_name = t.queue_name
                {round_filter}
                WHERE t.state = 'DISPATCHED' AND NOT EXISTS (
                    SELECT 1 FROM dispatch_attempts a
                    WHERE a.task_id = t.task_id AND a.outcome = 'IN_FLIGHT'
                )
                """,
                params,
            ).fetchall():
                violations.append(
                    {
                        "type": "DISPATCHED_WITHOUT_IN_FLIGHT_ATTEMPT",
                        **dict(row),
                    }
                )
            for row in conn.execute(
                f"""
                SELECT a.attempt_id, a.task_id, a.outcome
                FROM dispatch_attempts a
                JOIN rounds r ON r.round_id = a.round_id
                {round_filter}
                WHERE a.outcome = 'IN_FLIGHT' AND NOT EXISTS (
                    SELECT 1 FROM intents i
                    WHERE i.attempt_id = a.attempt_id AND i.state = 'OPEN'
                )
                """,
                params,
            ).fetchall():
                violations.append(
                    {"type": "IN_FLIGHT_WITHOUT_OPEN_INTENT", **dict(row)}
                )
        return {"ok": not violations, "violations": violations}

    def get_attempts_for_round(self, round_id: str) -> list[dict[str, Any]]:
        with self._read_tx() as conn:
            rows = conn.execute(
                "SELECT * FROM dispatch_attempts WHERE round_id = ? "
                "ORDER BY started_at_ms, attempt_id",
                (round_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_transitions_for_round(self, round_id: str) -> list[dict[str, Any]]:
        with self._read_tx() as conn:
            rows = conn.execute(
                "SELECT * FROM task_transitions WHERE metadata_json LIKE ? "
                "ORDER BY occurred_at_ms, transition_id",
                ('%"round_id"%',),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["metadata"] = json.loads(item.pop("metadata_json"))
                if item["metadata"].get("round_id") == round_id:
                    result.append(item)
            return result

    def get_transitions_for_attempt(self, attempt_id: str) -> list[dict[str, Any]]:
        with self._read_tx() as conn:
            rows = conn.execute(
                "SELECT * FROM task_transitions WHERE metadata_json LIKE ? "
                "ORDER BY occurred_at_ms, transition_id",
                (f'%"attempt_id":"{attempt_id}"%',),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["metadata"] = json.loads(item.pop("metadata_json"))
                result.append(item)
            return result

    def get_intent(self, intent_id: str) -> dict[str, Any] | None:
        with self._read_tx() as conn:
            row = conn.execute(
                "SELECT * FROM intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            if row is None:
                return None
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json"))
            return item

    def recover_incomplete_intents(self, at_ms: int) -> dict[str, Any]:
        actions: list[dict[str, Any]] = []
        interrupted_rounds: list[dict[str, Any]] = []
        with self._write_tx() as conn:
            open_intents = conn.execute(
                "SELECT * FROM intents WHERE state = 'OPEN' ORDER BY created_at_ms, intent_id"
            ).fetchall()
            for intent in open_intents:
                payload = json.loads(intent["payload_json"])
                task = conn.execute(
                    "SELECT * FROM tasks WHERE task_id = ?", (intent["task_id"],)
                ).fetchone()
                attempt = conn.execute(
                    "SELECT * FROM dispatch_attempts WHERE attempt_id = ?",
                    (intent["attempt_id"],),
                ).fetchone()
                action: dict[str, Any] = {
                    "intent_id": intent["intent_id"],
                    "round_id": intent["round_id"],
                    "task_id": intent["task_id"],
                    "queue": intent["queue_name"],
                    "attempt_id": intent["attempt_id"],
                    "persisted_task_state": task["state"] if task else None,
                    "persisted_attempt_outcome": attempt["outcome"] if attempt else None,
                    "attempt_started_at_ms": attempt["started_at_ms"] if attempt else None,
                    "intent_created_at_ms": intent["created_at_ms"],
                    "wire_result_persisted": False,
                    "wire_outcome_unknown": True,
                }
                if (
                    task is not None
                    and attempt is not None
                    and task["state"] == TaskState.DISPATCHED.value
                    and attempt["outcome"] == "IN_FLIGHT"
                ):
                    conn.execute(
                        """
                        UPDATE dispatch_attempts
                        SET outcome = 'INTERRUPTED', finished_at_ms = ?,
                            http_status = NULL, duration_ms = NULL,
                            error_code = ?, error_detail = ?
                        WHERE attempt_id = ?
                        """,
                        (
                            at_ms,
                            E_PROCESS_INTERRUPTED,
                            "process stopped before an endpoint result was persisted",
                            intent["attempt_id"],
                        ),
                    )
                    conn.execute(
                        "UPDATE tasks SET state = ?, next_attempt_at_ms = ?, "
                        "updated_at_ms = ?, last_reason = ? WHERE task_id = ?",
                        (
                            TaskState.READY.value,
                            at_ms,
                            at_ms,
                            "RECOVERY_ROLLBACK_TO_READY",
                            intent["task_id"],
                        ),
                    )
                    self._insert_transition(
                        conn,
                        intent["task_id"],
                        intent["queue_name"],
                        TaskState.DISPATCHED.value,
                        TaskState.READY.value,
                        "RECOVERY_ROLLBACK_TO_READY",
                        at_ms,
                        {
                            "recovery": True,
                            "attempt_id": intent["attempt_id"],
                            "round_id": intent["round_id"],
                            "wire_outcome_unknown": True,
                        },
                    )
                    conn.execute(
                        "UPDATE intents SET state = 'CLOSED', closed_at_ms = ?, "
                        "resolution = 'RECOVERED_ROLLBACK_TO_READY' WHERE intent_id = ?",
                        (at_ms, intent["intent_id"]),
                    )
                    action["recovery_action"] = "ROLLBACK_DISPATCH_TO_READY"
                    action["state_after"] = TaskState.READY.value
                    action["attempt_outcome_after"] = "INTERRUPTED"
                else:
                    conn.execute(
                        "UPDATE intents SET state = 'CLOSED', closed_at_ms = ?, "
                        "resolution = 'CLOSED_WITHOUT_STATE_CHANGE' WHERE intent_id = ?",
                        (at_ms, intent["intent_id"]),
                    )
                    action["recovery_action"] = "CLOSE_INTENT_WITHOUT_STATE_CHANGE"
                    action["state_after"] = task["state"] if task else None
                    action["attempt_outcome_after"] = attempt["outcome"] if attempt else None
                action["recovered_at_ms"] = at_ms
                action["endpoint"] = payload.get("endpoint")
                actions.append(action)

            running_rounds = conn.execute(
                "SELECT * FROM rounds WHERE status = 'RUNNING' ORDER BY started_at_ms"
            ).fetchall()
            for row in running_rounds:
                intent_ids = [
                    action["intent_id"]
                    for action in actions
                    if action["round_id"] == row["round_id"]
                ]
                result = {
                    "round_id": row["round_id"],
                    "queue": row["queue_name"],
                    "status": "INTERRUPTED",
                    "error_code": E_PROCESS_INTERRUPTED,
                    "started_at_ms": row["started_at_ms"],
                    "recovered_at_ms": at_ms,
                    "recovered_intent_ids": intent_ids,
                    "facts_source": "SQLite rows present at the next process startup",
                }
                error = BusinessError(
                    E_PROCESS_INTERRUPTED,
                    "the process stopped while this round was running",
                    {
                        "round_id": row["round_id"],
                        "queue": row["queue_name"],
                        "recovered_intent_ids": intent_ids,
                        "scope": "startup recovery from persisted rows only",
                    },
                    409,
                )
                conn.execute(
                    "UPDATE rounds SET status = 'INTERRUPTED', finished_at_ms = ?, "
                    "result_json = ?, error_json = ? WHERE round_id = ?",
                    (
                        at_ms,
                        canonical_json(result),
                        canonical_json(error.as_dict()),
                        row["round_id"],
                    ),
                )
                interrupted_rounds.append(result)

            report_id = _stable_id("rec-", at_ms, uuid.uuid4().hex)
            report = {
                "report_id": report_id,
                "source": "STARTUP_RECOVERY",
                "scope_note": (
                    "This report is derived only from persisted SQLite facts observed at "
                    "startup. It does not claim knowledge of endpoint-side effects or any "
                    "in-memory state lost during a forced termination."
                ),
                "observed_at_ms": at_ms,
                "open_intents_scanned": len(open_intents),
                "actions": actions,
                "interrupted_rounds": interrupted_rounds,
                "counts": {
                    "open_intents_scanned": len(open_intents),
                    "rolled_back_to_ready": sum(
                        1
                        for action in actions
                        if action["recovery_action"] == "ROLLBACK_DISPATCH_TO_READY"
                    ),
                    "closed_without_state_change": sum(
                        1
                        for action in actions
                        if action["recovery_action"] == "CLOSE_INTENT_WITHOUT_STATE_CHANGE"
                    ),
                    "rounds_marked_interrupted": len(interrupted_rounds),
                },
                "graceful_exit_report_available_for_prior_process": False,
            }
            conn.execute(
                "INSERT INTO recovery_reports(report_id, queue_name, created_at_ms, report_json) "
                "VALUES (?, '*', ?, ?)",
                (report_id, at_ms, canonical_json(report)),
            )
        return report

    def latest_recovery_report(self, queue_name: str | None = None) -> dict[str, Any]:
        with self._read_tx() as conn:
            rows = conn.execute(
                "SELECT * FROM recovery_reports ORDER BY created_at_ms DESC, report_id DESC"
            ).fetchall()
            if not queue_name:
                return json.loads(rows[0]["report_json"]) if rows else {
                    "report_id": None,
                    "open_intents_scanned": 0,
                    "actions": [],
                    "interrupted_rounds": [],
                    "counts": {
                        "open_intents_scanned": 0,
                        "rolled_back_to_ready": 0,
                        "closed_without_state_change": 0,
                        "rounds_marked_interrupted": 0,
                    },
                    "message": "no startup recovery report has been generated",
                }
            for row in rows:
                report = json.loads(row["report_json"])
                if any(action.get("queue") == queue_name for action in report["actions"]) or any(
                    item.get("queue") == queue_name
                    for item in report.get("interrupted_rounds", [])
                ):
                    return report
            return {
                "report_id": None,
                "queue": queue_name,
                "open_intents_scanned": 0,
                "actions": [],
                "interrupted_rounds": [],
                "counts": {
                    "open_intents_scanned": 0,
                    "rolled_back_to_ready": 0,
                    "closed_without_state_change": 0,
                    "rounds_marked_interrupted": 0,
                },
                "message": "no queue-specific recovery actions were found",
            }
