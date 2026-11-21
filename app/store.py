from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar

from .errors import fail


T = TypeVar("T")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


SCHEMA = """
CREATE TABLE IF NOT EXISTS layouts (
    layout_version TEXT PRIMARY KEY,
    document_hash TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    loaded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rounds (
    round_id TEXT PRIMARY KEY,
    round_seq INTEGER NOT NULL,
    trace_id TEXT NOT NULL,
    layout_version TEXT NOT NULL,
    trace_json TEXT NOT NULL,
    trace_hash TEXT NOT NULL,
    options_json TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    committed_at TEXT,
    decision_digest TEXT,
    result_json TEXT,
    recovery_note TEXT,
    FOREIGN KEY(layout_version) REFERENCES layouts(layout_version)
);
CREATE INDEX IF NOT EXISTS idx_rounds_state ON rounds(state);
CREATE INDEX IF NOT EXISTS idx_rounds_seq ON rounds(round_seq DESC);

CREATE TABLE IF NOT EXISTS intents (
    intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    applied_at TEXT,
    recovery_note TEXT
);
CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    first_round_id TEXT NOT NULL,
    decision_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS round_decisions (
    round_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    decision_json TEXT NOT NULL,
    reused INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(round_id, request_id),
    FOREIGN KEY(round_id) REFERENCES rounds(round_id)
);
CREATE INDEX IF NOT EXISTS idx_round_decisions_seq ON round_decisions(round_id, seq);

CREATE TABLE IF NOT EXISTS rebuild_tasks (
    round_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    shard_index INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    arrival_ms INTEGER NOT NULL,
    state TEXT NOT NULL,
    wave INTEGER NOT NULL,
    estimated_start_us INTEGER NOT NULL,
    estimated_end_us INTEGER NOT NULL,
    duration_us INTEGER NOT NULL,
    result_digest TEXT,
    result_b64 TEXT,
    error_json TEXT,
    attempt INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(round_id, task_id),
    FOREIGN KEY(round_id) REFERENCES rounds(round_id)
);
CREATE INDEX IF NOT EXISTS idx_rebuild_tasks_state ON rebuild_tasks(round_id, state);

CREATE TABLE IF NOT EXISTS budget_ledger (
    round_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    state TEXT NOT NULL,
    reason TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(round_id, task_id),
    FOREIGN KEY(round_id) REFERENCES rounds(round_id)
);
CREATE INDEX IF NOT EXISTS idx_budget_ledger_state ON budget_ledger(round_id, state);

CREATE TABLE IF NOT EXISTS shard_results (
    layout_version TEXT NOT NULL,
    object_id TEXT NOT NULL,
    shard_index INTEGER NOT NULL,
    expected_digest TEXT NOT NULL,
    digest TEXT NOT NULL,
    content_b64 TEXT NOT NULL,
    task_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(layout_version, object_id, shard_index, expected_digest)
);
CREATE INDEX IF NOT EXISTS idx_shard_results_reuse ON shard_results(layout_version, object_id, shard_index, state);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    generated_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Store:
    """SQLite store with two-phase intent writes and WAL read snapshots."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()
        self._initialize()

    def _connect(self, *, readonly: bool = False) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30, isolation_level=None, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        if readonly:
            connection.execute("PRAGMA query_only=ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(SCHEMA)

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect(readonly=True)
        try:
            connection.execute("BEGIN")
            yield connection
            connection.commit()
        finally:
            connection.close()

    def write_with_intent(self, kind: str, payload: dict[str, Any], apply: Callable[[sqlite3.Connection], T]) -> T:
        """Persist an intent first, then atomically apply its effects.

        The second transaction also marks the intent APPLIED. A forced process
        stop therefore leaves either a complete, visible fact set or a durable
        PENDING intent that startup reconciliation can inspect.
        """

        with self._write_lock:
            intent_id = self._record_intent(kind, payload)
            try:
                with self._connect() as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    result = apply(connection)
                    connection.execute(
                        "UPDATE intents SET state='APPLIED', applied_at=? WHERE intent_id=?",
                        (utc_now(), intent_id),
                    )
                    connection.commit()
                    return result
            except BaseException:
                try:
                    with self._connect() as connection:
                        connection.execute("BEGIN IMMEDIATE")
                        connection.execute(
                            "UPDATE intents SET state='ROLLED_BACK', recovery_note=? WHERE intent_id=? AND state='PENDING'",
                            ("apply transaction did not commit", intent_id),
                        )
                        connection.commit()
                except sqlite3.Error:
                    pass
                raise

    def _record_intent(self, kind: str, payload: dict[str, Any]) -> int:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "INSERT INTO intents(kind, payload_json, state, created_at) VALUES(?, ?, 'PENDING', ?)",
                (kind, json_dumps(payload), utc_now()),
            )
            intent_id = int(cursor.lastrowid)
            connection.commit()
            return intent_id

    # ------------------------------------------------------------------ layouts
    def save_layout(self, layout_version: str, document_hash: str, raw_document: dict[str, Any]) -> bool:
        def apply(connection: sqlite3.Connection) -> bool:
            existing = connection.execute(
                "SELECT document_hash FROM layouts WHERE layout_version=?", (layout_version,)
            ).fetchone()
            if existing:
                if existing["document_hash"] != document_hash:
                    fail(
                        "E_LAYOUT_CONFLICT",
                        "同一 layout_version 已存在不同布局内容",
                        status_code=409,
                        details={"layout_version": layout_version},
                    )
                return False
            connection.execute(
                "INSERT INTO layouts(layout_version, document_hash, raw_json, loaded_at) VALUES(?, ?, ?, ?)",
                (layout_version, document_hash, json_dumps(raw_document), utc_now()),
            )
            return True

        return self.write_with_intent(
            "LAYOUT_CREATE",
            {"layout_version": layout_version, "document_hash": document_hash},
            apply,
        )

    def get_layout_document(self, layout_version: str) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute(
                "SELECT raw_json FROM layouts WHERE layout_version=?", (layout_version,)
            ).fetchone()
        return json.loads(row["raw_json"]) if row else None

    def get_latest_layout_document(self) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute("SELECT raw_json FROM layouts ORDER BY rowid DESC LIMIT 1").fetchone()
        return json.loads(row["raw_json"]) if row else None

    def list_layouts(self) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT layout_version, document_hash, loaded_at FROM layouts ORDER BY loaded_at ASC"
            ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------- rounds
    def next_round_sequence(self) -> int:
        with self.read() as connection:
            row = connection.execute("SELECT COALESCE(MAX(round_seq), 0) + 1 AS next_seq FROM rounds").fetchone()
        return int(row["next_seq"])

    def create_round(self, record: dict[str, Any]) -> None:
        def apply(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO rounds(
                    round_id, round_seq, trace_id, layout_version, trace_json, trace_hash,
                    options_json, state, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, 'BUILDING', ?)
                """,
                (
                    record["round_id"],
                    record["round_seq"],
                    record["trace_id"],
                    record["layout_version"],
                    json_dumps(record["trace_document"]),
                    record["trace_hash"],
                    json_dumps(record["options"]),
                    utc_now(),
                ),
            )

        self.write_with_intent("ROUND_CREATE", record, apply)

    def plan_round(self, round_id: str, tasks: list[dict[str, Any]]) -> None:
        if not tasks:
            return

        def apply(connection: sqlite3.Connection) -> None:
            round_row = connection.execute("SELECT state FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if not round_row or round_row["state"] != "BUILDING":
                fail("E_ROUND_NOT_WRITABLE", "轮次不在可写入状态", status_code=409, details={"round_id": round_id})
            for task in tasks:
                connection.execute(
                    """
                    INSERT INTO rebuild_tasks(
                        round_id, task_id, request_id, object_id, shard_index, bytes, priority,
                        arrival_ms, state, wave, estimated_start_us, estimated_end_us, duration_us, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'READY', ?, ?, ?, ?, ?)
                    """,
                    (
                        round_id,
                        task["task_id"],
                        task["request_id"],
                        task["object_id"],
                        task["shard_index"],
                        task["bytes"],
                        task["priority"],
                        task["arrival_ms"],
                        task["wave"],
                        task["estimated_start_us"],
                        task["estimated_end_us"],
                        task["duration_us"],
                        utc_now(),
                    ),
                )
                connection.execute(
                    "INSERT INTO budget_ledger(round_id, task_id, bytes, state, reason, updated_at) VALUES(?, ?, ?, 'RESERVED', ?, ?)",
                    (round_id, task["task_id"], task["bytes"], "PLANNED_WITHIN_BUDGET", utc_now()),
                )

        self.write_with_intent("ROUND_PLAN", {"round_id": round_id, "tasks": tasks}, apply)

    def mark_task_running(self, round_id: str, task_id: str) -> None:
        def apply(connection: sqlite3.Connection) -> None:
            cursor = connection.execute(
                "UPDATE rebuild_tasks SET state='RUNNING', updated_at=? WHERE round_id=? AND task_id=? AND state='READY'",
                (utc_now(), round_id, task_id),
            )
            if cursor.rowcount != 1:
                fail("E_TASK_STATE", "重建任务不在 READY 状态", status_code=409, details={"task_id": task_id})

        self.write_with_intent("TASK_START", {"round_id": round_id, "task_id": task_id}, apply)

    def commit_task_success(
        self,
        *,
        round_id: str,
        task: dict[str, Any],
        content_b64: str,
        digest: str,
        layout_version: str,
        expected_digest: str,
        metadata: dict[str, Any],
    ) -> None:
        def apply(connection: sqlite3.Connection) -> None:
            cursor = connection.execute(
                """
                UPDATE rebuild_tasks
                SET state='COMMITTED', result_digest=?, result_b64=?, error_json=?, updated_at=?
                WHERE round_id=? AND task_id=? AND state='RUNNING'
                """,
                (digest, content_b64, json_dumps(metadata), utc_now(), round_id, task["task_id"]),
            )
            if cursor.rowcount != 1:
                fail("E_TASK_STATE", "重建任务不在 RUNNING 状态", status_code=409, details={"task_id": task["task_id"]})
            connection.execute(
                """
                INSERT INTO shard_results(
                    layout_version, object_id, shard_index, expected_digest, digest,
                    content_b64, task_id, round_id, state, created_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'REUSABLE', ?)
                ON CONFLICT(layout_version, object_id, shard_index, expected_digest) DO UPDATE SET
                    digest=excluded.digest,
                    content_b64=excluded.content_b64,
                    task_id=excluded.task_id,
                    round_id=excluded.round_id,
                    state='REUSABLE',
                    created_at=excluded.created_at
                """,
                (
                    layout_version,
                    task["object_id"],
                    task["shard_index"],
                    expected_digest,
                    digest,
                    content_b64,
                    task["task_id"],
                    round_id,
                    utc_now(),
                ),
            )
            connection.execute(
                "UPDATE budget_ledger SET state='COMMITTED', reason=?, updated_at=? WHERE round_id=? AND task_id=? AND state='RESERVED'",
                ("TASK_RESULT_COMMITTED", utc_now(), round_id, task["task_id"]),
            )

        self.write_with_intent(
            "TASK_COMMIT",
            {"round_id": round_id, "task_id": task["task_id"], "digest": digest, "metadata": metadata},
            apply,
        )

    def commit_task_failure(
        self,
        *,
        round_id: str,
        task: dict[str, Any],
        error: dict[str, Any],
        budget_consumed: bool = True,
    ) -> None:
        def apply(connection: sqlite3.Connection) -> None:
            cursor = connection.execute(
                """
                UPDATE rebuild_tasks
                SET state='FAILED', error_json=?, updated_at=?
                WHERE round_id=? AND task_id=? AND state='RUNNING'
                """,
                (json_dumps(error), utc_now(), round_id, task["task_id"]),
            )
            if cursor.rowcount != 1:
                fail("E_TASK_STATE", "重建任务不在 RUNNING 状态", status_code=409, details={"task_id": task["task_id"]})
            ledger_state = "COMMITTED" if budget_consumed else "RELEASED"
            reason = error.get("code", "TASK_FAILED") if budget_consumed else "TASK_FAILED_WITHOUT_CHARGE"
            connection.execute(
                "UPDATE budget_ledger SET state=?, reason=?, updated_at=? WHERE round_id=? AND task_id=? AND state='RESERVED'",
                (ledger_state, reason, utc_now(), round_id, task["task_id"]),
            )

        self.write_with_intent(
            "TASK_FAILURE",
            {"round_id": round_id, "task_id": task["task_id"], "error": error, "budget_consumed": budget_consumed},
            apply,
        )

    def commit_round(
        self,
        *,
        round_id: str,
        result: dict[str, Any],
        decisions: list[dict[str, Any]],
        request_records: list[dict[str, Any]],
        decision_digest: str,
    ) -> None:
        def apply(connection: sqlite3.Connection) -> None:
            round_row = connection.execute("SELECT state FROM rounds WHERE round_id=?", (round_id,)).fetchone()
            if not round_row or round_row["state"] != "BUILDING":
                fail("E_ROUND_NOT_WRITABLE", "轮次不在可提交状态", status_code=409, details={"round_id": round_id})
            for decision in decisions:
                connection.execute(
                    "INSERT OR REPLACE INTO round_decisions(round_id, request_id, seq, decision_json, reused) VALUES(?, ?, ?, ?, ?)",
                    (
                        round_id,
                        decision["request_id"],
                        decision["seq"],
                        json_dumps(decision["decision"]),
                        1 if decision.get("reused") else 0,
                    ),
                )
            for record in request_records:
                connection.execute(
                    """
                    INSERT INTO requests(request_id, content_hash, first_round_id, decision_json, created_at)
                    VALUES(?, ?, ?, ?, ?)
                    ON CONFLICT(request_id) DO NOTHING
                    """,
                    (
                        record["request_id"],
                        record["content_hash"],
                        round_id,
                        json_dumps(record["decision"]),
                        utc_now(),
                    ),
                )
            connection.execute(
                """
                UPDATE rounds SET state='COMMITTED', committed_at=?, decision_digest=?, result_json=?
                WHERE round_id=? AND state='BUILDING'
                """,
                (utc_now(), decision_digest, json_dumps(result), round_id),
            )

        self.write_with_intent(
            "ROUND_COMMIT",
            {"round_id": round_id, "decision_digest": decision_digest},
            apply,
        )

    def abort_round(self, round_id: str, note: str) -> None:
        def apply(connection: sqlite3.Connection) -> None:
            connection.execute(
                "UPDATE rounds SET state='ABORTED', recovery_note=?, committed_at=? WHERE round_id=? AND state='BUILDING'",
                (note, utc_now(), round_id),
            )
            connection.execute(
                "UPDATE rebuild_tasks SET state='READY', updated_at=? WHERE round_id=? AND state IN ('READY','RUNNING')",
                (utc_now(), round_id),
            )
            connection.execute(
                "UPDATE budget_ledger SET state='RELEASED', reason=?, updated_at=? WHERE round_id=? AND state='RESERVED'",
                ("ROUND_ABORTED", utc_now(), round_id),
            )

        self.write_with_intent("ROUND_ABORT", {"round_id": round_id, "note": note}, apply)

    def get_existing_requests(self, request_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not request_ids:
            return {}
        placeholders = ",".join("?" for _ in request_ids)
        with self.read() as connection:
            rows = connection.execute(
                f"SELECT request_id, content_hash, first_round_id, decision_json FROM requests WHERE request_id IN ({placeholders})",
                request_ids,
            ).fetchall()
        return {
            row["request_id"]: {
                "content_hash": row["content_hash"],
                "first_round_id": row["first_round_id"],
                "decision": json.loads(row["decision_json"]),
            }
            for row in rows
        }

    def get_reusable_shard(
        self, layout_version: str, object_id: str, shard_index: int, expected_digest: str
    ) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute(
                """
                SELECT content_b64, digest, task_id, round_id
                FROM shard_results
                WHERE layout_version=? AND object_id=? AND shard_index=? AND expected_digest=? AND state='REUSABLE'
                """,
                (layout_version, object_id, shard_index, expected_digest),
            ).fetchone()
        return dict(row) if row else None

    def get_round_record(self, round_id: str, *, include_uncommitted: bool = False) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute("SELECT * FROM rounds WHERE round_id=?", (round_id,)).fetchone()
        if not row:
            return None
        record = dict(row)
        if record["state"] != "COMMITTED" and not include_uncommitted:
            return None
        return record

    def list_rounds(self, limit: int = 50) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                """
                SELECT round_id, round_seq, trace_id, layout_version, state, created_at,
                       committed_at, decision_digest
                FROM rounds ORDER BY round_seq DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_round_decisions(self, round_id: str) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT request_id, seq, decision_json, reused FROM round_decisions WHERE round_id=? ORDER BY seq ASC, request_id ASC",
                (round_id,),
            ).fetchall()
        result = []
        for row in rows:
            decision = json.loads(row["decision_json"])
            result.append({"seq": row["seq"], "reused": bool(row["reused"]), "decision": decision})
        return result

    def get_budget_facts(self, round_id: str) -> dict[str, Any]:
        with self.read() as connection:
            round_row = connection.execute(
                "SELECT options_json, state FROM rounds WHERE round_id=?", (round_id,)
            ).fetchone()
            if not round_row:
                return {}
            reserved = connection.execute(
                "SELECT COALESCE(SUM(bytes),0) AS total FROM budget_ledger WHERE round_id=? AND state='RESERVED'",
                (round_id,),
            ).fetchone()["total"]
            committed = connection.execute(
                "SELECT COALESCE(SUM(bytes),0) AS total FROM budget_ledger WHERE round_id=? AND state='COMMITTED'",
                (round_id,),
            ).fetchone()["total"]
            released = connection.execute(
                "SELECT COALESCE(SUM(bytes),0) AS total FROM budget_ledger WHERE round_id=? AND state='RELEASED'",
                (round_id,),
            ).fetchone()["total"]
            tasks = connection.execute(
                """
                SELECT t.round_id, t.task_id, t.request_id, t.object_id, t.shard_index,
                       t.bytes, t.priority, t.arrival_ms, t.state, t.wave,
                       t.estimated_start_us, t.estimated_end_us, t.duration_us,
                       t.result_digest, t.error_json, t.attempt, t.updated_at,
                       l.state AS ledger_state, l.reason AS ledger_reason
                FROM rebuild_tasks t LEFT JOIN budget_ledger l
                  ON l.round_id=t.round_id AND l.task_id=t.task_id
                WHERE t.round_id=? ORDER BY t.wave ASC, t.estimated_start_us ASC, t.priority DESC, t.task_id ASC
                """,
                (round_id,),
            ).fetchall()
        options = json.loads(round_row["options_json"])
        byte_budget = int(options["round_byte_budget"])
        needed = int(reserved) + int(committed)
        return {
            "round_id": round_id,
            "round_state": round_row["state"],
            "byte_budget": byte_budget,
            "reserved_bytes": int(reserved),
            "committed_bytes": int(committed),
            "released_bytes": int(released),
            "remaining_bytes_from_persisted_occupancy": max(byte_budget - needed, 0),
            "tasks": [dict(row) for row in tasks],
        }

    def get_latest_recovery_report(self) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
        return json.loads(row["report_json"]) if row else None

    def get_recovery_report(self, report_id: str) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute(
                "SELECT report_json FROM recovery_reports WHERE report_id=?", (report_id,)
            ).fetchone()
        return json.loads(row["report_json"]) if row else None

    # ---------------------------------------------------------------- recovery
    def recover(self) -> dict[str, Any]:
        """Reconcile only durable facts after a forced stop.

        In-flight reservations are released, unfinished tasks return to READY,
        committed shard results remain REUSABLE, and building rounds are marked
        ABORTED. No result or exit report is fabricated for interrupted work.
        """

        with self._write_lock:
            reconciliation_intent = self._record_intent(
                "RECOVERY_RECONCILE",
                {"scope": "startup_reconciliation", "started_at": utc_now()},
            )
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                pending_rows = connection.execute(
                    "SELECT * FROM intents WHERE state='PENDING' AND intent_id<>? ORDER BY intent_id ASC",
                    (reconciliation_intent,),
                ).fetchall()
                pending_intents = [dict(row) for row in pending_rows]
                now_value = utc_now()
                for row in pending_rows:
                    connection.execute(
                        "UPDATE intents SET state='ROLLED_BACK', applied_at=?, recovery_note=? WHERE intent_id=?",
                        (now_value, "startup reconciliation: apply transaction was not durable", row["intent_id"]),
                    )
                building_rows = connection.execute(
                    "SELECT round_id, round_seq, options_json FROM rounds WHERE state='BUILDING' ORDER BY round_seq ASC"
                ).fetchall()
                aborted_rounds: list[dict[str, Any]] = []
                recovered_tasks: list[dict[str, Any]] = []
                budget_facts: list[dict[str, Any]] = []
                reusable_shards: list[dict[str, Any]] = []
                for round_row in building_rows:
                    round_id = round_row["round_id"]
                    tasks = connection.execute(
                        "SELECT task_id, request_id, object_id, shard_index, state FROM rebuild_tasks WHERE round_id=? ORDER BY wave, estimated_start_us, task_id",
                        (round_id,),
                    ).fetchall()
                    for task in tasks:
                        if task["state"] in {"READY", "RUNNING"}:
                            recovered_tasks.append(
                                {
                                    "round_id": round_id,
                                    "task_id": task["task_id"],
                                    "request_id": task["request_id"],
                                    "object_id": task["object_id"],
                                    "shard_index": task["shard_index"],
                                    "previous_state": task["state"],
                                    "state": "READY",
                                }
                            )
                    connection.execute(
                        "UPDATE rebuild_tasks SET state='READY', updated_at=? WHERE round_id=? AND state IN ('READY','RUNNING')",
                        (now_value, round_id),
                    )
                    reserved = connection.execute(
                        "SELECT task_id, bytes FROM budget_ledger WHERE round_id=? AND state='RESERVED' ORDER BY task_id",
                        (round_id,),
                    ).fetchall()
                    for ledger_row in reserved:
                        connection.execute(
                            "UPDATE budget_ledger SET state='RELEASED', reason=?, updated_at=? WHERE round_id=? AND task_id=? AND state='RESERVED'",
                            ("RECOVERED_UNCOMMITTED_TASK", now_value, round_id, ledger_row["task_id"]),
                        )
                    connection.execute(
                        "UPDATE rounds SET state='ABORTED', recovery_note=? WHERE round_id=? AND state='BUILDING'",
                        ("startup reconciliation: process stopped before round commit", round_id),
                    )
                    committed_rows = connection.execute(
                        "SELECT COALESCE(SUM(bytes),0) AS total FROM budget_ledger WHERE round_id=? AND state='COMMITTED'",
                        (round_id,),
                    ).fetchone()
                    released_rows = connection.execute(
                        "SELECT COALESCE(SUM(bytes),0) AS total FROM budget_ledger WHERE round_id=? AND state='RELEASED'",
                        (round_id,),
                    ).fetchone()
                    released_reserved = sum(int(row["bytes"]) for row in reserved)
                    options = json.loads(round_row["options_json"])
                    byte_budget = int(options["round_byte_budget"])
                    budget_facts.append(
                        {
                            "round_id": round_id,
                            "round_seq": round_row["round_seq"],
                            "byte_budget": byte_budget,
                            "committed_bytes": int(committed_rows["total"]),
                            "released_bytes": int(released_rows["total"]),
                            "released_reserved_bytes_for_uncommitted_tasks": released_reserved,
                            "remaining_bytes_recomputed": max(byte_budget - int(committed_rows["total"]), 0),
                        }
                    )
                    aborted_rounds.append({"round_id": round_id, "round_seq": round_row["round_seq"], "state": "ABORTED"})
                    committed_tasks = connection.execute(
                        "SELECT task_id, object_id, shard_index, result_digest FROM rebuild_tasks WHERE round_id=? AND state='COMMITTED' ORDER BY wave, task_id",
                        (round_id,),
                    ).fetchall()
                    for task in committed_tasks:
                        reusable_shards.append(
                            {
                                "round_id": round_id,
                                "task_id": task["task_id"],
                                "object_id": task["object_id"],
                                "shard_index": task["shard_index"],
                                "digest": task["result_digest"],
                                "state": "REUSABLE",
                            }
                        )
                has_recovery = bool(pending_intents or building_rows)
                report_id = "rec-" + uuid.uuid4().hex
                report = {
                    "report_id": report_id,
                    "generated_at": now_value,
                    "scope": "startup_reconciliation",
                    "clean_shutdown_report": False,
                    "note": "仅依据 SQLite 已落盘事实生成启动对账报告；进程被强制终止时不承诺完整退出报告。",
                    "recovery_required": has_recovery,
                    "pending_intents": [
                        {
                            "intent_id": row["intent_id"],
                            "kind": row["kind"],
                            "created_at": row["created_at"],
                            "state": "ROLLED_BACK",
                        }
                        for row in pending_intents
                    ],
                    "aborted_rounds": aborted_rounds,
                    "recovered_tasks": recovered_tasks,
                    "budget_facts": budget_facts,
                    "reusable_shards": reusable_shards,
                    "summary": {
                        "pending_intents": len(pending_intents),
                        "aborted_rounds": len(aborted_rounds),
                        "tasks_returned_to_ready": len(recovered_tasks),
                        "reusable_committed_shards": len(reusable_shards),
                    },
                }
                connection.execute(
                    "INSERT INTO recovery_reports(report_id, generated_at, report_json) VALUES(?, ?, ?)",
                    (report_id, now_value, json_dumps(report)),
                )
                connection.execute(
                    "UPDATE intents SET state='APPLIED', applied_at=? WHERE intent_id=? AND state='PENDING'",
                    (now_value, reconciliation_intent),
                )
                connection.commit()
                return report

    def raw_sql_for_tests(self, sql: str, parameters: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self.read() as connection:
            return connection.execute(sql, parameters).fetchall()
