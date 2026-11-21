from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from .errors import (
    BusinessError,
    E_BATCH_NOT_FOUND,
    E_INVALID_REQUEST,
    E_MEMORY_BUDGET_EXCEEDED,
    E_PAGE_CONSUMED,
    E_PAGE_OUT_OF_RANGE,
)
from .utils import canonical_json_bytes, file_sha256, utc_now_iso


SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    result_handle TEXT NOT NULL UNIQUE,
    snapshot_version INTEGER NOT NULL,
    snapshot_root TEXT NOT NULL,
    snapshot_digest TEXT NOT NULL,
    keys_json TEXT NOT NULL,
    columns_json TEXT NOT NULL,
    page_size INTEGER NOT NULL,
    max_page_size INTEGER NOT NULL,
    requested_mode TEXT NOT NULL,
    state TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    reuse_json TEXT NOT NULL DEFAULT '{}',
    total_rows INTEGER,
    total_pages INTEGER,
    result_path TEXT,
    result_checksum TEXT,
    result_digest TEXT,
    error_code TEXT,
    error_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS block_tasks (
    batch_id TEXT NOT NULL,
    block_no INTEGER NOT NULL,
    key_min INTEGER NOT NULL,
    key_max INTEGER NOT NULL,
    state TEXT NOT NULL,
    artifact_path TEXT,
    artifact_checksum TEXT,
    reusable INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error_json TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (batch_id, block_no),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE TABLE IF NOT EXISTS memory_ledger (
    reservation_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    selected_path TEXT NOT NULL,
    state TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_at TEXT,
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE TABLE IF NOT EXISTS page_progress (
    batch_id TEXT NOT NULL,
    page_no INTEGER NOT NULL,
    row_start INTEGER NOT NULL,
    row_end INTEGER NOT NULL,
    state TEXT NOT NULL,
    consumer_id TEXT,
    consumed_at TEXT,
    PRIMARY KEY (batch_id, page_no),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    block_no INTEGER,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_intents_pending ON intents(state, batch_id);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON block_tasks(batch_id, state);
CREATE INDEX IF NOT EXISTS idx_ledger_state ON memory_ledger(state, batch_id);
CREATE INDEX IF NOT EXISTS idx_pages_batch ON page_progress(batch_id, page_no);
"""


def _json(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _loads(value: str | None) -> Any:
    if value is None:
        return None
    return json.loads(value)


class Persistence:
    """SQLite-backed facts and a serialized write path (WAL, single writer)."""

    def __init__(self, db_path: str | Path) -> None:
        self.path = Path(db_path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self) -> None:
        with self._write_lock:
            connection = self._connect()
            try:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                connection.executescript(SCHEMA)
                connection.execute("PRAGMA user_version=1")
            finally:
                connection.close()

    @contextmanager
    def _write_tx(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.execute("COMMIT")
            except BaseException:
                try:
                    connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                connection.close()

    @contextmanager
    def _read_connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @staticmethod
    def new_identifiers() -> tuple[str, str]:
        return f"b_{uuid.uuid4().hex}", f"rh_{uuid.uuid4().hex}"

    def remaining_memory_budget(self, total_budget: int) -> int:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT COALESCE(SUM(bytes), 0) AS reserved FROM memory_ledger WHERE state='reserved'"
            ).fetchone()
        return max(0, int(total_budget) - int(row["reserved"]))

    def has_reserved_budget(self, batch_id: str) -> bool:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT 1 FROM memory_ledger WHERE batch_id=? AND state='reserved' LIMIT 1",
                (batch_id,),
            ).fetchone()
        return row is not None

    def reserve_budget_for_existing_batch(
        self,
        *,
        batch_id: str,
        bytes_required: int,
        total_budget: int,
        selected_path: str,
        reason: str,
    ) -> bool:
        """Atomically reserve resume-time budget; return False when already reserved."""
        now = utc_now_iso()
        with self._write_tx() as connection:
            existing = connection.execute(
                "SELECT 1 FROM memory_ledger WHERE batch_id=? AND state='reserved' LIMIT 1",
                (batch_id,),
            ).fetchone()
            if existing is not None:
                return False
            row = connection.execute(
                "SELECT COALESCE(SUM(bytes),0) AS reserved FROM memory_ledger WHERE state='reserved'"
            ).fetchone()
            remaining = max(0, int(total_budget) - int(row["reserved"]))
            if bytes_required > remaining:
                raise BusinessError(
                    E_MEMORY_BUDGET_EXCEEDED,
                    "resume-time memory reservation exceeds the remaining budget",
                    {
                        "batch_id": batch_id,
                        "bytes_required": bytes_required,
                        "remaining_bytes": remaining,
                    },
                    507,
                )
            connection.execute(
                """
                INSERT INTO intents(batch_id,block_no,kind,state,payload_json,created_at)
                VALUES(?,?,?,?,?,?)
                """,
                (
                    batch_id,
                    None,
                    "budget_reserve",
                    "pending",
                    _json({"bytes": bytes_required, "selected_path": selected_path}),
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO memory_ledger(
                    reservation_id,batch_id,bytes,selected_path,state,reason,created_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    f"mem_{uuid.uuid4().hex}",
                    batch_id,
                    bytes_required,
                    selected_path,
                    "reserved",
                    reason,
                    now,
                ),
            )
        return True

    def insert_batch(
        self,
        *,
        batch_id: str,
        result_handle: str,
        snapshot_version: int,
        snapshot_root: str,
        snapshot_digest: str,
        keys: Sequence[int],
        columns: Sequence[str],
        page_size: int,
        max_page_size: int,
        requested_mode: str,
        plan: dict[str, Any],
        task_rows: Iterable[tuple[int, int, int]],
        reservation_bytes: int,
    ) -> None:
        now = utc_now_iso()
        reservation_id = f"mem_{uuid.uuid4().hex}"
        with self._write_tx() as connection:
            existing = connection.execute(
                "SELECT 1 FROM batches WHERE batch_id=? OR result_handle=?",
                (batch_id, result_handle),
            ).fetchone()
            if existing is not None:
                raise BusinessError(E_INVALID_REQUEST, "batch identifier already exists", {"batch_id": batch_id})
            connection.execute(
                """
                INSERT INTO batches(
                    batch_id,result_handle,snapshot_version,snapshot_root,snapshot_digest,
                    keys_json,columns_json,page_size,max_page_size,requested_mode,state,plan_json,
                    created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    batch_id,
                    result_handle,
                    snapshot_version,
                    snapshot_root,
                    snapshot_digest,
                    _json(list(keys)),
                    _json(list(columns)),
                    page_size,
                    max_page_size,
                    requested_mode,
                    "materializing",
                    _json(plan),
                    now,
                    now,
                ),
            )
            for block_no, key_min, key_max in task_rows:
                connection.execute(
                    """
                    INSERT INTO block_tasks(batch_id,block_no,key_min,key_max,state,updated_at)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (batch_id, block_no, key_min, key_max, "ready", now),
                )
            connection.execute(
                """
                INSERT INTO memory_ledger(
                    reservation_id,batch_id,bytes,selected_path,state,reason,created_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    reservation_id,
                    batch_id,
                    reservation_bytes,
                    str(plan.get("selected_path", "stream")),
                    "reserved",
                    "reserved before any block decode",
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO intents(batch_id,block_no,kind,state,payload_json,created_at)
                VALUES(?,?,?,?,?,?)
                """,
                (
                    batch_id,
                    None,
                    "batch_run",
                    "pending",
                    _json({"phase": "batch_created", "peak_memory": reservation_bytes}),
                    now,
                ),
            )

    def begin_task_group(self, batch_id: str, block_nos: Sequence[int]) -> None:
        now = utc_now_iso()
        with self._write_tx() as connection:
            batch = connection.execute("SELECT state FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if batch is None:
                raise BusinessError(E_BATCH_NOT_FOUND, "batch does not exist", {"batch_id": batch_id}, 404)
            if batch["state"] == "failed":
                raise BusinessError(E_BATCH_NOT_FOUND, "batch has failed", {"batch_id": batch_id}, 404)
            for block_no in block_nos:
                task = connection.execute(
                    "SELECT state FROM block_tasks WHERE batch_id=? AND block_no=?",
                    (batch_id, block_no),
                ).fetchone()
                if task is None:
                    raise BusinessError(E_INVALID_REQUEST, "block task does not exist", {"block_no": block_no})
                if task["state"] == "committed":
                    continue
                connection.execute(
                    """
                    INSERT INTO intents(batch_id,block_no,kind,state,payload_json,created_at)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (
                        batch_id,
                        block_no,
                        "task_materialize",
                        "pending",
                        _json({"block_no": block_no, "phase": "decode_intent"}),
                        now,
                    ),
                )
                connection.execute(
                    """
                    UPDATE block_tasks SET state='running', attempts=attempts+1, updated_at=?
                    WHERE batch_id=? AND block_no=?
                    """,
                    (now, batch_id, block_no),
                )

    def commit_task_group(self, batch_id: str, results: Sequence[dict[str, Any]]) -> None:
        now = utc_now_iso()
        with self._write_tx() as connection:
            for result in results:
                block_no = int(result["block_no"])
                connection.execute(
                    """
                    UPDATE block_tasks
                    SET state='committed', artifact_path=?, artifact_checksum=?, reusable=1,
                        last_error_json=NULL, updated_at=?
                    WHERE batch_id=? AND block_no=?
                    """,
                    (result["artifact_path"], result["artifact_checksum"], now, batch_id, block_no),
                )
                connection.execute(
                    """
                    UPDATE intents SET state='committed', finished_at=?
                    WHERE batch_id=? AND block_no=? AND kind='task_materialize' AND state='pending'
                    """,
                    (now, batch_id, block_no),
                )

    def mark_tasks_ready(self, batch_id: str, block_nos: Sequence[int], reason: str) -> None:
        now = utc_now_iso()
        with self._write_tx() as connection:
            for block_no in block_nos:
                connection.execute(
                    """
                    UPDATE block_tasks
                    SET state='ready', artifact_path=NULL, artifact_checksum=NULL, reusable=0,
                        last_error_json=?, updated_at=?
                    WHERE batch_id=? AND block_no=? AND state!='committed'
                    """,
                    (_json({"reason": reason}), now, batch_id, block_no),
                )

    def fail_batch(self, batch_id: str, error: BusinessError) -> None:
        now = utc_now_iso()
        with self._write_tx() as connection:
            connection.execute(
                """
                UPDATE batches SET state='failed', error_code=?, error_json=?, updated_at=?
                WHERE batch_id=?
                """,
                (error.code, _json(error.as_dict()), now, batch_id),
            )
            connection.execute(
                "UPDATE block_tasks SET state='failed', updated_at=? WHERE batch_id=? AND state!='committed'",
                (now, batch_id),
            )
            connection.execute(
                "UPDATE intents SET state='failed', finished_at=? WHERE batch_id=? AND state='pending'",
                (now, batch_id),
            )
            connection.execute(
                "UPDATE memory_ledger SET state='released', released_at=? WHERE batch_id=? AND state='reserved'",
                (now, batch_id),
            )

    def begin_finalize(self, batch_id: str) -> None:
        now = utc_now_iso()
        with self._write_tx() as connection:
            connection.execute(
                """
                INSERT INTO intents(batch_id,block_no,kind,state,payload_json,created_at)
                VALUES(?,?,?,?,?,?)
                """,
                (batch_id, None, "batch_finalize", "pending", _json({"phase": "assemble_result"}), now),
            )

    def finalize_batch(
        self,
        *,
        batch_id: str,
        result_path: str,
        result_checksum: str,
        result_digest: str,
        total_rows: int,
        total_pages: int,
        page_size: int,
        execution_summary: dict[str, Any],
    ) -> None:
        now = utc_now_iso()
        with self._write_tx() as connection:
            connection.execute(
                """
                UPDATE batches SET state='materialized', total_rows=?, total_pages=?,
                    result_path=?, result_checksum=?, result_digest=?, reuse_json=?,
                    updated_at=?, completed_at=?
                WHERE batch_id=?
                """,
                (
                    total_rows,
                    total_pages,
                    result_path,
                    result_checksum,
                    result_digest,
                    _json(execution_summary),
                    now,
                    now,
                    batch_id,
                ),
            )
            connection.execute("DELETE FROM page_progress WHERE batch_id=?", (batch_id,))
            for page_no in range(total_pages):
                row_start = page_no * page_size
                row_end = min(total_rows, row_start + page_size)
                connection.execute(
                    """
                    INSERT INTO page_progress(batch_id,page_no,row_start,row_end,state)
                    VALUES(?,?,?,?,?)
                    """,
                    (batch_id, page_no, row_start, row_end, "ready"),
                )
            connection.execute(
                "UPDATE intents SET state='committed', finished_at=? WHERE batch_id=? AND state='pending'",
                (now, batch_id),
            )
            connection.execute(
                "UPDATE memory_ledger SET state='released', released_at=? WHERE batch_id=? AND state='reserved'",
                (now, batch_id),
            )

    def get_batch(self, identifier: str) -> dict[str, Any]:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id=? OR result_handle=?",
                (identifier, identifier),
            ).fetchone()
        if row is None:
            raise BusinessError(E_BATCH_NOT_FOUND, "batch does not exist", {"identifier": identifier}, 404)
        return dict(row)

    def try_get_batch(self, identifier: str) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id=? OR result_handle=?",
                (identifier, identifier),
            ).fetchone()
        return None if row is None else dict(row)

    def get_tasks(self, batch_id: str) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            rows = connection.execute(
                "SELECT * FROM block_tasks WHERE batch_id=? ORDER BY block_no",
                (batch_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_pending_blocks(self, batch_id: str) -> list[int]:
        with self._read_connection() as connection:
            rows = connection.execute(
                "SELECT block_no FROM block_tasks WHERE batch_id=? AND state!='committed' ORDER BY block_no",
                (batch_id,),
            ).fetchall()
        return [int(row["block_no"]) for row in rows]

    def get_page(self, batch_id: str, page_no: int) -> dict[str, Any]:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM page_progress WHERE batch_id=? AND page_no=?",
                (batch_id, page_no),
            ).fetchone()
        if row is None:
            raise BusinessError(
                E_PAGE_OUT_OF_RANGE,
                "page number is outside the result range",
                {"batch_id": batch_id, "page_no": page_no},
                404,
            )
        return dict(row)

    def claim_page(self, batch_id: str, page_no: int, consumer_id: str) -> dict[str, Any]:
        if not consumer_id:
            raise BusinessError(E_INVALID_REQUEST, "consumer_id is required", {"page_no": page_no})
        now = utc_now_iso()
        with self._write_tx() as connection:
            page = connection.execute(
                "SELECT * FROM page_progress WHERE batch_id=? AND page_no=?",
                (batch_id, page_no),
            ).fetchone()
            if page is None:
                raise BusinessError(
                    E_PAGE_OUT_OF_RANGE,
                    "page number is outside the result range",
                    {"batch_id": batch_id, "page_no": page_no},
                    404,
                )
            if page["state"] != "ready":
                raise BusinessError(
                    E_PAGE_CONSUMED,
                    "page has already been consumed",
                    {
                        "batch_id": batch_id,
                        "page_no": page_no,
                        "consumer_id": page["consumer_id"],
                        "consumed_at": page["consumed_at"],
                    },
                    409,
                )
            updated = connection.execute(
                """
                UPDATE page_progress SET state='consumed', consumer_id=?, consumed_at=?
                WHERE batch_id=? AND page_no=? AND state='ready'
                """,
                (consumer_id, now, batch_id, page_no),
            )
            if updated.rowcount != 1:
                raise BusinessError(
                    E_PAGE_CONSUMED,
                    "page was consumed concurrently",
                    {"batch_id": batch_id, "page_no": page_no},
                    409,
                )
        return {**dict(page), "state": "consumed", "consumer_id": consumer_id, "consumed_at": now}

    def list_pages(self, batch_id: str) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            rows = connection.execute(
                "SELECT * FROM page_progress WHERE batch_id=? ORDER BY page_no",
                (batch_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def memory_usage(self) -> dict[str, Any]:
        with self._read_connection() as connection:
            reserved = connection.execute(
                "SELECT COALESCE(SUM(bytes),0) AS bytes, COUNT(*) AS count FROM memory_ledger WHERE state='reserved'"
            ).fetchone()
            history = connection.execute(
                "SELECT batch_id,bytes,selected_path,state,created_at,released_at FROM memory_ledger ORDER BY created_at DESC LIMIT 20"
            ).fetchall()
        return {"reserved_bytes": int(reserved["bytes"]), "reserved_count": int(reserved["count"]),
                "recent": [dict(row) for row in history]}

    def _artifact_is_valid(self, path: str | None, checksum: str | None) -> bool:
        if not path or not checksum:
            return False
        artifact = Path(path)
        if not artifact.is_file():
            return False
        try:
            return file_sha256(artifact) == checksum
        except OSError:
            return False

    def recover_incomplete(self) -> dict[str, Any]:
        """Reconcile pending intents using only committed SQLite rows and artifacts."""
        report: dict[str, Any] = {
            "generated_at": utc_now_iso(),
            "basis": "sqlite_committed_rows_and_verified_block_artifacts_only",
            "pending_intents": 0,
            "tasks_reused": [],
            "tasks_requeued": [],
            "batches_requeued": [],
            "batches_reused": [],
            "budget_reservations_reclaimed": 0,
            "note": "No clean-shutdown or complete-exit report is inferred after forced termination.",
        }
        now = utc_now_iso()
        with self._write_tx() as connection:
            pending = connection.execute(
                "SELECT * FROM intents WHERE state='pending' ORDER BY intent_id"
            ).fetchall()
            report["pending_intents"] = len(pending)
            affected_batches: set[str] = set()
            for intent in pending:
                batch_id = intent["batch_id"]
                affected_batches.add(batch_id)
                kind = intent["kind"]
                if kind == "task_materialize":
                    task = connection.execute(
                        "SELECT * FROM block_tasks WHERE batch_id=? AND block_no=?",
                        (batch_id, intent["block_no"]),
                    ).fetchone()
                    reusable = (
                        task is not None
                        and task["state"] == "committed"
                        and self._artifact_is_valid(task["artifact_path"], task["artifact_checksum"])
                    )
                    if reusable:
                        connection.execute(
                            "UPDATE block_tasks SET reusable=1, updated_at=? WHERE batch_id=? AND block_no=?",
                            (now, batch_id, intent["block_no"]),
                        )
                        connection.execute(
                            "UPDATE intents SET state='recovered_reuse', finished_at=? WHERE intent_id=?",
                            (now, intent["intent_id"]),
                        )
                        report["tasks_reused"].append(
                            {"batch_id": batch_id, "block_no": intent["block_no"], "artifact": task["artifact_path"]}
                        )
                    else:
                        if task is not None:
                            orphan = task["artifact_path"]
                            if orphan:
                                try:
                                    Path(orphan).unlink()
                                except OSError:
                                    pass
                            connection.execute(
                                """
                                UPDATE block_tasks SET state='ready', artifact_path=NULL, artifact_checksum=NULL,
                                    reusable=0, updated_at=? WHERE batch_id=? AND block_no=?
                                """,
                                (now, batch_id, intent["block_no"]),
                            )
                        connection.execute(
                            "UPDATE intents SET state='recovered_retry', finished_at=? WHERE intent_id=?",
                            (now, intent["intent_id"]),
                        )
                        report["tasks_requeued"].append(
                            {"batch_id": batch_id, "block_no": intent["block_no"], "reason": "no committed artifact"}
                        )
                elif kind == "batch_finalize":
                    batch = connection.execute(
                        "SELECT * FROM batches WHERE batch_id=?", (batch_id,)
                    ).fetchone()
                    reusable = (
                        batch is not None
                        and batch["state"] == "materialized"
                        and self._artifact_is_valid(batch["result_path"], batch["result_checksum"])
                    )
                    if reusable:
                        connection.execute(
                            "UPDATE intents SET state='recovered_reuse', finished_at=? WHERE intent_id=?",
                            (now, intent["intent_id"]),
                        )
                        report["batches_reused"].append({"batch_id": batch_id, "result": batch["result_path"]})
                    else:
                        connection.execute(
                            "UPDATE intents SET state='recovered_retry', finished_at=? WHERE intent_id=?",
                            (now, intent["intent_id"]),
                        )
                        connection.execute(
                            "UPDATE batches SET state='ready', updated_at=? WHERE batch_id=?",
                            (now, batch_id),
                        )
                        report["batches_requeued"].append({"batch_id": batch_id, "reason": "result was not committed"})
                else:
                    connection.execute(
                        "UPDATE intents SET state='recovered_retry', finished_at=? WHERE intent_id=?",
                        (now, intent["intent_id"]),
                    )
            for batch_id in sorted(affected_batches):
                batch = connection.execute(
                    "SELECT state FROM batches WHERE batch_id=?", (batch_id,)
                ).fetchone()
                committed_tasks = connection.execute(
                    "SELECT * FROM block_tasks WHERE batch_id=? AND state='committed' ORDER BY block_no",
                    (batch_id,),
                ).fetchall()
                for task in committed_tasks:
                    if self._artifact_is_valid(task["artifact_path"], task["artifact_checksum"]):
                        connection.execute(
                            "UPDATE block_tasks SET reusable=1, updated_at=? WHERE batch_id=? AND block_no=?",
                            (now, batch_id, task["block_no"]),
                        )
                        report["tasks_reused"].append(
                            {
                                "batch_id": batch_id,
                                "block_no": task["block_no"],
                                "artifact": task["artifact_path"],
                                "reason": "committed SQLite row with verified artifact",
                            }
                        )
                    else:
                        connection.execute(
                            """
                            UPDATE block_tasks SET state='ready', artifact_path=NULL, artifact_checksum=NULL,
                                reusable=0, updated_at=? WHERE batch_id=? AND block_no=?
                            """,
                            (now, batch_id, task["block_no"]),
                        )
                        report["tasks_requeued"].append(
                            {
                                "batch_id": batch_id,
                                "block_no": task["block_no"],
                                "reason": "committed artifact is missing or damaged",
                            }
                        )
                if batch is not None and batch["state"] == "materializing":
                    connection.execute(
                        "UPDATE batches SET state='ready', updated_at=? WHERE batch_id=?",
                        (now, batch_id),
                    )
                    if not any(item["batch_id"] == batch_id for item in report["batches_requeued"]):
                        report["batches_requeued"].append(
                            {"batch_id": batch_id, "reason": "materialization intent was unfinished"}
                        )
            # Any conversion row left by a dead writer must return to ready.
            conversion_tasks = connection.execute(
                "SELECT batch_id,block_no FROM block_tasks WHERE state='running'"
            ).fetchall()
            for task in conversion_tasks:
                connection.execute(
                    "UPDATE block_tasks SET state='ready', updated_at=? WHERE batch_id=? AND block_no=?",
                    (now, task["batch_id"], task["block_no"]),
                )
                report["tasks_requeued"].append(
                    {"batch_id": task["batch_id"], "block_no": task["block_no"], "reason": "task remained running"}
                )
            reclaimed = connection.execute(
                "UPDATE memory_ledger SET state='reclaimed', released_at=? WHERE state='reserved'",
                (now,),
            ).rowcount
            report["budget_reservations_reclaimed"] = int(reclaimed)
            report_json = _json(report)
            connection.execute(
                "INSERT INTO recovery_reports(created_at,report_json) VALUES(?,?)",
                (now, report_json),
            )
        return report

    def latest_recovery_report(self) -> dict[str, Any] | None:
        with self._read_connection() as connection:
            row = connection.execute(
                "SELECT created_at,report_json FROM recovery_reports ORDER BY report_id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return {"created_at": row["created_at"], "report": json.loads(row["report_json"])}

    def list_batches(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._read_connection() as connection:
            rows = connection.execute(
                """
                SELECT batch_id,result_handle,snapshot_version,state,total_rows,total_pages,
                       requested_mode,created_at,updated_at,error_code
                FROM batches ORDER BY created_at DESC LIMIT ?
                """,
                (max(1, min(limit, 500)),),
            ).fetchall()
        return [dict(row) for row in rows]
