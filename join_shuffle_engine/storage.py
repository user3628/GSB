from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .engine import ExecutionResult
from .errors import EngineError
from .snapshot import Snapshot, utc_now


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _payload_sha(payload: Any) -> str:
    return hashlib.sha256(_json(payload).encode("ascii")).hexdigest()


@dataclass(frozen=True)
class ActiveDataset:
    dataset_id: str
    pair_version: str
    left_version: int
    right_version: int
    left_snapshot_id: str
    right_snapshot_id: str
    left_path: str
    right_path: str
    left_digest: str
    right_digest: str
    left_generated_at: str
    right_generated_at: str
    activated_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "pair_version": self.pair_version,
            "left": {
                "snapshot_id": self.left_snapshot_id,
                "version": self.left_version,
                "path": self.left_path,
                "content_sha256": self.left_digest,
                "generated_at": self.left_generated_at,
            },
            "right": {
                "snapshot_id": self.right_snapshot_id,
                "version": self.right_version,
                "path": self.right_path,
                "content_sha256": self.right_digest,
                "generated_at": self.right_generated_at,
            },
            "activated_at": self.activated_at,
        }


_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot_history (
    dataset_id TEXT NOT NULL,
    pair_version TEXT NOT NULL,
    left_version INTEGER NOT NULL,
    right_version INTEGER NOT NULL,
    left_snapshot_id TEXT NOT NULL,
    right_snapshot_id TEXT NOT NULL,
    left_path TEXT NOT NULL,
    right_path TEXT NOT NULL,
    left_digest TEXT NOT NULL,
    right_digest TEXT NOT NULL,
    left_generated_at TEXT NOT NULL,
    right_generated_at TEXT NOT NULL,
    loaded_at TEXT NOT NULL,
    PRIMARY KEY (dataset_id, pair_version)
);

CREATE TABLE IF NOT EXISTS active_datasets (
    dataset_id TEXT PRIMARY KEY,
    pair_version TEXT NOT NULL,
    left_version INTEGER NOT NULL,
    right_version INTEGER NOT NULL,
    left_snapshot_id TEXT NOT NULL,
    right_snapshot_id TEXT NOT NULL,
    left_path TEXT NOT NULL,
    right_path TEXT NOT NULL,
    left_digest TEXT NOT NULL,
    right_digest TEXT NOT NULL,
    left_generated_at TEXT NOT NULL,
    right_generated_at TEXT NOT NULL,
    activated_at TEXT NOT NULL,
    FOREIGN KEY (dataset_id, pair_version)
        REFERENCES snapshot_history(dataset_id, pair_version)
);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    pair_version TEXT NOT NULL,
    status TEXT NOT NULL,
    execution_id TEXT,
    submitted_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    error_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_requests_status ON requests(status, submitted_at);

CREATE TABLE IF NOT EXISTS executions (
    execution_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    pair_version TEXT NOT NULL,
    left_version INTEGER NOT NULL,
    right_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    phase TEXT NOT NULL,
    partitions INTEGER NOT NULL,
    precision_path TEXT,
    operations INTEGER,
    joined_rows INTEGER,
    last_complete_partition INTEGER,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    error_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_executions_status ON executions(status);
CREATE INDEX IF NOT EXISTS idx_executions_request ON executions(request_id);

CREATE TABLE IF NOT EXISTS partition_plans (
    execution_id TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    left_count INTEGER NOT NULL,
    right_count INTEGER NOT NULL,
    total_count INTEGER NOT NULL,
    estimated_bytes INTEGER NOT NULL,
    share REAL NOT NULL,
    skew INTEGER NOT NULL,
    hotspots_json TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    PRIMARY KEY (execution_id, partition_id)
);

CREATE TABLE IF NOT EXISTS partial_sums (
    execution_id TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    group_key INTEGER NOT NULL,
    kahan_sum_value REAL NOT NULL,
    compensation REAL NOT NULL,
    naive_sum_value REAL NOT NULL,
    count INTEGER NOT NULL,
    non_null_count INTEGER NOT NULL,
    sum_abs REAL NOT NULL,
    PRIMARY KEY (execution_id, partition_id, group_key)
);

CREATE TABLE IF NOT EXISTS aggregates (
    execution_id TEXT NOT NULL,
    group_key INTEGER NOT NULL,
    sum_value REAL NOT NULL,
    mean_value REAL,
    count INTEGER NOT NULL,
    non_null_count INTEGER NOT NULL,
    sum_abs REAL NOT NULL,
    error_bound REAL,
    PRIMARY KEY (execution_id, group_key)
);

CREATE TABLE IF NOT EXISTS precision_contracts (
    execution_id TEXT PRIMARY KEY,
    contract_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    execution_id TEXT,
    intent_type TEXT NOT NULL,
    phase TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_intents_pending ON intents(status, execution_id);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    execution_id TEXT,
    request_id TEXT NOT NULL,
    phase TEXT,
    last_complete_partition INTEGER,
    persisted_partition_count INTEGER NOT NULL,
    pending_intents_json TEXT NOT NULL,
    outcome TEXT NOT NULL,
    facts_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recovery_created ON recovery_reports(created_at);
"""


class Repository:
    def __init__(self, db_path: str | Path):
        self.db_path = str(Path(db_path).resolve())
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_lock = threading.Lock()
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def initialize(self) -> None:
        with self._init_lock:
            conn = self._connect()
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.executescript(_SCHEMA)
            finally:
                conn.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
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
    def _read(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def activate_dataset(
        self, dataset_id: str, left: Snapshot, right: Snapshot
    ) -> ActiveDataset:
        if left.side != "left" or right.side != "right":
            raise EngineError(
                "E_SNAPSHOT_INVALID",
                "left snapshot must have side=left and right snapshot must have side=right",
                {"left_side": left.side, "right_side": right.side},
            )
        pair_version = f"L{left.version}:R{right.version}"
        now = utc_now()
        with self._write() as conn:
            current = conn.execute(
                "SELECT * FROM active_datasets WHERE dataset_id = ?", (dataset_id,)
            ).fetchone()
            if current is not None:
                if left.version <= int(current["left_version"]):
                    raise EngineError(
                        "E_SNAPSHOT_INVALID",
                        "left snapshot version is not strictly monotonic",
                        {
                            "active_version": int(current["left_version"]),
                            "incoming_version": left.version,
                            "side": "left",
                        },
                    )
                if right.version <= int(current["right_version"]):
                    raise EngineError(
                        "E_SNAPSHOT_INVALID",
                        "right snapshot version is not strictly monotonic",
                        {
                            "active_version": int(current["right_version"]),
                            "incoming_version": right.version,
                            "side": "right",
                        },
                    )
            conn.execute(
                """
                INSERT INTO snapshot_history (
                    dataset_id, pair_version, left_version, right_version,
                    left_snapshot_id, right_snapshot_id, left_path, right_path,
                    left_digest, right_digest, left_generated_at, right_generated_at, loaded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    dataset_id,
                    pair_version,
                    left.version,
                    right.version,
                    left.snapshot_id,
                    right.snapshot_id,
                    str(left.path),
                    str(right.path),
                    left.content_sha256,
                    right.content_sha256,
                    left.generated_at,
                    right.generated_at,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO active_datasets (
                    dataset_id, pair_version, left_version, right_version,
                    left_snapshot_id, right_snapshot_id, left_path, right_path,
                    left_digest, right_digest, left_generated_at, right_generated_at, activated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dataset_id) DO UPDATE SET
                    pair_version=excluded.pair_version,
                    left_version=excluded.left_version,
                    right_version=excluded.right_version,
                    left_snapshot_id=excluded.left_snapshot_id,
                    right_snapshot_id=excluded.right_snapshot_id,
                    left_path=excluded.left_path,
                    right_path=excluded.right_path,
                    left_digest=excluded.left_digest,
                    right_digest=excluded.right_digest,
                    left_generated_at=excluded.left_generated_at,
                    right_generated_at=excluded.right_generated_at,
                    activated_at=excluded.activated_at
                """,
                (
                    dataset_id,
                    pair_version,
                    left.version,
                    right.version,
                    left.snapshot_id,
                    right.snapshot_id,
                    str(left.path),
                    str(right.path),
                    left.content_sha256,
                    right.content_sha256,
                    left.generated_at,
                    right.generated_at,
                    now,
                ),
            )
        dataset = self.get_active_dataset(dataset_id)
        assert dataset is not None
        return dataset

    def _active_from_row(self, row: sqlite3.Row) -> ActiveDataset:
        keys = set(row.keys())
        activated_at = row["activated_at"] if "activated_at" in keys else row["loaded_at"]
        return ActiveDataset(
            dataset_id=row["dataset_id"],
            pair_version=row["pair_version"],
            left_version=int(row["left_version"]),
            right_version=int(row["right_version"]),
            left_snapshot_id=row["left_snapshot_id"],
            right_snapshot_id=row["right_snapshot_id"],
            left_path=row["left_path"],
            right_path=row["right_path"],
            left_digest=row["left_digest"],
            right_digest=row["right_digest"],
            left_generated_at=row["left_generated_at"],
            right_generated_at=row["right_generated_at"],
            activated_at=activated_at,
        )

    def get_active_dataset(self, dataset_id: str) -> ActiveDataset | None:
        with self._read() as conn:
            row = conn.execute(
                "SELECT * FROM active_datasets WHERE dataset_id = ?", (dataset_id,)
            ).fetchone()
        return self._active_from_row(row) if row else None

    def get_dataset_pair(self, dataset_id: str, pair_version: str) -> ActiveDataset | None:
        with self._read() as conn:
            row = conn.execute(
                "SELECT * FROM snapshot_history WHERE dataset_id = ? AND pair_version = ?",
                (dataset_id, pair_version),
            ).fetchone()
        return self._active_from_row(row) if row else None

    def list_active_datasets(self) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM active_datasets ORDER BY dataset_id"
            ).fetchall()
        return [self._active_from_row(row).to_dict() for row in rows]

    def list_snapshot_history(self, dataset_id: str | None = None) -> list[dict[str, Any]]:
        with self._read() as conn:
            if dataset_id is None:
                rows = conn.execute(
                    "SELECT * FROM snapshot_history ORDER BY dataset_id, loaded_at"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM snapshot_history WHERE dataset_id = ? ORDER BY loaded_at",
                    (dataset_id,),
                ).fetchall()
        return [self._active_from_row(row).to_dict() for row in rows]

    def admit_request(
        self,
        *,
        request_id: str,
        kind: str,
        payload: dict[str, Any],
        dataset_id: str,
        pair_version: str,
    ) -> tuple[dict[str, Any], bool]:
        payload_json = _json(payload)
        payload_hash = _payload_sha(payload)
        with self._write() as conn:
            existing = conn.execute(
                "SELECT * FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["kind"] != kind
                    or existing["payload_sha256"] != payload_hash
                    or existing["dataset_id"] != dataset_id
                ):
                    raise EngineError(
                        "E_REQUEST_CONFLICT",
                        "request_id already exists with a different payload",
                        {"request_id": request_id},
                    )
                return self._request_from_row(existing), False
            conn.execute(
                """
                INSERT INTO requests (
                    request_id, kind, payload_json, payload_sha256, dataset_id,
                    pair_version, status, submitted_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'queued', ?)
                """,
                (
                    request_id,
                    kind,
                    payload_json,
                    payload_hash,
                    dataset_id,
                    pair_version,
                    utc_now(),
                ),
            )
            row = conn.execute(
                "SELECT * FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
        return self._request_from_row(row), True

    def _request_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["payload"] = json.loads(result.pop("payload_json"))
        error_json = result.pop("error_json", None)
        result["error"] = json.loads(error_json) if error_json else None
        return result

    def get_request(self, request_id: str) -> dict[str, Any] | None:
        with self._read() as conn:
            row = conn.execute(
                "SELECT * FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
        return self._request_from_row(row) if row else None

    def get_request_status(self, request_id: str) -> dict[str, Any] | None:
        request = self.get_request(request_id)
        if request is None:
            return None
        with self._read() as conn:
            counts = conn.execute(
                "SELECT status, COUNT(*) AS n FROM requests WHERE status IN ('queued','running') GROUP BY status"
            ).fetchall()
        active = {row["status"]: int(row["n"]) for row in counts}
        request["queue_semantics"] = {
            "writer_model": "single FIFO worker; shuffle/join submissions execute serially",
            "snapshot_binding": "dataset pair is pinned at submission time",
            "duplicate_behavior": "same request_id and payload waits for and reuses one execution",
            "queued_count": active.get("queued", 0),
            "running_count": active.get("running", 0),
        }
        if request.get("execution_id"):
            with self._read() as conn:
                erow = conn.execute(
                    "SELECT * FROM executions WHERE execution_id = ?",
                    (request["execution_id"],),
                ).fetchone()
            request["execution"] = dict(erow) if erow else None
        return request

    def queue_position(self, request_id: str) -> int:
        with self._read() as conn:
            row = conn.execute(
                "SELECT submitted_at FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None:
                return 0
            count = conn.execute(
                """
                SELECT COUNT(*) AS n FROM requests
                WHERE status IN ('queued','running')
                  AND (submitted_at < ? OR (submitted_at = ? AND request_id <= ?))
                """,
                (row["submitted_at"], row["submitted_at"], request_id),
            ).fetchone()
        return int(count["n"])

    def start_execution(
        self,
        *,
        request_id: str,
        execution_id: str,
        dataset: ActiveDataset,
        partitions: int,
        kind: str = "join",
    ) -> str:
        now = utc_now()
        intent_id = str(uuid.uuid4())
        with self._write() as conn:
            request = conn.execute(
                "SELECT status FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if request is None:
                raise EngineError("E_REQUEST_NOT_FOUND", "request not found")
            if request["status"] not in {"queued", "running"}:
                raise EngineError(
                    "E_REQUEST_CONFLICT",
                    "request is already terminal",
                    {"status": request["status"]},
                )
            conn.execute(
                """
                INSERT INTO executions (
                    execution_id, request_id, kind, dataset_id, pair_version,
                    left_version, right_version, status, phase, partitions, started_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'starting', 'admitted', ?, ?)
                """,
                (
                    execution_id,
                    request_id,
                    kind,
                    dataset.dataset_id,
                    dataset.pair_version,
                    dataset.left_version,
                    dataset.right_version,
                    partitions,
                    now,
                ),
            )
            conn.execute(
                "UPDATE requests SET status='running', execution_id=?, started_at=? WHERE request_id=?",
                (execution_id, now, request_id),
            )
            conn.execute(
                """
                INSERT INTO intents (
                    intent_id, request_id, execution_id, intent_type, phase,
                    status, payload_json, created_at
                ) VALUES (?, ?, ?, 'begin_execution', 'admitted', 'pending', ?, ?)
                """,
                (intent_id, request_id, execution_id, _json({"partitions": partitions}), now),
            )
        with self._write() as conn:
            conn.execute(
                "UPDATE intents SET status='completed', completed_at=? WHERE intent_id=?",
                (utc_now(), intent_id),
            )
            conn.execute(
                "UPDATE executions SET status='running', phase='shuffle' WHERE execution_id=?",
                (execution_id,),
            )
        return intent_id

    def begin_partition(self, execution_id: str, request_id: str, partition_id: int) -> str:
        now = utc_now()
        intent_id = str(uuid.uuid4())
        with self._write() as conn:
            conn.execute(
                """
                INSERT INTO intents (
                    intent_id, request_id, execution_id, intent_type, phase,
                    status, payload_json, created_at
                ) VALUES (?, ?, ?, 'shuffle_partition', 'shuffle', 'pending', ?, ?)
                """,
                (
                    intent_id,
                    request_id,
                    execution_id,
                    _json({"partition_id": partition_id}),
                    now,
                ),
            )
            conn.execute(
                "UPDATE executions SET phase='shuffle' WHERE execution_id=?",
                (execution_id,),
            )
        return intent_id

    def complete_partition(
        self,
        *,
        execution_id: str,
        request_id: str,
        partition_id: int,
        plan: dict[str, Any],
        partials: list[dict[str, Any]],
    ) -> None:
        now = utc_now()
        with self._write() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO partition_plans (
                    execution_id, partition_id, left_count, right_count, total_count,
                    estimated_bytes, share, skew, hotspots_json, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    execution_id,
                    partition_id,
                    int(plan["left_count"]),
                    int(plan["right_count"]),
                    int(plan["total_count"]),
                    int(plan["estimated_bytes"]),
                    float(plan["share"]),
                    1 if plan["skew"] else 0,
                    _json(plan.get("hotspot_keys", [])),
                    now,
                ),
            )
            conn.execute(
                "DELETE FROM partial_sums WHERE execution_id=? AND partition_id=?",
                (execution_id, partition_id),
            )
            for partial in partials:
                conn.execute(
                    """
                    INSERT INTO partial_sums (
                        execution_id, partition_id, group_key, kahan_sum_value,
                        compensation, naive_sum_value, count, non_null_count, sum_abs
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution_id,
                        partition_id,
                        int(partial["group_key"]),
                        float(partial["kahan_sum_value"]),
                        float(partial["compensation"]),
                        float(partial["naive_sum_value"]),
                        int(partial["count"]),
                        int(partial["non_null_count"]),
                        float(partial["sum_abs"]),
                    ),
                )
            conn.execute(
                """
                UPDATE intents SET status='completed', completed_at=?
                WHERE execution_id=? AND intent_type='shuffle_partition'
                  AND status='pending'
                  AND json_extract(payload_json, '$.partition_id') = ?
                """,
                (now, execution_id, partition_id),
            )
            conn.execute(
                """
                UPDATE executions
                SET last_complete_partition = CASE
                    WHEN last_complete_partition IS NULL OR last_complete_partition < ? THEN ?
                    ELSE last_complete_partition END,
                    phase='shuffle'
                WHERE execution_id=?
                """,
                (partition_id, partition_id, execution_id),
            )

    def begin_merge(self, execution_id: str, request_id: str, requested_path: str) -> str:
        now = utc_now()
        intent_id = str(uuid.uuid4())
        with self._write() as conn:
            conn.execute(
                """
                INSERT INTO intents (
                    intent_id, request_id, execution_id, intent_type, phase,
                    status, payload_json, created_at
                ) VALUES (?, ?, ?, 'final_merge', 'merge', 'pending', ?, ?)
                """,
                (intent_id, request_id, execution_id, _json({"path": requested_path}), now),
            )
            conn.execute(
                "UPDATE executions SET phase='merge' WHERE execution_id=?",
                (execution_id,),
            )
        return intent_id

    def commit_execution(
        self,
        *,
        execution_id: str,
        request_id: str,
        merge_intent_id: str | None,
        result: ExecutionResult,
        kind: str,
    ) -> None:
        now = utc_now()
        with self._write() as conn:
            for aggregate in result.aggregates:
                payload = aggregate.to_dict()
                conn.execute(
                    """
                    INSERT INTO aggregates (
                        execution_id, group_key, sum_value, mean_value, count,
                        non_null_count, sum_abs, error_bound
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        execution_id,
                        aggregate.group_key,
                        aggregate.sum_value,
                        aggregate.mean_value,
                        aggregate.count,
                        aggregate.non_null_count,
                        aggregate.sum_abs,
                        _finite(aggregate.error_bound),
                    ),
                )
            conn.execute(
                "INSERT OR REPLACE INTO precision_contracts (execution_id, contract_json, created_at) VALUES (?, ?, ?)",
                (execution_id, _json(result.precision_contract), now),
            )
            if merge_intent_id:
                conn.execute(
                    "UPDATE intents SET status='completed', completed_at=? WHERE intent_id=?",
                    (now, merge_intent_id),
                )
            conn.execute(
                """
                UPDATE executions SET status='committed', phase='complete',
                    precision_path=?, operations=?, joined_rows=?, finished_at=?
                WHERE execution_id=?
                """,
                (
                    result.selected_path,
                    result.operations,
                    result.joined_rows,
                    now,
                    execution_id,
                ),
            )
            updated = conn.execute(
                "UPDATE requests SET status='committed', finished_at=? WHERE request_id=? AND status='running'",
                (now, request_id),
            )
            if updated.rowcount != 1:
                raise EngineError(
                    "E_REQUEST_CONFLICT",
                    "request left running state before final commit",
                    {"request_id": request_id},
                )

    def fail_request(
        self,
        *,
        request_id: str,
        execution_id: str | None,
        error: EngineError,
    ) -> None:
        now = utc_now()
        error_json = _json(error.to_dict())
        with self._write() as conn:
            request = conn.execute(
                "SELECT status FROM requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if request is not None and request["status"] not in {"committed", "aborted"}:
                conn.execute(
                    "UPDATE requests SET status='failed', finished_at=?, error_json=? WHERE request_id=?",
                    (now, error_json, request_id),
                )
            if execution_id:
                conn.execute(
                    """
                    UPDATE executions SET status='failed', phase='failed', finished_at=?, error_json=?
                    WHERE execution_id=? AND status NOT IN ('committed','aborted')
                    """,
                    (now, error_json, execution_id),
                )
                conn.execute(
                    """
                    UPDATE intents SET status='failed', completed_at=?
                    WHERE execution_id=? AND status='pending'
                    """,
                    (now, execution_id),
                )

    def get_partitions(self, request_id: str) -> dict[str, Any] | None:
        request = self.get_request(request_id)
        if request is None:
            return None
        if request["status"] != "committed":
            raise self._not_committed(request, "partition statistics")
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM partition_plans WHERE execution_id=? ORDER BY partition_id",
                (request["execution_id"],),
            ).fetchall()
            execution = conn.execute(
                "SELECT * FROM executions WHERE execution_id=?",
                (request["execution_id"],),
            ).fetchone()
        partitions = []
        for row in rows:
            item = dict(row)
            item["skew"] = bool(item["skew"])
            item["hotspot_keys"] = json.loads(item.pop("hotspots_json"))
            partitions.append(item)
        return {
            "request_id": request_id,
            "dataset_id": request["dataset_id"],
            "pair_version": request["pair_version"],
            "execution_id": request["execution_id"],
            "selected_path": execution["precision_path"] if execution else None,
            "partitions": partitions,
        }

    def get_aggregates(self, request_id: str) -> dict[str, Any] | None:
        request = self.get_request(request_id)
        if request is None:
            return None
        if request["status"] != "committed":
            raise self._not_committed(request, "aggregate results")
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM aggregates WHERE execution_id=? ORDER BY group_key",
                (request["execution_id"],),
            ).fetchall()
            execution = conn.execute(
                "SELECT * FROM executions WHERE execution_id=?",
                (request["execution_id"],),
            ).fetchone()
        return {
            "request_id": request_id,
            "dataset_id": request["dataset_id"],
            "pair_version": request["pair_version"],
            "execution_id": request["execution_id"],
            "selected_path": execution["precision_path"] if execution else None,
            "operations": execution["operations"] if execution else None,
            "joined_rows": execution["joined_rows"] if execution else None,
            "results": [dict(row) for row in rows],
        }

    def get_precision_contract(self, request_id: str) -> dict[str, Any] | None:
        request = self.get_request(request_id)
        if request is None:
            return None
        if request["status"] != "committed":
            raise self._not_committed(request, "precision contract")
        with self._read() as conn:
            row = conn.execute(
                "SELECT contract_json, created_at FROM precision_contracts WHERE execution_id=?",
                (request["execution_id"],),
            ).fetchone()
        if row is None:
            raise EngineError(
                "E_REQUEST_NOT_FOUND",
                "precision contract not found for committed request",
                {"request_id": request_id},
            )
        return {
            "request_id": request_id,
            "execution_id": request["execution_id"],
            "pair_version": request["pair_version"],
            "created_at": row["created_at"],
            "contract": json.loads(row["contract_json"]),
        }

    def get_partials(self, request_id: str) -> dict[str, Any] | None:
        request = self.get_request(request_id)
        if request is None:
            return None
        if request["status"] != "committed":
            raise self._not_committed(request, "partial sums")
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM partial_sums WHERE execution_id=? ORDER BY partition_id, group_key",
                (request["execution_id"],),
            ).fetchall()
        return {
            "request_id": request_id,
            "execution_id": request["execution_id"],
            "partial_sums": [dict(row) for row in rows],
        }

    def recover_incomplete(self) -> list[dict[str, Any]]:
        reports: list[dict[str, Any]] = []
        now = utc_now()
        with self._write() as conn:
            executions = conn.execute(
                "SELECT * FROM executions WHERE status IN ('starting','running') ORDER BY started_at"
            ).fetchall()
            for execution in executions:
                execution_id = execution["execution_id"]
                request_id = execution["request_id"]
                plans = conn.execute(
                    "SELECT partition_id FROM partition_plans WHERE execution_id=? ORDER BY partition_id",
                    (execution_id,),
                ).fetchall()
                persisted = [int(row["partition_id"]) for row in plans]
                last_complete = max(persisted) if persisted else None
                pending_rows = conn.execute(
                    "SELECT intent_type, phase, payload_json FROM intents WHERE execution_id=? AND status='pending' ORDER BY created_at",
                    (execution_id,),
                ).fetchall()
                pending = [
                    {
                        "intent_type": row["intent_type"],
                        "phase": row["phase"],
                        "payload": json.loads(row["payload_json"]),
                    }
                    for row in pending_rows
                ]
                error = EngineError(
                    "E_EXECUTION_ABORTED",
                    "execution was interrupted and marked aborted during startup recovery",
                    {
                        "execution_id": execution_id,
                        "last_complete_partition": last_complete,
                        "persisted_partition_count": len(persisted),
                    },
                )
                conn.execute(
                    "UPDATE executions SET status='aborted', phase='recovered', finished_at=?, error_json=? WHERE execution_id=?",
                    (now, _json(error.to_dict()), execution_id),
                )
                conn.execute(
                    "UPDATE requests SET status='aborted', finished_at=?, error_json=? WHERE request_id=? AND status IN ('queued','running')",
                    (now, _json(error.to_dict()), request_id),
                )
                conn.execute(
                    "UPDATE intents SET status='aborted', completed_at=? WHERE execution_id=? AND status='pending'",
                    (now, execution_id),
                )
                facts = {
                    "execution_status_before": execution["status"],
                    "execution_phase_before": execution["phase"],
                    "partitions_requested": int(execution["partitions"]),
                    "persisted_partition_ids": persisted,
                    "last_complete_partition": last_complete,
                    "pending_intents": pending,
                    "outcome_basis": "only committed SQLite rows and pending intents were used",
                    "exit_report_complete": False,
                }
                report_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO recovery_reports (
                        report_id, execution_id, request_id, phase,
                        last_complete_partition, persisted_partition_count,
                        pending_intents_json, outcome, facts_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'aborted', ?, ?)
                    """,
                    (
                        report_id,
                        execution_id,
                        request_id,
                        execution["phase"],
                        last_complete,
                        len(persisted),
                        _json(pending),
                        _json(facts),
                        now,
                    ),
                )
                reports.append(self._recovery_from_values(report_id, execution_id, request_id, execution["phase"], last_complete, len(persisted), pending, facts, now))
            queued = conn.execute(
                """
                SELECT r.* FROM requests r
                LEFT JOIN executions e ON e.request_id = r.request_id
                WHERE r.status='queued' AND e.execution_id IS NULL
                ORDER BY r.submitted_at
                """
            ).fetchall()
            for request in queued:
                error = EngineError(
                    "E_EXECUTION_ABORTED",
                    "queued request was not started before the previous process stopped",
                    {"request_id": request["request_id"]},
                )
                conn.execute(
                    "UPDATE requests SET status='aborted', finished_at=?, error_json=? WHERE request_id=?",
                    (now, _json(error.to_dict()), request["request_id"]),
                )
                facts = {
                    "request_status_before": "queued",
                    "persisted_partition_ids": [],
                    "last_complete_partition": None,
                    "pending_intents": [],
                    "outcome_basis": "request row only; no execution intent was persisted",
                    "exit_report_complete": False,
                }
                report_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO recovery_reports (
                        report_id, execution_id, request_id, phase,
                        last_complete_partition, persisted_partition_count,
                        pending_intents_json, outcome, facts_json, created_at
                    ) VALUES (?, NULL, ?, 'queued', NULL, 0, '[]', 'aborted_queued', ?, ?)
                    """,
                    (report_id, request["request_id"], _json(facts), now),
                )
                reports.append(self._recovery_from_values(report_id, None, request["request_id"], "queued", None, 0, [], facts, now))
        return reports

    def _recovery_from_values(
        self,
        report_id: str,
        execution_id: str | None,
        request_id: str,
        phase: str | None,
        last_complete: int | None,
        persisted_count: int,
        pending: list[dict[str, Any]],
        facts: dict[str, Any],
        created_at: str,
    ) -> dict[str, Any]:
        return {
            "report_id": report_id,
            "execution_id": execution_id,
            "request_id": request_id,
            "phase": phase,
            "last_complete_partition": last_complete,
            "persisted_partition_count": persisted_count,
            "pending_intents": pending,
            "outcome": "aborted",
            "facts": facts,
            "created_at": created_at,
            "note": (
                "Recovery is inferred only from persisted SQLite facts. No complete process exit "
                "report is claimed after forced termination."
            ),
        }

    def get_recovery_reports(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM recovery_reports ORDER BY created_at DESC, report_id DESC LIMIT ?",
                (max(1, int(limit)),),
            ).fetchall()
        reports = []
        for row in rows:
            reports.append(
                {
                    "report_id": row["report_id"],
                    "execution_id": row["execution_id"],
                    "request_id": row["request_id"],
                    "phase": row["phase"],
                    "last_complete_partition": row["last_complete_partition"],
                    "persisted_partition_count": int(row["persisted_partition_count"]),
                    "pending_intents": json.loads(row["pending_intents_json"]),
                    "outcome": row["outcome"],
                    "facts": json.loads(row["facts_json"]),
                    "created_at": row["created_at"],
                    "note": (
                        "Recovery is inferred only from persisted SQLite facts. No complete "
                        "process exit report is claimed after forced termination."
                    ),
                }
            )
        return reports

    @staticmethod
    def _not_committed(request: dict[str, Any], resource: str) -> EngineError:
        if request.get("error"):
            return EngineError.from_stored(request["error"])
        return EngineError(
            "E_REQUEST_NOT_COMMITTED",
            f"request has not committed {resource}",
            {"request_id": request["request_id"], "status": request["status"]},
        )

    def count_executions(self, request_id: str) -> int:
        with self._read() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM executions WHERE request_id=?", (request_id,)
            ).fetchone()
        return int(row["n"])

    def database_facts(self) -> dict[str, Any]:
        with self._read() as conn:
            tables = [
                "snapshot_history",
                "active_datasets",
                "requests",
                "executions",
                "partition_plans",
                "partial_sums",
                "aggregates",
                "intents",
                "recovery_reports",
            ]
            counts = {
                table: int(conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"])
                for table in tables
            }
            journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        return {"database": self.db_path, "journal_mode": journal_mode, "row_counts": counts}


def _finite(value: float | None) -> float | None:
    if value is None:
        return None
    value = float(value)
    return value if value == value and value not in {float("inf"), float("-inf")} else None
