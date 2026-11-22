"""SQLite persistence with write-ahead intent records and factual recovery."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .models import BatchSpec, PlacementFact, ResourceVector
from .spec import canonical_json

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    nodes_json TEXT NOT NULL,
    pods_json TEXT NOT NULL,
    budget_json TEXT NOT NULL,
    original_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pod_states (
    batch_id TEXT NOT NULL,
    pod_name TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (batch_id, pod_name)
);

CREATE TABLE IF NOT EXISTS intents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status);
CREATE INDEX IF NOT EXISTS idx_intents_batch ON intents(batch_id, id);

CREATE TABLE IF NOT EXISTS rounds (
    batch_id TEXT NOT NULL,
    round_no INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    budget_json TEXT NOT NULL,
    spent_ms INTEGER,
    concurrency_used INTEGER,
    skipped_json TEXT,
    result_json TEXT,
    PRIMARY KEY (batch_id, round_no)
);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    round_no INTEGER NOT NULL,
    pod_name TEXT NOT NULL,
    node_name TEXT,
    phase TEXT NOT NULL,
    constraint_type TEXT NOT NULL,
    constraint_text TEXT,
    remaining_snapshot_json TEXT,
    detail_json TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_batch_pod ON decisions(batch_id, pod_name, id);
CREATE INDEX IF NOT EXISTS idx_decisions_batch_node ON decisions(batch_id, node_name, id);

CREATE TABLE IF NOT EXISTS placements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    round_no INTEGER NOT NULL,
    seq_no INTEGER NOT NULL,
    pod_name TEXT NOT NULL,
    node_name TEXT NOT NULL,
    requests_json TEXT NOT NULL,
    remaining_before_json TEXT NOT NULL,
    remaining_after_json TEXT NOT NULL,
    score INTEGER NOT NULL,
    score_breakdown_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (batch_id, pod_name),
    UNIQUE (batch_id, seq_no)
);
CREATE INDEX IF NOT EXISTS idx_placements_batch_node ON placements(batch_id, node_name, id);

CREATE TABLE IF NOT EXISTS recoveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    target_intent_id INTEGER NOT NULL,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_recoveries_batch ON recoveries(batch_id, id);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class Storage:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.executescript(SCHEMA)
            connection.commit()
        finally:
            connection.close()

    def create_intent(self, batch_id: str, operation: str, payload: dict[str, Any]) -> int:
        now = utc_now()
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                INSERT INTO intents(batch_id, operation, status, payload_json, created_at, updated_at)
                VALUES (?, ?, 'PENDING', ?, ?, ?)
                """,
                (batch_id, operation, canonical_json(payload), now, now),
            )
            return int(cursor.lastrowid)

    def insert_batch(
        self,
        spec: BatchSpec,
        original_payload: dict[str, Any],
        intent_id: int,
    ) -> None:
        now = utc_now()
        stored_payload = dict(original_payload)
        stored_payload["batch_id"] = spec.batch_id
        stored_payload["budget"] = spec.budget.to_dict()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO batches(
                    batch_id, status, created_at, updated_at, nodes_json, pods_json,
                    budget_json, original_json
                ) VALUES (?, 'PENDING', ?, ?, ?, ?, ?, ?)
                """,
                (
                    spec.batch_id,
                    now,
                    now,
                    canonical_json(stored_payload.get("nodes", [])),
                    canonical_json(stored_payload.get("pods", [])),
                    canonical_json(spec.budget.to_dict()),
                    canonical_json(stored_payload),
                ),
            )
            connection.executemany(
                """
                INSERT INTO pod_states(batch_id, pod_name, status, reason, updated_at)
                VALUES (?, ?, 'PENDING', NULL, ?)
                """,
                [(spec.batch_id, pod.name, now) for pod in spec.pods],
            )
            connection.execute(
                "UPDATE intents SET status = 'COMMITTED', updated_at = ? WHERE id = ?",
                (now, intent_id),
            )

    def batch_exists(self, batch_id: str) -> bool:
        with self.read() as connection:
            row = connection.execute(
                "SELECT 1 FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            return row is not None

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["original_payload"] = json.loads(result.pop("original_json"))
            result["budget"] = json.loads(result.pop("budget_json"))
            result["nodes_json"] = json.loads(result["nodes_json"])
            result["pods_json"] = json.loads(result["pods_json"])
            return result

    def list_batches(self) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT batch_id, status, created_at, updated_at FROM batches ORDER BY created_at, batch_id"
            ).fetchall()
            return [dict(row) for row in rows]

    def create_round(self, batch_id: str, round_no: int, budget: dict[str, int]) -> None:
        now = utc_now()
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO rounds(batch_id, round_no, status, started_at, budget_json)
                VALUES (?, ?, 'RUNNING', ?, ?)
                """,
                (batch_id, round_no, now, canonical_json(budget)),
            )
            connection.execute(
                "UPDATE batches SET status = 'RUNNING', updated_at = ? WHERE batch_id = ?",
                (now, batch_id),
            )

    def next_round_no(self, batch_id: str) -> int:
        with self.read() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(round_no), 0) + 1 AS next_no FROM rounds WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            return int(row["next_no"])

    def get_pod_states(self, batch_id: str) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT * FROM pod_states WHERE batch_id = ? ORDER BY pod_name", (batch_id,)
            ).fetchall()
            return [dict(row) for row in rows]

    def get_pod_states_map(self, batch_id: str) -> dict[str, dict[str, Any]]:
        return {row["pod_name"]: row for row in self.get_pod_states(batch_id)}

    def get_placements(self, batch_id: str) -> list[PlacementFact]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT * FROM placements WHERE batch_id = ? ORDER BY seq_no, id", (batch_id,)
            ).fetchall()
        result: list[PlacementFact] = []
        for row in rows:
            requests = json.loads(row["requests_json"])
            before = json.loads(row["remaining_before_json"])
            after = json.loads(row["remaining_after_json"])
            result.append(
                PlacementFact(
                    placement_id=int(row["id"]),
                    batch_id=row["batch_id"],
                    round_no=int(row["round_no"]),
                    sequence=int(row["seq_no"]),
                    pod_name=row["pod_name"],
                    node_name=row["node_name"],
                    requests=ResourceVector(
                        int(requests["cpu_millicores"]), int(requests["memory_bytes"])
                    ),
                    remaining_before=ResourceVector(
                        int(before["cpu_millicores"]), int(before["memory_bytes"])
                    ),
                    remaining_after=ResourceVector(
                        int(after["cpu_millicores"]), int(after["memory_bytes"])
                    ),
                    score=int(row["score"]),
                    score_breakdown=json.loads(row["score_breakdown_json"]),
                )
            )
        return result

    def next_placement_sequence(self, batch_id: str) -> int:
        with self.read() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(seq_no), 0) + 1 AS next_seq FROM placements WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            return int(row["next_seq"])

    def commit_pod_decision(
        self,
        *,
        batch_id: str,
        round_no: int,
        pod_name: str,
        decisions: list[dict[str, Any]],
        placement: dict[str, Any] | None,
        pod_status: str,
        pod_reason: str | None,
    ) -> None:
        """Atomically persist one Pod's denials, verdict, and placement fact."""

        now = utc_now()
        with self.transaction() as connection:
            for decision in decisions:
                connection.execute(
                    """
                    INSERT INTO decisions(
                        batch_id, round_no, pod_name, node_name, phase, constraint_type,
                        constraint_text, remaining_snapshot_json, detail_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        round_no,
                        pod_name,
                        decision.get("node_name"),
                        decision.get("phase", "rejection"),
                        decision["constraint_type"],
                        decision.get("constraint_text"),
                        canonical_json(decision.get("remaining_snapshot"))
                        if decision.get("remaining_snapshot") is not None
                        else None,
                        canonical_json(decision.get("detail")) if decision.get("detail") is not None else None,
                        now,
                    ),
                )
            if placement is not None:
                connection.execute(
                    """
                    INSERT INTO placements(
                        batch_id, round_no, seq_no, pod_name, node_name, requests_json,
                        remaining_before_json, remaining_after_json, score,
                        score_breakdown_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        round_no,
                        placement["seq_no"],
                        pod_name,
                        placement["node_name"],
                        canonical_json(placement["requests"].to_dict()),
                        canonical_json(placement["remaining_before"].to_dict()),
                        canonical_json(placement["remaining_after"].to_dict()),
                        placement["score"],
                        canonical_json(placement["score_breakdown"]),
                        now,
                    ),
                )
            connection.execute(
                """
                INSERT INTO pod_states(batch_id, pod_name, status, reason, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(batch_id, pod_name) DO UPDATE SET
                    status = excluded.status,
                    reason = excluded.reason,
                    updated_at = excluded.updated_at
                """,
                (batch_id, pod_name, pod_status, pod_reason, now),
            )

    def finalize_round(
        self,
        *,
        batch_id: str,
        round_no: int,
        intent_id: int,
        spent_ms: int,
        concurrency_used: int,
        skipped: list[dict[str, Any]],
        result: dict[str, Any],
        batch_status: str,
    ) -> None:
        now = utc_now()
        with self.transaction() as connection:
            connection.execute(
                """
                UPDATE rounds SET status = 'COMPLETED', completed_at = ?, spent_ms = ?,
                    concurrency_used = ?, skipped_json = ?, result_json = ?
                WHERE batch_id = ? AND round_no = ?
                """,
                (
                    now,
                    spent_ms,
                    concurrency_used,
                    canonical_json(skipped),
                    canonical_json(result),
                    batch_id,
                    round_no,
                ),
            )
            connection.execute(
                "UPDATE batches SET status = ?, updated_at = ? WHERE batch_id = ?",
                (batch_status, now, batch_id),
            )
            connection.execute(
                "UPDATE intents SET status = 'COMMITTED', updated_at = ? WHERE id = ?",
                (now, intent_id),
            )

    def abort_batch(self, batch_id: str, intent_id: int, reason: str) -> None:
        now = utc_now()
        with self.transaction() as connection:
            connection.execute(
                "UPDATE batches SET status = 'ABORTED', updated_at = ? WHERE batch_id = ?",
                (now, batch_id),
            )
            connection.execute(
                "UPDATE rounds SET status = 'ABORTED', completed_at = ? WHERE batch_id = ? AND status = 'RUNNING'",
                (now, batch_id),
            )
            for row in connection.execute(
                "SELECT pod_name FROM pod_states WHERE batch_id = ? AND status IN ('PENDING', 'DEFERRED')",
                (batch_id,),
            ).fetchall():
                connection.execute(
                    "UPDATE pod_states SET status = 'ABORTED', reason = ?, updated_at = ? WHERE batch_id = ? AND pod_name = ?",
                    (reason, now, batch_id, row["pod_name"]),
                )
            connection.execute(
                "UPDATE intents SET status = 'COMMITTED', updated_at = ? WHERE id = ?",
                (now, intent_id),
            )

    def get_rounds(self, batch_id: str) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT * FROM rounds WHERE batch_id = ? ORDER BY round_no", (batch_id,)
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["budget"] = json.loads(item.pop("budget_json"))
            item["skipped"] = json.loads(item["skipped_json"]) if item["skipped_json"] else []
            item["result"] = json.loads(item["result_json"]) if item["result_json"] else None
            item.pop("skipped_json", None)
            item.pop("result_json", None)
            result.append(item)
        return result

    def get_decisions(
        self, batch_id: str, pod_name: str | None = None
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM decisions WHERE batch_id = ?"
        params: list[Any] = [batch_id]
        if pod_name is not None:
            query += " AND pod_name = ?"
            params.append(pod_name)
        query += " ORDER BY id"
        with self.read() as connection:
            rows = connection.execute(query, params).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            remaining_json = item.pop("remaining_snapshot_json")
            detail_json = item.pop("detail_json")
            item["remaining_snapshot"] = json.loads(remaining_json) if remaining_json else None
            item["detail"] = json.loads(detail_json) if detail_json else None
            result.append(item)
        return result

    def find_pending_intents(self) -> list[dict[str, Any]]:
        with self.read() as connection:
            rows = connection.execute(
                "SELECT * FROM intents WHERE status = 'PENDING' ORDER BY id"
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["payload"] = json.loads(item["payload_json"])
                result.append(item)
            return result

    def get_intent(self, intent_id: int) -> dict[str, Any] | None:
        with self.read() as connection:
            row = connection.execute("SELECT * FROM intents WHERE id = ?", (intent_id,)).fetchone()
            if row is None:
                return None
            item = dict(row)
            item["payload"] = json.loads(item["payload_json"])
            return item

    def apply_recovery(
        self,
        *,
        batch_id: str,
        target_intent_id: int,
        report: dict[str, Any],
        batch_status_after: str,
    ) -> int:
        now = utc_now()
        with self.transaction() as connection:
            recovery_cursor = connection.execute(
                """
                INSERT INTO intents(batch_id, operation, status, payload_json, created_at, updated_at)
                VALUES (?, 'RECOVER_BATCH', 'PENDING', ?, ?, ?)
                """,
                (batch_id, canonical_json({"target_intent_id": target_intent_id}), now, now),
            )
            recovery_intent_id = int(recovery_cursor.lastrowid)
            connection.execute(
                "UPDATE intents SET status = 'RECOVERED', updated_at = ? WHERE id = ?",
                (now, target_intent_id),
            )
            batch_exists = connection.execute(
                "SELECT 1 FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch_exists is not None:
                connection.execute(
                    "UPDATE batches SET status = ?, updated_at = ? WHERE batch_id = ?",
                    (batch_status_after, now, batch_id),
                )
                if batch_status_after == "ABORTED":
                    connection.execute(
                        "UPDATE rounds SET status = 'ABORTED', completed_at = ? WHERE batch_id = ? AND status = 'RUNNING'",
                        (now, batch_id),
                    )
                    connection.execute(
                        """
                        UPDATE pod_states SET status = 'ABORTED', reason = 'recovered abort intent', updated_at = ?
                        WHERE batch_id = ? AND status IN ('PENDING', 'DEFERRED')
                        """,
                        (now, batch_id),
                    )
                else:
                    connection.execute(
                        "UPDATE rounds SET status = 'INTERRUPTED', completed_at = ? WHERE batch_id = ? AND status = 'RUNNING'",
                        (now, batch_id),
                    )
            report_to_store = dict(report)
            report_to_store["recovery_intent_id"] = recovery_intent_id
            report_to_store["persisted_at"] = now
            connection.execute(
                """
                INSERT INTO recoveries(batch_id, target_intent_id, report_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (batch_id, target_intent_id, canonical_json(report_to_store), now),
            )
            connection.execute(
                "UPDATE intents SET status = 'COMMITTED', updated_at = ? WHERE id = ?",
                (now, recovery_intent_id),
            )
            return recovery_intent_id

    def get_recoveries(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        query = "SELECT * FROM recoveries"
        params: tuple[Any, ...] = ()
        if batch_id is not None:
            query += " WHERE batch_id = ?"
            params = (batch_id,)
        query += " ORDER BY id"
        with self.read() as connection:
            rows = connection.execute(query, params).fetchall()
        return [
            {
                "id": int(row["id"]),
                "batch_id": row["batch_id"],
                "target_intent_id": int(row["target_intent_id"]),
                "report": json.loads(row["report_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_counts(self, batch_id: str) -> dict[str, int]:
        with self.read() as connection:
            placements = int(
                connection.execute(
                    "SELECT COUNT(*) FROM placements WHERE batch_id = ?", (batch_id,)
                ).fetchone()[0]
            )
            rejections = int(
                connection.execute(
                    "SELECT COUNT(*) FROM decisions WHERE batch_id = ? AND phase = 'rejection'",
                    (batch_id,),
                ).fetchone()[0]
            )
            pending_intents = int(
                connection.execute(
                    "SELECT COUNT(*) FROM intents WHERE batch_id = ? AND status = 'PENDING'",
                    (batch_id,),
                ).fetchone()[0]
            )
            return {
                "placements": placements,
                "rejections": rejections,
                "pending_intents": pending_intents,
            }

