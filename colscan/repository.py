from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

from .errors import error
from .snapshot import Snapshot


ACTIVE_QUERY_STATUSES = ("PENDING", "RUNNING", "RECOVERING")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def compact_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def json_hash(value: Any) -> str:
    return hashlib.sha256(compact_json(value).encode("utf-8")).hexdigest()


SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS snapshots (
        version INTEGER PRIMARY KEY,
        checksum TEXT NOT NULL,
        generated_at TEXT NOT NULL,
        loaded_at TEXT NOT NULL,
        document_json TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS active_snapshot (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        version INTEGER NOT NULL,
        generation INTEGER NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (version) REFERENCES snapshots(version)
    )
    """,
    "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    """
    CREATE TABLE IF NOT EXISTS queries (
        query_id TEXT PRIMARY KEY,
        request_hash TEXT NOT NULL,
        request_json TEXT NOT NULL,
        snapshot_version INTEGER NOT NULL,
        status TEXT NOT NULL,
        error_json TEXT,
        created_at TEXT NOT NULL,
        started_at TEXT,
        completed_at TEXT,
        commit_generation INTEGER NOT NULL DEFAULT 0,
        total_blocks INTEGER NOT NULL,
        committed_blocks INTEGER NOT NULL DEFAULT 0,
        result_json TEXT,
        total_matched INTEGER,
        window_offset INTEGER NOT NULL DEFAULT 0,
        window_limit INTEGER,
        FOREIGN KEY (snapshot_version) REFERENCES snapshots(version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS scan_shards (
        query_id TEXT NOT NULL,
        block_seq INTEGER NOT NULL,
        block_id TEXT NOT NULL,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        intent_hash TEXT,
        intent_at TEXT,
        result_hash TEXT,
        committed_at TEXT,
        reused INTEGER NOT NULL DEFAULT 0,
        recovery_count INTEGER NOT NULL DEFAULT 0,
        error_json TEXT,
        PRIMARY KEY (query_id, block_seq),
        FOREIGN KEY (query_id) REFERENCES queries(query_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shard_results (
        query_id TEXT NOT NULL,
        block_seq INTEGER NOT NULL,
        result_json TEXT NOT NULL,
        result_hash TEXT NOT NULL,
        row_count INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (query_id, block_seq),
        FOREIGN KEY (query_id, block_seq) REFERENCES scan_shards(query_id, block_seq)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS budget_decisions (
        decision_seq INTEGER PRIMARY KEY AUTOINCREMENT,
        query_id TEXT NOT NULL,
        round_no INTEGER NOT NULL,
        phase TEXT NOT NULL,
        action TEXT NOT NULL,
        block_seq INTEGER NOT NULL,
        work_id TEXT NOT NULL,
        detail_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY (query_id) REFERENCES queries(query_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS recovery_reports (
        report_id INTEGER PRIMARY KEY AUTOINCREMENT,
        recovered_at TEXT NOT NULL,
        process_instance TEXT NOT NULL,
        query_id TEXT NOT NULL,
        query_status_before TEXT NOT NULL,
        query_status_after TEXT NOT NULL,
        reused_shards INTEGER NOT NULL,
        resumed_shards INTEGER NOT NULL,
        pending_shards INTEGER NOT NULL,
        evidence TEXT NOT NULL,
        details_json TEXT NOT NULL,
        FOREIGN KEY (query_id) REFERENCES queries(query_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_queries_status ON queries(status)",
    "CREATE INDEX IF NOT EXISTS idx_shards_status ON scan_shards(query_id, status)",
    "CREATE INDEX IF NOT EXISTS idx_decisions_query ON budget_decisions(query_id, decision_seq)",
)


class StateStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def initialize(self) -> None:
        with self.connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            for statement in SCHEMA:
                conn.execute(statement)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.path), timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=FULL")
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    def install_snapshot(self, snapshot: Snapshot) -> dict[str, Any]:
        installed_at = utc_now()
        document_json = compact_json(snapshot.document)
        with self.transaction() as conn:
            active = conn.execute(
                "SELECT s.version, s.checksum, a.generation FROM active_snapshot a "
                "JOIN snapshots s ON s.version=a.version WHERE a.singleton=1"
            ).fetchone()
            current_version = int(active["version"]) if active else 0
            current_checksum = str(active["checksum"]) if active else None
            if snapshot.version <= current_version:
                raise error(
                    "E_VERSION_RETIRED",
                    "snapshot version must be strictly greater than the active version",
                    http_status=409,
                    details={
                        "requested_version": snapshot.version,
                        "active_version": current_version,
                        "active_checksum": current_checksum,
                        "requested_checksum": snapshot.checksum,
                    },
                )
            existing = conn.execute("SELECT checksum FROM snapshots WHERE version=?", (snapshot.version,)).fetchone()
            if existing is not None:
                raise error(
                    "E_SNAPSHOT_INVALID",
                    "snapshot version already exists in history",
                    details={"version": snapshot.version, "checksum": snapshot.checksum},
                )
            conn.execute(
                "INSERT INTO snapshots(version, checksum, generated_at, loaded_at, document_json) VALUES (?, ?, ?, ?, ?)",
                (snapshot.version, snapshot.checksum, snapshot.generated_at, installed_at, document_json),
            )
            generation = int(active["generation"]) + 1 if active else 1
            conn.execute(
                "INSERT INTO active_snapshot(singleton, version, generation, updated_at) VALUES (1, ?, ?, ?) "
                "ON CONFLICT(singleton) DO UPDATE SET version=excluded.version, generation=excluded.generation, updated_at=excluded.updated_at",
                (snapshot.version, generation, installed_at),
            )
        return {
            "version": snapshot.version,
            "checksum": snapshot.checksum,
            "generated_at": snapshot.generated_at,
            "loaded_at": installed_at,
            "generation": generation,
        }

    def active_snapshot_info(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT s.version, s.checksum, s.generated_at, s.loaded_at, a.generation, s.document_json "
                "FROM active_snapshot a JOIN snapshots s ON s.version=a.version WHERE a.singleton=1"
            ).fetchone()
        if row is None:
            return None
        return {
            "version": int(row["version"]),
            "checksum": row["checksum"],
            "generated_at": row["generated_at"],
            "loaded_at": row["loaded_at"],
            "generation": int(row["generation"]),
            "document": json.loads(row["document_json"]),
        }

    def active_snapshot_document(self) -> tuple[int, dict[str, Any]]:
        info = self.active_snapshot_info()
        if info is None:
            raise error(
                "E_SNAPSHOT_INVALID",
                "no active snapshot is loaded",
                http_status=409,
                details={"reason": "no_active_snapshot"},
            )
        return int(info["version"]), info["document"]

    def snapshot_document(self, version: int) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute("SELECT document_json FROM snapshots WHERE version=?", (version,)).fetchone()
        if row is None:
            raise error(
                "E_VERSION_RETIRED",
                "requested snapshot version is not available",
                http_status=409,
                details={"requested_version": version},
            )
        return json.loads(row["document_json"])

    def create_query(
        self,
        *,
        query_id: str,
        request_hash: str,
        request: dict[str, Any],
        snapshot_version: int,
        shards: Sequence[tuple[int, str]],
        window_offset: int,
        window_limit: int | None,
    ) -> tuple[dict[str, Any], bool]:
        created_at = utc_now()
        with self.transaction() as conn:
            existing = conn.execute("SELECT * FROM queries WHERE query_id=?", (query_id,)).fetchone()
            if existing is not None:
                return self._query_row(existing), False
            conn.execute(
                "INSERT INTO queries(query_id, request_hash, request_json, snapshot_version, status, created_at, "
                "total_blocks, committed_blocks, window_offset, window_limit) VALUES (?, ?, ?, ?, 'PENDING', ?, ?, 0, ?, ?)",
                (
                    query_id,
                    request_hash,
                    compact_json(request),
                    snapshot_version,
                    created_at,
                    len(shards),
                    window_offset,
                    window_limit,
                ),
            )
            conn.executemany(
                "INSERT INTO scan_shards(query_id, block_seq, block_id, status) VALUES (?, ?, ?, 'READY')",
                [(query_id, block_seq, block_id) for block_seq, block_id in shards],
            )
            row = conn.execute("SELECT * FROM queries WHERE query_id=?", (query_id,)).fetchone()
        return self._query_row(row), True

    def query(self, query_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM queries WHERE query_id=?", (query_id,)).fetchone()
        return self._query_row(row) if row else None

    def incomplete_queries(self) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in ACTIVE_QUERY_STATUSES)
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM queries WHERE status IN ({placeholders}) ORDER BY created_at, query_id",
                ACTIVE_QUERY_STATUSES,
            ).fetchall()
        return [self._query_row(row) for row in rows]

    def set_query_status(self, query_id: str, status: str, *, error_payload: dict[str, Any] | None = None) -> None:
        now = utc_now()
        with self.transaction() as conn:
            if status == "RUNNING":
                conn.execute(
                    "UPDATE queries SET status=?, started_at=COALESCE(started_at, ?), error_json=NULL "
                    "WHERE query_id=? AND status IN ('PENDING','RECOVERING','RUNNING')",
                    (status, now, query_id),
                )
            elif status == "FAILED":
                conn.execute(
                    "UPDATE queries SET status='FAILED', error_json=?, completed_at=? WHERE query_id=?",
                    (compact_json(error_payload or {}), now, query_id),
                )
            else:
                conn.execute("UPDATE queries SET status=? WHERE query_id=?", (status, query_id))

    def shards(self, query_id: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM scan_shards WHERE query_id=? ORDER BY block_seq", (query_id,)
            ).fetchall()
        return [self._shard_row(row) for row in rows]

    def claim_shard(self, query_id: str, block_seq: int) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE scan_shards SET status='RUNNING', attempts=attempts+1, error_json=NULL "
                "WHERE query_id=? AND block_seq=? AND status='READY'",
                (query_id, block_seq),
            )
            return cursor.rowcount == 1

    def record_intent(self, query_id: str, block_seq: int, intent_hash: str) -> None:
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE scan_shards SET status='INTENT', intent_hash=?, intent_at=? "
                "WHERE query_id=? AND block_seq=? AND status='RUNNING'",
                (intent_hash, utc_now(), query_id, block_seq),
            )
            if cursor.rowcount != 1:
                raise error(
                    "E_STATE_CONFLICT",
                    "shard intent transition is no longer valid",
                    http_status=409,
                    details={"query_id": query_id, "block_seq": block_seq},
                )

    def commit_shard_result(self, query_id: str, block_seq: int, result: dict[str, Any]) -> bool:
        result_json = compact_json(result)
        result_hash = hashlib.sha256(result_json.encode("utf-8")).hexdigest()
        now = utc_now()
        with self.transaction() as conn:
            shard = conn.execute(
                "SELECT status FROM scan_shards WHERE query_id=? AND block_seq=?", (query_id, block_seq)
            ).fetchone()
            if shard is None:
                raise error(
                    "E_STATE_CONFLICT",
                    "scan shard does not exist",
                    http_status=409,
                    details={"query_id": query_id, "block_seq": block_seq},
                )
            if shard["status"] == "COMMITTED":
                return False
            if shard["status"] != "INTENT":
                raise error(
                    "E_STATE_CONFLICT",
                    "shard must be in INTENT state before commit",
                    http_status=409,
                    details={"query_id": query_id, "block_seq": block_seq, "status": shard["status"]},
                )
            conn.execute(
                "INSERT INTO shard_results(query_id, block_seq, result_json, result_hash, row_count, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (query_id, block_seq, result_json, result_hash, int(result.get("row_count", 0)), now),
            )
            conn.execute(
                "UPDATE scan_shards SET status='COMMITTED', result_hash=?, committed_at=? WHERE query_id=? AND block_seq=?",
                (result_hash, now, query_id, block_seq),
            )
            committed = conn.execute(
                "SELECT COUNT(*) AS count FROM scan_shards WHERE query_id=? AND status='COMMITTED'", (query_id,)
            ).fetchone()["count"]
            conn.execute("UPDATE queries SET committed_blocks=? WHERE query_id=?", (int(committed), query_id))
        return True

    def mark_shard_failed(self, query_id: str, block_seq: int, error_payload: dict[str, Any]) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE scan_shards SET status='FAILED', error_json=? WHERE query_id=? AND block_seq=?",
                (compact_json(error_payload), query_id, block_seq),
            )

    def shard_results(self, query_id: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT block_seq, result_json, result_hash, row_count FROM shard_results "
                "WHERE query_id=? ORDER BY block_seq",
                (query_id,),
            ).fetchall()
        return [
            {
                "block_seq": int(row["block_seq"]),
                "result": json.loads(row["result_json"]),
                "result_hash": row["result_hash"],
                "row_count": int(row["row_count"]),
            }
            for row in rows
        ]

    def finalize_query(self, query_id: str, *, result: dict[str, Any], total_matched: int) -> int:
        result_json = compact_json(result)
        now = utc_now()
        with self.transaction() as conn:
            query_row = conn.execute(
                "SELECT status, commit_generation FROM queries WHERE query_id=?", (query_id,)
            ).fetchone()
            if query_row is None:
                raise error("E_STATE_CONFLICT", "query does not exist", http_status=409, details={"query_id": query_id})
            if query_row["status"] == "COMPLETE":
                return int(query_row["commit_generation"])
            counts = conn.execute(
                "SELECT COUNT(*) AS total, SUM(CASE WHEN status='COMMITTED' THEN 1 ELSE 0 END) AS committed "
                "FROM scan_shards WHERE query_id=?",
                (query_id,),
            ).fetchone()
            if int(counts["total"]) != int(counts["committed"] or 0):
                raise error(
                    "E_STATE_CONFLICT",
                    "cannot finalize query before every shard is committed",
                    http_status=409,
                    details={
                        "query_id": query_id,
                        "total": int(counts["total"]),
                        "committed": int(counts["committed"] or 0),
                    },
                )
            meta_row = conn.execute("SELECT value FROM meta WHERE key='query_commit_generation'").fetchone()
            generation = int(meta_row["value"]) + 1 if meta_row else 1
            conn.execute(
                "INSERT INTO meta(key, value) VALUES ('query_commit_generation', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(generation),),
            )
            conn.execute(
                "UPDATE queries SET status='COMPLETE', result_json=?, total_matched=?, commit_generation=?, completed_at=? "
                "WHERE query_id=?",
                (result_json, total_matched, generation, now, query_id),
            )
        return generation

    def result(self, query_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT status, result_json, total_matched, commit_generation, window_offset, window_limit, error_json "
                "FROM queries WHERE query_id=?",
                (query_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "status": row["status"],
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "total_matched": int(row["total_matched"]) if row["total_matched"] is not None else None,
            "commit_generation": int(row["commit_generation"]),
            "window_offset": int(row["window_offset"]),
            "window_limit": int(row["window_limit"]) if row["window_limit"] is not None else None,
            "error": json.loads(row["error_json"]) if row["error_json"] else None,
        }

    def add_decisions(self, query_id: str, decisions: Sequence[dict[str, Any]]) -> None:
        if not decisions:
            return
        now = utc_now()
        with self.transaction() as conn:
            conn.executemany(
                "INSERT INTO budget_decisions(query_id, round_no, phase, action, block_seq, work_id, detail_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        query_id,
                        int(item.get("round", 0)),
                        str(item.get("phase", "schedule")),
                        str(item.get("action", "unknown")),
                        int(item.get("block_seq", 0)),
                        str(item.get("work_id", "")),
                        compact_json(item),
                        now,
                    )
                    for item in decisions
                ],
            )

    def decisions(self, query_id: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT decision_seq, detail_json FROM budget_decisions WHERE query_id=? ORDER BY decision_seq",
                (query_id,),
            ).fetchall()
        result = []
        for row in rows:
            detail = json.loads(row["detail_json"])
            detail["decision_seq"] = int(row["decision_seq"])
            result.append(detail)
        return result

    def recover_incomplete_queries(self, process_instance: str) -> list[dict[str, Any]]:
        reports: list[dict[str, Any]] = []
        recovered_at = utc_now()
        with self.transaction() as conn:
            placeholders = ",".join("?" for _ in ACTIVE_QUERY_STATUSES)
            queries = conn.execute(
                f"SELECT * FROM queries WHERE status IN ({placeholders}) ORDER BY created_at, query_id",
                ACTIVE_QUERY_STATUSES,
            ).fetchall()
            for query_row in queries:
                query_id = str(query_row["query_id"])
                status_before = str(query_row["status"])
                shard_rows = conn.execute(
                    "SELECT block_seq, status FROM scan_shards WHERE query_id=? ORDER BY block_seq", (query_id,)
                ).fetchall()
                reused_shards = 0
                resumed_shards = 0
                details: list[dict[str, Any]] = []
                for shard_row in shard_rows:
                    block_seq = int(shard_row["block_seq"])
                    before = str(shard_row["status"])
                    has_result = conn.execute(
                        "SELECT 1 FROM shard_results WHERE query_id=? AND block_seq=?", (query_id, block_seq)
                    ).fetchone() is not None
                    if before == "COMMITTED":
                        reused_shards += 1
                        conn.execute(
                            "UPDATE scan_shards SET reused=1 WHERE query_id=? AND block_seq=?",
                            (query_id, block_seq),
                        )
                        after = "COMMITTED"
                    elif before == "INTENT" and has_result:
                        reinterpreted = conn.execute(
                            "UPDATE scan_shards SET status='COMMITTED', reused=1 "
                            "WHERE query_id=? AND block_seq=? AND status='INTENT'",
                            (query_id, block_seq),
                        ).rowcount
                        reused_shards += int(reinterpreted)
                        after = "COMMITTED"
                    else:
                        after = "READY"
                        conn.execute(
                            "UPDATE scan_shards SET status='READY', error_json=NULL, recovery_count=recovery_count+1 "
                            "WHERE query_id=? AND block_seq=?",
                            (query_id, block_seq),
                        )
                        resumed_shards += 1
                    details.append(
                        {
                            "block_seq": block_seq,
                            "before": before,
                            "after": after,
                            "result_present": has_result,
                        }
                    )
                conn.execute("UPDATE queries SET committed_blocks=? WHERE query_id=?", (reused_shards, query_id))
                conn.execute("UPDATE queries SET status='RECOVERING' WHERE query_id=?", (query_id,))
                report_details = {
                    "query_id": query_id,
                    "shards": details,
                    "snapshot_version": int(query_row["snapshot_version"]),
                    "active_or_pending_at_crash": status_before,
                    "evidence_tables": ["scan_shards", "shard_results", "queries"],
                    "guarantee": "report is reconstructed only from committed SQLite facts; the terminated process produced no exit report",
                }
                cursor = conn.execute(
                    "INSERT INTO recovery_reports(recovered_at, process_instance, query_id, query_status_before, "
                    "query_status_after, reused_shards, resumed_shards, pending_shards, evidence, details_json) "
                    "VALUES (?, ?, ?, ?, 'RECOVERING', ?, ?, 0, ?, ?)",
                    (
                        recovered_at,
                        process_instance,
                        query_id,
                        status_before,
                        reused_shards,
                        resumed_shards,
                        "sqlite_persisted_facts",
                        compact_json(report_details),
                    ),
                )
                reports.append(
                    {
                        "report_id": int(cursor.lastrowid),
                        "recovered_at": recovered_at,
                        "process_instance": process_instance,
                        "query_id": query_id,
                        "query_status_before": status_before,
                        "query_status_after": "RECOVERING",
                        "reused_shards": reused_shards,
                        "resumed_shards": resumed_shards,
                        "pending_shards": 0,
                        "evidence": "sqlite_persisted_facts",
                        "details": report_details,
                    }
                )
        return reports

    def recovery_reports(self, query_id: str | None = None) -> list[dict[str, Any]]:
        with self.connection() as conn:
            if query_id:
                rows = conn.execute(
                    "SELECT * FROM recovery_reports WHERE query_id=? ORDER BY report_id", (query_id,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM recovery_reports ORDER BY report_id").fetchall()
        return [
            {
                "report_id": int(row["report_id"]),
                "recovered_at": row["recovered_at"],
                "process_instance": row["process_instance"],
                "query_id": row["query_id"],
                "query_status_before": row["query_status_before"],
                "query_status_after": row["query_status_after"],
                "reused_shards": int(row["reused_shards"]),
                "resumed_shards": int(row["resumed_shards"]),
                "pending_shards": int(row["pending_shards"]),
                "evidence": row["evidence"],
                "details": json.loads(row["details_json"]),
            }
            for row in rows
        ]

    @staticmethod
    def _query_row(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "query_id": row["query_id"],
            "request_hash": row["request_hash"],
            "request": json.loads(row["request_json"]),
            "snapshot_version": int(row["snapshot_version"]),
            "status": row["status"],
            "error": json.loads(row["error_json"]) if row["error_json"] else None,
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "commit_generation": int(row["commit_generation"]),
            "total_blocks": int(row["total_blocks"]),
            "committed_blocks": int(row["committed_blocks"]),
            "total_matched": int(row["total_matched"]) if row["total_matched"] is not None else None,
            "window_offset": int(row["window_offset"]),
            "window_limit": int(row["window_limit"]) if row["window_limit"] is not None else None,
        }

    @staticmethod
    def _shard_row(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "query_id": row["query_id"],
            "block_seq": int(row["block_seq"]),
            "block_id": row["block_id"],
            "status": row["status"],
            "attempts": int(row["attempts"]),
            "intent_hash": row["intent_hash"],
            "intent_at": row["intent_at"],
            "result_hash": row["result_hash"],
            "committed_at": row["committed_at"],
            "reused": bool(row["reused"]),
            "recovery_count": int(row["recovery_count"]),
            "error": json.loads(row["error_json"]) if row["error_json"] else None,
        }


def process_instance_id() -> str:
    return f"pid={os.getpid()};instance={os.urandom(8).hex()}"

