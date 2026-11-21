from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .canonical import canonical_json_bytes
from .errors import PipelineError, error_from_stored
from .policy import PolicySnapshot, parse_policy_snapshot


TERMINAL_BATCH_STATUSES = {"committed", "failed", "aborted"}
INCOMPLETE_BATCH_STATUSES = {"reserved", "running", "committing"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policies (
    version INTEGER PRIMARY KEY,
    base_version INTEGER NOT NULL,
    generated_at TEXT NOT NULL,
    checksum TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    submitted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS policy_head (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    version INTEGER NOT NULL
);
INSERT OR IGNORE INTO policy_head(singleton, version) VALUES (1, 0);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL UNIQUE,
    policy_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    request_json TEXT NOT NULL,
    source_path TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(policy_version) REFERENCES policies(version)
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    request_fingerprint TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    input_path TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('reserved', 'running', 'committing', 'committed', 'failed', 'aborted')
    ),
    intent_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    owner_pid INTEGER NOT NULL,
    error_code TEXT,
    error_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    committed_at TEXT,
    summary_json TEXT,
    FOREIGN KEY(policy_version) REFERENCES policies(version),
    FOREIGN KEY(intent_id) REFERENCES intents(intent_id)
);

CREATE INDEX IF NOT EXISTS idx_batches_status ON batches(status);
CREATE INDEX IF NOT EXISTS idx_batches_source_policy
    ON batches(source_sha256, policy_version, status);

CREATE TABLE IF NOT EXISTS policy_locks (
    policy_version INTEGER PRIMARY KEY,
    batch_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stage_facts (
    batch_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    stage_seq INTEGER NOT NULL,
    stage_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    input_digest TEXT NOT NULL,
    output_digest TEXT,
    stats_json TEXT,
    details_json TEXT,
    error_code TEXT,
    error_json TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY(batch_id, stage_name)
);

CREATE INDEX IF NOT EXISTS idx_stage_facts_batch_seq
    ON stage_facts(batch_id, stage_seq);

CREATE TABLE IF NOT EXISTS batch_shards (
    batch_id TEXT NOT NULL,
    shard_id INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    byte_count INTEGER NOT NULL,
    relative_path TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    tensor_digest TEXT NOT NULL,
    dtype_json TEXT NOT NULL,
    null_count INTEGER NOT NULL,
    warning_json TEXT NOT NULL,
    PRIMARY KEY(batch_id, shard_id)
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL UNIQUE,
    intent_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS verification_runs (
    verification_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    baseline_batch_id TEXT,
    selected_policy_version INTEGER NOT NULL,
    status TEXT NOT NULL,
    first_divergence_stage TEXT,
    created_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Database:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA synchronous = FULL")
        return conn

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(SCHEMA)
        finally:
            conn.close()

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self.connect()
        try:
            conn.execute("BEGIN")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def submit_policy(self, snapshot: PolicySnapshot) -> dict[str, Any]:
        snapshot.validate_business()
        snapshot_json = snapshot.canonical_json()
        with self.write_transaction() as conn:
            head_row = conn.execute(
                "SELECT version FROM policy_head WHERE singleton = 1"
            ).fetchone()
            current_version = int(head_row["version"])
            existing = conn.execute(
                "SELECT checksum FROM policies WHERE version = ?", (snapshot.version,)
            ).fetchone()

            if snapshot.version < current_version:
                raise PipelineError(
                    "E_STALE_SNAPSHOT",
                    "policy version is older than the current head",
                    {"submitted_version": snapshot.version, "current_version": current_version},
                )
            if snapshot.version == current_version:
                if existing is not None and existing["checksum"] == snapshot.checksum:
                    raise PipelineError(
                        "E_STALE_SNAPSHOT",
                        "policy snapshot is already the current version",
                        {"submitted_version": snapshot.version, "current_version": current_version},
                    )
                raise PipelineError(
                    "E_STALE_BASE",
                    "another increment from the same base version won the race",
                    {
                        "submitted_version": snapshot.version,
                        "base_version": snapshot.base_version,
                        "current_version": current_version,
                    },
                )
            if snapshot.base_version != current_version:
                raise PipelineError(
                    "E_STALE_BASE",
                    "policy base_version no longer equals the current head",
                    {
                        "submitted_version": snapshot.version,
                        "base_version": snapshot.base_version,
                        "current_version": current_version,
                    },
                )

            submitted_at = utc_now()
            conn.execute(
                """
                INSERT INTO policies(
                    version, base_version, generated_at, checksum, snapshot_json, submitted_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.version,
                    snapshot.base_version,
                    snapshot.generated_at,
                    snapshot.checksum,
                    snapshot_json,
                    submitted_at,
                ),
            )
            conn.execute(
                "UPDATE policy_head SET version = ? WHERE singleton = 1", (snapshot.version,)
            )
            return {
                "version": snapshot.version,
                "base_version": snapshot.base_version,
                "checksum": snapshot.checksum,
                "submitted_at": submitted_at,
                "current_version": snapshot.version,
            }

    def get_policy(self, version: int) -> PolicySnapshot:
        with self.read_transaction() as conn:
            row = conn.execute(
                "SELECT snapshot_json FROM policies WHERE version = ?", (version,)
            ).fetchone()
        if row is None:
            raise PipelineError(
                "E_POLICY_NOT_FOUND", "policy version was not found", {"version": version}
            )
        return parse_policy_snapshot(json.loads(row["snapshot_json"]))

    def policy_head(self) -> int:
        with self.read_transaction() as conn:
            row = conn.execute(
                "SELECT version FROM policy_head WHERE singleton = 1"
            ).fetchone()
        return int(row["version"])

    def claim_batch(
        self,
        *,
        batch_id: str,
        request_fingerprint: str,
        policy_version: int,
        input_path: str,
        source_sha256: str,
        intent_id: str,
        attempt_id: str,
        request_json: str,
    ) -> tuple[bool, dict[str, Any]]:
        now = utc_now()
        with self.write_transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if existing is not None:
                if existing["request_fingerprint"] != request_fingerprint:
                    raise PipelineError(
                        "E_BATCH_ID_CONFLICT",
                        "batch_id was already used for a different request",
                        {
                            "batch_id": batch_id,
                            "existing_status": existing["status"],
                            "existing_policy_version": existing["policy_version"],
                            "existing_source_sha256": existing["source_sha256"],
                        },
                    )
                return False, dict(existing)

            conn.execute(
                """
                INSERT INTO intents(
                    intent_id, batch_id, policy_version, status, request_json,
                    source_path, source_sha256, created_at, updated_at
                ) VALUES (?, ?, ?, 'reserved', ?, ?, ?, ?, ?)
                """,
                (
                    intent_id,
                    batch_id,
                    policy_version,
                    request_json,
                    input_path,
                    source_sha256,
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO batches(
                    batch_id, request_fingerprint, policy_version, input_path,
                    source_sha256, status, intent_id, attempt_id, owner_pid,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'reserved', ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    request_fingerprint,
                    policy_version,
                    input_path,
                    source_sha256,
                    intent_id,
                    attempt_id,
                    os.getpid(),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            return True, dict(row)

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        with self.read_transaction() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        if row is None:
            raise PipelineError(
                "E_BATCH_NOT_FOUND", "batch was not found", {"batch_id": batch_id}
            )
        return dict(row)

    def get_batch_optional(self, batch_id: str) -> dict[str, Any] | None:
        with self.read_transaction() as conn:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        return _row_dict(row)

    def mark_batch_running(self, batch_id: str, attempt_id: str) -> None:
        now = utc_now()
        with self.write_transaction() as conn:
            updated = conn.execute(
                """
                UPDATE batches SET status = 'running', updated_at = ?
                WHERE batch_id = ? AND attempt_id = ? AND status = 'reserved'
                """,
                (now, batch_id, attempt_id),
            ).rowcount
            if updated != 1:
                raise PipelineError(
                    "E_INTERNAL",
                    "batch could not transition from reserved to running",
                    {"batch_id": batch_id, "attempt_id": attempt_id},
                )
            conn.execute(
                "UPDATE intents SET status = 'running', updated_at = ? WHERE batch_id = ?",
                (now, batch_id),
            )

    def acquire_policy_lock(
        self,
        policy_version: int,
        batch_id: str,
        attempt_id: str,
        timeout_seconds: float,
        poll_interval_seconds: float,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        while True:
            with self.write_transaction() as conn:
                row = conn.execute(
                    "SELECT * FROM policy_locks WHERE policy_version = ?",
                    (policy_version,),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO policy_locks(policy_version, batch_id, attempt_id, acquired_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (policy_version, batch_id, attempt_id, utc_now()),
                    )
                    return
                if row["batch_id"] == batch_id and row["attempt_id"] == attempt_id:
                    return
                holder = dict(row)
            if time.monotonic() >= deadline:
                raise PipelineError(
                    "E_POLICY_BUSY",
                    "timed out waiting for the policy-version single-writer lock",
                    {
                        "policy_version": policy_version,
                        "holder": holder,
                        "timeout_seconds": timeout_seconds,
                    },
                )
            time.sleep(poll_interval_seconds)

    def release_policy_lock(self, policy_version: int, attempt_id: str) -> None:
        with self.write_transaction() as conn:
            conn.execute(
                "DELETE FROM policy_locks WHERE policy_version = ? AND attempt_id = ?",
                (policy_version, attempt_id),
            )

    def start_stage(
        self, batch_id: str, attempt_id: str, stage_seq: int, stage_name: str, input_digest: str
    ) -> None:
        with self.write_transaction() as conn:
            conn.execute(
                """
                INSERT INTO stage_facts(
                    batch_id, attempt_id, stage_seq, stage_name, status,
                    input_digest, started_at
                ) VALUES (?, ?, ?, ?, 'running', ?, ?)
                """,
                (batch_id, attempt_id, stage_seq, stage_name, input_digest, utc_now()),
            )

    def complete_stage(
        self,
        batch_id: str,
        stage_name: str,
        output_digest: str,
        stats: dict[str, Any],
        details: dict[str, Any],
    ) -> None:
        with self.write_transaction() as conn:
            updated = conn.execute(
                """
                UPDATE stage_facts
                SET status = 'completed', output_digest = ?, stats_json = ?,
                    details_json = ?, completed_at = ?
                WHERE batch_id = ? AND stage_name = ? AND status = 'running'
                """,
                (
                    output_digest,
                    canonical_json_bytes(stats).decode("utf-8"),
                    canonical_json_bytes(details).decode("utf-8"),
                    utc_now(),
                    batch_id,
                    stage_name,
                ),
            ).rowcount
            if updated != 1:
                raise PipelineError(
                    "E_INTERNAL",
                    "stage completion did not match a running stage fact",
                    {"batch_id": batch_id, "stage_name": stage_name},
                )

    def fail_incomplete_stage(
        self, batch_id: str, code: str, error_context: dict[str, Any]
    ) -> None:
        error_json = canonical_json_bytes(
            {"error": {"code": code, "message": "stage interrupted", "context": error_context}}
        ).decode("utf-8")
        with self.write_transaction() as conn:
            conn.execute(
                """
                UPDATE stage_facts
                SET status = 'failed', error_code = ?, error_json = ?, completed_at = ?
                WHERE batch_id = ? AND status = 'running'
                """,
                (code, error_json, utc_now(), batch_id),
            )

    def fail_batch(
        self,
        batch_id: str,
        attempt_id: str,
        code: str,
        message: str,
        context: dict[str, Any],
    ) -> None:
        now = utc_now()
        error_json = canonical_json_bytes(
            {"error": {"code": code, "message": message, "context": context}}
        ).decode("utf-8")
        with self.write_transaction() as conn:
            conn.execute(
                """
                UPDATE batches
                SET status = 'failed', error_code = ?, error_json = ?, updated_at = ?
                WHERE batch_id = ? AND attempt_id = ?
                """,
                (code, error_json, now, batch_id, attempt_id),
            )
            conn.execute(
                "UPDATE intents SET status = 'failed', updated_at = ? WHERE batch_id = ?",
                (now, batch_id),
            )

    def commit_batch(
        self,
        batch_id: str,
        attempt_id: str,
        summary: dict[str, Any],
        shards: list[dict[str, Any]],
    ) -> None:
        now = utc_now()
        summary_json = canonical_json_bytes(summary).decode("utf-8")
        with self.write_transaction() as conn:
            started = conn.execute(
                "UPDATE batches SET status = 'committing', updated_at = ? "
                "WHERE batch_id = ? AND attempt_id = ? AND status = 'running'",
                (now, batch_id, attempt_id),
            ).rowcount
            if started != 1:
                raise PipelineError(
                    "E_INTERNAL",
                    "batch could not enter the committing state",
                    {"batch_id": batch_id, "attempt_id": attempt_id},
                )
            conn.execute("DELETE FROM batch_shards WHERE batch_id = ?", (batch_id,))
            for shard in shards:
                conn.execute(
                    """
                    INSERT INTO batch_shards(
                        batch_id, shard_id, sample_count, byte_count, relative_path,
                        manifest_sha256, tensor_digest, dtype_json, null_count, warning_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        shard["shard_id"],
                        shard["sample_count"],
                        shard["byte_count"],
                        shard["relative_path"],
                        shard["manifest_sha256"],
                        shard["tensor_digest"],
                        canonical_json_bytes(shard["dtypes"]).decode("utf-8"),
                        shard["null_count"],
                        canonical_json_bytes(shard["warnings"]).decode("utf-8"),
                    ),
                )
            conn.execute(
                """
                UPDATE batches
                SET status = 'committed', committed_at = ?, updated_at = ?,
                    summary_json = ?, error_code = NULL, error_json = NULL
                WHERE batch_id = ? AND attempt_id = ? AND status = 'committing'
                """,
                (now, now, summary_json, batch_id, attempt_id),
            )
            conn.execute(
                "UPDATE intents SET status = 'committed', updated_at = ? WHERE batch_id = ?",
                (now, batch_id),
            )
            conn.execute(
                "DELETE FROM policy_locks WHERE policy_version = ("
                "SELECT policy_version FROM batches WHERE batch_id = ?"
                ") AND attempt_id = ?",
                (batch_id, attempt_id),
            )

    def wait_for_terminal(self, batch_id: str, timeout_seconds: float, poll_seconds: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while True:
            row = self.get_batch(batch_id)
            if row["status"] in TERMINAL_BATCH_STATUSES:
                return row
            if time.monotonic() >= deadline:
                raise PipelineError(
                    "E_BATCH_WAIT_TIMEOUT",
                    "timed out waiting for an in-flight batch to finish",
                    {"batch_id": batch_id, "status": row["status"], "timeout_seconds": timeout_seconds},
                )
            time.sleep(poll_seconds)

    def get_committed_batch(self, batch_id: str) -> dict[str, Any]:
        with self.read_transaction() as conn:
            batch_row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch_row is None:
                raise PipelineError(
                    "E_BATCH_NOT_FOUND", "batch was not found", {"batch_id": batch_id}
                )
            if batch_row["status"] != "committed":
                raise PipelineError(
                    "E_BATCH_NOT_COMMITTED",
                    "only complete committed batches are visible to normal queries",
                    {
                        "batch_id": batch_id,
                        "status": batch_row["status"],
                        "error_code": batch_row["error_code"],
                        "visibility_boundary": "status=committed is published by one SQLite transaction",
                    },
                )
            stage_rows = conn.execute(
                "SELECT * FROM stage_facts WHERE batch_id = ? ORDER BY stage_seq",
                (batch_id,),
            ).fetchall()
            shard_rows = conn.execute(
                "SELECT * FROM batch_shards WHERE batch_id = ? ORDER BY shard_id",
                (batch_id,),
            ).fetchall()

        stages = []
        for row in stage_rows:
            stages.append(
                {
                    "stage": row["stage_name"],
                    "sequence": row["stage_seq"],
                    "status": row["status"],
                    "input_digest": row["input_digest"],
                    "output_digest": row["output_digest"],
                    "stats": json.loads(row["stats_json"]) if row["stats_json"] else None,
                    "details": json.loads(row["details_json"]) if row["details_json"] else None,
                    "started_at": row["started_at"],
                    "completed_at": row["completed_at"],
                }
            )
        shards = [
            {
                "shard_id": row["shard_id"],
                "sample_count": row["sample_count"],
                "byte_count": row["byte_count"],
                "relative_path": row["relative_path"],
                "manifest_sha256": row["manifest_sha256"],
                "tensor_digest": row["tensor_digest"],
                "dtypes": json.loads(row["dtype_json"]),
                "null_count": row["null_count"],
                "warnings": json.loads(row["warning_json"]),
            }
            for row in shard_rows
        ]
        batch = dict(batch_row)
        batch["summary"] = json.loads(batch["summary_json"]) if batch["summary_json"] else None
        batch["stages"] = stages
        batch["shards"] = shards
        batch["visibility"] = {
            "boundary": "single SQLite read transaction over status=committed batches",
            "committed_at": batch["committed_at"],
            "partial_stage_visibility": False,
        }
        return batch

    def get_stage_facts(self, batch_id: str) -> list[dict[str, Any]]:
        with self.read_transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM stage_facts WHERE batch_id = ? ORDER BY stage_seq",
                (batch_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_shard_rows(self, batch_id: str) -> list[dict[str, Any]]:
        with self.read_transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM batch_shards WHERE batch_id = ? ORDER BY shard_id",
                (batch_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def find_baseline_batch(
        self, source_sha256: str, policy_version: int, exclude_batch_id: str | None = None
    ) -> dict[str, Any] | None:
        with self.read_transaction() as conn:
            if exclude_batch_id is None:
                row = conn.execute(
                    """
                    SELECT * FROM batches
                    WHERE source_sha256 = ? AND policy_version = ? AND status = 'committed'
                    ORDER BY committed_at DESC LIMIT 1
                    """,
                    (source_sha256, policy_version),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM batches
                    WHERE source_sha256 = ? AND policy_version = ? AND status = 'committed'
                      AND batch_id <> ?
                    ORDER BY committed_at DESC LIMIT 1
                    """,
                    (source_sha256, policy_version, exclude_batch_id),
                ).fetchone()
        return _row_dict(row)

    def insert_recovery_report(self, report: dict[str, Any]) -> None:
        with self.write_transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO recovery_reports(
                    report_id, batch_id, intent_id, created_at, report_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    report["report_id"],
                    report["batch_id"],
                    report["intent_id"],
                    report["generated_at"],
                    canonical_json_bytes(report).decode("utf-8"),
                ),
            )

    def get_recovery_reports(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        with self.read_transaction() as conn:
            if batch_id is None:
                rows = conn.execute(
                    "SELECT report_json FROM recovery_reports ORDER BY created_at DESC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT report_json FROM recovery_reports WHERE batch_id = ? ORDER BY created_at DESC",
                    (batch_id,),
                ).fetchall()
        return [json.loads(row["report_json"]) for row in rows]

    def record_verification(self, report: dict[str, Any]) -> None:
        with self.write_transaction() as conn:
            conn.execute(
                """
                INSERT INTO verification_runs(
                    verification_id, batch_id, baseline_batch_id, selected_policy_version,
                    status, first_divergence_stage, created_at, report_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report["verification_id"],
                    report["batch_id"],
                    report.get("baseline_batch_id"),
                    report["selected_policy_version"],
                    report["status"],
                    report.get("first_divergence_stage"),
                    report["generated_at"],
                    canonical_json_bytes(report).decode("utf-8"),
                ),
            )

    def stored_failure(self, row: dict[str, Any]) -> PipelineError:
        return error_from_stored(
            row["error_code"] or "E_RECOVERY_ABORTED",
            json.loads(row["error_json"]) if row.get("error_json") else None,
        )
