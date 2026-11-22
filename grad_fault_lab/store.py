from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

from .errors import AppError, ErrorCode, HTTP_STATUS
from .state_machine import BatchState, LEGAL_TRANSITIONS


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class Store:
    """SQLite persistence with atomic final commits and WAL-based readers."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY,
                    request_sha256 TEXT NOT NULL,
                    round INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    is_final INTEGER NOT NULL DEFAULT 0,
                    request_json TEXT NOT NULL,
                    final_code TEXT,
                    final_http_status INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    recovered_at TEXT
                );

                CREATE TABLE IF NOT EXISTS fault_events (
                    fault_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    injection_type TEXT NOT NULL,
                    target_node TEXT NOT NULL,
                    target_shard INTEGER NOT NULL,
                    injection_round INTEGER NOT NULL,
                    classification TEXT NOT NULL,
                    code TEXT NOT NULL,
                    retryable INTEGER NOT NULL,
                    spec_json TEXT NOT NULL,
                    observation_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
                );

                CREATE TABLE IF NOT EXISTS retry_attempts (
                    batch_id TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    injected INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    classification TEXT NOT NULL,
                    retryable INTEGER NOT NULL,
                    elapsed_ms REAL NOT NULL,
                    backoff_ms REAL NOT NULL,
                    observation_json TEXT NOT NULL,
                    PRIMARY KEY(batch_id, attempt_no),
                    FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
                );

                CREATE TABLE IF NOT EXISTS transitions (
                    batch_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    from_state TEXT NOT NULL,
                    to_state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    code TEXT NOT NULL,
                    at TEXT NOT NULL,
                    PRIMARY KEY(batch_id, sequence),
                    FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
                );

                CREATE TABLE IF NOT EXISTS aggregation_results (
                    batch_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    degraded INTEGER NOT NULL,
                    omitted_nodes_json TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
                );

                CREATE TABLE IF NOT EXISTS recovery_reports (
                    batch_id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    before_json TEXT NOT NULL,
                    after_json TEXT NOT NULL,
                    diff_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(batch_id) REFERENCES batches(batch_id)
                );

                CREATE INDEX IF NOT EXISTS idx_faults_batch ON fault_events(batch_id);
                CREATE INDEX IF NOT EXISTS idx_batches_final ON batches(is_final, created_at);
                """
            )
        finally:
            connection.close()

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    @contextmanager
    def _read_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def prepare_batch(
        self,
        *,
        batch_id: str,
        request_sha256: str,
        round_no: int,
        request_payload: dict[str, Any],
    ) -> dict[str, Any]:
        now = utc_now()
        with self._write_transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if existing is not None:
                if existing["request_sha256"] != request_sha256:
                    raise AppError(
                        ErrorCode.BATCH_PAYLOAD_CONFLICT,
                        "This batch_id was already used with a different payload",
                        {
                            "batch_id": batch_id,
                            "existing_request_sha256": existing["request_sha256"],
                            "received_request_sha256": request_sha256,
                        },
                    )
                return dict(existing)
            connection.execute(
                """
                INSERT INTO batches(
                    batch_id, request_sha256, round, state, phase, is_final,
                    request_json, final_code, final_http_status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'RUNNING', 0, ?, NULL, NULL, ?, ?)
                """,
                (
                    batch_id,
                    request_sha256,
                    round_no,
                    BatchState.INJECTING.value,
                    json.dumps(request_payload, sort_keys=True),
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            assert row is not None
            return dict(row)

    def commit_final(
        self,
        *,
        batch_id: str,
        final_state: BatchState | str,
        final_code: ErrorCode | str,
        transitions: list[dict[str, Any]],
        fault_events: list[dict[str, Any]],
        retry_attempts: list[dict[str, Any]],
        aggregation: dict[str, Any],
    ) -> None:
        target_state = BatchState(final_state)
        code = ErrorCode(final_code)
        if target_state not in {BatchState.VERIFIED, BatchState.ABORTED}:
            raise AppError(
                ErrorCode.INVALID_TRANSITION,
                "A batch can only be committed in VERIFIED or ABORTED",
                {"target_state": target_state.value},
            )
        now = utc_now()
        with self._write_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                raise AppError(
                    ErrorCode.NOT_FOUND,
                    "Batch intent does not exist",
                    {"batch_id": batch_id},
                )
            if bool(row["is_final"]):
                if row["state"] != target_state.value:
                    raise AppError(
                        ErrorCode.INVALID_TRANSITION,
                        "A final batch cannot transition again",
                        {
                            "batch_id": batch_id,
                            "current_state": row["state"],
                            "requested_state": target_state.value,
                        },
                    )
                return

            current = BatchState(row["state"])
            for transition in transitions:
                from_state = BatchState(transition["from_state"])
                to_state = BatchState(transition["to_state"])
                if from_state is not current:
                    raise AppError(
                        ErrorCode.INVALID_TRANSITION,
                        "Persisted transition chain is inconsistent",
                        {
                            "batch_id": batch_id,
                            "expected_from": current.value,
                            "received_from": from_state.value,
                        },
                    )
                if to_state not in LEGAL_TRANSITIONS[from_state]:
                    raise AppError(
                        ErrorCode.INVALID_TRANSITION,
                        f"Illegal final state transition {from_state.value} -> {to_state.value}",
                        {"batch_id": batch_id},
                    )
                current = to_state
            if current is not target_state:
                raise AppError(
                    ErrorCode.INVALID_TRANSITION,
                    "Final state does not match the transition chain",
                    {"chain_state": current.value, "final_state": target_state.value},
                )

            for transition in transitions:
                connection.execute(
                    """
                    INSERT INTO transitions(
                        batch_id, sequence, from_state, to_state, reason, code, at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        int(transition["sequence"]),
                        str(transition["from_state"]),
                        str(transition["to_state"]),
                        str(transition["reason"]),
                        str(transition["code"]),
                        str(transition["at"]),
                    ),
                )
            for event in fault_events:
                connection.execute(
                    """
                    INSERT INTO fault_events(
                        fault_id, batch_id, attempt_no, injection_type, target_node,
                        target_shard, injection_round, classification, code, retryable,
                        spec_json, observation_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event["fault_id"],
                        batch_id,
                        int(event["attempt_no"]),
                        str(event["injection_type"]),
                        str(event["target_node"]),
                        int(event["target_shard"]),
                        int(event["injection_round"]),
                        str(event["classification"]),
                        str(event["code"]),
                        int(bool(event["retryable"])),
                        json.dumps(event["spec"], sort_keys=True),
                        json.dumps(event["observation"], sort_keys=True),
                        str(event.get("created_at", now)),
                    ),
                )
            for attempt in retry_attempts:
                connection.execute(
                    """
                    INSERT INTO retry_attempts(
                        batch_id, attempt_no, injected, code, classification,
                        retryable, elapsed_ms, backoff_ms, observation_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        int(attempt["attempt_no"]),
                        int(bool(attempt["injected"])),
                        str(attempt["code"]),
                        str(attempt["classification"]),
                        int(bool(attempt["retryable"])),
                        float(attempt["elapsed_ms"]),
                        float(attempt["backoff_ms"]),
                        json.dumps(attempt["observation"], sort_keys=True),
                    ),
                )
            connection.execute(
                """
                INSERT INTO aggregation_results(
                    batch_id, status, degraded, omitted_nodes_json, result_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    str(aggregation["status"]),
                    int(bool(aggregation.get("degraded", False))),
                    json.dumps(aggregation.get("omitted_nodes", []), sort_keys=True),
                    json.dumps(aggregation, sort_keys=True),
                    now,
                ),
            )
            connection.execute(
                """
                UPDATE batches
                SET state = ?, phase = 'COMPLETED', is_final = 1,
                    final_code = ?, final_http_status = ?, updated_at = ?
                WHERE batch_id = ?
                """,
                (target_state.value, code.value, HTTP_STATUS[code], now, batch_id),
            )

    def get_final_batch(self, batch_id: str) -> dict[str, Any]:
        with self._read_transaction() as connection:
            batch = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                raise AppError(
                    ErrorCode.NOT_FOUND,
                    "Batch was not found",
                    {"batch_id": batch_id},
                )
            if not bool(batch["is_final"]):
                raise AppError(
                    ErrorCode.BATCH_NOT_READY,
                    "Batch intent exists, but no final transaction has committed",
                    {"batch_id": batch_id, "state": batch["state"], "phase": batch["phase"]},
                )
            faults = [
                self._json_row(row, {"spec_json": "spec", "observation_json": "observation"})
                for row in connection.execute(
                    "SELECT * FROM fault_events WHERE batch_id = ? ORDER BY attempt_no, fault_id",
                    (batch_id,),
                )
            ]
            retries = [
                self._json_row(row, {"observation_json": "observation"})
                for row in connection.execute(
                    "SELECT * FROM retry_attempts WHERE batch_id = ? ORDER BY attempt_no",
                    (batch_id,),
                )
            ]
            for fault in faults:
                fault["retryable"] = bool(fault["retryable"])
            for attempt in retries:
                attempt["injected"] = bool(attempt["injected"])
                attempt["retryable"] = bool(attempt["retryable"])
            transitions = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM transitions WHERE batch_id = ? ORDER BY sequence",
                    (batch_id,),
                )
            ]
            aggregate_row = connection.execute(
                "SELECT * FROM aggregation_results WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            recovery_row = connection.execute(
                "SELECT * FROM recovery_reports WHERE batch_id = ?", (batch_id,)
            ).fetchone()

        return {
            "batch_id": batch["batch_id"],
            "round": batch["round"],
            "state": batch["state"],
            "phase": batch["phase"],
            "final_code": batch["final_code"],
            "is_final": bool(batch["is_final"]),
            "request": json.loads(batch["request_json"]),
            "request_sha256": batch["request_sha256"],
            "created_at": batch["created_at"],
            "updated_at": batch["updated_at"],
            "recovered_at": batch["recovered_at"],
            "faults": faults,
            "retry_sequence": retries,
            "transitions": transitions,
            "aggregation": json.loads(aggregate_row["result_json"]) if aggregate_row else None,
            "recovery": (
                self._json_row(
                    recovery_row,
                    {
                        "before_json": "before",
                        "after_json": "after",
                        "diff_json": "diff",
                    },
                )
                if recovery_row
                else None
            ),
        }

    def peek_batch(self, batch_id: str) -> dict[str, Any] | None:
        with self._read_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            return dict(row) if row is not None else None

    @staticmethod
    def _json_row(row: sqlite3.Row, mappings: dict[str, str]) -> dict[str, Any]:
        result = dict(row)
        for source, target in mappings.items():
            if source in result:
                result[target] = json.loads(result.pop(source))
        return result

    def count_batches(self) -> int:
        with self._read_transaction() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM batches").fetchone()
            return int(row["count"])

    def count_faults(self, batch_id: str) -> int:
        with self._read_transaction() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM fault_events WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            return int(row["count"])

    def list_final_batches(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._read_transaction() as connection:
            rows = connection.execute(
                """
                SELECT batch_id, round, state, final_code, created_at, updated_at
                FROM batches WHERE is_final = 1 ORDER BY created_at DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_recovery_reports(self) -> list[dict[str, Any]]:
        with self._read_transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY created_at"
            ).fetchall()
        return [
            self._json_row(
                row,
                {"before_json": "before", "after_json": "after", "diff_json": "diff"},
            )
            for row in rows
        ]

    def recover_incomplete(self) -> list[dict[str, Any]]:
        reports: list[dict[str, Any]] = []
        with self._write_transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM batches WHERE is_final = 0 ORDER BY created_at"
            ).fetchall()
            for row in rows:
                batch_id = row["batch_id"]
                current = BatchState(row["state"])
                if BatchState.ABORTED not in LEGAL_TRANSITIONS[current]:
                    raise AppError(
                        ErrorCode.INVALID_TRANSITION,
                        "Incomplete batch cannot be recovered to ABORTED",
                        {"batch_id": batch_id, "state": current.value},
                    )
                fault_count = connection.execute(
                    "SELECT COUNT(*) AS count FROM fault_events WHERE batch_id = ?",
                    (batch_id,),
                ).fetchone()["count"]
                retry_count = connection.execute(
                    "SELECT COUNT(*) AS count FROM retry_attempts WHERE batch_id = ?",
                    (batch_id,),
                ).fetchone()["count"]
                aggregate_count = connection.execute(
                    "SELECT COUNT(*) AS count FROM aggregation_results WHERE batch_id = ?",
                    (batch_id,),
                ).fetchone()["count"]
                now = utc_now()
                next_sequence = int(
                    connection.execute(
                        "SELECT COALESCE(MAX(sequence), 0) AS maximum "
                        "FROM transitions WHERE batch_id = ?",
                        (batch_id,),
                    ).fetchone()["maximum"]
                ) + 1
                before = {
                    "batch_id": batch_id,
                    "state": row["state"],
                    "phase": row["phase"],
                    "request": json.loads(row["request_json"]),
                    "committed_fault_events": int(fault_count),
                    "committed_retry_attempts": int(retry_count),
                    "committed_aggregation_results": int(aggregate_count),
                }
                after = {
                    "batch_id": batch_id,
                    "state": BatchState.ABORTED.value,
                    "phase": "RECOVERED",
                    "final_code": ErrorCode.RECOVERED_AFTER_RESTART.value,
                }
                diff = {
                    "recovery_basis": "committed_intent_and_rows_only",
                    "exit_report_complete": False,
                    "missing_after_restart": [
                        "in-flight fault observation",
                        "in-flight retry sequence",
                        "in-flight aggregation result",
                    ],
                    "committed_fault_events_preserved": int(fault_count),
                    "committed_retry_attempts_preserved": int(retry_count),
                    "committed_aggregation_results_preserved": int(aggregate_count),
                }
                connection.execute(
                    """
                    INSERT INTO transitions(
                        batch_id, sequence, from_state, to_state, reason, code, at
                    ) VALUES (?, ?, ?, 'ABORTED', ?, ?, ?)
                    """,
                    (
                        batch_id,
                        next_sequence,
                        current.value,
                        "Recovered from an unfinished intent after process restart",
                        ErrorCode.RECOVERED_AFTER_RESTART.value,
                        now,
                    ),
                )
                aborted_result = {
                    "status": "ABORTED",
                    "degraded": False,
                    "participant_nodes": [],
                    "participant_blocks": 0,
                    "omitted_nodes": [],
                    "sample_weights": {},
                    "tensor": None,
                    "checksum": None,
                    "recovery_note": (
                        "No final aggregation transaction was committed before restart"
                    ),
                    "error_bounds": None,
                }
                connection.execute(
                    """
                    INSERT INTO aggregation_results(
                        batch_id, status, degraded, omitted_nodes_json, result_json, created_at
                    ) VALUES (?, 'ABORTED', 0, '[]', ?, ?)
                    """,
                    (batch_id, json.dumps(aborted_result, sort_keys=True), now),
                )
                connection.execute(
                    """
                    INSERT INTO recovery_reports(
                        batch_id, action, before_json, after_json, diff_json, created_at
                    ) VALUES (?, 'MARK_ABORTED_FROM_COMMITTED_INTENT', ?, ?, ?, ?)
                    """,
                    (
                        batch_id,
                        json.dumps(before, sort_keys=True),
                        json.dumps(after, sort_keys=True),
                        json.dumps(diff, sort_keys=True),
                        now,
                    ),
                )
                connection.execute(
                    """
                    UPDATE batches
                    SET state = 'ABORTED', phase = 'RECOVERED', is_final = 1,
                        final_code = ?, final_http_status = ?, updated_at = ?, recovered_at = ?
                    WHERE batch_id = ?
                    """,
                    (
                        ErrorCode.RECOVERED_AFTER_RESTART.value,
                        HTTP_STATUS[ErrorCode.RECOVERED_AFTER_RESTART],
                        now,
                        now,
                        batch_id,
                    ),
                )
                reports.append(
                    {
                        "batch_id": batch_id,
                        "action": "MARK_ABORTED_FROM_COMMITTED_INTENT",
                        "before": before,
                        "after": after,
                        "diff": diff,
                        "created_at": now,
                    }
                )
        return reports

