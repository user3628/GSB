from __future__ import annotations

import json
import os
import signal
import sqlite3
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

from .config import SimulatorConfig
from .db import Database
from .errors import BusinessError
from .events import assign_global_indices, parse_events
from .simulator import Simulator, build_snapshot, clone_state
from .state import SimulationState
from .util import canonical_json, digest_json, digest_text, utc_now_iso


@dataclass
class BatchCommand:
    trace_id: str
    run_id: str
    batch_id: str
    events: list[Any]
    final_batch: bool
    expected_base_checkpoint_id: str | None = None
    config: dict[str, Any] | None = None

    def request_digest(self) -> str:
        return digest_json(
            {
                "trace_id": self.trace_id,
                "run_id": self.run_id,
                "batch_id": self.batch_id,
                "events": self.events,
                "final_batch": self.final_batch,
                "expected_base_checkpoint_id": self.expected_base_checkpoint_id,
                "config": self.config,
            }
        )


class SimulationService:
    """SQLite-backed single-writer simulation service."""

    def __init__(self, db: Database):
        self.db = db
        self._writer_lock = threading.Lock()
        self._condition = threading.Condition()
        self._active_batches: set[str] = set()

    # ------------------------------------------------------------------
    # Submission and deterministic checkpointed execution
    # ------------------------------------------------------------------
    def submit_batch(self, command: BatchCommand) -> dict[str, Any]:
        request_digest = command.request_digest()
        while True:
            with self._condition:
                if command.batch_id in self._active_batches:
                    self._condition.wait(timeout=0.05)
                    continue
                existing = self._batch_row(command.batch_id)
                if existing is not None:
                    return self._existing_batch_response(existing, request_digest)
                self._active_batches.add(command.batch_id)
                break

        try:
            with self._writer_lock:
                existing = self._batch_row(command.batch_id)
                if existing is not None:
                    return self._existing_batch_response(existing, request_digest)
                return self._process_owned_batch(command, request_digest)
        finally:
            with self._condition:
                self._active_batches.discard(command.batch_id)
                self._condition.notify_all()

    def _process_owned_batch(
        self,
        command: BatchCommand,
        request_digest: str,
    ) -> dict[str, Any]:
        started_at = time.perf_counter()
        try:
            return self._process_owned_batch_inner(command, request_digest, started_at)
        except BusinessError as error:
            error.context.setdefault("batch_id", command.batch_id)
            error.context.setdefault("run_id", command.run_id)
            existing = self._batch_row(command.batch_id)
            if existing is None:
                self._record_rejected_batch(command, request_digest, error)
            elif existing["state"] == "RUNNING":
                self._rollback_to_checkpoint(
                    run_id=command.run_id,
                    batch_id=command.batch_id,
                    base_checkpoint_id=existing["base_checkpoint_id"],
                    reason="business_error_during_event_commit",
                    error=error,
                )
            raise
        except Exception as error:  # Defensive: roll back any partial event commits.
            failure = BusinessError(
                "E_INTERNAL",
                context={
                    "batch_id": command.batch_id,
                    "run_id": command.run_id,
                    "exception_type": type(error).__name__,
                    "message": str(error),
                },
            )
            try:
                self._abort_and_rollback(command, failure)
            except BusinessError:
                raise
            raise failure from error

    def _process_owned_batch_inner(
        self,
        command: BatchCommand,
        request_digest: str,
        started_at: float,
    ) -> dict[str, Any]:
        run_row = self._run_row(command.run_id)
        if run_row is not None:
            if run_row["status"] == "FINALIZED":
                raise BusinessError(
                    "E_RUN_FINALIZED",
                    context={"run_id": command.run_id, "batch_id": command.batch_id},
                )
            if run_row["status"] == "CORRUPT":
                raise BusinessError(
                    "E_CHECKPOINT_CORRUPT",
                    context={
                        "run_id": command.run_id,
                        "batch_id": command.batch_id,
                        "reason": "run_previously_marked_corrupt",
                    },
                )

        stored_config = None
        if run_row is not None:
            stored_config = SimulatorConfig.from_dict(json.loads(run_row["config_json"]))
            if command.config is not None:
                requested_config = SimulatorConfig.from_mapping(command.config)
                if requested_config != stored_config:
                    raise BusinessError(
                        "E_CONFIG_MISMATCH",
                        context={
                            "run_id": command.run_id,
                            "batch_id": command.batch_id,
                            "stored": stored_config.to_dict(),
                            "requested": requested_config.to_dict(),
                        },
                    )
            config = stored_config
        else:
            config = SimulatorConfig.from_mapping(command.config)

        current_base_id = run_row["last_checkpoint_id"] if run_row is not None else None
        if run_row is None and command.expected_base_checkpoint_id is not None:
            raise BusinessError(
                "E_STALE_BASE",
                context={
                    "run_id": command.run_id,
                    "batch_id": command.batch_id,
                    "expected_base_checkpoint_id": command.expected_base_checkpoint_id,
                    "current_base_checkpoint_id": None,
                    "run_exists": False,
                },
            )
        if (
            run_row is not None
            and command.expected_base_checkpoint_id is not None
            and command.expected_base_checkpoint_id != current_base_id
        ):
            raise BusinessError(
                "E_STALE_BASE",
                context={
                    "run_id": command.run_id,
                    "batch_id": command.batch_id,
                    "expected_base_checkpoint_id": command.expected_base_checkpoint_id,
                    "current_base_checkpoint_id": current_base_id,
                },
            )

        if run_row is not None:
            base_checkpoint = self._load_checkpoint_row(run_row["last_checkpoint_id"])
            base_state = self._decode_checkpoint(base_checkpoint)
            if base_state.state_config() != config:
                raise BusinessError(
                    "E_CHECKPOINT_CORRUPT",
                    context={
                        "checkpoint_id": run_row["last_checkpoint_id"],
                        "reason": "checkpoint_config_differs_from_run_config",
                    },
                )
            base_seq = int(base_checkpoint["seq"])
            previous_ts_ms = int(run_row["last_ts_ms"])
        else:
            base_checkpoint = None
            base_state = SimulationState.empty(config)
            base_seq = -1
            previous_ts_ms = -1

        try:
            parsed_events = parse_events(
                command.events,
                previous_ts_ms=previous_ts_ms,
                max_events=config.max_events_per_batch,
            )
        except BusinessError as error:
            error.context.setdefault("batch_id", command.batch_id)
            error.context.setdefault("run_id", command.run_id)
            raise

        start_event_index = base_state.last_event_index + 1
        events = assign_global_indices(parsed_events, start_event_index)

        # Preflight the complete batch against the current checkpoint.  Any
        # trace/transition error is therefore reported before a committed state
        # change, satisfying whole-batch no-progress semantics.
        preview_state = clone_state(base_state)
        preview = Simulator(config, preview_state)
        for event in events:
            try:
                preview.apply(event)
            except BusinessError as error:
                error.context.setdefault("batch_id", command.batch_id)
                error.context.setdefault("run_id", command.run_id)
                raise

        base_checkpoint_id, base_seq = self._start_batch_transaction(
            command=command,
            request_digest=request_digest,
            config=config,
            run_row=run_row,
            base_state=base_state,
            base_seq=base_seq,
        )

        state = clone_state(base_state)
        simulator = Simulator(config, state)
        checkpoint_seq = base_seq
        committed_checkpoint_id = base_checkpoint_id
        snapshot_hashes: list[str] = []
        processed_count = 0
        ending_event_index = base_state.last_event_index
        event_connection = self.db.connect()

        try:
            for event in events:
                decision = simulator.apply(event)
                snapshot_payload = build_snapshot(state, decision)
                state_json = canonical_json(state.to_dict())
                state_sha = digest_text(state_json)
                payload_json = canonical_json(snapshot_payload)
                payload_sha = digest_text(payload_json)
                checkpoint_seq += 1
                checkpoint_id = f"ckpt_{uuid.uuid4().hex}"
                snapshot_id = f"snap_{uuid.uuid4().hex}"

                with self.db.transaction_on(event_connection) as conn:
                    checkpoint_intent_id = self._insert_intent(
                        conn,
                        run_id=command.run_id,
                        batch_id=command.batch_id,
                        kind="CHECKPOINT_WRITE",
                        status="PREPARED",
                        details={
                            "checkpoint_id": checkpoint_id,
                            "event_index": event.index,
                            "state_sha256": state_sha,
                        },
                    )
                    snapshot_intent_id = self._insert_intent(
                        conn,
                        run_id=command.run_id,
                        batch_id=command.batch_id,
                        kind="SNAPSHOT_COMMIT",
                        status="PREPARED",
                        details={
                            "snapshot_id": snapshot_id,
                            "checkpoint_id": checkpoint_id,
                            "payload_sha256": payload_sha,
                        },
                    )
                    conn.execute(
                        """
                        INSERT INTO checkpoints(
                            checkpoint_id, run_id, seq, event_index, sim_time_ms,
                            state_json, state_sha256, batch_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            checkpoint_id,
                            command.run_id,
                            checkpoint_seq,
                            event.index,
                            event.ts_ms,
                            state_json,
                            state_sha,
                            command.batch_id,
                            utc_now_iso(),
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO snapshots(
                            snapshot_id, run_id, checkpoint_id, event_index,
                            sim_time_ms, payload_json, payload_sha256, batch_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            snapshot_id,
                            command.run_id,
                            checkpoint_id,
                            event.index,
                            event.ts_ms,
                            payload_json,
                            payload_sha,
                            command.batch_id,
                            utc_now_iso(),
                        ),
                    )
                    conn.execute(
                        """
                        UPDATE runs
                        SET last_checkpoint_id = ?, last_event_index = ?,
                            last_ts_ms = ?, event_count = ?, updated_at = ?
                        WHERE run_id = ?
                        """,
                        (
                            checkpoint_id,
                            event.index,
                            event.ts_ms,
                            event.index + 1,
                            utc_now_iso(),
                            command.run_id,
                        ),
                    )
                    conn.execute(
                        """
                        UPDATE batches
                        SET processed_count = ?, updated_at = ?
                        WHERE batch_id = ?
                        """,
                        (processed_count + 1, utc_now_iso(), command.batch_id),
                    )
                    self._mark_intents_committed(conn, (checkpoint_intent_id, snapshot_intent_id))

                processed_count += 1
                committed_checkpoint_id = checkpoint_id
                ending_event_index = event.index
                snapshot_hashes.append(payload_sha)
                self._maybe_inject_crash(command.batch_id, processed_count, event.index)
        except BusinessError:
            raise
        except BaseException as error:
            failure = BusinessError(
                "E_INTERNAL",
                context={
                    "batch_id": command.batch_id,
                    "run_id": command.run_id,
                    "exception_type": type(error).__name__,
                    "message": str(error),
                },
            )
            self._rollback_to_checkpoint(
                run_id=command.run_id,
                batch_id=command.batch_id,
                base_checkpoint_id=base_checkpoint_id,
                reason="unexpected_error_during_event_commit",
                error=failure,
            )
            raise failure from error
        finally:
            event_connection.close()

        latency_ms = round((time.perf_counter() - started_at) * 1000.0, 3)
        result = {
            "run_id": command.run_id,
            "trace_id": command.trace_id,
            "batch_id": command.batch_id,
            "status": "COMMITTED",
            "base_checkpoint_id": base_checkpoint_id,
            "committed_checkpoint_id": committed_checkpoint_id,
            "starting_event_index": start_event_index if events else None,
            "ending_event_index": ending_event_index if events else None,
            "processed_events": processed_count,
            "total_run_events": state.committed_event_count,
            "final_batch": command.final_batch,
            "state_digest": digest_json(state.to_dict()),
            "batch_snapshot_digest": digest_json(snapshot_hashes),
            "latency_ms": latency_ms,
            "latency_budget_ms": config.latency_budget_ms,
            "within_latency_budget": latency_ms <= config.latency_budget_ms,
            "latency_measurement_scope": "process_start_to_before_final_batch_commit",
        }
        result_json = canonical_json(result)
        with self.db.transaction() as conn:
            intent_id = self._insert_intent(
                conn,
                run_id=command.run_id,
                batch_id=command.batch_id,
                kind="BATCH_COMMIT",
                status="PREPARED",
                details={
                    "processed_events": processed_count,
                    "last_checkpoint_id": committed_checkpoint_id,
                    "state_digest": result["state_digest"],
                },
            )
            conn.execute(
                """
                UPDATE batches
                SET state = 'COMMITTED', processed_count = ?, result_json = ?,
                    error_json = NULL, updated_at = ?
                WHERE batch_id = ?
                """,
                (processed_count, result_json, utc_now_iso(), command.batch_id),
            )
            conn.execute(
                """
                UPDATE runs
                SET status = ?, updated_at = ?
                WHERE run_id = ?
                """,
                (
                    "FINALIZED" if command.final_batch else "ACTIVE",
                    utc_now_iso(),
                    command.run_id,
                ),
            )
            self._mark_intents_committed(conn, (intent_id,))
        return result

    def _start_batch_transaction(
        self,
        *,
        command: BatchCommand,
        request_digest: str,
        config: SimulatorConfig,
        run_row: sqlite3.Row | None,
        base_state: SimulationState,
        base_seq: int,
    ) -> tuple[str, int]:
        now = utc_now_iso()
        with self.db.transaction() as conn:
            current = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (command.run_id,),
            ).fetchone()
            if run_row is None:
                if current is not None:
                    raise BusinessError(
                        "E_STALE_BASE",
                        context={
                            "run_id": command.run_id,
                            "batch_id": command.batch_id,
                            "reason": "run_created_concurrently",
                        },
                    )
                base_checkpoint_id = f"ckpt_{uuid.uuid4().hex}"
                state_json = canonical_json(base_state.to_dict())
                init_intent = self._insert_intent(
                    conn,
                    run_id=command.run_id,
                    batch_id=command.batch_id,
                    kind="RUN_INITIALIZE",
                    status="PREPARED",
                    details={"checkpoint_id": base_checkpoint_id},
                )
                conn.execute(
                    """
                    INSERT INTO checkpoints(
                        checkpoint_id, run_id, seq, event_index, sim_time_ms,
                        state_json, state_sha256, batch_id, created_at
                    ) VALUES (?, ?, 0, -1, -1, ?, ?, ?, ?)
                    """,
                    (
                        base_checkpoint_id,
                        command.run_id,
                        state_json,
                        digest_text(state_json),
                        command.batch_id,
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO runs(
                        run_id, trace_id, config_json, config_sha256, status,
                        base_checkpoint_id, last_checkpoint_id, last_event_index,
                        last_ts_ms, event_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'ACTIVE', ?, ?, -1, -1, 0, ?, ?)
                    """,
                    (
                        command.run_id,
                        command.trace_id,
                        canonical_json(config.to_dict()),
                        digest_json(config.to_dict()),
                        base_checkpoint_id,
                        base_checkpoint_id,
                        now,
                        now,
                    ),
                )
                self._mark_intents_committed(conn, (init_intent,))
                base_seq = 0
            else:
                if current is None:
                    raise BusinessError(
                        "E_STALE_BASE",
                        context={
                            "run_id": command.run_id,
                            "batch_id": command.batch_id,
                            "reason": "run_disappeared_during_batch_start",
                        },
                    )
                if current["last_checkpoint_id"] != run_row["last_checkpoint_id"]:
                    raise BusinessError(
                        "E_STALE_BASE",
                        context={
                            "run_id": command.run_id,
                            "batch_id": command.batch_id,
                            "expected_base_checkpoint_id": run_row["last_checkpoint_id"],
                            "current_base_checkpoint_id": current["last_checkpoint_id"],
                        },
                    )
                base_checkpoint_id = current["last_checkpoint_id"]

            batch_intent = self._insert_intent(
                conn,
                run_id=command.run_id,
                batch_id=command.batch_id,
                kind="BATCH_PROGRESS",
                status="PREPARED",
                details={
                    "base_checkpoint_id": base_checkpoint_id,
                    "event_count": len(command.events),
                    "final_batch": command.final_batch,
                },
            )
            conn.execute(
                """
                INSERT INTO batches(
                    batch_id, run_id, trace_id, request_digest,
                    expected_base_checkpoint_id, final_batch, events_json,
                    event_count, state, processed_count, base_checkpoint_id,
                    result_json, error_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', 0, ?, NULL, NULL, ?, ?)
                """,
                (
                    command.batch_id,
                    command.run_id,
                    command.trace_id,
                    request_digest,
                    command.expected_base_checkpoint_id,
                    1 if command.final_batch else 0,
                    canonical_json(command.events),
                    len(command.events),
                    base_checkpoint_id,
                    now,
                    now,
                ),
            )
            self._mark_intents_committed(conn, (batch_intent,))
        return base_checkpoint_id, base_seq

    def _record_rejected_batch(
        self,
        command: BatchCommand,
        request_digest: str,
        error: BusinessError,
    ) -> None:
        now = utc_now_iso()
        error_json = canonical_json(error.to_dict())
        with self.db.transaction() as conn:
            intent_id = self._insert_intent(
                conn,
                run_id=command.run_id,
                batch_id=command.batch_id,
                kind="BATCH_REJECT",
                status="PREPARED",
                details={"code": error.code, "context": error.context},
            )
            conn.execute(
                """
                INSERT INTO batches(
                    batch_id, run_id, trace_id, request_digest,
                    expected_base_checkpoint_id, final_batch, events_json,
                    event_count, state, processed_count, base_checkpoint_id,
                    result_json, error_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'REJECTED', 0, NULL, NULL, ?, ?, ?)
                """,
                (
                    command.batch_id,
                    command.run_id,
                    command.trace_id,
                    request_digest,
                    command.expected_base_checkpoint_id,
                    1 if command.final_batch else 0,
                    canonical_json(command.events),
                    len(command.events),
                    error_json,
                    now,
                    now,
                ),
            )
            self._mark_intents_committed(conn, (intent_id,))

    # ------------------------------------------------------------------
    # Recovery, rollback and intent scanning
    # ------------------------------------------------------------------
    def recover_incomplete(self) -> dict[str, Any]:
        with self._writer_lock:
            running = self.db.fetch_all(
                "SELECT * FROM batches WHERE state IN ('RUNNING', 'STARTING') ORDER BY created_at, batch_id"
            )
            aborted: list[dict[str, Any]] = []
            corrupt: list[dict[str, Any]] = []
            for row in running:
                try:
                    facts = self._rollback_to_checkpoint(
                        run_id=row["run_id"],
                        batch_id=row["batch_id"],
                        base_checkpoint_id=row["base_checkpoint_id"],
                        reason="startup_recovery_of_incomplete_batch",
                        error=BusinessError(
                            "E_BATCH_ABORTED",
                            context={
                                "batch_id": row["batch_id"],
                                "run_id": row["run_id"],
                                "reason": "process_terminated_before_batch_commit",
                            },
                        ),
                    )
                    aborted.append(facts)
                except BusinessError as error:
                    if error.code == "E_CHECKPOINT_CORRUPT":
                        corrupt.append(
                            {
                                "batch_id": row["batch_id"],
                                "run_id": row["run_id"],
                                "code": error.code,
                                "context": error.context,
                            }
                        )
                        self._mark_batch_aborted_without_rollback(row, error)
                    else:
                        raise

            closed_dangling = self._close_dangling_prepared_intents()
            run_rows = self.db.fetch_all("SELECT * FROM runs ORDER BY run_id")
            runs = [
                {
                    "run_id": row["run_id"],
                    "trace_id": row["trace_id"],
                    "status": row["status"],
                    "last_checkpoint_id": row["last_checkpoint_id"],
                    "last_event_index": row["last_event_index"],
                    "last_ts_ms": row["last_ts_ms"],
                    "event_count": row["event_count"],
                }
                for row in run_rows
            ]
            report_id = f"rec_{uuid.uuid4().hex}"
            report = {
                "report_id": report_id,
                "generated_at": utc_now_iso(),
                "basis": "persisted_sqlite_rows_only",
                "forced_termination_detected": bool(running),
                "incomplete_batches_observed": len(running),
                "aborted_batches": aborted,
                "corrupt_checkpoints": corrupt,
                "dangling_prepared_intents_closed": closed_dangling,
                "persisted_runs": runs,
                "disclaimer": (
                    "This report contains only facts persisted before the forced "
                    "termination. No complete graceful-exit report is promised, and "
                    "continuation must start from the reported persisted checkpoint."
                ),
            }
            with self.db.transaction() as conn:
                intent_id = self._insert_intent(
                    conn,
                    run_id=None,
                    batch_id=None,
                    kind="RECOVERY_SCAN",
                    status="PREPARED",
                    details={
                        "report_id": report_id,
                        "aborted_count": len(aborted),
                        "corrupt_count": len(corrupt),
                    },
                )
                conn.execute(
                    "INSERT INTO recovery_reports(report_id, created_at, report_json) VALUES (?, ?, ?)",
                    (report_id, report["generated_at"], canonical_json(report)),
                )
                self._mark_intents_committed(conn, (intent_id,))
            return report

    def _rollback_to_checkpoint(
        self,
        *,
        run_id: str,
        batch_id: str,
        base_checkpoint_id: str | None,
        reason: str,
        error: BusinessError,
    ) -> dict[str, Any]:
        if not base_checkpoint_id:
            raise BusinessError(
                "E_CHECKPOINT_CORRUPT",
                context={"run_id": run_id, "batch_id": batch_id, "reason": "missing_base_checkpoint_id"},
            )
        checkpoint_row = self._load_checkpoint_row(base_checkpoint_id)
        base_state = self._decode_checkpoint(checkpoint_row)
        base_seq = int(checkpoint_row["seq"])
        base_event_index = int(checkpoint_row["event_index"])
        base_ts_ms = int(checkpoint_row["sim_time_ms"])
        batch_row = self._batch_row(batch_id)
        observed_processed = int(batch_row["processed_count"]) if batch_row is not None else 0
        error_json = canonical_json(error.to_dict())
        now = utc_now_iso()

        with self.db.transaction() as conn:
            intent_id = self._insert_intent(
                conn,
                run_id=run_id,
                batch_id=batch_id,
                kind="RECOVERY_ROLLBACK",
                status="PREPARED",
                details={
                    "base_checkpoint_id": base_checkpoint_id,
                    "base_event_index": base_event_index,
                    "reason": reason,
                    "observed_processed_count": observed_processed,
                },
            )
            deleted_snapshots = conn.execute(
                "DELETE FROM snapshots WHERE run_id = ? AND event_index > ?",
                (run_id, base_event_index),
            ).rowcount
            deleted_checkpoints = conn.execute(
                "DELETE FROM checkpoints WHERE run_id = ? AND seq > ?",
                (run_id, base_seq),
            ).rowcount
            conn.execute(
                """
                UPDATE runs
                SET status = 'ACTIVE', last_checkpoint_id = ?,
                    last_event_index = ?, last_ts_ms = ?, event_count = ?, updated_at = ?
                WHERE run_id = ?
                """,
                (
                    base_checkpoint_id,
                    base_event_index,
                    base_ts_ms,
                    max(0, base_event_index + 1),
                    now,
                    run_id,
                ),
            )
            conn.execute(
                """
                UPDATE batches
                SET state = 'ABORTED', error_json = ?, updated_at = ?
                WHERE batch_id = ?
                """,
                (error_json, now, batch_id),
            )
            conn.execute(
                "UPDATE intent_log SET status = 'ABORTED' WHERE batch_id = ? AND status = 'PREPARED'",
                (batch_id,),
            )
            self._mark_intents_committed(conn, (intent_id,))

        return {
            "batch_id": batch_id,
            "run_id": run_id,
            "outcome": "ABORTED_AND_ROLLED_BACK",
            "observed_processed_count": observed_processed,
            "rollback_checkpoint_id": base_checkpoint_id,
            "rollback_event_index": base_event_index,
            "rollback_sim_time_ms": base_ts_ms,
            "deleted_snapshots": deleted_snapshots,
            "deleted_checkpoints": deleted_checkpoints,
            "reason": reason,
            "persisted_base_state_sha256": digest_json(base_state.to_dict()),
        }

    def _mark_batch_aborted_without_rollback(
        self,
        row: sqlite3.Row,
        error: BusinessError,
    ) -> None:
        now = utc_now_iso()
        with self.db.transaction() as conn:
            intent_id = self._insert_intent(
                conn,
                run_id=row["run_id"],
                batch_id=row["batch_id"],
                kind="RECOVERY_CORRUPT",
                status="PREPARED",
                details={"code": error.code, "context": error.context},
            )
            conn.execute(
                "UPDATE batches SET state = 'ABORTED', error_json = ?, updated_at = ? WHERE batch_id = ?",
                (canonical_json(error.to_dict()), now, row["batch_id"]),
            )
            conn.execute(
                "UPDATE runs SET status = 'CORRUPT', updated_at = ? WHERE run_id = ?",
                (now, row["run_id"]),
            )
            self._mark_intents_committed(conn, (intent_id,))

    def _close_dangling_prepared_intents(self) -> int:
        running = {
            row["batch_id"]
            for row in self.db.fetch_all(
                "SELECT batch_id FROM batches WHERE state IN ('RUNNING', 'STARTING')"
            )
        }
        count = 0
        with self.db.transaction() as conn:
            rows = conn.execute(
                "SELECT intent_id, batch_id FROM intent_log WHERE status = 'PREPARED'"
            ).fetchall()
            for row in rows:
                if row["batch_id"] in running:
                    continue
                conn.execute(
                    "UPDATE intent_log SET status = 'ABANDONED' WHERE intent_id = ?",
                    (row["intent_id"],),
                )
                count += 1
        return count

    def latest_recovery_report(self) -> dict[str, Any]:
        row = self.db.fetch_one(
            "SELECT * FROM recovery_reports ORDER BY created_at DESC, rowid DESC LIMIT 1"
        )
        if row is None:
            return {
                "status": "NO_RECOVERY_SCAN",
                "basis": "persisted_sqlite_rows_only",
                "disclaimer": "No recovery scan has been persisted.",
            }
        return json.loads(row["report_json"])

    def _abort_and_rollback(self, command: BatchCommand, error: BusinessError) -> None:
        batch = self._batch_row(command.batch_id)
        if batch is None:
            self._record_rejected_batch(command, command.request_digest(), error)
            return
        if batch["state"] != "RUNNING":
            return
        self._rollback_to_checkpoint(
            run_id=command.run_id,
            batch_id=command.batch_id,
            base_checkpoint_id=batch["base_checkpoint_id"],
            reason="unexpected_exception_before_batch_commit",
            error=error,
        )

    # ------------------------------------------------------------------
    # Query API used by FastAPI readers
    # ------------------------------------------------------------------
    def query_connection_state(
        self,
        *,
        run_id: str,
        conn_id: str | None,
        at_ms: int,
        wait_ms: int = 0,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + max(0, wait_ms) / 1000.0
        while True:
            with self.db.read_transaction() as conn:
                run_row = conn.execute(
                    "SELECT * FROM runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                snapshot_row = conn.execute(
                    """
                    SELECT * FROM snapshots
                    WHERE run_id = ? AND sim_time_ms <= ?
                    ORDER BY event_index DESC
                    LIMIT 1
                    """,
                    (run_id, at_ms),
                ).fetchone()
                base_checkpoint_row = None
                if snapshot_row is None and run_row is not None:
                    base_checkpoint_row = conn.execute(
                        "SELECT * FROM checkpoints WHERE checkpoint_id = ?",
                        (run_row["base_checkpoint_id"],),
                    ).fetchone()
            if run_row is None:
                raise BusinessError("E_RUN_NOT_FOUND", context={"run_id": run_id})
            if run_row["status"] == "CORRUPT":
                raise BusinessError(
                    "E_CHECKPOINT_CORRUPT",
                    context={"run_id": run_id, "reason": "run_status_corrupt"},
                )
            stable = run_row["status"] == "FINALIZED" or (
                int(run_row["last_ts_ms"]) >= 0 and at_ms < int(run_row["last_ts_ms"])
            )
            if stable:
                if snapshot_row is not None:
                    payload = self._decode_snapshot(snapshot_row)
                else:
                    if int(run_row["last_ts_ms"]) >= 0:
                        raise BusinessError(
                            "E_SNAPSHOT_NOT_READY",
                            context={
                                "run_id": run_id,
                                "at_ms": at_ms,
                                "reason": "requested_time_precedes_first_committed_event",
                                "first_committed_ts_ms": int(run_row["last_ts_ms"]),
                            },
                        )
                    if base_checkpoint_row is None:
                        raise BusinessError(
                            "E_CHECKPOINT_CORRUPT",
                            context={
                                "run_id": run_id,
                                "checkpoint_id": run_row["base_checkpoint_id"],
                                "reason": "base_checkpoint_row_missing",
                            },
                        )
                    payload = self._base_snapshot_from_row(base_checkpoint_row)
                return self._snapshot_query_response(
                    run_row=run_row,
                    snapshot_row=snapshot_row,
                    payload=payload,
                    at_ms=at_ms,
                    conn_id=conn_id,
                )

            if time.monotonic() >= deadline:
                raise BusinessError(
                    "E_SNAPSHOT_NOT_READY",
                    context={
                        "run_id": run_id,
                        "at_ms": at_ms,
                        "run_status": run_row["status"],
                        "committed_event_index": int(run_row["last_event_index"]),
                        "committed_sim_time_ms": int(run_row["last_ts_ms"]),
                        "wait_ms": wait_ms,
                        "reason": "requested_time_not_yet_stable",
                    },
                )
            time.sleep(0.02)

    def _snapshot_query_response(
        self,
        *,
        run_row: sqlite3.Row,
        snapshot_row: sqlite3.Row | None,
        payload: dict[str, Any],
        at_ms: int,
        conn_id: str | None,
    ) -> dict[str, Any]:
        connections = payload.get("connections", {})
        selected = None
        if conn_id is not None:
            selected = connections.get(conn_id)
            if selected is None:
                raise BusinessError(
                    "E_CONNECTION_NOT_FOUND",
                    context={
                        "run_id": run_row["run_id"],
                        "conn_id": conn_id,
                        "at_ms": at_ms,
                        "available_connections": sorted(connections),
                    },
                )
        return {
            "run_id": run_row["run_id"],
            "trace_id": run_row["trace_id"],
            "run_status": run_row["status"],
            "requested_at_ms": at_ms,
            "event_index": int(payload.get("event_index", -1)),
            "sim_time_ms": int(payload.get("sim_time_ms", -1)),
            "snapshot_sha256": snapshot_row["payload_sha256"] if snapshot_row else None,
            "snapshot": payload,
            "connection": selected,
            "committed_event_index": int(run_row["last_event_index"]),
            "committed_event_count": int(run_row["event_count"]),
        }

    def window_trace(self, run_id: str, conn_id: str) -> dict[str, Any]:
        with self.db.read_transaction() as conn:
            run_row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            rows = conn.execute(
                "SELECT * FROM snapshots WHERE run_id = ? ORDER BY event_index",
                (run_id,),
            ).fetchall()
        if run_row is None:
            raise BusinessError("E_RUN_NOT_FOUND", context={"run_id": run_id})
        trace: list[dict[str, Any]] = []
        min_observed_cwnd: int | None = None
        for row in rows:
            payload = self._decode_snapshot(row)
            connection = payload.get("connections", {}).get(conn_id)
            if connection is None:
                continue
            cwnd = int(connection["cwnd_bytes"])
            min_observed_cwnd = cwnd if min_observed_cwnd is None else min(min_observed_cwnd, cwnd)
            decision = payload.get("decision") or {}
            trace.append(
                {
                    "event_index": payload["event_index"],
                    "sim_time_ms": payload["sim_time_ms"],
                    "event_type": decision.get("event_type"),
                    "ack_class": decision.get("ack_class"),
                    "state": connection["state"],
                    "cwnd_bytes": cwnd,
                    "ssthresh_bytes": int(connection["ssthresh_bytes"]),
                    "srtt_us": int(connection["srtt_us"]),
                    "rttvar_us": int(connection["rttvar_us"]),
                    "rto_us": int(connection["rto_us"]),
                    "dup_ack_count": int(connection["dup_ack_count"]),
                    "fast_recovery": bool(connection["fast_recovery"]),
                    "growth_blocked_until_us": int(connection["growth_blocked_until_us"]),
                    "reasons": list(decision.get("reasons", [])),
                }
            )
        if not trace:
            raise BusinessError(
                "E_CONNECTION_NOT_FOUND",
                context={"run_id": run_id, "conn_id": conn_id, "available_snapshots": len(rows)},
            )
        return {
            "run_id": run_id,
            "trace_id": run_row["trace_id"],
            "run_status": run_row["status"],
            "conn_id": conn_id,
            "events_observed": len(trace),
            "min_observed_cwnd_bytes": min_observed_cwnd,
            "trace": trace,
        }

    def diagnostics(self, run_id: str) -> dict[str, Any]:
        with self.db.read_transaction() as conn:
            run_row = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            checkpoint_row = None
            snapshot_rows: list[sqlite3.Row] = []
            if run_row is not None:
                checkpoint_row = conn.execute(
                    "SELECT * FROM checkpoints WHERE checkpoint_id = ?",
                    (run_row["last_checkpoint_id"],),
                ).fetchone()
                snapshot_rows = conn.execute(
                    "SELECT * FROM snapshots WHERE run_id = ? ORDER BY event_index",
                    (run_id,),
                ).fetchall()
        if run_row is None:
            raise BusinessError("E_RUN_NOT_FOUND", context={"run_id": run_id})
        if checkpoint_row is None:
            raise BusinessError(
                "E_CHECKPOINT_CORRUPT",
                context={
                    "run_id": run_id,
                    "checkpoint_id": run_row["last_checkpoint_id"],
                    "reason": "checkpoint_row_missing",
                },
            )
        self._verify_checkpoint_row(checkpoint_row)
        state = self._decode_checkpoint(checkpoint_row)
        config = state.state_config()
        summaries: list[dict[str, Any]] = []
        for conn_id in sorted(state.connections):
            conn = state.connections[conn_id]
            min_cwnd: int | None = None
            max_cwnd: int | None = None
            for row in snapshot_rows:
                payload = self._decode_snapshot(row)
                value = payload.get("connections", {}).get(conn_id)
                if value is None:
                    continue
                cwnd = int(value["cwnd_bytes"])
                min_cwnd = cwnd if min_cwnd is None else min(min_cwnd, cwnd)
                max_cwnd = cwnd if max_cwnd is None else max(max_cwnd, cwnd)
            findings: list[str] = []
            counters = conn.counters
            if counters["slow_start_acks"]:
                findings.append("Slow-start acknowledged growth was observed.")
            if counters["congestion_avoidance_acks"]:
                findings.append("Congestion avoidance used integer RTT-proportional growth.")
            if counters["fast_retransmits"]:
                findings.append(
                    f"Fast retransmit count={counters['fast_retransmits']}; three duplicate ACKs were required."
                )
            if counters["timeout_events"]:
                findings.append("Timeout reset cwnd to the configured initial value and halved ssthresh.")
            if counters["out_of_order_acks"]:
                findings.append(
                    f"Out-of-order ACKs={counters['out_of_order_acks']} were excluded from duplicate-ACK counting."
                )
            if counters["timeout_growth_suppressed"]:
                findings.append(
                    f"Post-timeout growth was suppressed for {counters['timeout_growth_suppressed']} ACK event(s)."
                )
            if min_cwnd is not None and min_cwnd < config.min_cwnd_bytes:
                findings.append("INVARIANT_VETO: observed cwnd below configured minimum.")
            else:
                findings.append(
                    f"Minimum-window invariant held (configured={config.min_cwnd_bytes}, observed={min_cwnd})."
                )
            summaries.append(
                {
                    "conn_id": conn_id,
                    "state": conn.state,
                    "cwnd_bytes": conn.cwnd_bytes,
                    "ssthresh_bytes": conn.ssthresh_bytes,
                    "srtt_us": conn.srtt_us,
                    "rttvar_us": conn.rttvar_us,
                    "rto_us": conn.rto_us,
                    "rto_bounds_us": [config.min_rto_us, config.max_rto_us],
                    "min_observed_cwnd_bytes": min_cwnd,
                    "max_observed_cwnd_bytes": max_cwnd,
                    "counters": counters,
                    "findings": findings,
                }
            )
        return {
            "run_id": run_id,
            "trace_id": run_row["trace_id"],
            "run_status": run_row["status"],
            "event_count": int(run_row["event_count"]),
            "last_checkpoint_id": run_row["last_checkpoint_id"],
            "config": config.to_dict(),
            "connections": summaries,
        }

    def compare_runs(self, left_run_id: str, right_run_id: str) -> dict[str, Any]:
        with self.db.read_transaction() as conn:
            left_run = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (left_run_id,),
            ).fetchone()
            right_run = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                (right_run_id,),
            ).fetchone()
            left = conn.execute(
                "SELECT * FROM snapshots WHERE run_id = ? ORDER BY event_index",
                (left_run_id,),
            ).fetchall()
            right = conn.execute(
                "SELECT * FROM snapshots WHERE run_id = ? ORDER BY event_index",
                (right_run_id,),
            ).fetchall()
        if left_run is None:
            raise BusinessError("E_RUN_NOT_FOUND", context={"run_id": left_run_id, "side": "left"})
        if right_run is None:
            raise BusinessError("E_RUN_NOT_FOUND", context={"run_id": right_run_id, "side": "right"})
        left_hashes = [row["payload_sha256"] for row in left]
        right_hashes = [row["payload_sha256"] for row in right]
        first_mismatch = None
        for index, row in enumerate(left):
            self._decode_snapshot(row)
        for index, row in enumerate(right):
            self._decode_snapshot(row)
        for index in range(max(len(left), len(right))):
            left_hash = left_hashes[index] if index < len(left_hashes) else None
            right_hash = right_hashes[index] if index < len(right_hashes) else None
            if left_hash != right_hash:
                first_mismatch = {
                    "position": index,
                    "left": {
                        "event_index": int(left[index]["event_index"]) if index < len(left) else None,
                        "payload_sha256": left_hash,
                    },
                    "right": {
                        "event_index": int(right[index]["event_index"]) if index < len(right) else None,
                        "payload_sha256": right_hash,
                    },
                }
                break
        return {
            "left_run_id": left_run_id,
            "right_run_id": right_run_id,
            "left_status": left_run["status"],
            "right_status": right_run["status"],
            "identical": first_mismatch is None and len(left) == len(right),
            "left_snapshots": len(left),
            "right_snapshots": len(right),
            "left_snapshot_digest": digest_json(left_hashes),
            "right_snapshot_digest": digest_json(right_hashes),
            "first_mismatch": first_mismatch,
        }

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        row = self._batch_row(batch_id)
        if row is None:
            raise BusinessError("E_BATCH_NOT_FOUND", context={"batch_id": batch_id})
        result = None
        error = None
        if row["result_json"]:
            result = json.loads(row["result_json"])
        if row["error_json"]:
            error = json.loads(row["error_json"])
        return {
            "batch_id": row["batch_id"],
            "run_id": row["run_id"],
            "trace_id": row["trace_id"],
            "state": row["state"],
            "processed_count": int(row["processed_count"]),
            "event_count": int(row["event_count"]),
            "expected_base_checkpoint_id": row["expected_base_checkpoint_id"],
            "base_checkpoint_id": row["base_checkpoint_id"],
            "final_batch": bool(row["final_batch"]),
            "result": result,
            "error": error,
        }

    def get_checkpoint(self, checkpoint_id: str) -> dict[str, Any]:
        row = self._load_checkpoint_row(checkpoint_id)
        state = self._decode_checkpoint(row)
        return {
            "checkpoint_id": row["checkpoint_id"],
            "run_id": row["run_id"],
            "seq": int(row["seq"]),
            "event_index": int(row["event_index"]),
            "sim_time_ms": int(row["sim_time_ms"]),
            "state_sha256": row["state_sha256"],
            "state": state.to_dict(),
        }

    def get_run(self, run_id: str) -> dict[str, Any]:
        row = self._run_row(run_id)
        if row is None:
            raise BusinessError("E_RUN_NOT_FOUND", context={"run_id": run_id})
        return {
            "run_id": row["run_id"],
            "trace_id": row["trace_id"],
            "status": row["status"],
            "base_checkpoint_id": row["base_checkpoint_id"],
            "last_checkpoint_id": row["last_checkpoint_id"],
            "last_event_index": int(row["last_event_index"]),
            "last_ts_ms": int(row["last_ts_ms"]),
            "event_count": int(row["event_count"]),
            "config": json.loads(row["config_json"]),
        }

    # ------------------------------------------------------------------
    # Low-level checked row decoding
    # ------------------------------------------------------------------
    def _batch_row(self, batch_id: str) -> sqlite3.Row | None:
        return self.db.fetch_one("SELECT * FROM batches WHERE batch_id = ?", (batch_id,))

    def _run_row(self, run_id: str) -> sqlite3.Row | None:
        return self.db.fetch_one("SELECT * FROM runs WHERE run_id = ?", (run_id,))

    def _existing_batch_response(self, row: sqlite3.Row, request_digest: str) -> dict[str, Any]:
        if row["request_digest"] != request_digest:
            raise BusinessError(
                "E_IDEMPOTENCY_CONFLICT",
                context={
                    "batch_id": row["batch_id"],
                    "stored_request_digest": row["request_digest"],
                    "incoming_request_digest": request_digest,
                },
            )
        if row["state"] == "COMMITTED":
            return json.loads(row["result_json"])
        if row["state"] in {"REJECTED", "ABORTED"} and row["error_json"]:
            raise BusinessError.from_stored(json.loads(row["error_json"]))
        if row["state"] in {"RUNNING", "STARTING"}:
            raise BusinessError(
                "E_BATCH_ABORTED",
                context={
                    "batch_id": row["batch_id"],
                    "run_id": row["run_id"],
                    "state": row["state"],
                    "reason": "batch_requires_startup_recovery_before_reuse",
                },
            )
        raise BusinessError(
            "E_INTERNAL",
            context={"batch_id": row["batch_id"], "state": row["state"]},
        )

    def _load_checkpoint_row(self, checkpoint_id: str) -> sqlite3.Row:
        row = self.db.fetch_one(
            "SELECT * FROM checkpoints WHERE checkpoint_id = ?",
            (checkpoint_id,),
        )
        if row is None:
            raise BusinessError(
                "E_CHECKPOINT_CORRUPT",
                context={"checkpoint_id": checkpoint_id, "reason": "checkpoint_row_missing"},
            )
        self._verify_checkpoint_row(row)
        return row

    @staticmethod
    def _verify_checkpoint_row(row: sqlite3.Row) -> None:
        actual = digest_text(row["state_json"])
        if actual != row["state_sha256"]:
            raise BusinessError(
                "E_CHECKPOINT_CORRUPT",
                context={
                    "checkpoint_id": row["checkpoint_id"],
                    "expected_sha256": row["state_sha256"],
                    "actual_sha256": actual,
                    "reason": "checkpoint_digest_mismatch",
                },
            )

    def _decode_checkpoint(self, row: sqlite3.Row) -> SimulationState:
        try:
            value = json.loads(row["state_json"])
            return SimulationState.from_dict(value)
        except Exception as error:
            raise BusinessError(
                "E_CHECKPOINT_CORRUPT",
                context={
                    "checkpoint_id": row["checkpoint_id"],
                    "reason": "checkpoint_json_decode_failed",
                    "exception_type": type(error).__name__,
                },
            ) from error

    def _decode_snapshot(self, row: sqlite3.Row) -> dict[str, Any]:
        actual = digest_text(row["payload_json"])
        if actual != row["payload_sha256"]:
            raise BusinessError(
                "E_CHECKPOINT_CORRUPT",
                context={
                    "snapshot_id": row["snapshot_id"],
                    "expected_sha256": row["payload_sha256"],
                    "actual_sha256": actual,
                    "reason": "snapshot_digest_mismatch",
                },
            )
        try:
            return json.loads(row["payload_json"])
        except Exception as error:
            raise BusinessError(
                "E_CHECKPOINT_CORRUPT",
                context={
                    "snapshot_id": row["snapshot_id"],
                    "reason": "snapshot_json_decode_failed",
                    "exception_type": type(error).__name__,
                },
            ) from error

    def _base_snapshot(self, run_row: sqlite3.Row) -> dict[str, Any]:
        checkpoint_row = self._load_checkpoint_row(run_row["base_checkpoint_id"])
        return self._base_snapshot_from_row(checkpoint_row)

    def _base_snapshot_from_row(self, checkpoint_row: sqlite3.Row) -> dict[str, Any]:
        self._verify_checkpoint_row(checkpoint_row)
        state = self._decode_checkpoint(checkpoint_row)
        config = state.state_config()
        return {
            "schema_version": 1,
            "event_index": -1,
            "sim_time_ms": -1,
            "decision": None,
            "connections": {
                conn_id: state.connections[conn_id].public_dict(config)
                for conn_id in sorted(state.connections)
            },
        }

    @staticmethod
    def _insert_intent(
        conn: sqlite3.Connection,
        *,
        run_id: str | None,
        batch_id: str | None,
        kind: str,
        status: str,
        details: dict[str, Any],
    ) -> int:
        cursor = conn.execute(
            """
            INSERT INTO intent_log(run_id, batch_id, kind, status, details_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, batch_id, kind, status, canonical_json(details), utc_now_iso()),
        )
        return int(cursor.lastrowid)

    @staticmethod
    def _mark_intents_committed(
        conn: sqlite3.Connection,
        intent_ids: tuple[int, ...],
    ) -> None:
        for intent_id in intent_ids:
            conn.execute(
                "UPDATE intent_log SET status = 'COMMITTED' WHERE intent_id = ?",
                (intent_id,),
            )

    @staticmethod
    def _maybe_inject_crash(batch_id: str, processed_count: int, event_index: int) -> None:
        target_batch = os.getenv("TCP_SIM_CRASH_BATCH_ID", "").strip()
        target_count = os.getenv("TCP_SIM_CRASH_AFTER_CHECKPOINTS", "").strip()
        if not target_batch or not target_count:
            return
        if batch_id != target_batch:
            return
        try:
            threshold = int(target_count)
        except ValueError:
            return
        if processed_count != threshold:
            return
        sys.stderr.write(
            "TCP_SIM_FORCED_TERMINATION "
            f"batch_id={batch_id} processed_count={processed_count} event_index={event_index}\n"
        )
        sys.stderr.flush()
        if os.name == "nt":
            os._exit(97)
        os.kill(os.getpid(), signal.SIGKILL)
