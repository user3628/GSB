from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from app.compare import BatchComputation
from app.errors import ConflictError, RunNotFoundError
from app.scheduler import BatchPlan, ScheduleResult
from app.spec import NormalizedTrace


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _loads(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


SCHEMA = """
CREATE TABLE IF NOT EXISTS traces (
    trace_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    source_schema TEXT NOT NULL,
    normalized_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trace_models (
    trace_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    memory_bytes INTEGER NOT NULL,
    cost_us INTEGER NOT NULL,
    max_batch_samples INTEGER NOT NULL,
    PRIMARY KEY (trace_id, model_id),
    FOREIGN KEY (trace_id) REFERENCES traces(trace_id)
);

CREATE TABLE IF NOT EXISTS trace_requests (
    trace_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    arrival_us INTEGER NOT NULL,
    model_id TEXT NOT NULL,
    samples INTEGER NOT NULL,
    expected_latency_us INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    PRIMARY KEY (trace_id, request_id),
    FOREIGN KEY (trace_id) REFERENCES traces(trace_id)
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    trace_id TEXT,
    run_id TEXT,
    status TEXT NOT NULL,
    checkpoint TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    error_json TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    resolution TEXT
);

CREATE INDEX IF NOT EXISTS idx_intents_status ON intents(status, created_at);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    trace_id TEXT NOT NULL,
    state TEXT NOT NULL,
    error_code TEXT,
    deterministic_digest TEXT,
    schedule_digest TEXT,
    algorithm_version TEXT NOT NULL,
    node_json TEXT NOT NULL,
    schedule_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    committed_at TEXT,
    FOREIGN KEY (trace_id) REFERENCES traces(trace_id)
);

CREATE INDEX IF NOT EXISTS idx_runs_trace ON runs(trace_id, created_at);

CREATE TABLE IF NOT EXISTS batches (
    batch_uid TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    state TEXT NOT NULL,
    model_id TEXT NOT NULL,
    priority INTEGER NOT NULL,
    memory_bytes INTEGER NOT NULL,
    latency_us INTEGER NOT NULL,
    latency_ms_x100 INTEGER NOT NULL,
    latency_rounding_error_us INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    member_count INTEGER NOT NULL,
    first_arrival_us INTEGER NOT NULL,
    last_arrival_us INTEGER NOT NULL,
    timeline_start_us INTEGER NOT NULL,
    timeline_end_us INTEGER NOT NULL,
    region_start INTEGER NOT NULL,
    region_size INTEGER NOT NULL,
    min_expected_latency_us INTEGER NOT NULL,
    split_reasons_json TEXT NOT NULL,
    split_notes_json TEXT NOT NULL,
    bitmap_json TEXT NOT NULL,
    validation_state TEXT,
    failed_fields_json TEXT,
    engine_trace_json TEXT,
    error_chain_json TEXT,
    rounding_json TEXT,
    fragmentation_event_id TEXT,
    UNIQUE (run_id, ordinal),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_batches_run ON batches(run_id, ordinal);

CREATE TABLE IF NOT EXISTS batch_members (
    member_uid TEXT PRIMARY KEY,
    batch_uid TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_seq INTEGER NOT NULL,
    arrival_us INTEGER NOT NULL,
    model_id TEXT NOT NULL,
    samples INTEGER NOT NULL,
    part_index INTEGER NOT NULL,
    part_count INTEGER NOT NULL,
    memory_bytes INTEGER NOT NULL,
    latency_us INTEGER NOT NULL,
    expected_latency_us INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    origin TEXT NOT NULL,
    FOREIGN KEY (batch_uid) REFERENCES batches(batch_uid)
);

CREATE INDEX IF NOT EXISTS idx_members_batch ON batch_members(batch_uid, request_seq);

CREATE TABLE IF NOT EXISTS batch_quantiles (
    batch_uid TEXT NOT NULL,
    label TEXT NOT NULL,
    percentile INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    raw_us INTEGER NOT NULL,
    output_ms_x100 INTEGER NOT NULL,
    rounded_us INTEGER NOT NULL,
    rounding_error_us INTEGER NOT NULL,
    PRIMARY KEY (batch_uid, label),
    FOREIGN KEY (batch_uid) REFERENCES batches(batch_uid)
);

CREATE TABLE IF NOT EXISTS precision_checks (
    check_id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_uid TEXT NOT NULL,
    scope TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    metric TEXT NOT NULL,
    unit TEXT NOT NULL,
    truth INTEGER NOT NULL,
    estimate INTEGER NOT NULL,
    absolute_deviation INTEGER NOT NULL,
    relative_deviation_ppm INTEGER,
    absolute_bound INTEGER NOT NULL,
    relative_bound_ppm INTEGER NOT NULL,
    passed INTEGER NOT NULL,
    note TEXT NOT NULL,
    tolerance_policy TEXT NOT NULL,
    FOREIGN KEY (batch_uid) REFERENCES batches(batch_uid)
);

CREATE INDEX IF NOT EXISTS idx_checks_batch ON precision_checks(batch_uid, passed);

CREATE TABLE IF NOT EXISTS run_quantiles (
    run_id TEXT NOT NULL,
    label TEXT NOT NULL,
    percentile INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    raw_us INTEGER NOT NULL,
    output_ms_x100 INTEGER NOT NULL,
    rounded_us INTEGER NOT NULL,
    rounding_error_us INTEGER NOT NULL,
    PRIMARY KEY (run_id, label),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    generated_at TEXT NOT NULL,
    pending_count INTEGER NOT NULL,
    body_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_events (
    event_id TEXT PRIMARY KEY,
    report_id TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    run_id TEXT,
    action TEXT NOT NULL,
    details_json TEXT NOT NULL,
    FOREIGN KEY (report_id) REFERENCES recovery_reports(report_id)
);
"""


class Store:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def initialize(self) -> None:
        with self.lock:
            connection = self._connect()
            try:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.executescript(SCHEMA)
            finally:
                connection.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self.lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            finally:
                connection.close()

    def _read(self, callback) -> Any:
        with self.lock:
            connection = self._connect()
            try:
                return callback(connection)
            finally:
                connection.close()

    def _create_intent(
        self,
        *,
        kind: str,
        trace_id: str | None,
        run_id: str | None,
        payload: dict[str, Any],
    ) -> str:
        intent_id = _new_id("intent")
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO intents(
                    intent_id, kind, trace_id, run_id, status, checkpoint,
                    payload_json, created_at
                ) VALUES (?, ?, ?, ?, 'PENDING', 'INTENT_COMMITTED', ?, ?)
                """,
                (intent_id, kind, trace_id, run_id, _json(payload), _now()),
            )
        return intent_id

    def fail_intent(
        self,
        intent_id: str,
        error: BaseException,
        *,
        run_id: str | None = None,
    ) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status FROM intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            if row is None or row["status"] != "PENDING":
                return
            connection.execute(
                """
                UPDATE intents
                SET status = 'ABORTED', error_json = ?, resolved_at = ?, resolution = ?
                WHERE intent_id = ?
                """,
                (
                    _json({"type": type(error).__name__, "message": str(error)}),
                    _now(),
                    "OPERATION_FAILED_BEFORE_FINAL_COMMIT",
                    intent_id,
                ),
            )
            if run_id:
                connection.execute(
                    """
                    UPDATE runs
                    SET state = 'ABORTED', error_code = 'E_INTERNAL',
                        committed_at = NULL
                    WHERE run_id = ? AND state = 'QUANTILES_PENDING'
                    """,
                    (run_id,),
                )
                connection.execute(
                    """
                    UPDATE batches SET state = 'ABORTED'
                    WHERE run_id = ? AND state = 'QUANTILES_PENDING'
                    """,
                    (run_id,),
                )

    def submit_trace(self, trace: NormalizedTrace) -> dict[str, Any]:
        existing = self.get_trace(trace.trace_id)
        if existing is not None:
            if existing["payload_digest"] != trace.payload_digest:
                raise ConflictError(
                    "trace_id already exists with a different normalized payload",
                    details={
                        "trace_id": trace.trace_id,
                        "existing_digest": existing["payload_digest"],
                        "submitted_digest": trace.payload_digest,
                    },
                )
            return {"inserted": False, "trace": existing}

        intent_id = self._create_intent(
            kind="SUBMIT_TRACE",
            trace_id=trace.trace_id,
            run_id=None,
            payload={"payload_digest": trace.payload_digest, "request_count": len(trace.requests)},
        )
        try:
            with self._transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO traces(
                        trace_id, name, payload_digest, source_schema,
                        normalized_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trace.trace_id,
                        trace.name,
                        trace.payload_digest,
                        trace.source_schema,
                        _json(trace.as_dict()),
                        _now(),
                    ),
                )
                for model in trace.models.values():
                    connection.execute(
                        """
                        INSERT INTO trace_models(
                            trace_id, model_id, memory_bytes, cost_us, max_batch_samples
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            trace.trace_id,
                            model.model_id,
                            model.memory_bytes,
                            model.cost_us,
                            model.max_batch_samples,
                        ),
                    )
                for request in trace.requests:
                    connection.execute(
                        """
                        INSERT INTO trace_requests(
                            trace_id, request_id, seq, arrival_us, model_id,
                            samples, expected_latency_us, priority
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            trace.trace_id,
                            request.request_id,
                            request.seq,
                            request.arrival_us,
                            request.model_id,
                            request.samples,
                            request.expected_latency_us,
                            request.priority,
                        ),
                    )
                connection.execute(
                    """
                    UPDATE intents
                    SET status = 'COMMITTED', checkpoint = 'TRACE_COMMITTED',
                        resolved_at = ?, resolution = 'TRACE_WRITTEN'
                    WHERE intent_id = ?
                    """,
                    (_now(), intent_id),
                )
        except BaseException as exc:
            self.fail_intent(intent_id, exc)
            raise
        return {"inserted": True, "trace": self.get_trace(trace.trace_id)}

    def get_trace(self, trace_id: str) -> dict[str, Any] | None:
        def operation(connection: sqlite3.Connection):
            row = connection.execute(
                "SELECT * FROM traces WHERE trace_id = ?", (trace_id,)
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["normalized"] = _loads(result.pop("normalized_json"))
            return result

        return self._read(operation)

    def create_run_intent(
        self,
        *,
        trace_id: str,
        run_id: str,
        crash_at: str | None,
    ) -> str:
        return self._create_intent(
            kind="ORGANIZE_RUN",
            trace_id=trace_id,
            run_id=run_id,
            payload={"crash_at": crash_at, "stage": "before_schedule_persist"},
        )

    def persist_run_skeleton(
        self,
        *,
        intent_id: str,
        run_id: str,
        trace: NormalizedTrace,
        schedule: ScheduleResult,
    ) -> None:
        schedule_body = {
            "algorithm_version": schedule.algorithm_version,
            "deterministic_schedule_digest": schedule.deterministic_digest,
            "node_total_bytes": schedule.node_total_bytes,
            "node_initial_free_bytes": schedule.node_initial_free_bytes,
            "node_final_free_bytes": schedule.node_final_free_bytes,
            "initial_arena": schedule.initial_arena,
            "final_arena": schedule.final_arena,
            "initial_bitmap": schedule.initial_bitmap,
            "final_bitmap": schedule.final_bitmap,
            "fragmentation_events": [event.as_dict() for event in schedule.fragmentation_events],
            "mixed_rejections": schedule.mixed_rejections,
        }
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO runs(
                    run_id, trace_id, state, error_code, deterministic_digest,
                    schedule_digest, algorithm_version, node_json, schedule_json,
                    created_at
                ) VALUES (?, ?, 'QUANTILES_PENDING', NULL, NULL, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    trace.trace_id,
                    schedule.deterministic_digest,
                    schedule.algorithm_version,
                    _json(trace.node.as_dict()),
                    _json(schedule_body),
                    _now(),
                ),
            )
            for batch in schedule.batches:
                batch_uid = f"{run_id}:{batch.batch_id}"
                connection.execute(
                    """
                    INSERT INTO batches(
                        batch_uid, run_id, batch_id, ordinal, state, model_id, priority,
                        memory_bytes, latency_us, latency_ms_x100, latency_rounding_error_us,
                        sample_count, member_count, first_arrival_us, last_arrival_us,
                        timeline_start_us, timeline_end_us, region_start, region_size,
                        min_expected_latency_us, split_reasons_json, split_notes_json,
                        bitmap_json, fragmentation_event_id
                    ) VALUES (?, ?, ?, ?, 'QUANTILES_PENDING', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        batch_uid,
                        run_id,
                        batch.batch_id,
                        batch.ordinal,
                        batch.model_id,
                        batch.priority,
                        batch.memory_bytes,
                        batch.latency_us,
                        batch.latency_us // 10,
                        0,
                        batch.sample_count,
                        batch.member_count,
                        batch.first_arrival_us,
                        batch.last_arrival_us,
                        batch.timeline_start_us,
                        batch.timeline_end_us,
                        batch.region_start,
                        batch.region_size,
                        batch.min_expected_latency_us,
                        _json(batch.split_reasons),
                        _json(batch.split_notes),
                        _json(batch.bitmap_after),
                        batch.fragmentation_event_id,
                    ),
                )
                for index, member in enumerate(batch.members, start=1):
                    connection.execute(
                        """
                        INSERT INTO batch_members(
                            member_uid, batch_uid, request_id, request_seq, arrival_us,
                            model_id, samples, part_index, part_count, memory_bytes,
                            latency_us, expected_latency_us, priority, origin
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            f"{batch_uid}:m{index:04d}",
                            batch_uid,
                            member.request.request_id,
                            member.request.seq,
                            member.request.arrival_us,
                            member.model.model_id,
                            member.samples,
                            member.part_index,
                            member.part_count,
                            member.memory_bytes,
                            member.latency_us,
                            member.request.expected_latency_us,
                            member.request.priority,
                            member.origin,
                        ),
                    )
            connection.execute(
                """
                UPDATE intents
                SET checkpoint = 'BATCHES_PERSISTED_QUANTILES_PENDING'
                WHERE intent_id = ? AND status = 'PENDING'
                """,
                (intent_id,),
            )

    def finalize_run(
        self,
        *,
        intent_id: str,
        run_id: str,
        computations: list[BatchComputation],
        run_quantiles: list,
        deterministic_digest: str,
    ) -> dict[str, Any]:
        failed_batches = [item.batch.batch_id for item in computations if not item.passed]
        state = "VALIDATION_FAILED" if failed_batches else "COMMITTED"
        error_code = "E_PRECISION_DRIFT" if failed_batches else None
        with self._transaction() as connection:
            for computation in computations:
                batch = computation.batch
                batch_uid = f"{run_id}:{batch.batch_id}"
                failed_fields = computation.failed_fields
                connection.execute(
                    """
                    UPDATE batches
                    SET state = ?, validation_state = ?, failed_fields_json = ?,
                        engine_trace_json = ?, error_chain_json = ?, rounding_json = ?,
                        latency_rounding_error_us = ?
                    WHERE batch_uid = ?
                    """,
                    (
                        computation.validation_state,
                        computation.validation_state,
                        _json(failed_fields),
                        _json(computation.engine_trace),
                        _json(computation.error_chain),
                        _json(computation.rounding_summary),
                        int(computation.rounding_summary["cumulative_rounding_error_us_signed"]),
                        batch_uid,
                    ),
                )
                for quantile in computation.quantiles:
                    connection.execute(
                        """
                        INSERT INTO batch_quantiles(
                            batch_uid, label, percentile, sample_count, rank, raw_us,
                            output_ms_x100, rounded_us, rounding_error_us
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            batch_uid,
                            quantile.label,
                            quantile.percentile,
                            quantile.sample_count,
                            quantile.rank,
                            quantile.raw_us,
                            quantile.output_ms_x100,
                            quantile.rounded_us,
                            quantile.rounding_error_us,
                        ),
                    )
                for check in computation.checks:
                    connection.execute(
                        """
                        INSERT INTO precision_checks(
                            batch_uid, scope, subject_id, metric, unit, truth, estimate,
                            absolute_deviation, relative_deviation_ppm, absolute_bound,
                            relative_bound_ppm, passed, note, tolerance_policy
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            batch_uid,
                            check.scope,
                            check.subject_id,
                            check.metric,
                            check.unit,
                            check.truth,
                            check.estimate,
                            check.absolute_deviation,
                            check.relative_deviation_ppm,
                            check.absolute_bound,
                            check.relative_bound_ppm,
                            1 if check.passed else 0,
                            check.note,
                            check.as_dict()["tolerance_policy"],
                        ),
                    )
            for quantile in run_quantiles:
                connection.execute(
                    """
                    INSERT INTO run_quantiles(
                        run_id, label, percentile, sample_count, rank, raw_us,
                        output_ms_x100, rounded_us, rounding_error_us
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        quantile.label,
                        quantile.percentile,
                        quantile.sample_count,
                        quantile.rank,
                        quantile.raw_us,
                        quantile.output_ms_x100,
                        quantile.rounded_us,
                        quantile.rounding_error_us,
                    ),
                )
            connection.execute(
                """
                UPDATE runs
                SET state = ?, error_code = ?, deterministic_digest = ?,
                    committed_at = ?
                WHERE run_id = ?
                """,
                (state, error_code, deterministic_digest, _now(), run_id),
            )
            connection.execute(
                """
                UPDATE intents
                SET status = 'COMMITTED', checkpoint = 'RUN_FINAL_COMMIT',
                    resolved_at = ?, resolution = ?
                WHERE intent_id = ?
                """,
                (
                    _now(),
                    "RUN_COMMITTED_WITH_PRECISION_DRIFT"
                    if failed_batches
                    else "RUN_COMMITTED",
                    intent_id,
                ),
            )
        return {
            "run_id": run_id,
            "state": state,
            "error_code": error_code,
            "failed_batches": failed_batches,
        }

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        def operation(connection: sqlite3.Connection):
            row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["node"] = _loads(result.pop("node_json"))
            result["schedule"] = _loads(result.pop("schedule_json"))
            result["quantiles"] = [
                dict(item)
                for item in connection.execute(
                    "SELECT * FROM run_quantiles WHERE run_id = ? ORDER BY percentile", (run_id,)
                )
            ]
            result["batches"] = [
                dict(item)
                for item in connection.execute(
                    """
                    SELECT * FROM batches WHERE run_id = ? ORDER BY ordinal
                    """,
                    (run_id,),
                )
            ]
            return result

        return self._read(operation)

    def require_run(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run is None:
            raise RunNotFoundError("run not found", details={"run_id": run_id})
        return run

    def list_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        def operation(connection: sqlite3.Connection):
            rows = connection.execute(
                """
                SELECT run_id, trace_id, state, error_code, deterministic_digest,
                       algorithm_version, created_at, committed_at
                FROM runs ORDER BY created_at DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]

        return self._read(operation)

    def get_batch_bundle(self, batch_uid: str) -> dict[str, Any] | None:
        def operation(connection: sqlite3.Connection):
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_uid = ?", (batch_uid,)
            ).fetchone()
            if row is None:
                return None
            batch = dict(row)
            for key in ("split_reasons_json", "split_notes_json", "bitmap_json", "failed_fields_json"):
                if key in batch:
                    batch[key] = _loads(batch[key], [])
            for key in ("engine_trace_json", "error_chain_json", "rounding_json"):
                if key in batch:
                    batch[key] = _loads(batch[key])
            batch["members"] = [
                dict(item)
                for item in connection.execute(
                    "SELECT * FROM batch_members WHERE batch_uid = ? ORDER BY request_seq, part_index",
                    (batch_uid,),
                )
            ]
            batch["quantiles"] = [
                dict(item)
                for item in connection.execute(
                    "SELECT * FROM batch_quantiles WHERE batch_uid = ? ORDER BY percentile",
                    (batch_uid,),
                )
            ]
            batch["checks"] = [
                dict(item)
                for item in connection.execute(
                    "SELECT * FROM precision_checks WHERE batch_uid = ? ORDER BY check_id",
                    (batch_uid,),
                )
            ]
            for check in batch["checks"]:
                check["passed"] = bool(check["passed"])
            return batch

        return self._read(operation)

    def recover_pending_intents(self) -> dict[str, Any]:
        def find_pending(connection: sqlite3.Connection):
            return [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM intents WHERE status = 'PENDING' ORDER BY created_at, intent_id"
                )
            ]

        pending = self._read(find_pending)
        report_intent = self._create_intent(
            kind="RECOVERY_SCAN",
            trace_id=None,
            run_id=None,
            payload={"pending_count": len(pending), "scope": "startup"},
        )
        report_id = _new_id("recovery")
        generated_at = _now()
        events: list[dict[str, Any]] = []
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO recovery_reports(report_id, generated_at, pending_count, body_json)
                VALUES (?, ?, ?, '{}')
                """,
                (report_id, generated_at, len(pending)),
            )
            for intent in pending:
                run_id = intent["run_id"]
                action = "INTENT_ABORTED_NO_PERSISTED_RUN"
                run_state = None
                marked_batches = 0
                if run_id:
                    run_row = connection.execute(
                        "SELECT state FROM runs WHERE run_id = ?", (run_id,)
                    ).fetchone()
                    if run_row is None:
                        action = "INTENT_ABORTED_NO_PERSISTED_RUN"
                    elif run_row["state"] == "COMMITTED":
                        action = "KEPT_LAST_COMPLETE_COMMIT"
                        run_state = run_row["state"]
                    elif run_row["state"] == "VALIDATION_FAILED":
                        action = "KEPT_LAST_COMPLETE_COMMIT"
                        run_state = run_row["state"]
                    else:
                        connection.execute(
                            """
                            UPDATE runs
                            SET state = 'ABORTED', error_code = 'E_PROCESS_TERMINATED',
                                committed_at = NULL
                            WHERE run_id = ?
                            """,
                            (run_id,),
                        )
                        cursor = connection.execute(
                            """
                            UPDATE batches SET state = 'ABORTED'
                            WHERE run_id = ? AND state NOT IN ('VERIFIED', 'VALIDATION_FAILED')
                            """,
                            (run_id,),
                        )
                        marked_batches = cursor.rowcount if cursor.rowcount > 0 else 0
                        action = "RUN_AND_PENDING_BATCHES_ABORTED"
                        run_state = "ABORTED"
                connection.execute(
                    """
                    UPDATE intents
                    SET status = 'ABORTED', resolved_at = ?, resolution = ?,
                        error_json = ?
                    WHERE intent_id = ? AND status = 'PENDING'
                    """,
                    (
                        generated_at,
                        action,
                        _json(
                            {
                                "code": "E_PROCESS_TERMINATED",
                                "message": "Recovered after an interrupted process; no final commit exists.",
                            }
                        ),
                        intent["intent_id"],
                    ),
                )
                event = {
                    "event_id": _new_id("recovery-event"),
                    "intent_id": intent["intent_id"],
                    "intent_kind": intent["kind"],
                    "run_id": run_id,
                    "last_checkpoint": intent["checkpoint"],
                    "run_state_after_recovery": run_state,
                    "action": action,
                    "batches_marked_aborted": marked_batches,
                }
                events.append(event)
                connection.execute(
                    """
                    INSERT INTO recovery_events(
                        event_id, report_id, intent_id, run_id, action, details_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event["event_id"],
                        report_id,
                        intent["intent_id"],
                        run_id,
                        action,
                        _json(event),
                    ),
                )
            recent_runs = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT run_id, trace_id, state, error_code, committed_at
                    FROM runs ORDER BY created_at DESC LIMIT 10
                    """
                )
            ]
            body = {
                "report_id": report_id,
                "generated_at": generated_at,
                "scope": "SQLite committed rows only; generated during startup scan",
                "pending_intents_found": len(pending),
                "events": events,
                "recent_runs": recent_runs,
                "guarantees": [
                    "Completed final transactions are preserved atomically.",
                    "Pending intents without a final commit are marked aborted.",
                    "No metrics are fabricated for an aborted batch.",
                ],
                "limitations": [
                    "A forcibly terminated process cannot emit a graceful shutdown report.",
                    "This report does not claim that an interrupted request completed.",
                ],
            }
            connection.execute(
                "UPDATE recovery_reports SET body_json = ? WHERE report_id = ?",
                (_json(body), report_id),
            )
            connection.execute(
                """
                UPDATE intents
                SET status = 'COMMITTED', checkpoint = 'RECOVERY_REPORT_COMMITTED',
                    resolved_at = ?, resolution = 'RECOVERY_SCAN_COMPLETED'
                WHERE intent_id = ?
                """,
                (_now(), report_intent),
            )
        return body

    def latest_recovery_report(self) -> dict[str, Any] | None:
        def operation(connection: sqlite3.Connection):
            row = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY generated_at DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["body"] = _loads(result.pop("body_json"))
            return result

        return self._read(operation)

    def intent_stats(self) -> dict[str, int]:
        def operation(connection: sqlite3.Connection):
            rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM intents GROUP BY status"
            ).fetchall()
            return {row["status"]: row["count"] for row in rows}

        return self._read(operation)

    def intent_log(self, limit: int = 100) -> list[dict[str, Any]]:
        def operation(connection: sqlite3.Connection):
            rows = connection.execute(
                """
                SELECT intent_id, kind, trace_id, run_id, status, checkpoint,
                       payload_json, error_json, created_at, resolved_at, resolution
                FROM intents ORDER BY created_at DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["payload"] = _loads(item.pop("payload_json"), {})
                item["error"] = _loads(item.pop("error_json"))
                result.append(item)
            return result

        return self._read(operation)
