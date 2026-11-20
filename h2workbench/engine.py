from __future__ import annotations

import copy
import hashlib
import json
import os
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Any, Mapping

from .db import Database, json_dumps, json_loads, utc_now
from .errors import BusinessError
from .ingest import IngestPlan, build_ingest_plan
from .models import (
    ConnectionConfig,
    JobStatus,
    RunStatus,
    SampleSpec,
    SchedulePlan,
    SimConnection,
    SimJob,
    SimStream,
    StreamState,
)
from .scheduler import ScheduleOptions, build_schedule_plan
from .validation import normalize_sample


def _config_from_json(raw: str) -> ConnectionConfig:
    value = json.loads(raw)
    return ConnectionConfig(
        connection_window=int(value["connection_window"]),
        stream_window=int(value["stream_window"]),
        round_budget=int(value["round_budget"]),
        candidate_threshold=int(value["candidate_threshold"]),
        candidate_limit=int(value["candidate_limit"]),
        priority_scale=int(value["priority_scale"]),
        max_rounds=int(value["max_rounds"]),
    )


def _connection_from_row(row: sqlite3.Row) -> SimConnection:
    return SimConnection(
        connection_id=row["connection_id"],
        initial_connection_window=row["initial_connection_window"],
        connection_window=row["connection_window"],
        initial_stream_window=row["initial_stream_window"],
        closed=bool(row["closed"]),
        config=_config_from_json(row["config_json"]),
        protocol_violations=row["protocol_violations"],
    )


def _stream_from_row(row: sqlite3.Row) -> SimStream:
    return SimStream(
        stream_id=row["stream_id"],
        state=StreamState(row["state"]),
        initial_window=row["initial_window"],
        window=row["window"],
        sent_unacked=row["sent_unacked"],
        pending_bytes=row["pending_bytes"],
        weight=row["weight"],
        priority=row["priority"],
        sent_total=row["sent_total"],
        pending_end_stream=bool(row["pending_end_stream"]),
    )


def _job_from_row(row: sqlite3.Row) -> SimJob:
    return SimJob(
        job_key=row["job_id"],
        sample_id=row["sample_id"],
        frame_index=row["frame_index"],
        stream_id=row["stream_id"],
        remaining_bytes=row["remaining_bytes"],
        original_bytes=row["original_bytes"],
        end_stream=bool(row["end_stream"]),
        status=row["status"],
    )


class Workbench:
    """SQLite-backed core service.

    The engine is intentionally synchronous. FastAPI runs synchronous routes in
    its thread pool; a per-connection lock serializes writes while independent
    read transactions provide snapshot visibility.
    """

    def __init__(self, db_path: str | Path, *, recover: bool = True):
        self.db = Database(db_path)
        self.db.initialize()
        self._lock_guard = threading.Lock()
        self._connection_locks: dict[str, threading.RLock] = {}
        self._sample_locks: dict[str, threading.RLock] = {}
        self.recovery_reports: list[dict[str, Any]] = []
        if recover:
            self.recovery_reports = self.recover_pending_intents()

    def _connection_lock(self, connection_id: str) -> threading.RLock:
        with self._lock_guard:
            return self._connection_locks.setdefault(connection_id, threading.RLock())

    def _sample_lock(self, sample_id: str) -> threading.RLock:
        with self._lock_guard:
            return self._sample_locks.setdefault(sample_id, threading.RLock())

    # ------------------------------------------------------------------
    # Read helpers and snapshots
    # ------------------------------------------------------------------

    def _load_connection(self, conn: sqlite3.Connection, connection_id: str) -> SimConnection | None:
        row = conn.execute(
            "SELECT * FROM connections WHERE connection_id = ?", (connection_id,)
        ).fetchone()
        return _connection_from_row(row) if row else None

    def _load_streams(self, conn: sqlite3.Connection, connection_id: str) -> dict[int, SimStream]:
        rows = conn.execute(
            "SELECT * FROM streams WHERE connection_id = ? ORDER BY stream_id",
            (connection_id,),
        ).fetchall()
        streams = [_stream_from_row(row) for row in rows]
        return {stream.stream_id: stream for stream in streams}

    def _load_jobs(
        self,
        conn: sqlite3.Connection,
        connection_id: str,
        *,
        sample_id: str | None = None,
        pending_only: bool = True,
    ) -> list[SimJob]:
        clauses = ["connection_id = ?"]
        params: list[Any] = [connection_id]
        if sample_id is not None:
            clauses.append("sample_id = ?")
            params.append(sample_id)
        if pending_only:
            clauses.append("status = 'PENDING'")
        rows = conn.execute(
            "SELECT * FROM data_jobs WHERE "
            + " AND ".join(clauses)
            + " ORDER BY job_id",
            tuple(params),
        ).fetchall()
        return [_job_from_row(row) for row in rows]

    def _connection_snapshot(
        self, conn: sqlite3.Connection, connection_id: str
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM connections WHERE connection_id = ?", (connection_id,)
        ).fetchone()
        if row is None:
            raise BusinessError(
                code="E_CONNECTION_NOT_FOUND",
                message="connection was not found",
                connection_id=connection_id,
                context={"connection_id": connection_id},
            )
        stream_rows = conn.execute(
            "SELECT * FROM streams WHERE connection_id = ? ORDER BY stream_id",
            (connection_id,),
        ).fetchall()
        pending = conn.execute(
            "SELECT COUNT(*) AS jobs, COALESCE(SUM(remaining_bytes), 0) AS bytes "
            "FROM data_jobs WHERE connection_id = ? AND status = 'PENDING'",
            (connection_id,),
        ).fetchone()
        streams = [
            {
                "stream_id": item["stream_id"],
                "state": item["state"],
                "initial_window": item["initial_window"],
                "window": item["window"],
                "sent_unacked": item["sent_unacked"],
                "pending_bytes": item["pending_bytes"],
                "weight": item["weight"],
                "priority": item["priority"],
                "sent_total": item["sent_total"],
                "pending_end_stream": bool(item["pending_end_stream"]),
            }
            for item in stream_rows
        ]
        return {
            "connection_id": row["connection_id"],
            "initial_connection_window": row["initial_connection_window"],
            "connection_window": row["connection_window"],
            "initial_stream_window": row["initial_stream_window"],
            "closed": bool(row["closed"]),
            "protocol_violations": row["protocol_violations"],
            "config": json_loads(row["config_json"], {}),
            "streams": streams,
            "pending_jobs": pending["jobs"],
            "pending_bytes": pending["bytes"],
        }

    def get_connection(self, connection_id: str) -> dict[str, Any]:
        with self.db.snapshot() as conn:
            return self._connection_snapshot(conn, connection_id)

    def get_stream(self, connection_id: str, stream_id: int) -> dict[str, Any]:
        with self.db.snapshot() as conn:
            snapshot = self._connection_snapshot(conn, connection_id)
            for stream in snapshot["streams"]:
                if stream["stream_id"] == stream_id:
                    return {
                        "connection_id": connection_id,
                        "connection_window": snapshot["connection_window"],
                        "stream": stream,
                        "snapshot_pending_jobs": snapshot["pending_jobs"],
                    }
        raise BusinessError(
            code="E_STREAM_NOT_FOUND",
            message="stream was not found",
            connection_id=connection_id,
            context={"stream_id": stream_id},
        )

    def list_connections(self) -> dict[str, Any]:
        with self.db.snapshot() as conn:
            rows = conn.execute(
                "SELECT c.connection_id, c.connection_window, c.initial_connection_window, "
                "c.closed, c.protocol_violations, COUNT(s.stream_id) AS stream_count "
                "FROM connections c LEFT JOIN streams s ON s.connection_id = c.connection_id "
                "GROUP BY c.connection_id ORDER BY c.connection_id"
            ).fetchall()
            return {"connections": [dict(row) for row in rows]}

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def _cached_sample(self, row: sqlite3.Row) -> dict[str, Any]:
        response = json_loads(row["response_json"], {})
        response = copy.deepcopy(response)
        response["cache_replayed"] = True
        return response

    def _existing_sample(self, sample_id: str) -> sqlite3.Row | None:
        return self.db.fetch_one("SELECT * FROM samples WHERE sample_id = ?", (sample_id,))

    def submit_sample(
        self, raw: Mapping[str, Any] | SampleSpec
    ) -> dict[str, Any]:
        spec = raw if isinstance(raw, SampleSpec) else normalize_sample(dict(raw))
        sample_lock = self._sample_lock(spec.sample_id)
        with sample_lock:
            existing = self._existing_sample(spec.sample_id)
            if existing is not None:
                return self._reuse_or_conflict(existing, spec)

            connection_lock = self._connection_lock(spec.connection_id)
            with connection_lock:
                existing = self._existing_sample(spec.sample_id)
                if existing is not None:
                    return self._reuse_or_conflict(existing, spec)
                return self._ingest_locked(spec)

    def _reuse_or_conflict(self, row: sqlite3.Row, spec: SampleSpec) -> dict[str, Any]:
        if row["content_hash"] != spec.content_hash:
            raise BusinessError(
                code="E_CONFLICTING_SAMPLE",
                message="sample_id was already submitted with different content",
                connection_id=spec.connection_id,
                context={
                    "sample_id": spec.sample_id,
                    "existing_hash": row["content_hash"],
                    "submitted_hash": spec.content_hash,
                    "existing_connection_id": row["connection_id"],
                },
            )
        return self._cached_sample(row)

    def _ingest_locked(self, spec: SampleSpec) -> dict[str, Any]:
        with self.db.snapshot() as conn:
            connection = self._load_connection(conn, spec.connection_id)
            streams = self._load_streams(conn, spec.connection_id)

        plan = build_ingest_plan(spec, connection=connection, streams=streams)
        intent_id = f"ingest-{uuid.uuid4().hex}"
        now = utc_now()
        intent_summary = {
            "kind": "INGEST",
            "sample_id": spec.sample_id,
            "connection_id": spec.connection_id,
            "frame_count": len(spec.frames),
            "content_hash": spec.content_hash,
        }
        with self.db.transaction(immediate=True) as conn:
            conn.execute(
                "INSERT INTO intents(intent_id, kind, connection_id, sample_id, status, plan_json, created_at) "
                "VALUES (?, 'INGEST', ?, ?, 'PENDING', ?, ?)",
                (intent_id, spec.connection_id, spec.sample_id, json_dumps(intent_summary), now),
            )

        try:
            with self.db.transaction(immediate=True) as conn:
                response = self._apply_ingest(conn, plan, intent_id, now)
        except sqlite3.IntegrityError:
            self._abort_intent(intent_id, "ingest transaction lost a concurrent sample-id race")
            existing = self._existing_sample(spec.sample_id)
            if existing is not None:
                return self._reuse_or_conflict(existing, spec)
            raise
        except BaseException:
            self._abort_intent(intent_id, "ingest transaction did not commit")
            raise
        return response

    def _apply_ingest(
        self,
        conn: sqlite3.Connection,
        plan: IngestPlan,
        intent_id: str,
        started_at: str,
    ) -> dict[str, Any]:
        now = utc_now()
        spec = plan.spec
        sim_conn = plan.connection
        if plan.cancel_all_existing_jobs:
            conn.execute(
                "UPDATE data_jobs SET status = 'CANCELLED', remaining_bytes = 0, updated_at = ? "
                "WHERE connection_id = ? AND status = 'PENDING'",
                (now, spec.connection_id),
            )
        elif plan.cancel_existing_stream_ids:
            placeholders = ",".join("?" for _ in plan.cancel_existing_stream_ids)
            params = [now, spec.connection_id, *sorted(plan.cancel_existing_stream_ids)]
            conn.execute(
                "UPDATE data_jobs SET status = 'CANCELLED', remaining_bytes = 0, updated_at = ? "
                f"WHERE connection_id = ? AND status = 'PENDING' AND stream_id IN ({placeholders})",
                tuple(params),
            )

        conn.execute(
            "INSERT INTO connections(connection_id, initial_connection_window, connection_window, "
            "initial_stream_window, closed, config_json, protocol_violations, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(connection_id) DO UPDATE SET "
            "connection_window = excluded.connection_window, closed = excluded.closed, "
            "protocol_violations = excluded.protocol_violations, updated_at = excluded.updated_at",
            (
                sim_conn.connection_id,
                sim_conn.initial_connection_window,
                sim_conn.connection_window,
                sim_conn.initial_stream_window,
                int(sim_conn.closed),
                json_dumps(sim_conn.config.to_dict()),
                sim_conn.protocol_violations,
                now,
                now,
            ),
        )

        for stream in sorted(plan.streams.values(), key=lambda item: item.stream_id):
            conn.execute(
                "INSERT INTO streams(connection_id, stream_id, state, initial_window, window, "
                "sent_unacked, pending_bytes, weight, priority, sent_total, pending_end_stream, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(connection_id, stream_id) DO UPDATE SET "
                "state = excluded.state, initial_window = excluded.initial_window, "
                "window = excluded.window, sent_unacked = excluded.sent_unacked, "
                "pending_bytes = excluded.pending_bytes, weight = excluded.weight, "
                "priority = excluded.priority, sent_total = excluded.sent_total, "
                "pending_end_stream = excluded.pending_end_stream, updated_at = excluded.updated_at",
                (
                    spec.connection_id,
                    stream.stream_id,
                    stream.state.value,
                    stream.initial_window,
                    stream.window,
                    stream.sent_unacked,
                    stream.pending_bytes,
                    stream.weight,
                    stream.priority,
                    stream.sent_total,
                    int(stream.pending_end_stream),
                    now,
                    now,
                ),
            )

        conn.execute(
            "INSERT INTO samples(sample_id, connection_id, content_hash, canonical_json, "
            "frame_count, batch_size, status, response_json, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'INGESTED', '{}', ?, ?)",
            (
                spec.sample_id,
                spec.connection_id,
                spec.content_hash,
                spec.canonical_json,
                len(spec.frames),
                spec.batch_size,
                now,
                now,
            ),
        )

        for job in plan.jobs:
            conn.execute(
                "INSERT INTO data_jobs(connection_id, sample_id, frame_index, stream_id, "
                "original_bytes, remaining_bytes, end_stream, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    spec.connection_id,
                    spec.sample_id,
                    job.frame_index,
                    job.stream_id,
                    job.original_bytes,
                    job.remaining_bytes,
                    int(job.end_stream),
                    job.status,
                    now,
                    now,
                ),
            )

        for item in plan.transitions:
            conn.execute(
                "INSERT INTO state_transitions(connection_id, stream_id, sample_id, frame_index, "
                "from_state, to_state, trigger, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    spec.connection_id,
                    item.stream_id,
                    spec.sample_id,
                    item.frame_index,
                    item.from_state.value,
                    item.to_state.value,
                    item.trigger,
                    now,
                ),
            )

        for item in plan.violations:
            conn.execute(
                "INSERT INTO protocol_violations(connection_id, stream_id, sample_id, frame_index, "
                "code, requested_increment, applied_increment, current_before, initial_limit, "
                "details_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    spec.connection_id,
                    item.stream_id,
                    spec.sample_id,
                    item.frame_index,
                    item.code,
                    item.requested_increment,
                    item.applied_increment,
                    item.current_before,
                    item.initial_limit,
                    json_dumps(item.details),
                    now,
                ),
            )

        conn.execute(
            "UPDATE intents SET status = 'COMMITTED', committed_at = ? WHERE intent_id = ?",
            (now, intent_id),
        )

        snapshot = self._connection_snapshot(conn, spec.connection_id)
        pending_jobs = [job for job in plan.jobs if job.status == JobStatus.PENDING.value]
        response = {
            "sample": {
                "sample_id": spec.sample_id,
                "connection_id": spec.connection_id,
                "content_hash": spec.content_hash,
                "frame_count": len(spec.frames),
                "batch_size": spec.batch_size,
                "status": "INGESTED",
            },
            "batches": plan.batches,
            "connection": snapshot,
            "transitions": [
                {
                    "stream_id": item.stream_id,
                    "frame_index": item.frame_index,
                    "from_state": item.from_state.value,
                    "to_state": item.to_state.value,
                    "trigger": item.trigger,
                }
                for item in plan.transitions
            ],
            "protocol_violations": [
                {
                    "stream_id": item.stream_id,
                    "frame_index": item.frame_index,
                    "code": item.code,
                    "requested_increment": item.requested_increment,
                    "applied_increment": item.applied_increment,
                    "current_before": item.current_before,
                    "initial_limit": item.initial_limit,
                    "details": item.details,
                }
                for item in plan.violations
            ],
            "queued_data": {
                "jobs": len(pending_jobs),
                "bytes": sum(job.remaining_bytes for job in pending_jobs),
                "cancelled_jobs": sum(
                    1 for job in plan.jobs if job.status == JobStatus.CANCELLED.value
                ),
            },
            "cache_replayed": False,
        }
        conn.execute(
            "UPDATE samples SET response_json = ?, updated_at = ? WHERE sample_id = ?",
            (json_dumps(response), now, spec.sample_id),
        )
        return response

    def _abort_intent(self, intent_id: str, reason: str) -> None:
        try:
            with self.db.transaction(immediate=True) as conn:
                conn.execute(
                    "UPDATE intents SET status = 'ABORTED', recovered_at = ?, failure_reason = ? "
                    "WHERE intent_id = ? AND status = 'PENDING'",
                    (utc_now(), reason, intent_id),
                )
        except sqlite3.Error:
            pass

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------

    def _parse_schedule_options(
        self, config: ConnectionConfig, raw: Mapping[str, Any] | None
    ) -> ScheduleOptions:
        raw = dict(raw or {})

        def optional_int(name: str) -> int | None:
            if name not in raw or raw[name] is None:
                return None
            value = raw[name]
            if isinstance(value, bool) or not isinstance(value, int):
                raise BusinessError(
                    code="E_INVALID_CONFIG",
                    message=f"{name} must be an integer",
                    context={"field": name, "value": value},
                )
            return value

        round_budget = optional_int("round_budget")
        max_rounds = optional_int("max_rounds")
        threshold = optional_int("candidate_threshold")
        limit = optional_int("candidate_limit")
        if round_budget is not None and not 1 <= round_budget <= (1 << 31) - 1:
            raise BusinessError(
                code="E_INVALID_CONFIG",
                message="round_budget must be positive",
                context={"field": "round_budget", "value": round_budget},
            )
        if max_rounds is not None and not 1 <= max_rounds <= 10_000_000:
            raise BusinessError(
                code="E_INVALID_CONFIG",
                message="max_rounds must be positive",
                context={"field": "max_rounds", "value": max_rounds},
            )
        if threshold is not None and threshold < 1:
            raise BusinessError(
                code="E_INVALID_CONFIG",
                message="candidate_threshold must be positive",
                context={"field": "candidate_threshold", "value": threshold},
            )
        if limit is not None and limit < 1:
            raise BusinessError(
                code="E_INVALID_CONFIG",
                message="candidate_limit must be positive",
                context={"field": "candidate_limit", "value": limit},
            )
        try:
            return ScheduleOptions.from_config(
                config,
                mode=raw.get("scheduler_mode", raw.get("mode")),
                round_budget=round_budget,
                max_rounds=max_rounds,
                candidate_threshold=threshold,
                candidate_limit=limit,
            )
        except ValueError as exc:
            raise BusinessError(
                code="E_INVALID_CONFIG",
                message=str(exc),
                context={"field": "scheduler_mode", "value": raw.get("scheduler_mode")},
            ) from exc

    def schedule_sample(
        self,
        sample_id: str,
        options: Mapping[str, Any] | None = None,
        *,
        fail_on_block: bool = True,
    ) -> dict[str, Any]:
        sample_lock = self._sample_lock(sample_id)
        with sample_lock:
            sample = self.db.fetch_one("SELECT * FROM samples WHERE sample_id = ?", (sample_id,))
            if sample is None:
                raise BusinessError(
                    code="E_SAMPLE_NOT_FOUND",
                    message="sample was not found",
                    context={"sample_id": sample_id},
                )
            connection_id = sample["connection_id"]
            with self._connection_lock(connection_id):
                existing = self.db.fetch_one(
                    "SELECT * FROM schedule_runs WHERE sample_id = ? AND mode = 'sample'",
                    (sample_id,),
                )
                if existing is not None:
                    cached = self._cached_run_result(existing)
                    if fail_on_block and cached.get("status") == RunStatus.BLOCKED.value:
                        raise self._blocked_error(cached)
                    return cached
                return self._execute_schedule(
                    connection_id=connection_id,
                    sample_id=sample_id,
                    mode="sample",
                    options=options,
                    fail_on_block=fail_on_block,
                )

    def schedule_connection(
        self,
        connection_id: str,
        options: Mapping[str, Any] | None = None,
        *,
        fail_on_block: bool = True,
    ) -> dict[str, Any]:
        with self._connection_lock(connection_id):
            return self._execute_schedule(
                connection_id=connection_id,
                sample_id=None,
                mode="connection",
                options=options,
                fail_on_block=fail_on_block,
            )

    def _new_run_id(self, connection_id: str, sample_id: str | None, mode: str) -> str:
        if mode == "sample" and sample_id is not None:
            return f"run-sample-{sample_id}"
        count = self.db.fetch_one(
            "SELECT COUNT(*) AS n FROM schedule_runs WHERE connection_id = ? AND mode = 'connection'",
            (connection_id,),
        )
        seq = int(count["n"]) + 1
        digest = hashlib.sha256(connection_id.encode("utf-8")).hexdigest()[:16]
        return f"run-conn-{digest}-{seq:08d}"

    def _execute_schedule(
        self,
        *,
        connection_id: str,
        sample_id: str | None,
        mode: str,
        options: Mapping[str, Any] | None,
        fail_on_block: bool,
    ) -> dict[str, Any]:
        with self.db.snapshot() as conn:
            connection = self._load_connection(conn, connection_id)
            if connection is None:
                raise BusinessError(
                    code="E_CONNECTION_NOT_FOUND",
                    message="connection was not found",
                    connection_id=connection_id,
                    context={"connection_id": connection_id},
                )
            streams = self._load_streams(conn, connection_id)
            jobs = self._load_jobs(
                conn, connection_id, sample_id=sample_id if mode == "sample" else None
            )

        schedule_options = self._parse_schedule_options(connection.config, options)
        run_id = self._new_run_id(connection_id, sample_id, mode)
        plan = build_schedule_plan(
            run_id=run_id,
            conn=connection,
            streams=streams,
            jobs=jobs,
            options=schedule_options,
        )
        plan.sample_id = sample_id
        started_at = utc_now()
        request_payload = {
            "scheduler_mode": schedule_options.mode,
            "round_budget": schedule_options.round_budget,
            "max_rounds": schedule_options.max_rounds,
            "candidate_threshold": schedule_options.candidate_threshold,
            "candidate_limit": schedule_options.candidate_limit,
        }
        with self.db.transaction(immediate=True) as conn:
            conn.execute(
                "INSERT INTO schedule_runs(run_id, connection_id, sample_id, mode, status, "
                "request_json, started_at) VALUES (?, ?, ?, ?, 'RUNNING', ?, ?)",
                (
                    run_id,
                    connection_id,
                    sample_id,
                    mode,
                    json_dumps(request_payload),
                    started_at,
                ),
            )

        for round_plan in plan.rounds:
            intent_id = f"round-{run_id}-{round_plan.round_index:08d}"
            intent_plan = self._round_plan_to_dict(round_plan, sample_id)
            with self.db.transaction(immediate=True) as conn:
                conn.execute(
                    "INSERT INTO intents(intent_id, kind, connection_id, sample_id, run_id, "
                    "round_index, status, plan_json, created_at) "
                    "VALUES (?, 'SCHEDULE_ROUND', ?, ?, ?, ?, 'PENDING', ?, ?)",
                    (
                        intent_id,
                        connection_id,
                        sample_id,
                        run_id,
                        round_plan.round_index,
                        json_dumps(intent_plan),
                        utc_now(),
                    ),
                )
            self._crash_point(round_plan.round_index)
            try:
                with self.db.transaction(immediate=True) as conn:
                    self._apply_round(
                        conn,
                        connection_id=connection_id,
                        sample_id=sample_id,
                        run_id=run_id,
                        round_plan=round_plan,
                        intent_id=intent_id,
                        is_final=round_plan is plan.rounds[-1] if plan.rounds else False,
                        final_status=plan.final_status,
                    )
            except BaseException:
                self._abort_intent(intent_id, "schedule round transaction did not commit")
                raise

        with self.db.transaction(immediate=True) as conn:
            result = self._build_run_result(conn, run_id)
            error_payload = None
            if plan.final_status == RunStatus.BLOCKED.value:
                error_payload = {
                    "error": {
                        "code": "E_FLOW_CONTROL_BLOCKED",
                        "message": "one or more DATA frames are blocked by flow-control windows",
                        "connection_id": connection_id,
                        "context": plan.error_context or {},
                    }
                }
            if plan.final_status == RunStatus.LIMIT_REACHED.value:
                result["note"] = plan.note
            result["status"] = plan.final_status
            result["error"] = error_payload["error"] if error_payload else None
            conn.execute(
                "UPDATE schedule_runs SET status = ?, result_json = ?, error_json = ?, "
                "round_count = ?, selected_bytes = ?, finished_at = ? WHERE run_id = ?",
                (
                    plan.final_status,
                    json_dumps(result),
                    json_dumps(error_payload) if error_payload else None,
                    len(plan.rounds),
                    sum(item.selected_bytes for item in plan.rounds),
                    utc_now(),
                    run_id,
                ),
            )

        if fail_on_block and plan.final_status == RunStatus.BLOCKED.value:
            raise BusinessError(
                code="E_FLOW_CONTROL_BLOCKED",
                message="one or more DATA frames are blocked by flow-control windows",
                connection_id=connection_id,
                context={"run_id": run_id, **(plan.error_context or {})},
            )
        result["cache_replayed"] = False
        return result

    def _crash_point(self, round_index: int) -> None:
        crash_after = os.environ.get("H2WB_CRASH_AFTER_INTENT", "").lower()
        crash_round_raw = os.environ.get("H2WB_CRASH_AT_ROUND")
        should_crash = crash_after in {"1", "true", "yes"}
        if crash_round_raw:
            try:
                should_crash = int(crash_round_raw) == round_index
            except ValueError:
                should_crash = False
        if should_crash:
            os._exit(137)

    def _round_plan_to_dict(
        self, round_plan: Any, sample_id: str | None
    ) -> dict[str, Any]:
        return {
            "round_index": round_plan.round_index,
            "sample_id": sample_id,
            "algorithm": round_plan.algorithm,
            "candidate_count": round_plan.candidate_count,
            "selected_candidate_count": round_plan.selected_candidate_count,
            "truncated_count": round_plan.truncated_count,
            "approximation": round_plan.approximation,
            "connection_window_before": round_plan.connection_window_before,
            "connection_window_after": round_plan.connection_window_after,
            "selected_bytes": round_plan.selected_bytes,
            "selections": [
                {
                    "stream_id": item.stream_id,
                    "bytes": item.bytes,
                    "priority": item.priority,
                    "weight": item.weight,
                    "reason": item.reason,
                    "job_consumption": [list(pair) for pair in item.job_consumption],
                    "end_stream": item.end_stream,
                    "state_before": item.state_before.value,
                    "state_after": item.state_after.value,
                }
                for item in round_plan.selections
            ],
            "blocked": [
                {
                    "stream_id": item.stream_id,
                    "code": item.code,
                    "required_bytes": item.required_bytes,
                    "pending_bytes": item.pending_bytes,
                    "connection_window": item.connection_window,
                    "stream_window": item.stream_window,
                    "reason": item.reason,
                }
                for item in round_plan.blocked
            ],
        }

    def _apply_round(
        self,
        conn: sqlite3.Connection,
        *,
        connection_id: str,
        sample_id: str | None,
        run_id: str,
        round_plan: Any,
        intent_id: str,
        is_final: bool,
        final_status: str,
    ) -> None:
        now = utc_now()
        conn.execute(
            "UPDATE connections SET connection_window = ?, updated_at = ? WHERE connection_id = ?",
            (round_plan.connection_window_after, now, connection_id),
        )

        for seq, selection in enumerate(round_plan.selections, start=1):
            for job_id, take in selection.job_consumption:
                if take <= 0:
                    conn.execute(
                        "UPDATE data_jobs SET status = 'SENT', remaining_bytes = 0, updated_at = ? "
                        "WHERE job_id = ?",
                        (now, job_id),
                    )
                else:
                    conn.execute(
                        "UPDATE data_jobs SET remaining_bytes = remaining_bytes - ?, "
                        "status = CASE WHEN remaining_bytes - ? = 0 THEN 'SENT' ELSE status END, "
                        "updated_at = ? WHERE job_id = ?",
                        (take, take, now, job_id),
                    )
            conn.execute(
                "UPDATE streams SET window = window - ?, sent_unacked = sent_unacked + ?, "
                "pending_bytes = MAX(0, pending_bytes - ?), sent_total = sent_total + ?, "
                "state = ?, pending_end_stream = CASE WHEN ? = 1 THEN 0 ELSE pending_end_stream END, "
                "updated_at = ? WHERE connection_id = ? AND stream_id = ?",
                (
                    selection.bytes,
                    selection.bytes,
                    selection.bytes,
                    selection.bytes,
                    selection.state_after.value,
                    int(selection.end_stream),
                    now,
                    connection_id,
                    selection.stream_id,
                ),
            )
            if selection.state_before is not selection.state_after:
                conn.execute(
                    "INSERT INTO state_transitions(connection_id, stream_id, sample_id, run_id, "
                    "round_index, from_state, to_state, trigger, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        connection_id,
                        selection.stream_id,
                        sample_id,
                        run_id,
                        round_plan.round_index,
                        selection.state_before.value,
                        selection.state_after.value,
                        "data_end_stream_sent",
                        now,
                    ),
                )
            conn.execute(
                "INSERT INTO trace_entries(run_id, round_index, seq, connection_id, sample_id, "
                "stream_id, bytes, priority, weight, reason, algorithm, state_before, state_after, "
                "job_consumption_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    round_plan.round_index,
                    seq,
                    connection_id,
                    sample_id,
                    selection.stream_id,
                    selection.bytes,
                    selection.priority,
                    selection.weight,
                    selection.reason,
                    round_plan.algorithm,
                    selection.state_before.value,
                    selection.state_after.value,
                    json_dumps([list(pair) for pair in selection.job_consumption]),
                    now,
                ),
            )

        for entry in round_plan.blocked:
            conn.execute(
                "INSERT INTO blocked_entries(run_id, round_index, connection_id, stream_id, code, "
                "required_bytes, pending_bytes, connection_window, stream_window, reason, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    round_plan.round_index,
                    connection_id,
                    entry.stream_id,
                    entry.code,
                    entry.required_bytes,
                    entry.pending_bytes,
                    entry.connection_window,
                    entry.stream_window,
                    entry.reason,
                    now,
                ),
            )

        selected_payload = [
            {
                "stream_id": item.stream_id,
                "bytes": item.bytes,
                "priority": item.priority,
                "weight": item.weight,
                "reason": item.reason,
                "job_consumption": [list(pair) for pair in item.job_consumption],
                "end_stream": item.end_stream,
                "state_before": item.state_before.value,
                "state_after": item.state_after.value,
            }
            for item in round_plan.selections
        ]
        blocked_payload = [
            {
                "stream_id": item.stream_id,
                "code": item.code,
                "required_bytes": item.required_bytes,
                "pending_bytes": item.pending_bytes,
                "connection_window": item.connection_window,
                "stream_window": item.stream_window,
                "reason": item.reason,
            }
            for item in round_plan.blocked
        ]
        conn.execute(
            "INSERT INTO schedule_rounds(run_id, round_index, algorithm, candidate_count, "
            "selected_candidate_count, truncated_count, approximation_json, selected_bytes, "
            "connection_window_before, connection_window_after, selected_json, blocked_json, "
            "final_status, committed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id,
                round_plan.round_index,
                round_plan.algorithm,
                round_plan.candidate_count,
                round_plan.selected_candidate_count,
                round_plan.truncated_count,
                json_dumps(round_plan.approximation),
                round_plan.selected_bytes,
                round_plan.connection_window_before,
                round_plan.connection_window_after,
                json_dumps(selected_payload),
                json_dumps(blocked_payload),
                final_status if is_final else round_plan.final_status,
                now,
            ),
        )
        conn.execute(
            "UPDATE schedule_runs SET round_count = round_count + 1, "
            "selected_bytes = selected_bytes + ? WHERE run_id = ?",
            (round_plan.selected_bytes, run_id),
        )
        conn.execute(
            "UPDATE intents SET status = 'COMMITTED', committed_at = ? WHERE intent_id = ?",
            (now, intent_id),
        )

    def _build_run_result(
        self, conn: sqlite3.Connection, run_id: str
    ) -> dict[str, Any]:
        run = conn.execute(
            "SELECT * FROM schedule_runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if run is None:
            raise BusinessError(
                code="E_SAMPLE_NOT_FOUND",
                message="schedule run was not found",
                context={"run_id": run_id},
            )
        connection_id = run["connection_id"]
        round_rows = conn.execute(
            "SELECT * FROM schedule_rounds WHERE run_id = ? ORDER BY round_index",
            (run_id,),
        ).fetchall()
        rounds: list[dict[str, Any]] = []
        flat_trace: list[dict[str, Any]] = []
        flat_blocked: list[dict[str, Any]] = []
        for row in round_rows:
            selections = json_loads(row["selected_json"], [])
            blocked = json_loads(row["blocked_json"], [])
            rounds.append(
                {
                    "round_index": row["round_index"],
                    "algorithm": row["algorithm"],
                    "candidate_count": row["candidate_count"],
                    "selected_candidate_count": row["selected_candidate_count"],
                    "truncated_count": row["truncated_count"],
                    "approximation": json_loads(row["approximation_json"], {}),
                    "selected_bytes": row["selected_bytes"],
                    "connection_window_before": row["connection_window_before"],
                    "connection_window_after": row["connection_window_after"],
                    "selections": selections,
                    "blocked": blocked,
                }
            )
            for seq, item in enumerate(selections, start=1):
                flat_trace.append(
                    {
                        "round_index": row["round_index"],
                        "seq": seq,
                        "stream_id": item["stream_id"],
                        "bytes": item["bytes"],
                        "priority": item["priority"],
                        "weight": item["weight"],
                        "reason": item["reason"],
                        "algorithm": row["algorithm"],
                        "state_before": item["state_before"],
                        "state_after": item["state_after"],
                        "job_consumption": item["job_consumption"],
                    }
                )
            for item in blocked:
                flat_blocked.append(
                    {
                        "round_index": row["round_index"],
                        **item,
                    }
                )
        error_payload = json_loads(run["error_json"], None)
        connection_snapshot = self._connection_snapshot(conn, connection_id)
        by_stream: dict[int, dict[str, Any]] = {}
        for item in flat_trace:
            entry = by_stream.setdefault(
                item["stream_id"],
                {
                    "selected_bytes": 0,
                    "selection_count": 0,
                    "rounds": set(),
                    "last_reason": None,
                    "blocked_code": None,
                    "blocked_reason": None,
                },
            )
            entry["selected_bytes"] += item["bytes"]
            entry["selection_count"] += 1
            entry["rounds"].add(item["round_index"])
            entry["last_reason"] = item["reason"]
        for item in flat_blocked:
            entry = by_stream.setdefault(
                item["stream_id"],
                {
                    "selected_bytes": 0,
                    "selection_count": 0,
                    "rounds": set(),
                    "last_reason": None,
                    "blocked_code": None,
                    "blocked_reason": None,
                },
            )
            entry["blocked_code"] = item["code"]
            entry["blocked_reason"] = item["reason"]
        diagnoses = []
        for stream in connection_snapshot["streams"]:
            stream_id = stream["stream_id"]
            entry = by_stream.get(
                stream_id,
                {
                    "selected_bytes": 0,
                    "selection_count": 0,
                    "rounds": set(),
                    "last_reason": None,
                    "blocked_code": None,
                    "blocked_reason": None,
                },
            )
            if entry["blocked_code"]:
                conclusion = entry["blocked_code"]
                detail = entry["blocked_reason"]
            elif stream["state"] == "RESET":
                conclusion = "reset_not_scheduled"
                detail = "stream is RESET and cannot be scheduled"
            elif stream["state"] == "CLOSED" and entry["selection_count"] == 0:
                conclusion = "closed_not_scheduled"
                detail = "stream is CLOSED and cannot be scheduled"
            elif entry["selection_count"] and stream["pending_bytes"] == 0:
                conclusion = "scheduled_complete"
                detail = entry["last_reason"]
            elif entry["selection_count"]:
                conclusion = "scheduled_partial"
                detail = entry["last_reason"]
            elif stream["pending_bytes"] > 0:
                conclusion = "pending_not_selected"
                detail = "pending DATA was not selected in a committed round"
            else:
                conclusion = "observed"
                detail = "no DATA was pending for this stream in the run scope"
            diagnoses.append(
                {
                    "stream_id": stream_id,
                    "state": stream["state"],
                    "window": stream["window"],
                    "sent_unacked": stream["sent_unacked"],
                    "pending_bytes": stream["pending_bytes"],
                    "selected_bytes": entry["selected_bytes"],
                    "selection_count": entry["selection_count"],
                    "rounds": sorted(entry["rounds"]),
                    "conclusion": conclusion,
                    "detail": detail,
                }
            )
        return {
            "run_id": run["run_id"],
            "connection_id": connection_id,
            "sample_id": run["sample_id"],
            "mode": run["mode"],
            "status": run["status"],
            "round_count": len(rounds),
            "selected_bytes": sum(item["selected_bytes"] for item in rounds),
            "rounds": rounds,
            "trace": flat_trace,
            "blocked": flat_blocked,
            "diagnoses": diagnoses,
            "connection": connection_snapshot,
            "error": error_payload["error"] if error_payload else None,
        }

    def _blocked_error(self, result: dict[str, Any]) -> BusinessError:
        error = result.get("error") or {}
        return BusinessError(
            code="E_FLOW_CONTROL_BLOCKED",
            message=error.get(
                "message", "one or more DATA frames are blocked by flow-control windows"
            ),
            connection_id=result.get("connection_id"),
            context={"run_id": result.get("run_id"), **error.get("context", {})},
        )

    def _cached_run_result(self, run_row: sqlite3.Row) -> dict[str, Any]:
        status = run_row["status"]
        if status == RunStatus.ABORTED.value:
            raise BusinessError(
                code="E_SCHEDULE_ABORTED",
                message="the schedule run was aborted during recovery; persisted rounds are queryable",
                connection_id=run_row["connection_id"],
                context={"run_id": run_row["run_id"], "sample_id": run_row["sample_id"]},
            )
        if status == RunStatus.RUNNING.value:
            raise BusinessError(
                code="E_SCHEDULE_IN_PROGRESS",
                message="the schedule run has not reached a committed final boundary",
                connection_id=run_row["connection_id"],
                context={"run_id": run_row["run_id"], "sample_id": run_row["sample_id"]},
            )
        result = json_loads(run_row["result_json"], None)
        if result is None:
            with self.db.snapshot() as conn:
                result = self._build_run_result(conn, run_row["run_id"])
        result = copy.deepcopy(result)
        result["status"] = status
        stored_error = json_loads(run_row["error_json"], None)
        if stored_error:
            result["error"] = stored_error.get("error")
        result["cache_replayed"] = True
        return result

    # ------------------------------------------------------------------
    # Trace and recovery queries
    # ------------------------------------------------------------------

    def get_sample(self, sample_id: str) -> dict[str, Any]:
        row = self.db.fetch_one("SELECT * FROM samples WHERE sample_id = ?", (sample_id,))
        if row is None:
            raise BusinessError(
                code="E_SAMPLE_NOT_FOUND",
                message="sample was not found",
                context={"sample_id": sample_id},
            )
        run = self.db.fetch_one(
            "SELECT run_id, status FROM schedule_runs WHERE sample_id = ? AND mode = 'sample'",
            (sample_id,),
        )
        return {
            "sample_id": row["sample_id"],
            "connection_id": row["connection_id"],
            "content_hash": row["content_hash"],
            "frame_count": row["frame_count"],
            "batch_size": row["batch_size"],
            "status": row["status"],
            "schedule": dict(run) if run else None,
        }

    def get_sample_trace(
        self, sample_id: str, *, stream_id: int | None = None
    ) -> dict[str, Any]:
        with self.db.snapshot() as conn:
            sample = conn.execute(
                "SELECT sample_id, connection_id FROM samples WHERE sample_id = ?",
                (sample_id,),
            ).fetchone()
            if sample is None:
                raise BusinessError(
                    code="E_SAMPLE_NOT_FOUND",
                    message="sample was not found",
                    context={"sample_id": sample_id},
                )
            run = conn.execute(
                "SELECT * FROM schedule_runs WHERE sample_id = ? AND mode = 'sample'",
                (sample_id,),
            ).fetchone()
            if run is None:
                return {
                    "sample_id": sample_id,
                    "connection_id": sample["connection_id"],
                    "runs": [],
                    "trace": [],
                    "blocked": [],
                }
            result = self._run_result_from_row(conn, run)
        return self._filter_trace(result, stream_id)

    def get_connection_trace(
        self,
        connection_id: str,
        *,
        sample_id: str | None = None,
        stream_id: int | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        with self.db.snapshot() as conn:
            self._connection_snapshot(conn, connection_id)
            clauses = ["connection_id = ?"]
            params: list[Any] = [connection_id]
            if sample_id is not None:
                clauses.append("sample_id = ?")
                params.append(sample_id)
            runs = conn.execute(
                "SELECT * FROM schedule_runs WHERE "
                + " AND ".join(clauses)
                + " ORDER BY started_at, run_id LIMIT ?",
                (*params, limit),
            ).fetchall()
            results = [self._run_result_from_row(conn, row) for row in runs]
        if stream_id is not None:
            results = [self._filter_trace(item, stream_id) for item in results]
        return {"connection_id": connection_id, "runs": results}

    def _run_result_from_row(
        self, conn: sqlite3.Connection, run_row: sqlite3.Row
    ) -> dict[str, Any]:
        stored = json_loads(run_row["result_json"], None)
        if stored is not None:
            return copy.deepcopy(stored)
        return self._build_run_result(conn, run_row["run_id"])

    def get_connection_transitions(
        self, connection_id: str, *, sample_id: str | None = None
    ) -> dict[str, Any]:
        with self.db.snapshot() as conn:
            self._connection_snapshot(conn, connection_id)
            clauses = ["connection_id = ?"]
            params: list[Any] = [connection_id]
            if sample_id is not None:
                clauses.append("sample_id = ?")
                params.append(sample_id)
            rows = conn.execute(
                "SELECT transition_id, stream_id, sample_id, frame_index, round_index, "
                "from_state, to_state, trigger FROM state_transitions WHERE "
                + " AND ".join(clauses)
                + " ORDER BY transition_id",
                tuple(params),
            ).fetchall()
        return {
            "connection_id": connection_id,
            "sample_id": sample_id,
            "transitions": [dict(row) for row in rows],
        }

    def get_connection_violations(self, connection_id: str) -> dict[str, Any]:
        with self.db.snapshot() as conn:
            self._connection_snapshot(conn, connection_id)
            rows = conn.execute(
                "SELECT violation_id, stream_id, sample_id, frame_index, code, "
                "requested_increment, applied_increment, current_before, initial_limit, "
                "details_json FROM protocol_violations WHERE connection_id = ? "
                "ORDER BY violation_id",
                (connection_id,),
            ).fetchall()
        return {
            "connection_id": connection_id,
            "violations": [
                {
                    **{key: row[key] for key in row.keys() if key != "details_json"},
                    "details": json_loads(row["details_json"], {}),
                }
                for row in rows
            ],
        }

    def _filter_trace(self, result: dict[str, Any], stream_id: int | None) -> dict[str, Any]:
        result = copy.deepcopy(result)
        if stream_id is None:
            return result
        result["trace"] = [item for item in result["trace"] if item["stream_id"] == stream_id]
        result["blocked"] = [
            item for item in result["blocked"] if item["stream_id"] == stream_id
        ]
        for round_item in result["rounds"]:
            round_item["selections"] = [
                item
                for item in round_item["selections"]
                if item["stream_id"] == stream_id
            ]
            round_item["blocked"] = [
                item for item in round_item["blocked"] if item["stream_id"] == stream_id
            ]
        return result

    def close_connection(self, connection_id: str) -> dict[str, Any]:
        with self._connection_lock(connection_id):
            now = utc_now()
            with self.db.transaction(immediate=True) as conn:
                row = conn.execute(
                    "SELECT * FROM connections WHERE connection_id = ?", (connection_id,)
                ).fetchone()
                if row is None:
                    raise BusinessError(
                        code="E_CONNECTION_NOT_FOUND",
                        message="connection was not found",
                        connection_id=connection_id,
                        context={"connection_id": connection_id},
                    )
                if row["closed"]:
                    raise BusinessError(
                        code="E_INVALID_TRANSITION",
                        message="connection is already closed",
                        connection_id=connection_id,
                        context={"connection_id": connection_id, "state": "CLOSED"},
                    )
                conn.execute(
                    "UPDATE connections SET closed = 1, updated_at = ? WHERE connection_id = ?",
                    (now, connection_id),
                )
                active_rows = conn.execute(
                    "SELECT stream_id, state FROM streams WHERE connection_id = ? "
                    "AND state IN ('OPEN', 'HALF_CLOSED_LOCAL', 'HALF_CLOSED_REMOTE') "
                    "ORDER BY stream_id",
                    (connection_id,),
                ).fetchall()
                for item in active_rows:
                    conn.execute(
                        "UPDATE streams SET state = 'CLOSED', pending_bytes = 0, "
                        "pending_end_stream = 0, updated_at = ? "
                        "WHERE connection_id = ? AND stream_id = ?",
                        (now, connection_id, item["stream_id"]),
                    )
                    conn.execute(
                        "INSERT INTO state_transitions(connection_id, stream_id, from_state, "
                        "to_state, trigger, created_at) VALUES (?, ?, ?, 'CLOSED', "
                        "'api_connection_close', ?)",
                        (connection_id, item["stream_id"], item["state"], now),
                    )
                conn.execute(
                    "UPDATE data_jobs SET status = 'CANCELLED', remaining_bytes = 0, updated_at = ? "
                    "WHERE connection_id = ? AND status = 'PENDING'",
                    (now, connection_id),
                )
                return self._connection_snapshot(conn, connection_id)

    def recover_pending_intents(self) -> list[dict[str, Any]]:
        created: list[dict[str, Any]] = []
        now = utc_now()
        with self.db.transaction(immediate=True) as conn:
            pending = conn.execute(
                "SELECT * FROM intents WHERE status = 'PENDING' ORDER BY created_at, intent_id"
            ).fetchall()
            handled_runs: set[str] = set()
            for intent in pending:
                facts = self._recovery_facts(conn, intent["connection_id"], intent["run_id"])
                facts.update(
                    {
                        "intent_id": intent["intent_id"],
                        "intent_kind": intent["kind"],
                        "planned_round_index": intent["round_index"],
                        "commit_boundary": "last committed schedule_rounds row",
                        "scope": "persisted facts only",
                    }
                )
                conn.execute(
                    "UPDATE intents SET status = 'ABORTED', recovered_at = ?, "
                    "failure_reason = ? WHERE intent_id = ? AND status = 'PENDING'",
                    (
                        now,
                        "process terminated before the round state transaction committed",
                        intent["intent_id"],
                    ),
                )
                if intent["run_id"]:
                    conn.execute(
                        "UPDATE schedule_runs SET status = 'ABORTED', finished_at = ?, "
                        "error_json = ? WHERE run_id = ? AND status = 'RUNNING'",
                        (
                            now,
                            json_dumps(
                                {
                                    "error": {
                                        "code": "E_SCHEDULE_ABORTED",
                                        "message": "recovered after an incomplete scheduling round",
                                        "connection_id": intent["connection_id"],
                                        "context": facts,
                                    }
                                }
                            ),
                            intent["run_id"],
                        ),
                    )
                    handled_runs.add(intent["run_id"])
                    self._store_aborted_result(conn, intent["run_id"])
                conn.execute(
                    "INSERT INTO recovery_reports(connection_id, run_id, intent_id, action, "
                    "facts_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        intent["connection_id"],
                        intent["run_id"],
                        intent["intent_id"],
                        "ROLLED_BACK_TO_LAST_COMMITTED_BOUNDARY",
                        json_dumps(facts),
                        now,
                    ),
                )
                created.append(
                    {
                        "connection_id": intent["connection_id"],
                        "run_id": intent["run_id"],
                        "intent_id": intent["intent_id"],
                        "action": "ROLLED_BACK_TO_LAST_COMMITTED_BOUNDARY",
                        "facts": facts,
                    }
                )

            running = conn.execute(
                "SELECT * FROM schedule_runs WHERE status = 'RUNNING' ORDER BY started_at, run_id"
            ).fetchall()
            for run in running:
                if run["run_id"] in handled_runs:
                    continue
                facts = self._recovery_facts(conn, run["connection_id"], run["run_id"])
                facts.update(
                    {
                        "commit_boundary": "last committed schedule_rounds row",
                        "scope": "persisted facts only",
                        "note": "run row existed without a pending round intent; no final report was inferred",
                    }
                )
                conn.execute(
                    "UPDATE schedule_runs SET status = 'ABORTED', finished_at = ?, error_json = ? "
                    "WHERE run_id = ? AND status = 'RUNNING'",
                    (
                        now,
                        json_dumps(
                            {
                                "error": {
                                    "code": "E_SCHEDULE_ABORTED",
                                    "message": "run was aborted at recovery",
                                    "connection_id": run["connection_id"],
                                    "context": facts,
                                }
                            }
                        ),
                        run["run_id"],
                    ),
                )
                conn.execute(
                    "INSERT INTO recovery_reports(connection_id, run_id, intent_id, action, "
                    "facts_json, created_at) VALUES (?, ?, NULL, ?, ?, ?)",
                    (
                        run["connection_id"],
                        run["run_id"],
                        "RUN_MARKED_ABORTED_NO_PENDING_INTENT",
                        json_dumps(facts),
                        now,
                    ),
                )
                self._store_aborted_result(conn, run["run_id"])
                created.append(
                    {
                        "connection_id": run["connection_id"],
                        "run_id": run["run_id"],
                        "intent_id": None,
                        "action": "RUN_MARKED_ABORTED_NO_PENDING_INTENT",
                        "facts": facts,
                    }
                )
        return created

    def _store_aborted_result(
        self, conn: sqlite3.Connection, run_id: str | None
    ) -> None:
        if not run_id:
            return
        run = conn.execute(
            "SELECT connection_id, status FROM schedule_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if run is None or run["status"] != "ABORTED":
            return
        if conn.execute(
            "SELECT 1 FROM connections WHERE connection_id = ?",
            (run["connection_id"],),
        ).fetchone() is None:
            return
        result = self._build_run_result(conn, run_id)
        conn.execute(
            "UPDATE schedule_runs SET result_json = ? WHERE run_id = ?",
            (json_dumps(result), run_id),
        )

    def _recovery_facts(
        self,
        conn: sqlite3.Connection,
        connection_id: str | None,
        run_id: str | None,
    ) -> dict[str, Any]:
        facts: dict[str, Any] = {
            "connection_id": connection_id,
            "run_id": run_id,
        }
        if run_id:
            rounds = conn.execute(
                "SELECT COUNT(*) AS n, COALESCE(MAX(round_index), 0) AS last_round, "
                "COALESCE(SUM(selected_bytes), 0) AS selected_bytes "
                "FROM schedule_rounds WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            facts.update(
                {
                    "committed_round_count": rounds["n"],
                    "last_committed_round": rounds["last_round"],
                    "committed_selected_bytes": rounds["selected_bytes"],
                }
            )
        if connection_id:
            row = conn.execute(
                "SELECT connection_window, initial_connection_window, closed "
                "FROM connections WHERE connection_id = ?",
                (connection_id,),
            ).fetchone()
            if row:
                facts.update(
                    {
                        "connection_window": row["connection_window"],
                        "initial_connection_window": row["initial_connection_window"],
                        "connection_closed": bool(row["closed"]),
                    }
                )
                state_rows = conn.execute(
                    "SELECT state, COUNT(*) AS n FROM streams WHERE connection_id = ? "
                    "GROUP BY state ORDER BY state",
                    (connection_id,),
                ).fetchall()
                facts["stream_state_counts"] = {item["state"]: item["n"] for item in state_rows}
                pending = conn.execute(
                    "SELECT COUNT(*) AS jobs, COALESCE(SUM(remaining_bytes), 0) AS bytes "
                    "FROM data_jobs WHERE connection_id = ? AND status = 'PENDING'",
                    (connection_id,),
                ).fetchone()
                facts["pending_jobs"] = pending["jobs"]
                facts["pending_bytes"] = pending["bytes"]
            else:
                facts["connection_record"] = "absent (ingest transaction did not commit)"
        return facts

    def get_recovery_report(self, connection_id: str | None = None) -> dict[str, Any]:
        with self.db.snapshot() as conn:
            clauses: list[str] = []
            params: list[Any] = []
            if connection_id is not None:
                self._connection_snapshot(conn, connection_id)
                clauses.append("connection_id = ?")
                params.append(connection_id)
            sql = "SELECT * FROM recovery_reports"
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY report_id"
            rows = conn.execute(sql, tuple(params)).fetchall()
            pending = conn.execute(
                "SELECT intent_id, kind, connection_id, run_id, round_index, created_at "
                "FROM intents WHERE status = 'PENDING' ORDER BY created_at"
            ).fetchall()
            reports = [
                {
                    "report_id": row["report_id"],
                    "connection_id": row["connection_id"],
                    "run_id": row["run_id"],
                    "intent_id": row["intent_id"],
                    "action": row["action"],
                    "facts": json_loads(row["facts_json"], {}),
                }
                for row in rows
            ]
        return {
            "scope": "persisted_facts_only",
            "note": (
                "This report describes only state that existed after the last committed transaction. "
                "It does not claim that a forcibly terminated process produced a complete exit report."
            ),
            "reports": reports,
            "pending_intents": [dict(row) for row in pending],
        }
