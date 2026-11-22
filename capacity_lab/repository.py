"""SQLite persistence, transaction boundaries and crash recovery."""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .errors import Conflict, InvalidTransition, NotFound, RoundActive
from .models import normalize_plan, plan_fingerprint


ROUND_TRANSITIONS = {
    "PENDING": {"SAMPLING", "ABORTED"},
    "SAMPLING": {"CONVERGED", "ABORTED"},
    "CONVERGED": set(),
    "ABORTED": set(),
}

RUN_TRANSITIONS = {
    "PENDING": {"SAMPLING", "ABORTED"},
    "SAMPLING": {"CONVERGED", "ABORTED"},
    "CONVERGED": set(),
    "ABORTED": set(),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_lock = threading.Lock()
        self._initialised = False

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.path), timeout=30.0, isolation_level=None, check_same_thread=False
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def initialise(self) -> None:
        with self._init_lock:
            if self._initialised:
                return
            connection = self.connect()
            try:
                connection.execute("PRAGMA journal_mode = WAL")
                connection.executescript(SCHEMA)
            finally:
                connection.close()
            self._initialised = True

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        self.initialise()
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        self.initialise()
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.execute("COMMIT")
        finally:
            connection.close()

    def submit_plan(self, plan_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize_plan(payload, plan_id)
        fingerprint = plan_fingerprint(normalized)
        now = utc_now()
        with self.write_transaction() as connection:
            row = connection.execute(
                "SELECT latest_version FROM plans WHERE plan_id = ?", (plan_id,)
            ).fetchone()
            version = (row["latest_version"] + 1) if row else 1
            connection.execute(
                """
                INSERT INTO plans(plan_id, latest_version, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(plan_id) DO UPDATE SET
                    latest_version = excluded.latest_version,
                    updated_at = excluded.updated_at
                """,
                (plan_id, version, now, now),
            )
            connection.execute(
                """
                INSERT INTO plan_versions(
                    plan_id, version, fingerprint, plan_json, target_url, seed,
                    quantile_method, ratio_decimals, latency_decimals,
                    throughput_decimals, min_samples, p99_latency_max_fixed,
                    error_rate_max_fixed, min_throughput_fixed, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plan_id,
                    version,
                    fingerprint,
                    _json(normalized),
                    normalized["target"],
                    normalized["seed"],
                    normalized["numeric_contract"]["quantile_method"],
                    normalized["numeric_contract"]["ratio_decimals"],
                    normalized["numeric_contract"]["latency_decimals"],
                    normalized["numeric_contract"]["throughput_decimals"],
                    normalized["numeric_contract"]["min_samples"],
                    normalized["stored_thresholds"]["p99_latency_ms_max"]["fixed"],
                    normalized["stored_thresholds"]["error_rate_max"]["fixed"],
                    normalized["stored_thresholds"]["min_throughput_rps"]["fixed"],
                    now,
                ),
            )
        return self.get_plan(plan_id, version)

    def get_plan(self, plan_id: str, version: int | None = None) -> dict[str, Any]:
        with self.read_transaction() as connection:
            if version is None:
                row = connection.execute(
                    """
                    SELECT pv.* FROM plan_versions pv
                    JOIN plans p ON p.plan_id = pv.plan_id
                        AND p.latest_version = pv.version
                    WHERE pv.plan_id = ?
                    """,
                    (plan_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT * FROM plan_versions WHERE plan_id = ? AND version = ?",
                    (plan_id, version),
                ).fetchone()
        if row is None:
            raise NotFound(
                "plan version not found",
                details={"plan_id": plan_id, "version": version},
            )
        plan = json.loads(row["plan_json"])
        plan.update(
            {
                "plan_id": row["plan_id"],
                "version": row["version"],
                "fingerprint": row["fingerprint"],
                "submitted_at": row["created_at"],
            }
        )
        return plan

    def list_plan_versions(self, plan_id: str) -> list[dict[str, Any]]:
        with self.read_transaction() as connection:
            exists = connection.execute(
                "SELECT 1 FROM plans WHERE plan_id = ?", (plan_id,)
            ).fetchone()
            if not exists:
                raise NotFound("plan not found", details={"plan_id": plan_id})
            rows = connection.execute(
                """
                SELECT version, fingerprint, quantile_method, created_at
                FROM plan_versions WHERE plan_id = ? ORDER BY version
                """,
                (plan_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def create_run(self, plan_id: str, version: int | None = None, *, mode: str = "FULL") -> dict[str, Any]:
        plan = self.get_plan(plan_id, version)
        run_id = new_id("run")
        now = utc_now()
        with self.write_transaction() as connection:
            active = connection.execute(
                "SELECT run_id, status FROM runs WHERE plan_id = ? AND status IN ('PENDING','SAMPLING') LIMIT 1",
                (plan_id,),
            ).fetchone()
            if active:
                raise RoundActive(
                    "a run is already active for this plan",
                    details={"plan_id": plan_id, "active_run_id": active["run_id"], "status": active["status"]},
                )
            sampling_round = connection.execute(
                "SELECT round_id FROM rounds WHERE plan_id = ? AND status = 'SAMPLING' LIMIT 1",
                (plan_id,),
            ).fetchone()
            if sampling_round:
                raise RoundActive(
                    "a round is already sampling for this plan",
                    details={"plan_id": plan_id, "active_round_id": sampling_round["round_id"]},
                )
            connection.execute(
                """
                INSERT INTO runs(
                    run_id, plan_id, plan_version, mode, status, current_stage_index,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'PENDING', NULL, ?, ?)
                """,
                (run_id, plan_id, plan["version"], mode, now, now),
            )
            self._record_transition(connection, "RUN", run_id, None, "PENDING", "created", now)
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self.read_transaction() as connection:
            row = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise NotFound("run not found", details={"run_id": run_id})
            rounds = connection.execute(
                """
                SELECT r.*, c.conclusion_version AS c_conclusion_version,
                       c.verdict, c.metrics_json, c.thresholds_json, c.reason_json,
                       c.p99_both_json, c.computation_digest, c.created_at AS concluded_at
                FROM rounds r
                LEFT JOIN round_conclusions c ON c.round_id = r.round_id
                WHERE r.run_id = ?
                ORDER BY r.created_at, r.rowid
                """,
                (run_id,),
            ).fetchall()
        result = dict(row)
        for key in ("decision_basis_json",):
            if result.get(key):
                result[key.replace("_json", "")] = json.loads(result.pop(key))
            else:
                result.pop(key, None)
        result["inflection"] = (
            {
                "kind": result.get("inflection_kind"),
                "stage_index": result.get("inflection_stage_index"),
                "concurrency": result.get("inflection_concurrency"),
            }
            if result.get("inflection_kind")
            else None
        )
        result["rejected_stage"] = (
            {
                "stage_index": result.get("rejected_stage_index"),
                "concurrency": result.get("rejected_concurrency"),
            }
            if result.get("rejected_stage_index") is not None
            else None
        )
        result["rounds"] = [self._round_summary_from_row(round_row) for round_row in rounds]
        result["conclusion_version"] = max(
            (round_row["c_conclusion_version"] or 0 for round_row in rounds), default=0
        )
        return result

    def get_round(self, round_id: str, *, include_samples: bool = True) -> dict[str, Any]:
        with self.read_transaction() as connection:
            row = connection.execute("SELECT * FROM rounds WHERE round_id = ?", (round_id,)).fetchone()
            if row is None:
                raise NotFound("round not found", details={"round_id": round_id})
            result = dict(row)
            if row["status"] == "CONVERGED":
                conclusion = connection.execute(
                    "SELECT * FROM round_conclusions WHERE round_id = ?", (round_id,)
                ).fetchone()
                samples = connection.execute(
                    "SELECT * FROM round_samples WHERE round_id = ?", (round_id,)
                ).fetchone()
                if conclusion is None or samples is None:
                    raise Conflict(
                        "converged round is missing its atomic conclusion payload",
                        details={"round_id": round_id},
                    )
                result["conclusion"] = _decode_conclusion(conclusion)
                result["sample_aggregate"] = {
                    "sample_count": samples["sample_count"],
                    "success_count": samples["success_count"],
                    "error_count": samples["error_count"],
                    "timeout_count": samples["timeout_count"],
                    "sample_digest": samples["sample_digest"],
                    "aggregate": json.loads(samples["aggregate_json"]),
                }
                if include_samples:
                    result["samples"] = json.loads(samples["samples_json"])
            else:
                result.pop("conclusion_version", None)
        return result

    def trigger_round(
        self,
        *,
        run_id: str,
        stage_key: str,
        stage: dict[str, Any],
        kind: str,
        attempt_no: int,
        quantile_method: str,
        plan_fingerprint_value: str,
    ) -> tuple[dict[str, Any], bool]:
        existing = self.find_round(run_id, stage_key)
        if existing is not None:
            return existing, False
        round_id = new_id("rnd")
        intent_id = new_id("intent")
        now = utc_now()
        with self.write_transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if run is None:
                raise NotFound("run not found", details={"run_id": run_id})
            existing_row = connection.execute(
                "SELECT * FROM rounds WHERE run_id = ? AND stage_key = ?",
                (run_id, stage_key),
            ).fetchone()
            if existing_row is not None:
                return self._round_summary_from_row(existing_row), False
            if run["status"] in {"CONVERGED", "ABORTED"}:
                raise InvalidTransition(
                    "cannot start a round on a completed run",
                    details={"run_id": run_id, "status": run["status"]},
                )
            active = connection.execute(
                "SELECT round_id, run_id FROM rounds WHERE plan_id = ? AND status = 'SAMPLING' LIMIT 1",
                (run["plan_id"],),
            ).fetchone()
            if active:
                raise RoundActive(
                    "another round is already sampling for this plan",
                    details={
                        "plan_id": run["plan_id"],
                        "active_round_id": active["round_id"],
                        "active_run_id": active["run_id"],
                    },
                )
            connection.execute(
                """
                INSERT INTO write_intents(
                    intent_id, kind, plan_id, plan_version, run_id, round_id,
                    status, details_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'IN_PROGRESS', ?, ?)
                """,
                (
                    intent_id,
                    "START_AND_CONCLUDE_ROUND",
                    run["plan_id"],
                    run["plan_version"],
                    run_id,
                    round_id,
                    _json(
                        {
                            "stage_key": stage_key,
                            "kind": kind,
                            "attempt_no": attempt_no,
                            "concurrency": stage["concurrency"],
                            "sample_size": stage["sample_size"],
                        }
                    ),
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO rounds(
                    round_id, plan_id, plan_version, run_id, stage_key, stage_index,
                    attempt_no, kind, concurrency, duration_seconds, sample_size,
                    status, quantile_method, plan_fingerprint, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)
                """,
                (
                    round_id,
                    run["plan_id"],
                    run["plan_version"],
                    run_id,
                    stage_key,
                    stage["stage_index"],
                    attempt_no,
                    kind,
                    stage["concurrency"],
                    stage["duration_seconds"],
                    stage["sample_size"],
                    quantile_method,
                    plan_fingerprint_value,
                    now,
                ),
            )
            self._record_transition(connection, "ROUND", round_id, None, "PENDING", "created", now)
            self._transition_in_connection(
                connection,
                "ROUND",
                round_id,
                "PENDING",
                "SAMPLING",
                "sampling intent committed",
                now,
                extra_sql="started_at = ?",
                extra_values=(now,),
            )
            if run["status"] == "PENDING":
                self._transition_in_connection(
                    connection,
                    "RUN",
                    run_id,
                    "PENDING",
                    "SAMPLING",
                    "first round started",
                    now,
                )
                connection.execute(
                    "UPDATE runs SET current_stage_index = ?, updated_at = ? WHERE run_id = ?",
                    (stage["stage_index"], now, run_id),
                )
            else:
                connection.execute(
                    "UPDATE runs SET current_stage_index = ?, updated_at = ? WHERE run_id = ?",
                    (stage["stage_index"], now, run_id),
                )
            intent = connection.execute(
                "SELECT intent_id, round_id FROM write_intents WHERE round_id = ? AND status = 'IN_PROGRESS'",
                (round_id,),
            ).fetchone()
            intent_id = intent["intent_id"]
        result = self.get_round(round_id)
        result["_intent_id"] = intent_id
        return result, True

    def find_round(self, run_id: str, stage_key: str) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM rounds WHERE run_id = ? AND stage_key = ?",
                (run_id, stage_key),
            ).fetchone()
        if row is None:
            return None
        return self.get_round(row["round_id"], include_samples=True)

    def finalize_round(
        self,
        *,
        round_id: str,
        intent_id: str,
        sample_payload: dict[str, Any],
        conclusion_payload: dict[str, Any],
    ) -> dict[str, Any]:
        now = utc_now()
        with self.write_transaction() as connection:
            round_row = connection.execute(
                "SELECT * FROM rounds WHERE round_id = ?", (round_id,)
            ).fetchone()
            if round_row is None:
                raise NotFound("round not found", details={"round_id": round_id})
            if round_row["status"] != "SAMPLING":
                raise InvalidTransition(
                    "only a SAMPLING round can be finalized",
                    details={"round_id": round_id, "status": round_row["status"]},
                )
            seq_row = connection.execute(
                "SELECT CAST(value AS INTEGER) AS value FROM meta WHERE key = 'conclusion_seq'"
            ).fetchone()
            conclusion_version = int(seq_row["value"]) + 1
            connection.execute(
                "UPDATE meta SET value = ? WHERE key = 'conclusion_seq'",
                (str(conclusion_version),),
            )
            connection.execute(
                """
                INSERT INTO round_samples(
                    round_id, sample_count, success_count, error_count, timeout_count,
                    sample_digest, samples_json, aggregate_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    round_id,
                    sample_payload["sample_count"],
                    sample_payload["success_count"],
                    sample_payload["error_count"],
                    sample_payload["timeout_count"],
                    sample_payload["sample_digest"],
                    _json(sample_payload["samples"]),
                    _json(sample_payload["aggregate"]),
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO round_conclusions(
                    round_id, conclusion_version, plan_id, plan_version, verdict,
                    quantile_method, metrics_json, thresholds_json, reason_json,
                    p99_both_json, computation_digest, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    round_id,
                    conclusion_version,
                    round_row["plan_id"],
                    round_row["plan_version"],
                    conclusion_payload["verdict"],
                    conclusion_payload["quantile_method"],
                    _json(conclusion_payload["metrics"]),
                    _json(conclusion_payload["threshold_checks"]),
                    _json(conclusion_payload["reason"]),
                    _json(conclusion_payload["p99_comparison"]),
                    conclusion_payload["computation_digest"],
                    now,
                ),
            )
            connection.execute(
                """
                UPDATE rounds SET status = 'CONVERGED', conclusion_version = ?,
                    completed_at = ?, error_code = NULL WHERE round_id = ? AND status = 'SAMPLING'
                """,
                (conclusion_version, now, round_id),
            )
            self._record_transition(
                connection, "ROUND", round_id, "SAMPLING", "CONVERGED", "conclusion committed", now
            )
            connection.execute(
                """
                UPDATE write_intents SET status = 'SUCCEEDED', completed_at = ?
                WHERE intent_id = ? AND status = 'IN_PROGRESS'
                """,
                (now, intent_id),
            )
        return self.get_round(round_id)

    def abort_round(
        self,
        *,
        round_id: str,
        intent_id: str | None,
        error_code: str,
        message: str,
    ) -> None:
        now = utc_now()
        with self.write_transaction() as connection:
            row = connection.execute(
                "SELECT status FROM rounds WHERE round_id = ?", (round_id,)
            ).fetchone()
            if row is None:
                return
            if row["status"] in {"PENDING", "SAMPLING"}:
                self._transition_in_connection(
                    connection,
                    "ROUND",
                    round_id,
                    row["status"],
                    "ABORTED",
                    message,
                    now,
                    extra_sql="completed_at = ?, error_code = ?",
                    extra_values=(now, error_code),
                )
            if intent_id:
                connection.execute(
                    """
                    UPDATE write_intents SET status = 'FAILED', error_code = ?,
                        completed_at = ? WHERE intent_id = ? AND status = 'IN_PROGRESS'
                    """,
                    (error_code, now, intent_id),
                )

    def conclude_run(
        self,
        *,
        run_id: str,
        status: str,
        inflection: dict[str, Any] | None,
        decision_basis: dict[str, Any],
        rejected_stage: dict[str, Any] | None = None,
        error_code: str | None = None,
    ) -> dict[str, Any]:
        if status not in {"CONVERGED", "ABORTED"}:
            raise ValueError("run final status must be CONVERGED or ABORTED")
        now = utc_now()
        with self.write_transaction() as connection:
            row = connection.execute("SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise NotFound("run not found", details={"run_id": run_id})
            if row["status"] not in {"PENDING", "SAMPLING"}:
                raise InvalidTransition(
                    "run is already terminal",
                    details={"run_id": run_id, "status": row["status"]},
                )
            self._transition_in_connection(
                connection,
                "RUN",
                run_id,
                row["status"],
                status,
                "run completed",
                now,
            )
            connection.execute(
                """
                UPDATE runs SET inflection_kind = ?, inflection_stage_index = ?,
                    inflection_concurrency = ?, rejected_stage_index = ?,
                    rejected_concurrency = ?, decision_basis_json = ?, error_code = ?,
                    completed_at = ?, updated_at = ? WHERE run_id = ?
                """,
                (
                    inflection.get("kind") if inflection else None,
                    inflection.get("stage_index") if inflection else None,
                    inflection.get("concurrency") if inflection else None,
                    rejected_stage.get("stage_index") if rejected_stage else None,
                    rejected_stage.get("concurrency") if rejected_stage else None,
                    _json(decision_basis),
                    error_code,
                    now,
                    now,
                    run_id,
                ),
            )
        return self.get_run(run_id)

    def transition_round(
        self, round_id: str, expected: str, target: str, reason: str = "manual transition"
    ) -> dict[str, Any]:
        if target == "CONVERGED":
            raise InvalidTransition(
                "CONVERGED must be produced by atomic sample and conclusion finalization",
                details={"round_id": round_id, "requested_target": target},
            )
        now = utc_now()
        with self.write_transaction() as connection:
            self._transition_in_connection(
                connection, "ROUND", round_id, expected, target, reason, now
            )
        return self.get_round(round_id)

    def _transition_in_connection(
        self,
        connection: sqlite3.Connection,
        entity_type: str,
        entity_id: str,
        expected: str,
        target: str,
        reason: str,
        now: str,
        *,
        extra_sql: str = "",
        extra_values: tuple[Any, ...] = (),
    ) -> None:
        table = "rounds" if entity_type == "ROUND" else "runs"
        transitions = ROUND_TRANSITIONS if entity_type == "ROUND" else RUN_TRANSITIONS
        row = connection.execute(
            f"SELECT status FROM {table} WHERE {'round_id' if table == 'rounds' else 'run_id'} = ?",
            (entity_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"{entity_type.lower()} not found", details={"id": entity_id})
        current = row["status"]
        if current != expected or target not in transitions.get(expected, set()):
            raise InvalidTransition(
                "illegal state transition",
                details={
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "current_state": current,
                    "expected_state": expected,
                    "target_state": target,
                    "allowed_from_current": sorted(transitions.get(current, set())),
                },
            )
        sql = f"UPDATE {table} SET status = ?"
        values: list[Any] = [target]
        if extra_sql:
            sql += ", " + extra_sql
            values.extend(extra_values)
        sql += f" WHERE {'round_id' if table == 'rounds' else 'run_id'} = ? AND status = ?"
        values.extend([entity_id, expected])
        cursor = connection.execute(sql, values)
        if cursor.rowcount != 1:
            raise InvalidTransition(
                "state changed concurrently", details={"entity_id": entity_id}
            )
        self._record_transition(connection, entity_type, entity_id, expected, target, reason, now)

    @staticmethod
    def _record_transition(
        connection: sqlite3.Connection,
        entity_type: str,
        entity_id: str,
        from_state: str | None,
        to_state: str,
        reason: str,
        now: str,
    ) -> None:
        if entity_type == "ROUND":
            scope = connection.execute(
                "SELECT plan_id, plan_version FROM rounds WHERE round_id = ?",
                (entity_id,),
            ).fetchone()
        else:
            scope = connection.execute(
                "SELECT plan_id, plan_version FROM runs WHERE run_id = ?",
                (entity_id,),
            ).fetchone()
        plan_id = scope["plan_id"] if scope else None
        plan_version = scope["plan_version"] if scope else None
        connection.execute(
            """
            INSERT INTO round_transitions(
                entity_type, entity_id, plan_id, plan_version, from_state,
                to_state, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity_type,
                entity_id,
                plan_id,
                plan_version,
                from_state,
                to_state,
                reason,
                now,
            ),
        )

    def recover(self, *, policy: str = "MARK_UNFINISHED_ROUNDS_ABORTED") -> dict[str, Any]:
        now = utc_now()
        with self.write_transaction() as connection:
            intents = connection.execute(
                "SELECT * FROM write_intents WHERE status = 'IN_PROGRESS' ORDER BY created_at"
            ).fetchall()
            aborted_rounds: list[dict[str, Any]] = []
            persisted_rounds: list[dict[str, Any]] = []
            for intent in intents:
                round_id = intent["round_id"]
                round_row = connection.execute(
                    "SELECT * FROM rounds WHERE round_id = ?", (round_id,)
                ).fetchone()
                sample_row = connection.execute(
                    "SELECT sample_count, sample_digest FROM round_samples WHERE round_id = ?",
                    (round_id,),
                ).fetchone()
                conclusion_row = connection.execute(
                    "SELECT verdict, conclusion_version FROM round_conclusions WHERE round_id = ?",
                    (round_id,),
                ).fetchone()
                facts = {
                    "intent_id": intent["intent_id"],
                    "round_id": round_id,
                    "run_id": intent["run_id"],
                    "round_status_before": round_row["status"] if round_row else None,
                    "persisted_sample_rows": 1 if sample_row else 0,
                    "persisted_sample_count": sample_row["sample_count"] if sample_row else 0,
                    "persisted_sample_digest": sample_row["sample_digest"] if sample_row else None,
                    "persisted_conclusion": dict(conclusion_row) if conclusion_row else None,
                }
                if sample_row:
                    persisted_rounds.append(facts)
                if round_row and round_row["status"] in {"PENDING", "SAMPLING"}:
                    self._transition_in_connection(
                        connection,
                        "ROUND",
                        round_id,
                        round_row["status"],
                        "ABORTED",
                        "startup recovery: unresolved sampling intent",
                        now,
                        extra_sql="completed_at = ?, error_code = ?",
                        extra_values=(now, "E_PROCESS_INTERRUPTED"),
                    )
                    aborted_rounds.append({**facts, "round_status_after": "ABORTED"})
                connection.execute(
                    """
                    UPDATE write_intents SET status = 'ABORTED',
                        error_code = 'E_PROCESS_INTERRUPTED', completed_at = ?
                    WHERE intent_id = ? AND status = 'IN_PROGRESS'
                    """,
                    (now, intent["intent_id"]),
                )
            stuck_runs = connection.execute(
                "SELECT run_id, status FROM runs WHERE status IN ('PENDING','SAMPLING')"
            ).fetchall()
            aborted_runs: list[dict[str, Any]] = []
            for run in stuck_runs:
                try:
                    self._transition_in_connection(
                        connection,
                        "RUN",
                        run["run_id"],
                        run["status"],
                        "ABORTED",
                        "startup recovery: process interrupted",
                        now,
                    )
                    connection.execute(
                        "UPDATE runs SET error_code = ?, completed_at = ?, updated_at = ? WHERE run_id = ?",
                        ("E_PROCESS_INTERRUPTED", now, now, run["run_id"]),
                    )
                    aborted_runs.append({"run_id": run["run_id"], "status_before": run["status"]})
                except InvalidTransition:
                    pass
            report = {
                "report_version": 1,
                "generated_at": now,
                "policy": policy,
                "unresolved_intents": len(intents),
                "rounds_aborted": aborted_rounds,
                "runs_aborted": aborted_runs,
                "rounds_with_persisted_samples": persisted_rounds,
                "persisted_samples_available": sum(
                    item["persisted_sample_count"] for item in persisted_rounds
                ),
                "recomputable_rounds": [],
                "unsaved_samples_recovered": False,
                "resume_boundary": (
                    "ABORTED" if aborted_rounds else "NO_UNFINISHED_ROUND"
                ),
                "disclosure": (
                    "本报告仅依据 SQLite 已提交事实。强制终止时尚未提交到数据库的采样样本"
                    "无法恢复，报告不声称还原任何未落盘样本。"
                ),
            }
            cursor = connection.execute(
                "INSERT INTO recovery_reports(report_json, created_at) VALUES (?, ?)",
                (_json(report), now),
            )
            report["report_id"] = cursor.lastrowid
        return report

    def get_latest_recovery(self) -> dict[str, Any]:
        with self.read_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY report_id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            raise NotFound("no recovery report exists")
        report = json.loads(row["report_json"])
        report["report_id"] = row["report_id"]
        report["stored_at"] = row["created_at"]
        return report

    def list_recovery_reports(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.read_transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY report_id DESC LIMIT ?", (limit,)
            ).fetchall()
        reports = []
        for row in rows:
            report = json.loads(row["report_json"])
            report["report_id"] = row["report_id"]
            report["stored_at"] = row["created_at"]
            reports.append(report)
        return reports

    def latest_run_for_plan(self, plan_id: str, version: int | None = None) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            if version is None:
                row = connection.execute(
                    "SELECT run_id FROM runs WHERE plan_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
                    (plan_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT run_id FROM runs WHERE plan_id = ? AND plan_version = ?
                    ORDER BY created_at DESC, rowid DESC LIMIT 1
                    """,
                    (plan_id, version),
                ).fetchone()
        if row is None:
            return None
        return self.get_run(row["run_id"])

    @staticmethod
    def _round_summary_from_row(row: sqlite3.Row) -> dict[str, Any]:
        result = {
            key: row[key]
            for key in (
                "round_id",
                "plan_id",
                "plan_version",
                "run_id",
                "stage_key",
                "stage_index",
                "attempt_no",
                "kind",
                "concurrency",
                "duration_seconds",
                "sample_size",
                "status",
                "quantile_method",
                "plan_fingerprint",
                "conclusion_version",
                "error_code",
                "created_at",
                "started_at",
                "completed_at",
            )
            if key in row.keys()
        }
        if row["status"] == "CONVERGED" and "verdict" in row.keys() and row["verdict"]:
            result["conclusion"] = {
                "verdict": row["verdict"],
                "conclusion_version": row["c_conclusion_version"],
                "quantile_method": row["quantile_method"],
                "plan_version": row["plan_version"],
                "metrics": json.loads(row["metrics_json"]),
                "threshold_checks": json.loads(row["thresholds_json"]),
                "reason": json.loads(row["reason_json"]),
                "p99_comparison": json.loads(row["p99_both_json"]),
                "computation_digest": row["computation_digest"],
                "created_at": row["concluded_at"],
            }
        return result


def _decode_conclusion(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "conclusion_version": row["conclusion_version"],
        "plan_id": row["plan_id"],
        "plan_version": row["plan_version"],
        "verdict": row["verdict"],
        "quantile_method": row["quantile_method"],
        "metrics": json.loads(row["metrics_json"]),
        "threshold_checks": json.loads(row["thresholds_json"]),
        "reason": json.loads(row["reason_json"]),
        "p99_comparison": json.loads(row["p99_both_json"]),
        "computation_digest": row["computation_digest"],
        "created_at": row["created_at"],
    }


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


SCHEMA = r"""
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
INSERT OR IGNORE INTO meta(key, value) VALUES ('conclusion_seq', '0');

CREATE TABLE IF NOT EXISTS plans (
    plan_id TEXT PRIMARY KEY,
    latest_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plan_versions (
    plan_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    target_url TEXT NOT NULL,
    seed INTEGER NOT NULL,
    quantile_method TEXT NOT NULL,
    ratio_decimals INTEGER NOT NULL,
    latency_decimals INTEGER NOT NULL,
    throughput_decimals INTEGER NOT NULL,
    min_samples INTEGER NOT NULL,
    p99_latency_max_fixed INTEGER NOT NULL,
    error_rate_max_fixed INTEGER NOT NULL,
    min_throughput_fixed INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (plan_id, version),
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id)
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    plan_version INTEGER NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING','SAMPLING','CONVERGED','ABORTED')),
    current_stage_index INTEGER,
    inflection_kind TEXT,
    inflection_stage_index INTEGER,
    inflection_concurrency INTEGER,
    rejected_stage_index INTEGER,
    rejected_concurrency INTEGER,
    decision_basis_json TEXT,
    error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY (plan_id, plan_version) REFERENCES plan_versions(plan_id, version)
);

CREATE TABLE IF NOT EXISTS rounds (
    round_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    plan_version INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    stage_key TEXT NOT NULL,
    stage_index INTEGER NOT NULL,
    attempt_no INTEGER NOT NULL,
    kind TEXT NOT NULL,
    concurrency INTEGER NOT NULL,
    duration_seconds INTEGER NOT NULL,
    sample_size INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING','SAMPLING','CONVERGED','ABORTED')),
    quantile_method TEXT NOT NULL,
    plan_fingerprint TEXT NOT NULL,
    conclusion_version INTEGER,
    error_code TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    UNIQUE (run_id, stage_key),
    FOREIGN KEY (run_id) REFERENCES runs(run_id),
    FOREIGN KEY (plan_id, plan_version) REFERENCES plan_versions(plan_id, version)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_one_sampling_round_per_plan
    ON rounds(plan_id) WHERE status = 'SAMPLING';

CREATE TABLE IF NOT EXISTS round_samples (
    round_id TEXT PRIMARY KEY,
    sample_count INTEGER NOT NULL,
    success_count INTEGER NOT NULL,
    error_count INTEGER NOT NULL,
    timeout_count INTEGER NOT NULL,
    sample_digest TEXT NOT NULL,
    samples_json TEXT NOT NULL,
    aggregate_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (round_id) REFERENCES rounds(round_id)
);

CREATE TABLE IF NOT EXISTS round_conclusions (
    round_id TEXT PRIMARY KEY,
    conclusion_version INTEGER NOT NULL UNIQUE,
    plan_id TEXT NOT NULL,
    plan_version INTEGER NOT NULL,
    verdict TEXT NOT NULL CHECK (verdict IN ('PASS','REJECT')),
    quantile_method TEXT NOT NULL,
    metrics_json TEXT NOT NULL,
    thresholds_json TEXT NOT NULL,
    reason_json TEXT NOT NULL,
    p99_both_json TEXT NOT NULL,
    computation_digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (round_id) REFERENCES rounds(round_id)
);

CREATE TABLE IF NOT EXISTS round_transitions (
    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    plan_id TEXT,
    plan_version INTEGER,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS write_intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    plan_version INTEGER NOT NULL,
    run_id TEXT,
    round_id TEXT,
    status TEXT NOT NULL,
    details_json TEXT NOT NULL,
    error_code TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS trg_round_samples_no_update
BEFORE UPDATE ON round_samples BEGIN
    SELECT RAISE(ABORT, 'round_samples are immutable');
END;
CREATE TRIGGER IF NOT EXISTS trg_round_samples_no_delete
BEFORE DELETE ON round_samples BEGIN
    SELECT RAISE(ABORT, 'round_samples are immutable');
END;
CREATE TRIGGER IF NOT EXISTS trg_round_conclusions_no_update
BEFORE UPDATE ON round_conclusions BEGIN
    SELECT RAISE(ABORT, 'round conclusions are immutable');
END;
CREATE TRIGGER IF NOT EXISTS trg_round_conclusions_no_delete
BEFORE DELETE ON round_conclusions BEGIN
    SELECT RAISE(ABORT, 'round conclusions are immutable');
END;
"""

