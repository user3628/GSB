from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .errors import InvalidTransition, NotFound, PlanInvalid, RoundActive, WorkbenchError
from .models import (
    DecisionRule,
    PlanCreate,
    PlanRecord,
    RoundRecord,
    SamplingSpec,
    StageSpec,
    Thresholds,
)
from .state_machine import RoundStatus, validate_transition


SCHEMA = """
CREATE TABLE IF NOT EXISTS plans (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    target_url TEXT NOT NULL,
    seed INTEGER NOT NULL,
    profile TEXT NOT NULL,
    request_timeout_ms INTEGER NOT NULL,
    status TEXT NOT NULL,
    conclusion_json TEXT,
    plan_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stages (
    plan_id TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
    stage_index INTEGER NOT NULL,
    concurrency INTEGER NOT NULL,
    duration_seconds INTEGER NOT NULL,
    PRIMARY KEY (plan_id, stage_index)
);

CREATE TABLE IF NOT EXISTS rounds (
    id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
    stage_index INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    round_key TEXT NOT NULL,
    concurrency INTEGER NOT NULL,
    duration_seconds INTEGER NOT NULL,
    origin TEXT NOT NULL,
    retry_of_round_id TEXT REFERENCES rounds(id),
    status TEXT NOT NULL CHECK (status IN ('PENDING','SAMPLING','CONVERGED','ABORTED')),
    failure_reason TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    UNIQUE (plan_id, round_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_sampling_round_per_plan
ON rounds(plan_id) WHERE status = 'SAMPLING';

CREATE INDEX IF NOT EXISTS rounds_by_stage
ON rounds(plan_id, stage_index, attempt);

CREATE TABLE IF NOT EXISTS intents (
    id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
    round_id TEXT NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('OPEN','COMPLETED','RECOVERED')),
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    closed_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS one_open_intent_per_round
ON intents(round_id) WHERE status = 'OPEN';

CREATE TABLE IF NOT EXISTS round_transitions (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id TEXT NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
    plan_id TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
    from_status TEXT,
    to_status TEXT NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS samples (
    round_id TEXT NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL,
    latency_ms REAL NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('SUCCESS','HTTP_ERROR','TIMEOUT','TRANSPORT_ERROR','ABORTED')),
    error_kind TEXT,
    occurred_at TEXT NOT NULL,
    PRIMARY KEY (round_id, sequence)
);

CREATE TABLE IF NOT EXISTS aggregates (
    round_id TEXT PRIMARY KEY REFERENCES rounds(id) ON DELETE CASCADE,
    sample_size INTEGER NOT NULL,
    success_count INTEGER NOT NULL,
    error_count INTEGER NOT NULL,
    timeout_count INTEGER NOT NULL,
    throughput_rps REAL NOT NULL,
    p50_ms REAL NOT NULL,
    p95_ms REAL NOT NULL,
    p99_ms REAL NOT NULL,
    error_rate REAL NOT NULL,
    observed_duration_ms REAL NOT NULL,
    percentile_method TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS judgments (
    round_id TEXT PRIMARY KEY REFERENCES rounds(id) ON DELETE CASCADE,
    passed INTEGER NOT NULL CHECK (passed IN (0,1)),
    verdict TEXT NOT NULL,
    triggered_json TEXT NOT NULL,
    measured_json TEXT NOT NULL,
    sample_size INTEGER NOT NULL,
    percentile_method TEXT NOT NULL,
    rationale TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
    id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
    round_id TEXT REFERENCES rounds(id),
    stage_index INTEGER,
    action TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    id TEXT PRIMARY KEY,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS terminal_round_is_immutable
BEFORE UPDATE ON rounds
WHEN OLD.status IN ('CONVERGED','ABORTED')
BEGIN
    SELECT RAISE(ABORT, 'terminal round is immutable');
END;

CREATE TRIGGER IF NOT EXISTS judgment_is_immutable
BEFORE UPDATE ON judgments
BEGIN
    SELECT RAISE(ABORT, 'judgment is immutable');
END;

CREATE TRIGGER IF NOT EXISTS judgment_delete_is_forbidden
BEFORE DELETE ON judgments
BEGIN
    SELECT RAISE(ABORT, 'judgment is immutable');
END;

CREATE TRIGGER IF NOT EXISTS aggregate_is_immutable
BEFORE UPDATE ON aggregates
BEGIN
    SELECT RAISE(ABORT, 'aggregate is immutable');
END;

CREATE TRIGGER IF NOT EXISTS completed_plan_is_immutable
BEFORE UPDATE ON plans
WHEN OLD.status = 'COMPLETED'
BEGIN
    SELECT RAISE(ABORT, 'completed plan is immutable');
END;
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _loads(value: str | None) -> Any:
    return None if value is None else json.loads(value)


class Store:
    """SQLite repository. Every mutation uses an explicit transaction."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def init_schema(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.executescript(SCHEMA)
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.path), timeout=15.0, isolation_level=None
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
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
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        with self.transaction(immediate=False) as connection:
            yield connection

    # Plans -----------------------------------------------------------------
    def insert_plan(self, document: dict[str, Any]) -> PlanRecord:
        try:
            validated = PlanCreate.model_validate(document).document()
        except Exception as exc:
            raise PlanInvalid("Invalid plan", details=str(exc)) from exc
        plan_id = str(validated["id"])
        now = utc_now()
        with self.transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM plans WHERE id = ?", (plan_id,)
            ).fetchone():
                raise PlanInvalid(f"Plan id already exists: {plan_id}")
            connection.execute(
                """
                INSERT INTO plans
                    (id, name, target_url, seed, profile, request_timeout_ms,
                     status, conclusion_json, plan_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'READY', NULL, ?, ?, ?)
                """,
                (
                    plan_id,
                    validated["name"],
                    validated["target_url"],
                    validated["seed"],
                    validated["profile"],
                    validated["request_timeout_ms"],
                    _json(validated),
                    now,
                    now,
                ),
            )
            connection.executemany(
                "INSERT INTO stages(plan_id, stage_index, concurrency, duration_seconds) VALUES (?, ?, ?, ?)",
                [
                    (
                        plan_id,
                        index,
                        stage["concurrency"],
                        stage["duration_seconds"],
                    )
                    for index, stage in enumerate(validated["stages"])
                ],
            )
        return self.get_plan(plan_id)

    def get_plan(self, plan_id: str) -> PlanRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM plans WHERE id = ?", (plan_id,)
            ).fetchone()
            if row is None:
                raise NotFound("plan", plan_id)
            return self._plan_from_row(row)
        finally:
            connection.close()

    def list_plans(self) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT id, name, status, profile, seed, created_at, updated_at FROM plans ORDER BY created_at"
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            connection.close()

    def mark_plan_running(self, plan_id: str) -> None:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT status FROM plans WHERE id = ?", (plan_id,)
            ).fetchone()
            if row is None:
                raise NotFound("plan", plan_id)
            if row["status"] in {"COMPLETED"}:
                return
            connection.execute(
                "UPDATE plans SET status = 'RUNNING', updated_at = ? WHERE id = ?",
                (utc_now(), plan_id),
            )

    def finalize_plan(
        self,
        plan_id: str,
        conclusion: dict[str, Any],
        *,
        action: str,
        stage_index: int | None = None,
        round_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Persist the plan conclusion once; a completed plan is never overwritten."""

        with self.transaction() as connection:
            row = connection.execute(
                "SELECT status, conclusion_json FROM plans WHERE id = ?", (plan_id,)
            ).fetchone()
            if row is None:
                raise NotFound("plan", plan_id)
            if row["status"] == "COMPLETED":
                return _loads(row["conclusion_json"])
            now = utc_now()
            connection.execute(
                "UPDATE plans SET status = 'COMPLETED', conclusion_json = ?, updated_at = ? WHERE id = ?",
                (_json(conclusion), now, plan_id),
            )
            self._insert_decision(
                connection,
                plan_id=plan_id,
                round_id=round_id,
                stage_index=stage_index,
                action=action,
                details=details or conclusion,
                created_at=now,
            )
        return conclusion

    # Rounds ----------------------------------------------------------------
    def create_round(
        self,
        *,
        plan_id: str,
        stage_index: int,
        attempt: int,
        concurrency: int,
        duration_seconds: int,
        origin: str,
        retry_of_round_id: str | None = None,
    ) -> RoundRecord:
        round_id = f"round_{uuid.uuid4().hex}"
        round_key = f"stage-{stage_index}-r{attempt}"
        now = utc_now()
        try:
            with self.transaction() as connection:
                plan = connection.execute(
                    "SELECT 1 FROM plans WHERE id = ?", (plan_id,)
                ).fetchone()
                if plan is None:
                    raise NotFound("plan", plan_id)
                connection.execute(
                    """
                    INSERT INTO rounds
                        (id, plan_id, stage_index, attempt, round_key, concurrency,
                         duration_seconds, origin, retry_of_round_id, status,
                         failure_reason, created_at, started_at, ended_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', NULL, ?, NULL, NULL)
                    """,
                    (
                        round_id,
                        plan_id,
                        stage_index,
                        attempt,
                        round_key,
                        concurrency,
                        duration_seconds,
                        origin,
                        retry_of_round_id,
                        now,
                    ),
                )
                self._insert_transition(
                    connection,
                    round_id=round_id,
                    plan_id=plan_id,
                    from_status=None,
                    to_status=RoundStatus.PENDING.value,
                    reason="round_created",
                    created_at=now,
                )
        except sqlite3.IntegrityError as exc:
            raise WorkbenchError(
                "E_ROUND_CONFLICT",
                "A round with the same plan/stage/attempt already exists",
                http_status=409,
                details={"plan_id": plan_id, "round_key": round_key},
            ) from exc
        return self.get_round(round_id)

    def start_round(self, round_id: str) -> str:
        """Write an execution intent, then atomically enter SAMPLING."""

        now = utc_now()
        intent_id = f"intent_{uuid.uuid4().hex}"
        plan_id: str | None = None
        try:
            with self.transaction() as connection:
                row = connection.execute(
                    "SELECT * FROM rounds WHERE id = ?", (round_id,)
                ).fetchone()
                if row is None:
                    raise NotFound("round", round_id)
                plan_id = str(row["plan_id"])
                validate_transition(row["status"], RoundStatus.SAMPLING)
                connection.execute(
                    """
                    INSERT INTO intents
                        (id, plan_id, round_id, kind, status, details_json, created_at, closed_at)
                    VALUES (?, ?, ?, 'ROUND_EXECUTION', 'OPEN', ?, ?, NULL)
                    """,
                    (
                        intent_id,
                        row["plan_id"],
                        round_id,
                        _json(
                            {
                                "stage_index": row["stage_index"],
                                "attempt": row["attempt"],
                                "concurrency": row["concurrency"],
                            }
                        ),
                        now,
                    ),
                )
                connection.execute(
                    "UPDATE rounds SET status = 'SAMPLING', started_at = ? WHERE id = ?",
                    (now, round_id),
                )
                self._insert_transition(
                    connection,
                    round_id=round_id,
                    plan_id=row["plan_id"],
                    from_status=RoundStatus.PENDING.value,
                    to_status=RoundStatus.SAMPLING.value,
                    reason="sampling_started",
                    created_at=now,
                )
        except sqlite3.IntegrityError as exc:
            active = (
                self.get_active_round_for_plan(plan_id) if plan_id is not None else None
            )
            raise RoundActive(
                plan_id or "",
                active["id"] if active else None,
            ) from exc
        return intent_id

    def persist_round_result(
        self,
        *,
        round_id: str,
        samples: list[dict[str, Any]],
        aggregate: dict[str, Any],
        judgment: dict[str, Any],
    ) -> dict[str, Any]:
        """Commit samples, aggregate, judgment and CONVERGED as one visible unit."""

        now = utc_now()
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM rounds WHERE id = ?", (round_id,)
            ).fetchone()
            if row is None:
                raise NotFound("round", round_id)
            validate_transition(row["status"], RoundStatus.CONVERGED)
            if int(aggregate["sample_size"]) != len(samples):
                raise WorkbenchError(
                    "E_INTERNAL_INCONSISTENCY",
                    "Aggregate sample count does not match committed samples",
                    http_status=500,
                )
            connection.executemany(
                """
                INSERT INTO samples(round_id, sequence, latency_ms, outcome, error_kind, occurred_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        round_id,
                        sample["sequence"],
                        sample["latency_ms"],
                        sample["outcome"],
                        sample.get("error_kind"),
                        sample.get(
                            "occurred_at", aggregate.get("created_at", now)
                        ),
                    )
                    for sample in samples
                ],
            )
            connection.execute(
                """
                INSERT INTO aggregates
                    (round_id, sample_size, success_count, error_count, timeout_count,
                     throughput_rps, p50_ms, p95_ms, p99_ms, error_rate,
                     observed_duration_ms, percentile_method, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    round_id,
                    aggregate["sample_size"],
                    aggregate["success_count"],
                    aggregate["error_count"],
                    aggregate["timeout_count"],
                    aggregate["throughput_rps"],
                    aggregate["p50_ms"],
                    aggregate["p95_ms"],
                    aggregate["p99_ms"],
                    aggregate["error_rate"],
                    aggregate["observed_duration_ms"],
                    aggregate["percentile_method"],
                    now,
                ),
            )
            connection.execute(
                """
                INSERT INTO judgments
                    (round_id, passed, verdict, triggered_json, measured_json,
                     sample_size, percentile_method, rationale, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    round_id,
                    int(bool(judgment["passed"])),
                    judgment["verdict"],
                    _json(judgment["triggered"]),
                    _json(judgment["measured"]),
                    judgment["sample_size"],
                    judgment["percentile_method"],
                    judgment["rationale"],
                    now,
                ),
            )
            connection.execute(
                "UPDATE rounds SET status = 'CONVERGED', ended_at = ? WHERE id = ?",
                (now, round_id),
            )
            self._insert_transition(
                connection,
                round_id=round_id,
                plan_id=row["plan_id"],
                from_status=RoundStatus.SAMPLING.value,
                to_status=RoundStatus.CONVERGED.value,
                reason="decision_committed",
                created_at=now,
            )
            connection.execute(
                "UPDATE intents SET status = 'COMPLETED', closed_at = ? WHERE round_id = ? AND status = 'OPEN'",
                (now, round_id),
            )
            self._insert_decision(
                connection,
                plan_id=row["plan_id"],
                round_id=round_id,
                stage_index=row["stage_index"],
                action="judged_pass" if judgment["passed"] else "judged_fail",
                details={
                    "concurrency": row["concurrency"],
                    "attempt": row["attempt"],
                    "sample_size": judgment["sample_size"],
                    "triggered": judgment["triggered"],
                },
                created_at=now,
            )
        return self.get_round_detail(round_id)

    def abort_round(self, round_id: str, reason: str) -> RoundRecord:
        now = utc_now()
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM rounds WHERE id = ?", (round_id,)
            ).fetchone()
            if row is None:
                raise NotFound("round", round_id)
            validate_transition(row["status"], RoundStatus.ABORTED)
            connection.execute(
                "UPDATE rounds SET status = 'ABORTED', failure_reason = ?, ended_at = ? WHERE id = ?",
                (reason, now, round_id),
            )
            self._insert_transition(
                connection,
                round_id=round_id,
                plan_id=row["plan_id"],
                from_status=row["status"],
                to_status=RoundStatus.ABORTED.value,
                reason=reason,
                created_at=now,
            )
            connection.execute(
                "UPDATE intents SET status = 'COMPLETED', closed_at = ? WHERE round_id = ? AND status = 'OPEN'",
                (now, round_id),
            )
        return self.get_round(round_id)

    def transition_round(self, round_id: str, target: str, reason: str = "manual") -> RoundRecord:
        """Explicit transition API used by tests/administration.

        Invalid transitions raise before any UPDATE and are rolled back.
        """

        now = utc_now()
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM rounds WHERE id = ?", (round_id,)
            ).fetchone()
            if row is None:
                raise NotFound("round", round_id)
            validate_transition(row["status"], target)
            connection.execute(
                "UPDATE rounds SET status = ?, ended_at = CASE WHEN ? IN ('CONVERGED','ABORTED') THEN ? ELSE ended_at END WHERE id = ?",
                (target, target, now, round_id),
            )
            self._insert_transition(
                connection,
                round_id=round_id,
                plan_id=row["plan_id"],
                from_status=row["status"],
                to_status=target,
                reason=reason,
                created_at=now,
            )
        return self.get_round(round_id)

    def get_round(self, round_id: str) -> RoundRecord:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM rounds WHERE id = ?", (round_id,)
            ).fetchone()
            if row is None:
                raise NotFound("round", round_id)
            return self._round_from_row(row)
        finally:
            connection.close()

    def get_round_detail(self, round_id: str) -> dict[str, Any]:
        connection = self._connect()
        try:
            return self._round_detail(connection, round_id)
        finally:
            connection.close()

    def get_active_round_for_plan(self, plan_id: str) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM rounds WHERE plan_id = ? AND status = 'SAMPLING' LIMIT 1",
                (plan_id,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            connection.close()

    def list_rounds(self, plan_id: str) -> list[dict[str, Any]]:
        with self.read_transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM rounds WHERE plan_id = ? ORDER BY stage_index, attempt",
                (plan_id,),
            ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                result.append(self._round_detail(connection, row["id"]))
            return result

    def latest_round(self, plan_id: str, stage_index: int) -> RoundRecord | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM rounds WHERE plan_id = ? AND stage_index = ? ORDER BY attempt DESC LIMIT 1",
                (plan_id, stage_index),
            ).fetchone()
            return self._round_from_row(row) if row else None
        finally:
            connection.close()

    def attempt_count(self, plan_id: str, stage_index: int) -> int:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM rounds WHERE plan_id = ? AND stage_index = ?",
                (plan_id, stage_index),
            ).fetchone()
            return int(row["count"])
        finally:
            connection.close()

    def list_decisions(self, plan_id: str) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM decisions WHERE plan_id = ? ORDER BY created_at, id",
                (plan_id,),
            ).fetchall()
            return [
                {
                    "id": row["id"],
                    "round_id": row["round_id"],
                    "stage_index": row["stage_index"],
                    "action": row["action"],
                    "details": _loads(row["details_json"]),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]
        finally:
            connection.close()

    def record_decision(
        self,
        plan_id: str,
        *,
        action: str,
        stage_index: int | None = None,
        round_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        with self.transaction() as connection:
            if not connection.execute(
                "SELECT 1 FROM plans WHERE id = ?", (plan_id,)
            ).fetchone():
                raise NotFound("plan", plan_id)
            self._insert_decision(
                connection,
                plan_id=plan_id,
                round_id=round_id,
                stage_index=stage_index,
                action=action,
                details=details or {},
                created_at=utc_now(),
            )

    # Reports and recovery --------------------------------------------------
    def get_plan_report(self, plan_id: str) -> dict[str, Any]:
        with self.read_transaction() as connection:
            plan_row = connection.execute(
                "SELECT * FROM plans WHERE id = ?", (plan_id,)
            ).fetchone()
            if plan_row is None:
                raise NotFound("plan", plan_id)
            plan = self._plan_from_row(plan_row)
            round_rows = connection.execute(
                "SELECT id FROM rounds WHERE plan_id = ? ORDER BY stage_index, attempt",
                (plan_id,),
            ).fetchall()
            rounds = [
                self._round_detail(connection, row["id"]) for row in round_rows
            ]
            decision_rows = connection.execute(
                "SELECT * FROM decisions WHERE plan_id = ? ORDER BY created_at, id",
                (plan_id,),
            ).fetchall()
            decisions = [
                {
                    "id": row["id"],
                    "round_id": row["round_id"],
                    "stage_index": row["stage_index"],
                    "action": row["action"],
                    "details": _loads(row["details_json"]),
                    "created_at": row["created_at"],
                }
                for row in decision_rows
            ]
            return {
                "plan": {
                    "id": plan.id,
                    "name": plan.name,
                    "status": plan.status,
                    "target_url": plan.target_url,
                    "seed": plan.seed,
                    "profile": plan.profile,
                    "stages": [stage.model_dump(mode="json") for stage in plan.stages],
                    "thresholds": plan.thresholds.model_dump(mode="json"),
                    "decision_rule": plan.decision_rule.model_dump(mode="json"),
                    "sampling": plan.sampling.model_dump(mode="json"),
                    "request_timeout_ms": plan.request_timeout_ms,
                    "created_at": plan_row["created_at"],
                    "updated_at": plan_row["updated_at"],
                },
                "conclusion": plan.conclusion,
                "rounds": rounds,
                "decisions": decisions,
            }

    def snapshot_round_states(self, plan_id: str) -> dict[str, Any]:
        """A consistent read used by the forced-termination demo."""

        with self.read_transaction() as connection:
            rows = connection.execute(
                """
                SELECT r.id, r.status, r.stage_index, r.attempt,
                       (SELECT COUNT(*) FROM samples s WHERE s.round_id = r.id) AS sample_count,
                       EXISTS(SELECT 1 FROM aggregates a WHERE a.round_id = r.id) AS has_aggregate,
                       EXISTS(SELECT 1 FROM judgments j WHERE j.round_id = r.id) AS has_judgment,
                       (SELECT COUNT(*) FROM intents i WHERE i.round_id = r.id AND i.status = 'OPEN') AS open_intents
                FROM rounds r
                WHERE r.plan_id = ?
                ORDER BY r.stage_index, r.attempt
                """,
                (plan_id,),
            ).fetchall()
            return {
                "plan_id": plan_id,
                "rounds": [dict(row) for row in rows],
            }

    def recover_incomplete(self) -> dict[str, Any] | None:
        """Recover rounds left by a killed process using persisted facts only."""

        now = utc_now()
        report: dict[str, Any] = {
            "generated_at": now,
            "rounds": [],
            "limitations": [
                "No complete load-test exit report is fabricated after a forced termination."
            ],
        }
        with self.transaction() as connection:
            candidate_rows = connection.execute(
                """
                SELECT DISTINCT r.*
                FROM rounds r
                LEFT JOIN intents i ON i.round_id = r.id AND i.status = 'OPEN'
                WHERE r.status = 'SAMPLING' OR i.id IS NOT NULL
                ORDER BY r.created_at
                """
            ).fetchall()
            running_plan_rows = connection.execute(
                "SELECT id, status FROM plans WHERE status = 'RUNNING' ORDER BY created_at"
            ).fetchall()
            if not candidate_rows and not running_plan_rows:
                return None
            sampling_counts_before = {
                str(plan_row["id"]): int(
                    connection.execute(
                        "SELECT COUNT(*) AS count FROM rounds WHERE plan_id = ? AND status = 'SAMPLING'",
                        (plan_row["id"],),
                    ).fetchone()["count"]
                )
                for plan_row in running_plan_rows
            }

            plan_ids: set[str] = set()
            for row in candidate_rows:
                plan_ids.add(row["plan_id"])
                facts = connection.execute(
                    """
                    SELECT
                      (SELECT COUNT(*) FROM samples s WHERE s.round_id = ?) AS sample_count,
                      EXISTS(SELECT 1 FROM aggregates a WHERE a.round_id = ?) AS has_aggregate,
                      EXISTS(SELECT 1 FROM judgments j WHERE j.round_id = ?) AS has_judgment,
                      (SELECT COUNT(*) FROM intents i WHERE i.round_id = ? AND i.status = 'OPEN') AS open_intents
                    """,
                    (row["id"], row["id"], row["id"], row["id"]),
                ).fetchone()
                before = row["status"]
                if before in {"CONVERGED", "ABORTED"}:
                    after = before
                    action = "closed_orphan_intent"
                elif facts["has_judgment"] and facts["has_aggregate"] and facts["sample_count"] > 0:
                    # This branch can only occur if a previous implementation or
                    # manual repair committed the complete facts without the status.
                    # It reuses the persisted judgment and never re-evaluates it.
                    connection.execute(
                        "UPDATE rounds SET status = 'CONVERGED', ended_at = ?, failure_reason = NULL WHERE id = ?",
                        (now, row["id"]),
                    )
                    self._insert_transition(
                        connection,
                        round_id=row["id"],
                        plan_id=row["plan_id"],
                        from_status=before,
                        to_status="CONVERGED",
                        reason="recovery_from_committed_facts",
                        created_at=now,
                    )
                    after = "CONVERGED"
                    action = "restored_from_committed_judgment"
                else:
                    connection.execute(
                        "UPDATE rounds SET status = 'ABORTED', ended_at = ?, failure_reason = ? WHERE id = ?",
                        (now, "process_interrupted_during_sampling", row["id"]),
                    )
                    self._insert_transition(
                        connection,
                        round_id=row["id"],
                        plan_id=row["plan_id"],
                        from_status=before,
                        to_status="ABORTED",
                        reason="process_interrupted_during_sampling",
                        created_at=now,
                    )
                    after = "ABORTED"
                    action = "aborted_incomplete_round"
                connection.execute(
                    "UPDATE intents SET status = 'RECOVERED', closed_at = ? WHERE round_id = ? AND status = 'OPEN'",
                    (now, row["id"]),
                )
                report["rounds"].append(
                    {
                        "round_id": row["id"],
                        "plan_id": row["plan_id"],
                        "stage_index": row["stage_index"],
                        "attempt": row["attempt"],
                        "before_status": before,
                        "after_status": after,
                        "action": action,
                        "persisted_facts": {
                            "sample_count": int(facts["sample_count"]),
                            "aggregate_present": bool(facts["has_aggregate"]),
                            "judgment_present": bool(facts["has_judgment"]),
                            "open_intent_count": int(facts["open_intents"]),
                        },
                    }
                )

            report["plans"] = []
            for plan_row in running_plan_rows:
                plan_id = str(plan_row["id"])
                connection.execute(
                    "UPDATE plans SET status = 'READY', updated_at = ? WHERE id = ?",
                    (now, plan_id),
                )
                report["plans"].append(
                    {
                        "plan_id": plan_id,
                        "before_status": plan_row["status"],
                        "after_status": "READY",
                        "persisted_facts": {
                            "sampling_round_count_before": sampling_counts_before[plan_id]
                        },
                    }
                )
                plan_ids.add(plan_id)
            report_id = f"recovery_{uuid.uuid4().hex}"
            report["report_id"] = report_id
            report["affected_plan_ids"] = sorted(plan_ids)
            connection.execute(
                "INSERT INTO recovery_reports(id, report_json, created_at) VALUES (?, ?, ?)",
                (report_id, _json(report), now),
            )
        return report

    def list_recovery_reports(self) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            rows = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY created_at"
            ).fetchall()
            return [_loads(row["report_json"]) for row in rows]
        finally:
            connection.close()

    # Internal helpers ------------------------------------------------------
    def _plan_from_row(self, row: sqlite3.Row) -> PlanRecord:
        raw = _loads(row["plan_json"])
        return PlanRecord(
            id=row["id"],
            name=row["name"],
            target_url=row["target_url"],
            seed=int(row["seed"]),
            profile=row["profile"],
            request_timeout_ms=int(row["request_timeout_ms"]),
            stages=tuple(StageSpec.model_validate(item) for item in raw["stages"]),
            thresholds=Thresholds.model_validate(raw["thresholds"]),
            decision_rule=DecisionRule.model_validate(raw["decision_rule"]),
            sampling=SamplingSpec.model_validate(raw["sampling"]),
            status=row["status"],
            conclusion=_loads(row["conclusion_json"]),
            raw=raw,
        )

    @staticmethod
    def _round_from_row(row: sqlite3.Row) -> RoundRecord:
        return RoundRecord(
            id=row["id"],
            plan_id=row["plan_id"],
            stage_index=int(row["stage_index"]),
            attempt=int(row["attempt"]),
            round_key=row["round_key"],
            concurrency=int(row["concurrency"]),
            duration_seconds=int(row["duration_seconds"]),
            origin=row["origin"],
            retry_of_round_id=row["retry_of_round_id"],
            status=row["status"],
            failure_reason=row["failure_reason"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            ended_at=row["ended_at"],
        )

    def _round_detail(
        self, connection: sqlite3.Connection, round_id: str
    ) -> dict[str, Any]:
        row = connection.execute(
            "SELECT * FROM rounds WHERE id = ?", (round_id,)
        ).fetchone()
        if row is None:
            raise NotFound("round", round_id)
        aggregate_row = connection.execute(
            "SELECT * FROM aggregates WHERE round_id = ?", (round_id,)
        ).fetchone()
        judgment_row = connection.execute(
            "SELECT * FROM judgments WHERE round_id = ?", (round_id,)
        ).fetchone()
        # A reader only receives performance data when both the aggregate and
        # its judgment are committed. This prevents torn read views.
        if row["status"] != "CONVERGED" or aggregate_row is None or judgment_row is None:
            aggregate = None
            judgment = None
        else:
            aggregate = {
                key: aggregate_row[key]
                for key in aggregate_row.keys()
                if key not in {"round_id", "created_at"}
            }
            judgment = {
                "passed": bool(judgment_row["passed"]),
                "verdict": judgment_row["verdict"],
                "triggered": _loads(judgment_row["triggered_json"]),
                "measured": _loads(judgment_row["measured_json"]),
                "sample_size": judgment_row["sample_size"],
                "percentile_method": judgment_row["percentile_method"],
                "rationale": judgment_row["rationale"],
                "decided_at": judgment_row["created_at"],
            }
        return {
            "id": row["id"],
            "plan_id": row["plan_id"],
            "stage_index": row["stage_index"],
            "attempt": row["attempt"],
            "round_key": row["round_key"],
            "concurrency": row["concurrency"],
            "duration_seconds": row["duration_seconds"],
            "origin": row["origin"],
            "retry_of_round_id": row["retry_of_round_id"],
            "status": row["status"],
            "failure_reason": row["failure_reason"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "ended_at": row["ended_at"],
            "aggregate": aggregate,
            "judgment": judgment,
        }

    @staticmethod
    def _insert_transition(
        connection: sqlite3.Connection,
        *,
        round_id: str,
        plan_id: str,
        from_status: str | None,
        to_status: str,
        reason: str,
        created_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO round_transitions
                (round_id, plan_id, from_status, to_status, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (round_id, plan_id, from_status, to_status, reason, created_at),
        )

    @staticmethod
    def _insert_decision(
        connection: sqlite3.Connection,
        *,
        plan_id: str,
        round_id: str | None,
        stage_index: int | None,
        action: str,
        details: dict[str, Any],
        created_at: str,
    ) -> None:
        connection.execute(
            "INSERT INTO decisions(id, plan_id, round_id, stage_index, action, details_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                f"decision_{uuid.uuid4().hex}",
                plan_id,
                round_id,
                stage_index,
                action,
                _json(details),
                created_at,
            ),
        )

