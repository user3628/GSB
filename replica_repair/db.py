from __future__ import annotations

import json
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, Sequence

from .errors import (
    BusinessError,
    conflicting_report,
    conflicting_round,
    group_not_found,
    round_not_found,
    version_mismatch,
)
from .models import (
    ReplicaGroup,
    ReportInput,
    RoundSpec,
    format_timestamp,
)
from .numerics import canonical_json, digest_payload


SCHEMA = """
CREATE TABLE IF NOT EXISTS replica_groups (
    group_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    replica_count INTEGER NOT NULL,
    block_size_bytes INTEGER NOT NULL,
    weights_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS repair_rounds (
    round_id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    group_version INTEGER NOT NULL,
    deadline TEXT NOT NULL,
    target_progress_raw TEXT NOT NULL,
    target_progress TEXT NOT NULL,
    target_rounding_error TEXT NOT NULL,
    target_progress_scale INTEGER NOT NULL,
    relative_tolerance TEXT NOT NULL,
    created_at TEXT NOT NULL,
    sealed_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('OPEN','AGGREGATING','COMMITTED','ABORTED')),
    closed_early INTEGER NOT NULL DEFAULT 0,
    late_count_at_seal INTEGER NOT NULL DEFAULT 0,
    result_version INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (group_id) REFERENCES replica_groups(group_id)
);

CREATE TABLE IF NOT EXISTS replica_reports (
    report_id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    replica_id TEXT NOT NULL,
    submission_round_id TEXT NOT NULL,
    eligible_round_id TEXT,
    reported_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    block_count INTEGER NOT NULL CHECK (block_count >= 0),
    synced_bytes INTEGER NOT NULL CHECK (synced_bytes >= 0),
    late INTEGER NOT NULL CHECK (late IN (0,1)),
    late_reason TEXT,
    report_digest TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    FOREIGN KEY (group_id) REFERENCES replica_groups(group_id),
    FOREIGN KEY (submission_round_id) REFERENCES repair_rounds(round_id),
    FOREIGN KEY (eligible_round_id) REFERENCES repair_rounds(round_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_reports_eligible_replica
ON replica_reports(eligible_round_id, replica_id)
WHERE eligible_round_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_reports_pending_late_replica
ON replica_reports(group_id, replica_id)
WHERE late = 1 AND eligible_round_id IS NULL;

CREATE INDEX IF NOT EXISTS ix_reports_eligible_round
ON replica_reports(eligible_round_id, replica_id, report_id);

CREATE INDEX IF NOT EXISTS ix_reports_submission_round
ON replica_reports(submission_round_id, late);

CREATE TABLE IF NOT EXISTS round_intents (
    round_id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PREPARED','COMMITTED','ABORTED')),
    attempt INTEGER NOT NULL,
    prepared_at TEXT NOT NULL,
    finished_at TEXT,
    detail_json TEXT,
    FOREIGN KEY (round_id) REFERENCES repair_rounds(round_id)
);

CREATE TABLE IF NOT EXISTS round_conclusions (
    round_id TEXT PRIMARY KEY,
    group_id TEXT NOT NULL,
    group_version INTEGER NOT NULL,
    result_version INTEGER NOT NULL,
    input_fingerprint TEXT NOT NULL,
    conclusion_json TEXT NOT NULL,
    conclusion_digest TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (round_id) REFERENCES repair_rounds(round_id)
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    recovery_id INTEGER PRIMARY KEY AUTOINCREMENT,
    generated_at TEXT NOT NULL,
    report_digest TEXT NOT NULL,
    report_json TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = str(Path(path))
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA wal_autocheckpoint=1")
            connection.executescript(SCHEMA)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @contextmanager
    def read_connection(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def _group_from_row(self, row: sqlite3.Row) -> ReplicaGroup:
        weights = json.loads(row["weights_json"])
        return ReplicaGroup(
            group_id=row["group_id"],
            version=int(row["version"]),
            replica_count=int(row["replica_count"]),
            block_size_bytes=int(row["block_size_bytes"]),
            weights={key: Decimal(value) for key, value in weights.items()},
            created_at=row["created_at"],
        )

    def _round_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "round_id": row["round_id"],
            "group_id": row["group_id"],
            "group_version": int(row["group_version"]),
            "deadline": row["deadline"],
            "target_progress_raw": row["target_progress_raw"],
            "target_progress": row["target_progress"],
            "target_rounding_error": row["target_rounding_error"],
            "target_progress_scale": int(row["target_progress_scale"]),
            "relative_tolerance": row["relative_tolerance"],
            "created_at": row["created_at"],
            "sealed_at": row["sealed_at"],
            "status": row["status"],
            "closed_early": bool(row["closed_early"]),
            "late_count_at_seal": int(row["late_count_at_seal"]),
            "result_version": int(row["result_version"]),
        }

    def insert_group(self, group: ReplicaGroup, *, now: datetime) -> tuple[ReplicaGroup, bool]:
        encoded_weights = {
            key: str(value) for key, value in sorted(group.weights.items())
        }
        timestamp = format_timestamp(now)
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM replica_groups WHERE group_id = ?",
                (group.group_id,),
            ).fetchone()
            if existing is not None:
                current = self._group_from_row(existing)
                same = (
                    current.version == group.version
                    and current.replica_count == group.replica_count
                    and current.block_size_bytes == group.block_size_bytes
                    and {
                        key: str(value) for key, value in current.weights.items()
                    }
                    == encoded_weights
                )
                if same:
                    return current, True
                raise version_mismatch(
                    "副本组已存在且版本或内容不同",
                    {
                        "group_id": group.group_id,
                        "existing_version": current.version,
                        "requested_version": group.version,
                    },
                )
            connection.execute(
                """
                INSERT INTO replica_groups(
                    group_id, version, replica_count, block_size_bytes,
                    weights_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    group.group_id,
                    group.version,
                    group.replica_count,
                    group.block_size_bytes,
                    canonical_json(encoded_weights),
                    timestamp,
                    timestamp,
                ),
            )
            return ReplicaGroup(
                group_id=group.group_id,
                version=group.version,
                replica_count=group.replica_count,
                block_size_bytes=group.block_size_bytes,
                weights=group.weights,
                created_at=timestamp,
            ), False

    def get_group(self, group_id: str) -> ReplicaGroup:
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM replica_groups WHERE group_id = ?",
                (group_id,),
            ).fetchone()
        if row is None:
            raise group_not_found(group_id)
        return self._group_from_row(row)

    def create_round(self, spec: RoundSpec, *, now: datetime) -> dict[str, Any]:
        timestamp = format_timestamp(now)
        deadline = format_timestamp(spec.deadline)
        with self.transaction() as connection:
            group_row = connection.execute(
                "SELECT * FROM replica_groups WHERE group_id = ?",
                (spec.group_id,),
            ).fetchone()
            if group_row is None:
                raise group_not_found(spec.group_id)
            if int(group_row["version"]) != spec.group_version:
                raise version_mismatch(
                    "轮次声明的副本组版本与当前版本不一致",
                    {
                        "group_id": spec.group_id,
                        "current_version": int(group_row["version"]),
                        "requested_version": spec.group_version,
                    },
                )

            existing = connection.execute(
                "SELECT * FROM repair_rounds WHERE round_id = ?",
                (spec.round_id,),
            ).fetchone()
            if existing is not None:
                same = (
                    existing["group_id"] == spec.group_id
                    and int(existing["group_version"]) == spec.group_version
                    and existing["deadline"] == deadline
                    and Decimal(existing["target_progress_raw"])
                    == spec.target_progress_raw
                    and Decimal(existing["target_progress"])
                    == spec.target_progress
                    and Decimal(existing["relative_tolerance"])
                    == spec.relative_tolerance
                )
                if not same:
                    raise conflicting_round(
                        "round_id 已由内容不同的轮次占用",
                        {"round_id": spec.round_id},
                    )
                adopted = connection.execute(
                    """
                    SELECT * FROM replica_reports
                    WHERE eligible_round_id = ?
                    ORDER BY replica_id, report_id
                    """,
                    (spec.round_id,),
                ).fetchall()
                result = self._round_from_row(existing)
                result["adopted_late_report_ids"] = [
                    row["report_id"] for row in adopted if int(row["late"]) == 1
                ]
                return result

            connection.execute(
                """
                INSERT INTO repair_rounds(
                    round_id, group_id, group_version, deadline,
                    target_progress_raw, target_progress, target_rounding_error,
                    target_progress_scale, relative_tolerance, created_at,
                    sealed_at, status, closed_early, late_count_at_seal, result_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'OPEN', 0, 0, 0)
                """,
                (
                    spec.round_id,
                    spec.group_id,
                    spec.group_version,
                    deadline,
                    str(spec.target_progress_raw),
                    str(spec.target_progress),
                    str(spec.target_rounding_error),
                    12,
                    str(spec.relative_tolerance),
                    timestamp,
                ),
            )

            pending = connection.execute(
                """
                SELECT * FROM replica_reports
                WHERE group_id = ? AND late = 1 AND eligible_round_id IS NULL
                ORDER BY replica_id, reported_at, report_id
                """,
                (spec.group_id,),
            ).fetchall()
            adopted_ids: list[str] = []
            for report_row in pending:
                connection.execute(
                    """
                    UPDATE replica_reports
                    SET eligible_round_id = ?
                    WHERE report_id = ? AND eligible_round_id IS NULL
                    """,
                    (spec.round_id, report_row["report_id"]),
                )
                adopted_ids.append(report_row["report_id"])

            row = connection.execute(
                "SELECT * FROM repair_rounds WHERE round_id = ?",
                (spec.round_id,),
            ).fetchone()
            result = self._round_from_row(row)
            result["adopted_late_report_ids"] = adopted_ids
            return result

    def get_round(self, round_id: str) -> dict[str, Any]:
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM repair_rounds WHERE round_id = ?",
                (round_id,),
            ).fetchone()
        if row is None:
            raise round_not_found(round_id)
        return self._round_from_row(row)

    def accept_reports(
        self,
        round_row: dict[str, Any],
        group: ReplicaGroup,
        reports: Sequence[ReportInput],
        *,
        now: datetime,
    ) -> dict[str, Any]:
        received_at = format_timestamp(now)
        batch_id = digest_payload(
            {
                "group_id": group.group_id,
                "round_id": round_row["round_id"],
                "report_digests": sorted(report.digest for report in reports),
            }
        )
        accepted_ids: list[str] = []
        late_ids: list[str] = []
        idempotent_ids: list[str] = []
        late_reasons: dict[str, str] = {}
        late_assignments: dict[str, str | None] = {}
        with self.transaction() as connection:
            current_round = connection.execute(
                "SELECT * FROM repair_rounds WHERE round_id = ?",
                (round_row["round_id"],),
            ).fetchone()
            if current_round is None:
                raise round_not_found(round_row["round_id"])
            sealed = (
                current_round["sealed_at"] is not None
                or current_round["status"] != "OPEN"
            )
            deadline = datetime.fromisoformat(
                current_round["deadline"].replace("Z", "+00:00")
            )

            for report in reports:
                existing_by_id = connection.execute(
                    "SELECT * FROM replica_reports WHERE report_id = ?",
                    (report.report_id,),
                ).fetchone()
                if existing_by_id is not None:
                    if (
                        existing_by_id["group_id"] == group.group_id
                        and existing_by_id["report_digest"] == report.digest
                    ):
                        idempotent_ids.append(report.report_id)
                        continue
                    raise conflicting_report(
                        "report_id 已存在且内容不同",
                        {
                            "report_id": report.report_id,
                            "existing_digest": existing_by_id["report_digest"],
                            "incoming_digest": report.digest,
                        },
                    )

                is_late = (
                    sealed
                    or report.reported_at > deadline
                    or now > deadline
                )
                late_reason = None
                if sealed:
                    late_reason = "ARRIVED_AFTER_SEAL"
                elif report.reported_at > deadline:
                    late_reason = "REPORTED_AFTER_DEADLINE"
                elif now > deadline:
                    late_reason = "RECEIVED_AFTER_DEADLINE"

                if is_late:
                    pending_same_replica = connection.execute(
                        """
                        SELECT * FROM replica_reports
                        WHERE group_id = ? AND replica_id = ?
                          AND late = 1 AND eligible_round_id IS NULL
                        """,
                        (group.group_id, report.replica_id),
                    ).fetchone()
                    target_round = None
                    current_rowid = connection.execute(
                        "SELECT rowid AS rid FROM repair_rounds WHERE round_id = ?",
                        (round_row["round_id"],),
                    ).fetchone()["rid"]
                    candidates = connection.execute(
                        """
                        SELECT round_id FROM repair_rounds
                        WHERE group_id = ? AND rowid > ?
                          AND status = 'OPEN' AND sealed_at IS NULL
                        ORDER BY rowid
                        """,
                        (
                            group.group_id,
                            int(current_rowid),
                        ),
                    ).fetchall()
                    for candidate in candidates:
                        already_eligible = connection.execute(
                            """
                            SELECT 1 FROM replica_reports
                            WHERE eligible_round_id = ? AND replica_id = ?
                            """,
                            (candidate["round_id"], report.replica_id),
                        ).fetchone()
                        if already_eligible is None:
                            target_round = candidate["round_id"]
                            break
                    if pending_same_replica is not None and target_round is None:
                        raise conflicting_report(
                            "该副本已有待进入下一轮的迟到上报",
                            {
                                "replica_id": report.replica_id,
                                "pending_report_id": pending_same_replica["report_id"],
                            },
                        )
                else:
                    eligible_same_replica = connection.execute(
                        """
                        SELECT * FROM replica_reports
                        WHERE eligible_round_id = ? AND replica_id = ?
                        """,
                        (round_row["round_id"], report.replica_id),
                    ).fetchone()
                    if eligible_same_replica is not None:
                        raise conflicting_report(
                            "同一轮次同一副本只能有一条有效上报",
                            {
                                "round_id": round_row["round_id"],
                                "replica_id": report.replica_id,
                                "existing_report_id": eligible_same_replica["report_id"],
                            },
                        )

                if is_late:
                    eligible_round_id = target_round
                else:
                    eligible_round_id = round_row["round_id"]
                connection.execute(
                    """
                    INSERT INTO replica_reports(
                        report_id, group_id, replica_id, submission_round_id,
                        eligible_round_id, reported_at, received_at,
                        block_count, synced_bytes, late, late_reason,
                        report_digest, batch_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        report.report_id,
                        group.group_id,
                        report.replica_id,
                        round_row["round_id"],
                        eligible_round_id,
                        format_timestamp(report.reported_at),
                        received_at,
                        report.block_count,
                        report.synced_bytes,
                        1 if is_late else 0,
                        late_reason,
                        report.digest,
                        batch_id,
                    ),
                )
                accepted_ids.append(report.report_id)
                if is_late:
                    late_ids.append(report.report_id)
                    late_reasons[report.report_id] = late_reason or "LATE"
                    late_assignments[report.report_id] = eligible_round_id

            return {
                "batch_id": batch_id,
                "accepted_report_ids": accepted_ids,
                "late_report_ids": late_ids,
                "idempotent_report_ids": idempotent_ids,
                "late_reasons": late_reasons,
                "late_assignments": late_assignments,
                "accepted_late_context": (
                    "迟到上报未进入当前轮次；存在下一未封版轮次时立即归入，否则排队等待下一轮"
                    if late_ids
                    else None
                ),
            }

    def eligible_reports(self, round_id: str) -> list[sqlite3.Row]:
        with self.read_connection() as connection:
            return connection.execute(
                """
                SELECT * FROM replica_reports
                WHERE eligible_round_id = ?
                ORDER BY replica_id, report_id
                """,
                (round_id,),
            ).fetchall()

    def pending_late_count(self, group_id: str) -> int:
        with self.read_connection() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS n FROM replica_reports
                WHERE group_id = ? AND late = 1 AND eligible_round_id IS NULL
                """,
                (group_id,),
            ).fetchone()
        return int(row["n"])

    def late_reports_for_submission(self, round_id: str) -> list[sqlite3.Row]:
        with self.read_connection() as connection:
            return connection.execute(
                """
                SELECT * FROM replica_reports
                WHERE submission_round_id = ? AND late = 1
                ORDER BY replica_id, report_id
                """,
                (round_id,),
            ).fetchall()

    def get_intent(self, round_id: str) -> dict[str, Any] | None:
        with self.read_connection() as connection:
            row = connection.execute(
                "SELECT * FROM round_intents WHERE round_id = ?",
                (round_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "round_id": row["round_id"],
            "group_id": row["group_id"],
            "input_fingerprint": row["input_fingerprint"],
            "state": row["state"],
            "attempt": int(row["attempt"]),
            "prepared_at": row["prepared_at"],
            "finished_at": row["finished_at"],
            "detail": json.loads(row["detail_json"]) if row["detail_json"] else None,
        }

    def begin_intent(
        self,
        round_row: dict[str, Any],
        *,
        fingerprint: str,
        now: datetime,
    ) -> dict[str, Any]:
        timestamp = format_timestamp(now)
        with self.transaction() as connection:
            current = connection.execute(
                "SELECT * FROM repair_rounds WHERE round_id = ?",
                (round_row["round_id"],),
            ).fetchone()
            if current is None:
                raise round_not_found(round_row["round_id"])
            conclusion = connection.execute(
                "SELECT * FROM round_conclusions WHERE round_id = ?",
                (round_row["round_id"],),
            ).fetchone()
            if conclusion is not None:
                raise conflicting_round(
                    "轮次结论已提交，不能重新进入聚合",
                    {"round_id": round_row["round_id"]},
                )
            intent = connection.execute(
                "SELECT * FROM round_intents WHERE round_id = ?",
                (round_row["round_id"],),
            ).fetchone()
            if intent is not None:
                if intent["input_fingerprint"] != fingerprint:
                    raise conflicting_round(
                        "同一轮次使用了不一致的聚合输入",
                        {
                            "round_id": round_row["round_id"],
                            "existing_fingerprint": intent["input_fingerprint"],
                            "requested_fingerprint": fingerprint,
                        },
                    )
                attempt = int(intent["attempt"]) + 1
            else:
                attempt = 1

            sealed_at = current["sealed_at"]
            closed_early = int(current["closed_early"])
            if sealed_at is None:
                sealed_at = timestamp
                deadline = datetime.fromisoformat(
                    current["deadline"].replace("Z", "+00:00")
                )
                closed_early = 1 if now < deadline else 0

            connection.execute(
                """
                UPDATE repair_rounds
                SET status = 'AGGREGATING', sealed_at = ?, closed_early = ?
                WHERE round_id = ?
                """,
                (sealed_at, closed_early, round_row["round_id"]),
            )
            connection.execute(
                """
                INSERT INTO round_intents(
                    round_id, group_id, input_fingerprint, state,
                    attempt, prepared_at, finished_at, detail_json
                ) VALUES (?, ?, ?, 'PREPARED', ?, ?, NULL, NULL)
                ON CONFLICT(round_id) DO UPDATE SET
                    group_id = excluded.group_id,
                    input_fingerprint = excluded.input_fingerprint,
                    state = 'PREPARED',
                    attempt = excluded.attempt,
                    prepared_at = excluded.prepared_at,
                    finished_at = NULL,
                    detail_json = NULL
                """,
                (
                    round_row["round_id"],
                    round_row["group_id"],
                    fingerprint,
                    attempt,
                    timestamp,
                ),
            )
            return {
                "attempt": attempt,
                "sealed_at": sealed_at,
                "closed_early": bool(closed_early),
            }

    def abort_intent(
        self,
        round_id: str,
        *,
        now: datetime,
        error: BusinessError,
    ) -> None:
        timestamp = format_timestamp(now)
        detail = canonical_json(error.as_dict())
        with self.transaction() as connection:
            connection.execute(
                """
                UPDATE round_intents
                SET state = 'ABORTED', finished_at = ?, detail_json = ?
                WHERE round_id = ?
                """,
                (timestamp, detail, round_id),
            )
            connection.execute(
                "UPDATE repair_rounds SET status = 'ABORTED' WHERE round_id = ?",
                (round_id,),
            )

    def commit_conclusion(
        self,
        round_id: str,
        *,
        group_id: str,
        group_version: int,
        fingerprint: str,
        conclusion: dict[str, Any],
        now: datetime,
        late_count: int,
    ) -> dict[str, Any]:
        timestamp = format_timestamp(now)
        conclusion_json = canonical_json(conclusion)
        conclusion_digest = digest_payload(conclusion)
        with self.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM round_conclusions WHERE round_id = ?",
                (round_id,),
            ).fetchone()
            if existing is not None:
                if existing["input_fingerprint"] != fingerprint:
                    raise conflicting_round(
                        "轮次已由不同输入提交结论",
                        {
                            "round_id": round_id,
                            "existing_fingerprint": existing["input_fingerprint"],
                            "requested_fingerprint": fingerprint,
                        },
                    )
                return {
                    "conclusion": json.loads(existing["conclusion_json"]),
                    "conclusion_digest": existing["conclusion_digest"],
                    "result_version": int(existing["result_version"]),
                    "reused": True,
                }

            result_version = connection.execute(
                """
                SELECT COALESCE(MAX(result_version), 0) + 1 AS next_version
                FROM round_conclusions WHERE group_id = ?
                """,
                (group_id,),
            ).fetchone()["next_version"]
            connection.execute(
                """
                INSERT INTO round_conclusions(
                    round_id, group_id, group_version, result_version,
                    input_fingerprint, conclusion_json, conclusion_digest, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    round_id,
                    group_id,
                    group_version,
                    int(result_version),
                    fingerprint,
                    conclusion_json,
                    conclusion_digest,
                    timestamp,
                ),
            )
            connection.execute(
                """
                UPDATE repair_rounds
                SET status = 'COMMITTED', result_version = ?, late_count_at_seal = ?
                WHERE round_id = ?
                """,
                (int(result_version), late_count, round_id),
            )
            connection.execute(
                """
                UPDATE round_intents
                SET state = 'COMMITTED', finished_at = ?, detail_json = NULL
                WHERE round_id = ?
                """,
                (timestamp, round_id),
            )
            return {
                "conclusion": json.loads(conclusion_json),
                "conclusion_digest": conclusion_digest,
                "result_version": int(result_version),
                "reused": False,
            }

    def get_conclusion(self, round_id: str) -> dict[str, Any] | None:
        with self.read_connection() as connection:
            row = connection.execute(
                """
                SELECT rc.*, rr.status AS round_status
                FROM round_conclusions rc
                JOIN repair_rounds rr ON rr.round_id = rc.round_id
                WHERE rc.round_id = ?
                """,
                (round_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "conclusion": json.loads(row["conclusion_json"]),
            "conclusion_json": row["conclusion_json"],
            "conclusion_digest": row["conclusion_digest"],
            "result_version": int(row["result_version"]),
            "input_fingerprint": row["input_fingerprint"],
            "created_at": row["created_at"],
            "round_status": row["round_status"],
        }

    def recover_incomplete_intents(self, *, now: datetime) -> dict[str, Any] | None:
        timestamp = format_timestamp(now)
        with self.transaction() as connection:
            intents = connection.execute(
                """
                SELECT * FROM round_intents
                WHERE state = 'PREPARED'
                ORDER BY round_id
                """
            ).fetchall()
            if not intents:
                return None

            facts: list[dict[str, Any]] = []
            for intent in intents:
                round_row = connection.execute(
                    "SELECT * FROM repair_rounds WHERE round_id = ?",
                    (intent["round_id"],),
                ).fetchone()
                conclusion = connection.execute(
                    "SELECT * FROM round_conclusions WHERE round_id = ?",
                    (intent["round_id"],),
                ).fetchone()
                eligible = connection.execute(
                    """
                    SELECT COUNT(*) AS n FROM replica_reports
                    WHERE eligible_round_id = ?
                    """,
                    (intent["round_id"],),
                ).fetchone()["n"]
                late = connection.execute(
                    """
                    SELECT COUNT(*) AS n FROM replica_reports
                    WHERE submission_round_id = ? AND late = 1
                    """,
                    (intent["round_id"],),
                ).fetchone()["n"]

                round_status_before = round_row["status"] if round_row else None
                if conclusion is not None:
                    action = "ROLLED_FORWARD_TO_LAST_COMPLETE_VERSION"
                    round_status_after = "COMMITTED"
                    last_complete_result_version = int(conclusion["result_version"])
                    connection.execute(
                        """
                        UPDATE round_intents
                        SET state = 'COMMITTED', finished_at = ?
                        WHERE round_id = ?
                        """,
                        (timestamp, intent["round_id"]),
                    )
                else:
                    action = "MARKED_ABORTED"
                    round_status_after = "ABORTED"
                    last_complete_result_version = None
                    connection.execute(
                        """
                        UPDATE round_intents
                        SET state = 'ABORTED', finished_at = ?,
                            detail_json = ?
                        WHERE round_id = ?
                        """,
                        (
                            timestamp,
                            canonical_json(
                                {
                                    "code": "E_ROUND_ABORTED_BY_RECOVERY",
                                    "message": "进程在聚合提交前终止",
                                    "round_id": intent["round_id"],
                                }
                            ),
                            intent["round_id"],
                        ),
                    )
                connection.execute(
                    "UPDATE repair_rounds SET status = ? WHERE round_id = ?",
                    (round_status_after, intent["round_id"]),
                )
                facts.append(
                    {
                        "round_id": intent["round_id"],
                        "group_id": intent["group_id"],
                        "intent_state_before": intent["state"],
                        "intent_state_after": (
                            "COMMITTED" if conclusion is not None else "ABORTED"
                        ),
                        "attempt": int(intent["attempt"]),
                        "prepared_at": intent["prepared_at"],
                        "round_status_before": round_status_before,
                        "round_status_after": round_status_after,
                        "action": action,
                        "conclusion_present": conclusion is not None,
                        "last_complete_result_version": last_complete_result_version,
                        "eligible_report_count": int(eligible),
                        "late_report_count": int(late),
                    }
                )

            report = {
                "report_type": "STARTUP_INTENT_RECOVERY",
                "generated_at": timestamp,
                "source": "SQLITE_ROUND_INTENTS_AND_ROUND_CONCLUSIONS",
                "incomplete_intent_count": len(facts),
                "aborted_count": sum(1 for item in facts if item["action"] == "MARKED_ABORTED"),
                "rolled_forward_count": sum(
                    1 for item in facts if item["action"] == "ROLLED_FORWARD_TO_LAST_COMPLETE_VERSION"
                ),
                "intents": facts,
                "policy": (
                    "存在完整结论时回滚到最后一个完整版本；否则将轮次标记为 ABORTED。"
                    "恢复报告只读取已提交的 SQLite 事实。"
                ),
            }
            report_json = canonical_json(report)
            report_digest = digest_payload(report)
            connection.execute(
                """
                INSERT INTO recovery_reports(generated_at, report_digest, report_json)
                VALUES (?, ?, ?)
                """,
                (timestamp, report_digest, report_json),
            )
            report["report_digest"] = report_digest
            return report

    def latest_recovery_report(self) -> dict[str, Any] | None:
        with self.read_connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM recovery_reports
                ORDER BY recovery_id DESC LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        report = json.loads(row["report_json"])
        report["recovery_id"] = int(row["recovery_id"])
        report["report_digest"] = row["report_digest"]
        return report

    def round_status_payload(self, round_id: str) -> dict[str, Any]:
        with self.read_connection() as connection:
            round_row = connection.execute(
                "SELECT * FROM repair_rounds WHERE round_id = ?",
                (round_id,),
            ).fetchone()
            if round_row is None:
                raise round_not_found(round_id)
            eligible = connection.execute(
                """
                SELECT COUNT(*) AS n FROM replica_reports
                WHERE eligible_round_id = ?
                """,
                (round_id,),
            ).fetchone()["n"]
            pending = connection.execute(
                """
                SELECT COUNT(*) AS n FROM replica_reports
                WHERE submission_round_id = ? AND late = 1
                  AND eligible_round_id IS NULL
                """,
                (round_id,),
            ).fetchone()["n"]
            conclusion = connection.execute(
                """
                SELECT conclusion_digest, result_version, created_at
                FROM round_conclusions WHERE round_id = ?
                """,
                (round_id,),
            ).fetchone()
        result = self._round_from_row(round_row)
        result.update(
            {
                "eligible_report_count": int(eligible),
                "pending_late_report_count": int(pending),
                "conclusion_available": conclusion is not None,
                "conclusion_digest": (
                    conclusion["conclusion_digest"] if conclusion is not None else None
                ),
                "conclusion_created_at": (
                    conclusion["created_at"] if conclusion is not None else None
                ),
            }
        )
        return result

