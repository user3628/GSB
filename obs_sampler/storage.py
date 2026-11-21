"""SQLite persistence for facts and atomically committed derived snapshots."""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence

from .policy import Policy, canonical_json
from .sampling import Decision, DedupWindow, Record


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


SCHEMA = """
CREATE TABLE IF NOT EXISTS app_meta (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS policies (
    version INTEGER PRIMARY KEY,
    generated_at TEXT NOT NULL,
    seed INTEGER NOT NULL,
    dedup_window_ms INTEGER NOT NULL,
    dedup_scope TEXT NOT NULL,
    min_samples INTEGER NOT NULL,
    error_bound REAL NOT NULL,
    bias_correction INTEGER NOT NULL,
    digest TEXT NOT NULL,
    document_json TEXT NOT NULL,
    committed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    content_digest TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    record_count INTEGER NOT NULL,
    status TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    FOREIGN KEY (policy_version) REFERENCES policies(version)
);

CREATE TABLE IF NOT EXISTS records (
    record_uid TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('log', 'span')),
    source_index INTEGER NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    service TEXT NOT NULL,
    level TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    message_fingerprint TEXT,
    latency_us INTEGER NOT NULL,
    error_flag INTEGER NOT NULL,
    sample_weight REAL NOT NULL,
    UNIQUE (batch_id, kind, source_index),
    UNIQUE (batch_id, kind, trace_id, span_id),
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE TABLE IF NOT EXISTS decisions (
    record_uid TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'SAMPLED', 'DROPPED', 'EMITTED')),
    path TEXT NOT NULL,
    reason TEXT NOT NULL,
    head_sampled INTEGER NOT NULL,
    tail_triggered INTEGER NOT NULL,
    tail_sampled INTEGER NOT NULL,
    dedup_leader_uid TEXT,
    duplicate INTEGER NOT NULL,
    inclusion_probability REAL NOT NULL,
    upstream_weight REAL NOT NULL,
    effective_weight REAL NOT NULL,
    state_history_json TEXT NOT NULL,
    PRIMARY KEY (record_uid, policy_version),
    FOREIGN KEY (record_uid) REFERENCES records(record_uid),
    FOREIGN KEY (policy_version) REFERENCES policies(version)
);

CREATE TABLE IF NOT EXISTS decision_transitions (
    record_uid TEXT NOT NULL,
    policy_version INTEGER NOT NULL,
    step INTEGER NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    path TEXT NOT NULL,
    PRIMARY KEY (record_uid, policy_version, step),
    FOREIGN KEY (record_uid, policy_version)
        REFERENCES decisions(record_uid, policy_version)
);

CREATE TABLE IF NOT EXISTS dedup_windows (
    policy_version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    window_start_ms INTEGER NOT NULL,
    leader_uid TEXT NOT NULL,
    member_count INTEGER NOT NULL,
    member_uids_json TEXT NOT NULL,
    PRIMARY KEY (policy_version, kind, trace_id, window_start_ms),
    FOREIGN KEY (policy_version) REFERENCES policies(version),
    FOREIGN KEY (leader_uid) REFERENCES records(record_uid)
);

CREATE TABLE IF NOT EXISTS strata_counts (
    policy_version INTEGER NOT NULL,
    service TEXT NOT NULL,
    level TEXT NOT NULL,
    head_rate REAL NOT NULL,
    population_count INTEGER NOT NULL,
    log_leaders INTEGER NOT NULL,
    span_leaders INTEGER NOT NULL,
    emitted_count INTEGER NOT NULL,
    population_weight REAL NOT NULL,
    population_weight_compensation REAL NOT NULL,
    weighted_total REAL,
    weighted_total_compensation REAL NOT NULL,
    numeric_error_bound REAL NOT NULL,
    variance_sum REAL NOT NULL,
    status TEXT NOT NULL,
    insufficient_reason TEXT,
    PRIMARY KEY (policy_version, service, level),
    FOREIGN KEY (policy_version) REFERENCES policies(version)
);

CREATE TABLE IF NOT EXISTS bias_reports (
    policy_version INTEGER NOT NULL,
    service TEXT NOT NULL,
    level TEXT NOT NULL,
    report_json TEXT NOT NULL,
    PRIMARY KEY (policy_version, service, level),
    FOREIGN KEY (policy_version) REFERENCES policies(version)
);

CREATE TABLE IF NOT EXISTS derived_commits (
    policy_version INTEGER PRIMARY KEY,
    revision INTEGER NOT NULL,
    record_count INTEGER NOT NULL,
    leader_count INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL,
    decision_digest TEXT NOT NULL,
    counts_digest TEXT NOT NULL,
    report_digest TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    FOREIGN KEY (policy_version) REFERENCES policies(version)
);

CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    policy_version INTEGER,
    batch_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'ABORTED')),
    created_at TEXT NOT NULL,
    finished_at TEXT,
    details_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recovery_reports (
    report_id TEXT PRIMARY KEY,
    generated_at TEXT NOT NULL,
    report_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_records_order
    ON records(timestamp_ms, kind, batch_id, source_index);
CREATE INDEX IF NOT EXISTS idx_records_trace
    ON records(kind, trace_id, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_decisions_state
    ON decisions(policy_version, state);
CREATE INDEX IF NOT EXISTS idx_intents_status
    ON intents(status, created_at);
"""


class Storage:
    def __init__(self, db_path: str | Path) -> None:
        self.path = Path(db_path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.path),
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def initialize(self) -> None:
        connection = self._connect()
        try:
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise RuntimeError(f"SQLite WAL mode is required, got {mode!r}")
            connection.executescript(SCHEMA)
            connection.execute("PRAGMA wal_autocheckpoint = 1000")
        finally:
            connection.close()

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
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
        connection = self._connect()
        try:
            connection.execute("BEGIN")
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

    def begin_intent(
        self,
        kind: str,
        *,
        policy_version: int | None = None,
        batch_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> str:
        intent_id = str(uuid.uuid4())
        with self.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO intents(intent_id, kind, policy_version, batch_id, status, created_at, details_json)
                VALUES (?, ?, ?, ?, 'RUNNING', ?, ?)
                """,
                (
                    intent_id,
                    kind,
                    policy_version,
                    batch_id,
                    utc_now(),
                    canonical_json(details or {}),
                ),
            )
        return intent_id

    def complete_intent(self, connection: sqlite3.Connection, intent_id: str) -> None:
        connection.execute(
            "UPDATE intents SET status = 'COMPLETED', finished_at = ? WHERE intent_id = ? AND status = 'RUNNING'",
            (utc_now(), intent_id),
        )

    def abort_intent(self, intent_id: str, reason: str) -> None:
        with self.write_transaction() as connection:
            connection.execute(
                """
                UPDATE intents
                SET status = 'ABORTED', finished_at = ?,
                    details_json = json_set(details_json, '$.abort_reason', ?)
                WHERE intent_id = ? AND status = 'RUNNING'
                """,
                (utc_now(), reason, intent_id),
            )

    def get_intent(self, intent_id: str) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
        return _row_dict(row) if row is not None else None

    def policy_exists(self, version: int) -> bool:
        with self.read_transaction() as connection:
            row = connection.execute(
                "SELECT 1 FROM policies WHERE version = ?", (version,)
            ).fetchone()
        return row is not None

    def get_policy_row(self, version: int) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM policies WHERE version = ?", (version,)
            ).fetchone()
        return _row_dict(row) if row is not None else None

    def get_policy_row_in_tx(
        self, connection: sqlite3.Connection, version: int
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT * FROM policies WHERE version = ?", (version,)
        ).fetchone()
        return _row_dict(row) if row is not None else None

    def current_policy_version(self, connection: sqlite3.Connection | None = None) -> int | None:
        close = connection is None
        connection = connection or self._connect()
        try:
            row = connection.execute(
                "SELECT value FROM app_meta WHERE key = 'current_policy_version'"
            ).fetchone()
            return int(row["value"]) if row is not None and row["value"] is not None else None
        finally:
            if close:
                connection.close()

    def set_current_policy(
        self, connection: sqlite3.Connection, version: int
    ) -> None:
        connection.execute(
            """
            INSERT INTO app_meta(key, value) VALUES ('current_policy_version', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (str(version),),
        )

    def insert_policy(self, connection: sqlite3.Connection, policy: Policy) -> None:
        connection.execute(
            """
            INSERT INTO policies(
                version, generated_at, seed, dedup_window_ms, dedup_scope,
                min_samples, error_bound, bias_correction, digest, document_json, committed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                policy.version,
                policy.generated_at,
                policy.seed,
                policy.dedup_window_ms,
                policy.dedup_scope,
                policy.min_samples,
                policy.error_bound,
                int(policy.bias_correction),
                policy.digest,
                canonical_json(policy.document),
                utc_now(),
            ),
        )

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        return _row_dict(row) if row is not None else None

    def get_batch_in_tx(
        self, connection: sqlite3.Connection, batch_id: str
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        return _row_dict(row) if row is not None else None

    def insert_batch(
        self,
        connection: sqlite3.Connection,
        *,
        batch_id: str,
        content_digest: str,
        policy_version: int,
        record_count: int,
        ingested_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO batches(batch_id, content_digest, policy_version, record_count, status, ingested_at)
            VALUES (?, ?, ?, ?, 'COMMITTED', ?)
            """,
            (batch_id, content_digest, policy_version, record_count, ingested_at),
        )

    def insert_records(self, connection: sqlite3.Connection, records: Sequence[Record]) -> None:
        connection.executemany(
            """
            INSERT INTO records(
                record_uid, batch_id, kind, source_index, timestamp_ms, service, level,
                trace_id, span_id, parent_span_id, message_fingerprint,
                latency_us, error_flag, sample_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    record.record_uid,
                    record.batch_id,
                    record.kind,
                    record.source_index,
                    record.timestamp_ms,
                    record.service,
                    record.level,
                    record.trace_id,
                    record.span_id,
                    record.parent_span_id,
                    record.message_fingerprint,
                    record.latency_us,
                    int(record.error),
                    record.sample_weight,
                )
                for record in records
            ],
        )

    def load_all_records(self, connection: sqlite3.Connection) -> list[Record]:
        rows = connection.execute(
            """
            SELECT record_uid, batch_id, kind, source_index, timestamp_ms, service,
                   level, trace_id, span_id, parent_span_id, message_fingerprint,
                   latency_us, error_flag, sample_weight
            FROM records
            ORDER BY timestamp_ms, kind, batch_id, source_index, record_uid
            """
        ).fetchall()
        return [
            Record(
                record_uid=row["record_uid"],
                batch_id=row["batch_id"],
                kind=row["kind"],
                source_index=row["source_index"],
                timestamp_ms=row["timestamp_ms"],
                service=row["service"],
                level=row["level"],
                trace_id=row["trace_id"],
                span_id=row["span_id"],
                parent_span_id=row["parent_span_id"],
                message_fingerprint=row["message_fingerprint"],
                latency_us=row["latency_us"],
                error=bool(row["error_flag"]),
                sample_weight=row["sample_weight"],
            )
            for row in rows
        ]

    def replace_derived_state(
        self,
        connection: sqlite3.Connection,
        *,
        policy: Policy,
        decisions: Sequence[Decision],
        windows: Sequence[DedupWindow],
        count_rows: Sequence[dict[str, Any]],
        report_rows: Sequence[dict[str, Any]],
        record_count: int,
        leader_count: int,
        duplicate_count: int,
        decision_digest: str,
        counts_digest: str,
        report_digest: str,
        crash_hook: Callable[[str], None] | None = None,
    ) -> int:
        version = policy.version
        for table in ("decision_transitions", "decisions", "dedup_windows", "strata_counts", "bias_reports"):
            connection.execute(f"DELETE FROM {table} WHERE policy_version = ?", (version,))

        previous = connection.execute(
            "SELECT revision FROM derived_commits WHERE policy_version = ?", (version,)
        ).fetchone()
        revision = (int(previous["revision"]) + 1) if previous is not None else 1

        connection.executemany(
            """
            INSERT INTO decisions(
                record_uid, policy_version, state, path, reason, head_sampled,
                tail_triggered, tail_sampled, dedup_leader_uid, duplicate,
                inclusion_probability, upstream_weight, effective_weight, state_history_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    decision.record_uid,
                    decision.policy_version,
                    decision.state,
                    decision.path,
                    decision.reason,
                    int(decision.head_sampled),
                    int(decision.tail_triggered),
                    int(decision.tail_sampled),
                    decision.dedup_leader_uid,
                    int(decision.duplicate),
                    decision.inclusion_probability,
                    decision.upstream_weight,
                    decision.effective_weight,
                    canonical_json(decision.state_history),
                )
                for decision in decisions
            ],
        )
        if crash_hook is not None:
            crash_hook("after_decisions_before_counts")

        transition_rows: list[tuple[Any, ...]] = []
        for decision in decisions:
            for step, transition in enumerate(decision.transitions):
                transition_rows.append(
                    (
                        decision.record_uid,
                        decision.policy_version,
                        step,
                        transition["from"],
                        transition["to"],
                        transition["path"],
                    )
                )
        connection.executemany(
            """
            INSERT INTO decision_transitions(
                record_uid, policy_version, step, from_state, to_state, path
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            transition_rows,
        )

        connection.executemany(
            """
            INSERT INTO dedup_windows(
                policy_version, kind, trace_id, window_start_ms, leader_uid,
                member_count, member_uids_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    version,
                    window.kind,
                    window.trace_id,
                    window.window_start_ms,
                    window.leader_uid,
                    len(window.member_uids),
                    canonical_json(list(window.member_uids)),
                )
                for window in windows
            ],
        )

        connection.executemany(
            """
            INSERT INTO strata_counts(
                policy_version, service, level, head_rate, population_count,
                log_leaders, span_leaders, emitted_count, population_weight,
                population_weight_compensation, weighted_total,
                weighted_total_compensation, numeric_error_bound, variance_sum,
                status, insufficient_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    version,
                    row["service"],
                    row["level"],
                    row["head_rate"],
                    row["population_count"],
                    row["log_leaders"],
                    row["span_leaders"],
                    row["emitted_count"],
                    row["population_weight"],
                    row["population_weight_compensation"],
                    row["weighted_total"],
                    row["weighted_total_compensation"],
                    row["numeric_error_bound"],
                    row["variance_sum"],
                    row["status"],
                    row["insufficient_reason"],
                )
                for row in count_rows
            ],
        )
        connection.executemany(
            """
            INSERT INTO bias_reports(policy_version, service, level, report_json)
            VALUES (?, ?, ?, ?)
            """,
            [
                (version, row["service"], row["level"], canonical_json(row))
                for row in report_rows
            ],
        )
        if crash_hook is not None:
            crash_hook("after_counts_and_reports_before_commit_marker")
        connection.execute(
            """
            INSERT INTO derived_commits(
                policy_version, revision, record_count, leader_count, duplicate_count,
                decision_digest, counts_digest, report_digest, committed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(policy_version) DO UPDATE SET
                revision = excluded.revision,
                record_count = excluded.record_count,
                leader_count = excluded.leader_count,
                duplicate_count = excluded.duplicate_count,
                decision_digest = excluded.decision_digest,
                counts_digest = excluded.counts_digest,
                report_digest = excluded.report_digest,
                committed_at = excluded.committed_at
            """,
            (
                version,
                revision,
                record_count,
                leader_count,
                duplicate_count,
                decision_digest,
                counts_digest,
                report_digest,
                utc_now(),
            ),
        )
        return revision

    def get_derived_commit(self, version: int) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            row = connection.execute(
                "SELECT * FROM derived_commits WHERE policy_version = ?", (version,)
            ).fetchone()
        return _row_dict(row) if row is not None else None

    def get_policy_state(self, version: int) -> dict[str, Any]:
        with self.read_transaction() as connection:
            policy_row = connection.execute(
                "SELECT * FROM policies WHERE version = ?", (version,)
            ).fetchone()
            commit_row = connection.execute(
                "SELECT * FROM derived_commits WHERE policy_version = ?", (version,)
            ).fetchone()
            count_rows = connection.execute(
                "SELECT * FROM strata_counts WHERE policy_version = ? ORDER BY service, level",
                (version,),
            ).fetchall()
            report_rows = connection.execute(
                "SELECT report_json FROM bias_reports WHERE policy_version = ? ORDER BY service, level",
                (version,),
            ).fetchall()
            state_rows = connection.execute(
                "SELECT state, COUNT(*) AS count FROM decisions WHERE policy_version = ? GROUP BY state",
                (version,),
            ).fetchall()
            current_row = connection.execute(
                "SELECT value FROM app_meta WHERE key = 'current_policy_version'"
            ).fetchone()
            batch_count = connection.execute("SELECT COUNT(*) AS count FROM batches").fetchone()["count"]

        if policy_row is None:
            return {"policy_version": version, "exists": False}
        return {
            "policy_version": version,
            "exists": True,
            "is_current": current_row is not None and int(current_row["value"]) == version,
            "policy": json.loads(policy_row["document_json"]),
            "commit": _row_dict(commit_row) if commit_row is not None else None,
            "decision_states": {row["state"]: row["count"] for row in state_rows},
            "counts": [_row_dict(row) for row in count_rows],
            "bias_report": [json.loads(row["report_json"]) for row in report_rows],
            "batch_count": batch_count,
        }

    def get_bias_report(
        self, version: int, service: str, level: str
    ) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            row = connection.execute(
                "SELECT report_json FROM bias_reports WHERE policy_version = ? AND service = ? AND level = ?",
                (version, service, level.upper()),
            ).fetchone()
        return json.loads(row["report_json"]) if row is not None else None

    def get_batch_decisions(self, batch_id: str, version: int) -> list[dict[str, Any]]:
        with self.read_transaction() as connection:
            rows = connection.execute(
                """
                SELECT r.record_uid, r.kind, r.source_index, r.timestamp_ms, r.service,
                       r.level, r.trace_id, r.span_id, d.state, d.path, d.reason,
                       d.head_sampled, d.tail_triggered, d.tail_sampled,
                       d.dedup_leader_uid, d.duplicate, d.inclusion_probability,
                       d.upstream_weight, d.effective_weight, d.state_history_json
                FROM records r
                JOIN decisions d ON d.record_uid = r.record_uid AND d.policy_version = ?
                WHERE r.batch_id = ?
                ORDER BY r.kind, r.source_index
                """,
                (version, batch_id),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = _row_dict(row)
            item["state_history"] = json.loads(item.pop("state_history_json"))
            for key in ("head_sampled", "tail_triggered", "tail_sampled", "duplicate"):
                item[key] = bool(item[key])
            result.append(item)
        return result

    def recent_batches(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.read_transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM batches ORDER BY ingested_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [_row_dict(row) for row in rows]

    def recovery_report(self) -> dict[str, Any] | None:
        with self.read_transaction() as connection:
            row = connection.execute(
                "SELECT report_json FROM recovery_reports ORDER BY generated_at DESC LIMIT 1"
            ).fetchone()
        return json.loads(row["report_json"]) if row is not None else None

    def recover_after_forced_termination(self) -> dict[str, Any]:
        """Abort dangling intents and emit a committed-facts-only report."""

        generated_at = utc_now()
        with self.write_transaction() as connection:
            running_rows = connection.execute(
                "SELECT * FROM intents WHERE status = 'RUNNING' ORDER BY created_at, intent_id"
            ).fetchall()
            running = [_row_dict(row) for row in running_rows]
            aborted_at = utc_now()
            connection.executemany(
                "UPDATE intents SET status = 'ABORTED', finished_at = ? WHERE intent_id = ? AND status = 'RUNNING'",
                [(aborted_at, row["intent_id"]) for row in running],
            )

            current = self.current_policy_version(connection)
            current_commit = None
            cursor_reconciled_from = None
            cursor_reconciled_to = None
            if current is None:
                fallback = connection.execute(
                    "SELECT policy_version FROM derived_commits ORDER BY policy_version DESC LIMIT 1"
                ).fetchone()
                if fallback is not None:
                    current = int(fallback["policy_version"])
                    cursor_reconciled_to = current
                    self.set_current_policy(connection, current)
            if current is not None:
                current_commit = connection.execute(
                    "SELECT * FROM derived_commits WHERE policy_version = ?", (current,)
                ).fetchone()
                policy_exists = connection.execute(
                    "SELECT 1 FROM policies WHERE version = ?", (current,)
                ).fetchone()
                if policy_exists is None or current_commit is None:
                    fallback = connection.execute(
                        "SELECT policy_version FROM derived_commits ORDER BY policy_version DESC LIMIT 1"
                    ).fetchone()
                    cursor_reconciled_from = current
                    if fallback is None:
                        connection.execute(
                            "DELETE FROM app_meta WHERE key = 'current_policy_version'"
                        )
                        current = None
                    else:
                        current = int(fallback["policy_version"])
                        cursor_reconciled_to = current
                        self.set_current_policy(connection, current)
                        current_commit = connection.execute(
                            "SELECT * FROM derived_commits WHERE policy_version = ?", (current,)
                        ).fetchone()

            policy_rows = connection.execute(
                "SELECT version, digest, committed_at FROM policies ORDER BY version"
            ).fetchall()
            commit_rows = connection.execute(
                "SELECT * FROM derived_commits ORDER BY policy_version"
            ).fetchall()
            batches = connection.execute("SELECT COUNT(*) AS count FROM batches").fetchone()["count"]
            records = connection.execute("SELECT COUNT(*) AS count FROM records").fetchone()["count"]
            intent_counts = {
                row["status"]: row["count"]
                for row in connection.execute(
                    "SELECT status, COUNT(*) AS count FROM intents GROUP BY status"
                ).fetchall()
            }
            decision_states: dict[str, int] = {}
            if current is not None:
                decision_states = {
                    row["state"]: row["count"]
                    for row in connection.execute(
                        "SELECT state, COUNT(*) AS count FROM decisions WHERE policy_version = ? GROUP BY state",
                        (current,),
                    ).fetchall()
                }
            foreign_key_errors = [tuple(row) for row in connection.execute("PRAGMA foreign_key_check")]

            report = {
                "generated_at": generated_at,
                "scope": "COMMITTED_SQLITE_FACTS_ONLY",
                "journal_mode": "WAL",
                "crash_during_write_detected": bool(running),
                "forced_termination_exit_report_guaranteed": False,
                "limitations": [
                    "No complete graceful-exit report is promised after forced termination.",
                    "Only transactions committed before the last process exit are reported.",
                    "In-flight decisions, counts and bias rows were rolled back by SQLite or discarded with their transaction.",
                ],
                "intents": {
                    "running_found": len(running),
                    "aborted": [
                        {
                            "intent_id": row["intent_id"],
                            "kind": row["kind"],
                            "policy_version": row["policy_version"],
                            "batch_id": row["batch_id"],
                            "created_at": row["created_at"],
                        }
                        for row in running
                    ],
                    "status_counts": intent_counts,
                },
                "cursor_reconciled_from": cursor_reconciled_from,
                "cursor_reconciled_to": cursor_reconciled_to,
                "current_policy_version": current,
                "current_commit": _row_dict(current_commit) if current_commit is not None else None,
                "committed_policy_versions": [_row_dict(row) for row in policy_rows],
                "derived_commits": [_row_dict(row) for row in commit_rows],
                "facts": {
                    "batch_count": batches,
                    "record_count": records,
                    "current_decision_states": decision_states,
                },
                "integrity": {
                    "foreign_key_check": foreign_key_errors,
                    "current_has_policy_and_derived_commit": current is None
                    or (current_commit is not None),
                },
            }
            connection.execute(
                "INSERT INTO recovery_reports(report_id, generated_at, report_json) VALUES (?, ?, ?)",
                (str(uuid.uuid4()), generated_at, canonical_json(report)),
            )
        return report


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    return {key: row[key] for key in row.keys()}
