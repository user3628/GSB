from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from .codecs import decode_policy_content, encode_policy_content
from .diagnostics import build_diagnostics
from .errors import (
    E_INTERNAL,
    E_SNAPSHOT_INVALID,
    E_SNAPSHOT_MISSING,
    BusinessError,
)
from .models import Policy, PolicySnapshot, TrafficSample
from .simulation import normalize_samples, simulate_snapshot
from .validation import validate_snapshot


INT64_MAX = (1 << 63) - 1
SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class CommitResult:
    snapshot: PolicySnapshot
    snapshot_seq: int
    previous_snapshot_no: int | None
    committed: bool
    idempotent: bool
    intent_id: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "snapshot_seq": self.snapshot_seq,
            "snapshot_no": self.snapshot.snapshot_no,
            "format_version": self.snapshot.format_version,
            "generated_at_us": self.snapshot.generated_at_us,
            "policy_count": len(self.snapshot.policies),
            "checksum": self.snapshot.checksum,
            "previous_snapshot_no": self.previous_snapshot_no,
            "committed": self.committed,
            "idempotent": self.idempotent,
            "intent_id": self.intent_id,
        }


class SnapshotStore:
    """SQLite-backed immutable snapshot store with a single-writer commit path."""

    def __init__(self, db_path: str | Path, *, recover_on_start: bool = True):
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._writer_lock = threading.RLock()
        self._state_lock = threading.RLock()
        self._current: PolicySnapshot | None = None
        self._current_seq: int | None = None
        self._initialize_schema()
        self.recovery_report: dict[str, Any] = {
            "recovery_performed": False,
            "started_intents_found": 0,
            "facts_only": True,
        }
        if recover_on_start:
            self.recovery_report = self.recover_incomplete_intents()
        self._load_current_snapshot()

    def _now_us(self) -> int:
        return time.time_ns() // 1000

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    def _initialize_schema(self) -> None:
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_no INTEGER NOT NULL UNIQUE,
                    format_version INTEGER NOT NULL,
                    generated_at_us TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    policy_count INTEGER NOT NULL,
                    created_at_us INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshot_policies (
                    snapshot_seq INTEGER NOT NULL REFERENCES snapshots(seq) ON DELETE CASCADE,
                    queue_id TEXT NOT NULL,
                    cir_bps TEXT NOT NULL,
                    pir_bps TEXT NOT NULL,
                    cbs_bytes TEXT NOT NULL,
                    pbs_bytes TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    match_rule TEXT NOT NULL,
                    parent_queue_id TEXT,
                    timestamp_us TEXT NOT NULL,
                    PRIMARY KEY (snapshot_seq, queue_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS intents (
                    intent_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('started', 'committed', 'aborted')),
                    snapshot_no INTEGER,
                    snapshot_seq INTEGER,
                    subject_id TEXT,
                    payload_sha256 TEXT,
                    payload_json TEXT,
                    created_at_us INTEGER NOT NULL,
                    updated_at_us INTEGER NOT NULL,
                    detail_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS simulation_batches (
                    batch_id TEXT PRIMARY KEY,
                    snapshot_seq INTEGER NOT NULL,
                    snapshot_no INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('committed', 'aborted', 'running')),
                    sample_sha256 TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    result_json TEXT,
                    diagnostics_json TEXT,
                    error_code TEXT,
                    error_json TEXT,
                    created_at_us INTEGER NOT NULL,
                    completed_at_us INTEGER
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sim_batches_created ON simulation_batches(created_at_us DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_intents_state ON intents(state, created_at_us)"
            )
            conn.execute(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _load_current_snapshot(self) -> None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT seq FROM snapshots ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return
            snapshot = self._read_snapshot_by_seq(conn, int(row["seq"]))
            with self._state_lock:
                self._current = snapshot
                self._current_seq = int(row["seq"])
        finally:
            conn.close()

    def _read_snapshot_by_seq(self, conn: sqlite3.Connection, seq: int) -> PolicySnapshot:
        header = conn.execute("SELECT * FROM snapshots WHERE seq = ?", (seq,)).fetchone()
        if header is None:
            raise BusinessError(
                E_SNAPSHOT_MISSING,
                "snapshot sequence does not exist",
                {"snapshot_seq": seq},
                http_status=404,
            )
        rows = conn.execute(
            "SELECT * FROM snapshot_policies WHERE snapshot_seq = ? ORDER BY queue_id",
            (seq,),
        ).fetchall()
        policies = tuple(
            Policy(
                queue_id=row["queue_id"],
                cir_bps=int(row["cir_bps"]),
                pir_bps=int(row["pir_bps"]),
                cbs_bytes=int(row["cbs_bytes"]),
                pbs_bytes=int(row["pbs_bytes"]),
                priority=int(row["priority"]),
                match_rule=row["match_rule"],
                parent_queue_id=row["parent_queue_id"],
                timestamp_us=int(row["timestamp_us"]),
            )
            for row in rows
        )
        snapshot = PolicySnapshot(
            format_version=int(header["format_version"]),
            snapshot_no=int(header["snapshot_no"]),
            generated_at_us=int(header["generated_at_us"]),
            policies=policies,
        )
        expected_checksum = str(header["checksum"])
        if snapshot.checksum != expected_checksum:
            raise BusinessError(
                E_SNAPSHOT_INVALID,
                "persisted snapshot checksum does not match its policy rows",
                {
                    "snapshot_seq": seq,
                    "expected_checksum": expected_checksum,
                    "actual_checksum": snapshot.checksum,
                },
                http_status=500,
            )
        return snapshot

    def current_snapshot(self) -> tuple[int, PolicySnapshot]:
        with self._state_lock:
            if self._current is None or self._current_seq is None:
                raise BusinessError(
                    E_SNAPSHOT_MISSING,
                    "no complete policy snapshot is available",
                    {},
                    http_status=409,
                )
            return self._current_seq, self._current

    def get_snapshot_by_seq(self, seq: int) -> PolicySnapshot:
        conn = self._connect()
        try:
            return self._read_snapshot_by_seq(conn, seq)
        finally:
            conn.close()

    def list_snapshots(self, limit: int = 50) -> list[dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT seq, snapshot_no, format_version, generated_at_us, checksum, policy_count, created_at_us "
                "FROM snapshots ORDER BY seq DESC LIMIT ?",
                (max(1, min(limit, 500)),),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def begin_intent(
        self,
        *,
        kind: str,
        snapshot_no: int | None = None,
        snapshot_seq: int | None = None,
        subject_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
        payload_sha256: str | None = None,
    ) -> str:
        """Persist an operation intent before the corresponding write transaction."""
        intent_id = uuid.uuid4().hex
        now = self._now_us()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO intents("
                "intent_id, kind, state, snapshot_no, snapshot_seq, subject_id, payload_sha256, "
                "payload_json, created_at_us, updated_at_us) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    intent_id,
                    kind,
                    "started",
                    snapshot_no,
                    snapshot_seq,
                    subject_id,
                    payload_sha256,
                    json.dumps(payload, sort_keys=True, ensure_ascii=False) if payload is not None else None,
                    now,
                    now,
                ),
            )
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        return intent_id

    def _finish_intent(
        self,
        conn: sqlite3.Connection,
        intent_id: str,
        *,
        state: str,
        detail: Mapping[str, Any] | None = None,
        snapshot_seq: int | None = None,
    ) -> None:
        conn.execute(
            "UPDATE intents SET state=?, updated_at_us=?, detail_json=?, "
            "snapshot_seq=COALESCE(?, snapshot_seq) WHERE intent_id=?",
            (
                state,
                self._now_us(),
                json.dumps(detail, sort_keys=True, ensure_ascii=False) if detail else None,
                snapshot_seq,
                intent_id,
            ),
        )

    def abort_intent(
        self,
        intent_id: str,
        *,
        error_code: str,
        message: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        now = self._now_us()
        payload = {
            "error_code": error_code,
            "message": message,
            "details": dict(details or {}),
            "aborted_at_us": now,
        }
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE intents SET state='aborted', updated_at_us=?, detail_json=? "
                "WHERE intent_id=? AND state='started'",
                (now, json.dumps(payload, sort_keys=True, ensure_ascii=False), intent_id),
            )
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def recover_incomplete_intents(self) -> dict[str, Any]:
        recovered_at = self._now_us()
        aborted: list[dict[str, Any]] = []
        aborted_batches: list[str] = []
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                "SELECT * FROM intents WHERE state='started' ORDER BY created_at_us, intent_id"
            ).fetchall()
            for row in rows:
                payload: dict[str, Any] = {}
                if row["payload_json"]:
                    try:
                        parsed = json.loads(row["payload_json"])
                        if isinstance(parsed, dict):
                            payload = parsed
                    except json.JSONDecodeError:
                        payload = {"unparseable_payload_json": True}
                detail = {
                    "recovery_state": "aborted",
                    "reason": "incomplete intent found during startup recovery",
                    "fact_scope": "only the persisted started intent is known",
                    "recovered_at_us": recovered_at,
                }
                conn.execute(
                    "UPDATE intents SET state='aborted', updated_at_us=?, detail_json=? "
                    "WHERE intent_id=? AND state='started'",
                    (
                        recovered_at,
                        json.dumps(detail, sort_keys=True, ensure_ascii=False),
                        row["intent_id"],
                    ),
                )
                aborted.append(
                    {
                        "intent_id": row["intent_id"],
                        "kind": row["kind"],
                        "subject_id": row["subject_id"],
                        "snapshot_no": row["snapshot_no"],
                        "created_at_us": row["created_at_us"],
                    }
                )
                if row["kind"] == "simulation" and row["subject_id"]:
                    batch_id = str(row["subject_id"])
                    snapshot_seq = int(row["snapshot_seq"] or payload.get("snapshot_seq") or 0)
                    snapshot_no = int(row["snapshot_no"] or payload.get("snapshot_no") or 0)
                    sample_sha = str(row["payload_sha256"] or payload.get("sample_sha256") or "")
                    request_json = json.dumps(
                        payload.get("request", {"recovered_intent": True}),
                        sort_keys=True,
                        ensure_ascii=False,
                    )
                    error_json = json.dumps(
                        {
                            "code": "E_INTENT_INCOMPLETE",
                            "message": "simulation batch has no committed result; intent was recovered as aborted",
                            "recovered_at_us": recovered_at,
                        },
                        sort_keys=True,
                    )
                    cursor = conn.execute(
                        "INSERT OR IGNORE INTO simulation_batches("
                        "batch_id, snapshot_seq, snapshot_no, status, sample_sha256, request_json, "
                        "result_json, diagnostics_json, error_code, error_json, created_at_us, completed_at_us"
                        ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            batch_id,
                            snapshot_seq,
                            snapshot_no,
                            "aborted",
                            sample_sha,
                            request_json,
                            None,
                            None,
                            "E_INTENT_INCOMPLETE",
                            error_json,
                            int(row["created_at_us"]),
                            recovered_at,
                        ),
                    )
                    if cursor.rowcount:
                        aborted_batches.append(batch_id)
            max_row = conn.execute(
                "SELECT seq, snapshot_no, checksum, policy_count FROM snapshots ORDER BY seq DESC LIMIT 1"
            ).fetchone()
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

        last_complete = None
        if max_row is not None:
            last_complete = {
                "snapshot_seq": int(max_row["seq"]),
                "snapshot_no": int(max_row["snapshot_no"]),
                "checksum": max_row["checksum"],
                "policy_count": int(max_row["policy_count"]),
            }
        return {
            "recovery_performed": True,
            "recovered_at_us": recovered_at,
            "scan_scope": "sqlite:intents state=started",
            "started_intents_found": len(aborted),
            "aborted_intents": aborted,
            "aborted_simulation_batches_inserted": sorted(aborted_batches),
            "last_complete_snapshot": last_complete,
            "rollback_semantics": (
                "uncommitted SQLite transactions were discarded by the database; "
                "the active in-memory snapshot is loaded only from committed rows"
            ),
            "facts_only": True,
            "report_limitations": [
                "only persisted intent and commit facts are included",
                "an interrupted simulation has no fabricated per-packet result",
                "no graceful process shutdown report is promised after forced termination",
            ],
        }

    def commit_snapshot(self, snapshot: PolicySnapshot) -> CommitResult:
        validated = validate_snapshot(snapshot)
        if validated.snapshot_no > INT64_MAX:
            raise BusinessError(
                "E_UNIT_OVERFLOW",
                "snapshot_no exceeds SQLite signed 64-bit storage range",
                {"snapshot_no": str(validated.snapshot_no), "max": str(INT64_MAX)},
            )
        checksum = validated.checksum
        payload = validated.canonical_payload()

        with self._writer_lock:
            with self._state_lock:
                current_no = self._current.snapshot_no if self._current is not None else None
                current_seq = self._current_seq
            if current_no is not None and validated.snapshot_no < current_no:
                raise BusinessError(
                    E_SNAPSHOT_INVALID,
                    "snapshot_no is lower than the active snapshot number",
                    {
                        "incoming_snapshot_no": validated.snapshot_no,
                        "active_snapshot_no": current_no,
                        "monotonic_rule": "strictly_increasing_except_identical_idempotent_retry",
                    },
                    http_status=409,
                )
            if validated.snapshot_no == current_no and current_seq is not None:
                active_conn = self._connect()
                try:
                    active = self._read_snapshot_by_seq(active_conn, current_seq)
                finally:
                    active_conn.close()
                if active.checksum == checksum:
                    return CommitResult(
                        snapshot=active,
                        snapshot_seq=current_seq,
                        previous_snapshot_no=current_no,
                        committed=False,
                        idempotent=True,
                        intent_id=None,
                    )
                raise BusinessError(
                    E_SNAPSHOT_INVALID,
                    "snapshot_no equals the active snapshot but policy content differs",
                    {
                        "incoming_snapshot_no": validated.snapshot_no,
                        "active_checksum": active.checksum,
                        "incoming_checksum": checksum,
                    },
                    http_status=409,
                )

            intent_id = self.begin_intent(
                kind="snapshot_commit",
                snapshot_no=validated.snapshot_no,
                payload=payload,
                payload_sha256=checksum,
            )
            conn = self._connect()
            committed = False
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT snapshot_no, checksum FROM snapshots ORDER BY snapshot_no DESC LIMIT 1"
                ).fetchone()
                if row is not None:
                    db_no = int(row["snapshot_no"])
                    if validated.snapshot_no < db_no:
                        raise BusinessError(
                            E_SNAPSHOT_INVALID,
                            "snapshot_no is lower than a persisted snapshot number",
                            {
                                "incoming_snapshot_no": validated.snapshot_no,
                                "persisted_snapshot_no": db_no,
                            },
                            http_status=409,
                        )
                    if validated.snapshot_no == db_no and row["checksum"] != checksum:
                        raise BusinessError(
                            E_SNAPSHOT_INVALID,
                            "snapshot_no equals a persisted snapshot with different content",
                            {
                                "snapshot_no": validated.snapshot_no,
                                "persisted_checksum": row["checksum"],
                                "incoming_checksum": checksum,
                            },
                            http_status=409,
                        )
                cursor = conn.execute(
                    "INSERT INTO snapshots("
                    "snapshot_no, format_version, generated_at_us, checksum, policy_count, created_at_us"
                    ") VALUES(?,?,?,?,?,?)",
                    (
                        validated.snapshot_no,
                        validated.format_version,
                        str(validated.generated_at_us),
                        checksum,
                        len(validated.policies),
                        self._now_us(),
                    ),
                )
                seq = int(cursor.lastrowid)
                conn.executemany(
                    "INSERT INTO snapshot_policies("
                    "snapshot_seq, queue_id, cir_bps, pir_bps, cbs_bytes, pbs_bytes, priority, "
                    "match_rule, parent_queue_id, timestamp_us) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    [
                        (
                            seq,
                            policy.queue_id,
                            str(policy.cir_bps),
                            str(policy.pir_bps),
                            str(policy.cbs_bytes),
                            str(policy.pbs_bytes),
                            policy.priority,
                            policy.match_rule,
                            policy.parent_queue_id,
                            str(policy.timestamp_us),
                        )
                        for policy in validated.policies
                    ],
                )
                self._finish_intent(
                    conn,
                    intent_id,
                    state="committed",
                    detail={"snapshot_seq": seq, "checksum": checksum},
                    snapshot_seq=seq,
                )
                conn.execute("COMMIT")
                committed = True
            except Exception:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                if not committed:
                    try:
                        self.abort_intent(
                            intent_id,
                            error_code="E_SNAPSHOT_INVALID",
                            message="snapshot commit transaction did not complete",
                        )
                    except Exception:
                        pass
                raise
            finally:
                conn.close()

            with self._state_lock:
                self._current = validated
                self._current_seq = seq
            return CommitResult(
                snapshot=validated,
                snapshot_seq=seq,
                previous_snapshot_no=current_no,
                committed=True,
                idempotent=False,
                intent_id=intent_id,
            )

    def import_policy_content(
        self,
        content: bytes,
        *,
        format_name: str = "auto",
        timestamp_truncation: str | None = None,
        snapshot_no_override: int | None = None,
        generated_at_us_override: int | None = None,
        commit: bool = True,
    ) -> dict[str, Any]:
        decoded = decode_policy_content(
            content,
            format_name=format_name,
            timestamp_truncation=timestamp_truncation,
            snapshot_no_override=snapshot_no_override,
            generated_at_us_override=generated_at_us_override,
        )
        report = dict(decoded.import_report)
        if not commit:
            return {
                "ok": True,
                "committed": False,
                "snapshot": decoded.snapshot.canonical_payload(),
                "import_report": report,
            }
        result = self.commit_snapshot(decoded.snapshot)
        return {
            "ok": True,
            "committed": result.committed,
            "idempotent": result.idempotent,
            "commit": result.as_dict(),
            "snapshot": decoded.snapshot.canonical_payload(),
            "import_report": report,
        }

    def export_current(
        self,
        *,
        format_name: str,
        version: int = 3,
        endianness: str = "little",
    ) -> tuple[int, PolicySnapshot, bytes]:
        seq, snapshot = self.current_snapshot()
        return seq, snapshot, encode_policy_content(
            snapshot,
            format_name=format_name,
            version=version,
            endianness=endianness,
        )

    def run_simulation(
        self,
        samples: Iterable[Mapping[str, Any] | TrafficSample],
        *,
        relative_tolerance: str = "0.10",
    ) -> dict[str, Any]:
        normalized_samples = normalize_samples(samples)
        with self._writer_lock:
            snapshot_seq, snapshot = self.current_snapshot()
            batch_id = uuid.uuid4().hex
            created_at = self._now_us()
            sample_sha = _sample_sha(normalized_samples)
            request_payload = {
                "snapshot_seq": snapshot_seq,
                "snapshot_no": snapshot.snapshot_no,
                "relative_tolerance": relative_tolerance,
                "samples": [
                    {
                        "timestamp_us": sample.timestamp_us,
                        "queue_id": sample.queue_id,
                        "packet_bytes": sample.packet_bytes,
                        "color": sample.color,
                    }
                    for sample in normalized_samples
                ],
            }
            intent_id = self.begin_intent(
                kind="simulation",
                snapshot_no=snapshot.snapshot_no,
                snapshot_seq=snapshot_seq,
                subject_id=batch_id,
                payload_sha256=sample_sha,
                payload={
                    "sample_sha256": sample_sha,
                    "snapshot_seq": snapshot_seq,
                    "snapshot_no": snapshot.snapshot_no,
                    "request": request_payload,
                },
            )
            try:
                simulation = simulate_snapshot(snapshot, normalized_samples)
                diagnostics = build_diagnostics(
                    snapshot,
                    simulation,
                    relative_tolerance=relative_tolerance,
                )
                completed_at = self._now_us()
                wire_result = {
                    "ok": True,
                    "batch_id": batch_id,
                    "status": "committed",
                    "created_at_us": created_at,
                    "completed_at_us": completed_at,
                    "snapshot_seq": snapshot_seq,
                    "simulation": simulation,
                    "diagnostics": diagnostics,
                }
                self._persist_simulation_commit(
                    intent_id=intent_id,
                    batch_id=batch_id,
                    snapshot_seq=snapshot_seq,
                    snapshot_no=snapshot.snapshot_no,
                    sample_sha=sample_sha,
                    request_payload=request_payload,
                    simulation=simulation,
                    diagnostics=diagnostics,
                    created_at=created_at,
                    completed_at=completed_at,
                )
                return wire_result
            except BusinessError as exc:
                self.abort_intent(
                    intent_id,
                    error_code=exc.code,
                    message=exc.message,
                    details=exc.details,
                )
                raise
            except Exception as exc:
                self.abort_intent(
                    intent_id,
                    error_code=E_INTERNAL,
                    message="simulation failed before commit",
                )
                raise BusinessError(
                    E_INTERNAL,
                    "simulation failed before its batch could be committed",
                    {"error_type": type(exc).__name__},
                    http_status=500,
                ) from exc

    def _persist_simulation_commit(
        self,
        *,
        intent_id: str,
        batch_id: str,
        snapshot_seq: int,
        snapshot_no: int,
        sample_sha: str,
        request_payload: Mapping[str, Any],
        simulation: Mapping[str, Any],
        diagnostics: Mapping[str, Any],
        created_at: int,
        completed_at: int,
    ) -> None:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO simulation_batches("
                "batch_id, snapshot_seq, snapshot_no, status, sample_sha256, request_json, "
                "result_json, diagnostics_json, error_code, error_json, created_at_us, completed_at_us"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    batch_id,
                    snapshot_seq,
                    snapshot_no,
                    "committed",
                    sample_sha,
                    json.dumps(request_payload, sort_keys=True, ensure_ascii=False),
                    json.dumps(simulation, sort_keys=True, ensure_ascii=False),
                    json.dumps(diagnostics, sort_keys=True, ensure_ascii=False),
                    None,
                    None,
                    created_at,
                    completed_at,
                ),
            )
            self._finish_intent(
                conn,
                intent_id,
                state="committed",
                detail={"batch_id": batch_id, "snapshot_seq": snapshot_seq},
                snapshot_seq=snapshot_seq,
            )
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def get_simulation(self, batch_id: str) -> dict[str, Any]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM simulation_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                raise BusinessError(
                    "E_BATCH_NOT_FOUND",
                    "simulation batch does not exist",
                    {"batch_id": batch_id},
                    http_status=404,
                )
            result = dict(row)
            for key in ("request_json", "result_json", "diagnostics_json", "error_json"):
                if result[key] is not None:
                    result[key] = json.loads(result[key])
            return result
        finally:
            conn.close()

    def latest_simulation(self) -> dict[str, Any]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT batch_id FROM simulation_batches "
                "WHERE status='committed' ORDER BY created_at_us DESC, batch_id DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            raise BusinessError(
                "E_BATCH_NOT_FOUND",
                "no committed simulation batch is available",
                {},
                http_status=404,
            )
        return self.get_simulation(str(row["batch_id"]))


def _sample_sha(samples: Iterable[TrafficSample]) -> str:
    import hashlib

    canonical = [
        {
            "timestamp_us": sample.timestamp_us,
            "queue_id": sample.queue_id,
            "packet_bytes": sample.packet_bytes,
            "color": sample.color,
        }
        for sample in samples
    ]
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

