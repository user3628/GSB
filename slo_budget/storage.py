"""SQLite persistence, conflict resolution and crash-safe intent journaling."""

from __future__ import annotations

import dataclasses
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import Any, Iterator

from .errors import SloError
from .models import ParsedBatch, SnapshotManifest, WindowRecord, dumps_json, sorted_records


def default_clock_ms() -> int:
    fixed = os.environ.get("SLO_FIXED_NOW_MS")
    if fixed:
        try:
            return int(fixed)
        except ValueError as exc:
            raise SloError(
                "E_CONFIG_INVALID",
                "SLO_FIXED_NOW_MS must be an integer",
                details={"value": fixed},
            ) from exc
    return time.time_ns() // 1_000_000


class Storage:
    """Single-process SQLite store with a locked active in-memory index.

    SQLite transactions provide the durable rollback point. ``intent_log`` is
    committed before any business mutation; a successful business transaction
    also changes the intent to ``committed`` in the same transaction. Therefore a
    process death leaves either a committed intent or a pending intent with no
    committed window changes.
    """

    def __init__(self, db_path: str | Path, *, clock_ms=None) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock_ms or default_clock_ms
        self._lock = RLock()
        self._derived_cache: dict[str, Any] = {}
        self.connection = sqlite3.connect(
            str(self.db_path),
            isolation_level=None,
            check_same_thread=False,
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self._initialize_schema()
        self._recovered_intents = self.recover_pending_intents()
        with self._lock:
            self._index = self._load_index()

    def now_ms(self) -> int:
        return int(self._clock())

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    def __enter__(self) -> "Storage":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _initialize_schema(self) -> None:
        with self._lock:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS snapshot_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    snapshot_sequence INTEGER NOT NULL,
                    format TEXT NOT NULL,
                    format_version INTEGER NOT NULL,
                    generated_at_ms INTEGER NOT NULL,
                    activated_at_ms INTEGER NOT NULL,
                    record_count INTEGER NOT NULL
                );
                INSERT OR IGNORE INTO snapshot_state
                    (id, snapshot_sequence, format, format_version,
                     generated_at_ms, activated_at_ms, record_count)
                VALUES (1, 0, '', 0, 0, 0, 0);

                CREATE TABLE IF NOT EXISTS windows (
                    service TEXT NOT NULL,
                    sli_type TEXT NOT NULL,
                    window_start_ms INTEGER NOT NULL,
                    window_length_ms INTEGER NOT NULL,
                    good_events INTEGER NOT NULL,
                    total_events INTEGER NOT NULL,
                    late_count INTEGER NOT NULL,
                    format_version INTEGER NOT NULL,
                    last_write_ms INTEGER NOT NULL,
                    snapshot_sequence INTEGER NOT NULL,
                    source_format TEXT NOT NULL,
                    import_batch_id TEXT,
                    PRIMARY KEY (service, sli_type, window_start_ms)
                );
                CREATE INDEX IF NOT EXISTS windows_snapshot_idx ON windows(snapshot_sequence);

                CREATE TABLE IF NOT EXISTS snapshot_versions (
                    snapshot_sequence INTEGER PRIMARY KEY,
                    format TEXT NOT NULL,
                    format_version INTEGER NOT NULL,
                    generated_at_ms INTEGER NOT NULL,
                    loaded_at_ms INTEGER NOT NULL,
                    record_count INTEGER NOT NULL,
                    checksum_sha256 TEXT,
                    status TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS import_reports (
                    batch_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    business_code TEXT,
                    format TEXT,
                    format_version INTEGER,
                    snapshot_sequence INTEGER,
                    source_path TEXT,
                    created_at_ms INTEGER NOT NULL,
                    counts_json TEXT NOT NULL,
                    report_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS import_reports_created_idx
                    ON import_reports(created_at_ms);

                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_sequence INTEGER NOT NULL,
                    service TEXT NOT NULL,
                    sli_type TEXT NOT NULL,
                    as_of_ms INTEGER NOT NULL,
                    grade TEXT NOT NULL,
                    result_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    UNIQUE(snapshot_sequence, service, sli_type, as_of_ms)
                );

                CREATE TABLE IF NOT EXISTS intent_log (
                    intent_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    batch_id TEXT,
                    status TEXT NOT NULL CHECK (
                        status IN ('pending', 'committed', 'rolled_back', 'aborted')
                    ),
                    payload_json TEXT NOT NULL,
                    error_json TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS intent_log_status_idx
                    ON intent_log(status, created_at_ms);

                CREATE TABLE IF NOT EXISTS recovery_reports (
                    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    intent_id TEXT NOT NULL,
                    batch_id TEXT,
                    action TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL
                );
                """
            )

    def recover_pending_intents(self) -> list[dict[str, Any]]:
        """Mark pre-restart pending intents aborted; SQLite rolled back data."""

        with self._lock:
            rows = self.connection.execute(
                "SELECT * FROM intent_log WHERE status='pending' ORDER BY created_at_ms, intent_id"
            ).fetchall()
            if not rows:
                return []
            now = self.now_ms()
            active = self.connection.execute(
                "SELECT * FROM snapshot_state WHERE id=1"
            ).fetchone()
            recovery_rows: list[dict[str, Any]] = []
            with self._transaction():
                for row in rows:
                    payload = json.loads(row["payload_json"] or "{}")
                    details = {
                        "action": "rolled_back_to_last_complete_version",
                        "active_snapshot_sequence": active["snapshot_sequence"],
                        "active_record_count": active["record_count"],
                        "pending_kind": row["kind"],
                        "pending_payload": payload,
                    }
                    self.connection.execute(
                        """
                        UPDATE intent_log
                           SET status='aborted', error_json=?, updated_at_ms=?
                         WHERE intent_id=? AND status='pending'
                        """,
                        (
                            dumps_json(
                                {
                                    "code": "E_PROCESS_INTERRUPTED",
                                    "message": "pending intent found during restart recovery",
                                }
                            ),
                            now,
                            row["intent_id"],
                        ),
                    )
                    self.connection.execute(
                        """
                        INSERT INTO recovery_reports
                            (intent_id, batch_id, action, details_json, created_at_ms)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            row["intent_id"],
                            row["batch_id"],
                            "ROLLED_BACK_TO_LAST_COMPLETE_VERSION",
                            dumps_json(details),
                            now,
                        ),
                    )
                    if row["kind"] in {"import", "snapshot_replace"} and row["batch_id"]:
                        existing = self.connection.execute(
                            "SELECT 1 FROM import_reports WHERE batch_id=?",
                            (row["batch_id"],),
                        ).fetchone()
                        if not existing:
                            report = {
                                "batch_id": row["batch_id"],
                                "kind": row["kind"],
                                "status": "ABORTED",
                                "business_code": "E_PROCESS_INTERRUPTED",
                                "format": payload.get("format"),
                                "format_version": payload.get("format_version"),
                                "snapshot_sequence": payload.get("snapshot_sequence"),
                                "source_path": payload.get("source_path"),
                                "created_at_ms": now,
                                "atomic": True,
                                "counts": {
                                    "inserted": 0,
                                    "overwritten": 0,
                                    "skipped": 0,
                                    "rejected": 0,
                                    "accepted": 0,
                                },
                                "decisions": [],
                                "degradations": [],
                                "rounding": {},
                                "previous_state": details,
                                "result_state": details,
                                "error": {
                                    "code": "E_PROCESS_INTERRUPTED",
                                    "message": "batch was aborted during restart recovery",
                                },
                            }
                            self.connection.execute(
                                """
                                INSERT INTO import_reports
                                    (batch_id, kind, status, business_code, format,
                                     format_version, snapshot_sequence, source_path,
                                     created_at_ms, counts_json, report_json)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    row["batch_id"],
                                    row["kind"],
                                    report["status"],
                                    report["business_code"],
                                    report["format"],
                                    report["format_version"],
                                    report["snapshot_sequence"],
                                    report["source_path"],
                                    now,
                                    dumps_json(report["counts"]),
                                    dumps_json(report),
                                ),
                            )
                    recovery_rows.append(
                        {
                            "intent_id": row["intent_id"],
                            "batch_id": row["batch_id"],
                            "kind": row["kind"],
                            "action": "ROLLED_BACK_TO_LAST_COMPLETE_VERSION",
                            "details": details,
                            "created_at_ms": now,
                        }
                    )
            return recovery_rows

    def _load_index(self) -> dict[tuple[str, str, int], WindowRecord]:
        rows = self.connection.execute("SELECT * FROM windows").fetchall()
        return {self._row_key(row): self._row_to_record(row) for row in rows}

    @staticmethod
    def _row_key(row: sqlite3.Row) -> tuple[str, str, int]:
        return (row["service"], row["sli_type"], row["window_start_ms"])

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> WindowRecord:
        return WindowRecord(
            service=row["service"],
            sli_type=row["sli_type"],
            window_start_ms=row["window_start_ms"],
            window_length_ms=row["window_length_ms"],
            good_events=row["good_events"],
            total_events=row["total_events"],
            late_count=row["late_count"],
            last_write_ms=row["last_write_ms"],
            format_version=row["format_version"],
            snapshot_sequence=row["snapshot_sequence"],
            source_format=row["source_format"],
        )

    def begin_write_intent(
        self,
        kind: str,
        *,
        batch_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> str:
        intent_id = uuid.uuid4().hex
        now = self.now_ms()
        with self._lock, self._transaction():
            self.connection.execute(
                """
                INSERT INTO intent_log
                    (intent_id, kind, batch_id, status, payload_json,
                     error_json, created_at_ms, updated_at_ms)
                VALUES (?, ?, ?, 'pending', ?, NULL, ?, ?)
                """,
                (intent_id, kind, batch_id, dumps_json(payload or {}), now, now),
            )
        return intent_id

    def _intent_row(self, intent_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM intent_log WHERE intent_id=?", (intent_id,)
        ).fetchone()
        if row is None:
            raise SloError(
                "E_INTERNAL",
                "write intent does not exist",
                details={"intent_id": intent_id},
            )
        return row

    def abort_write_intent(
        self,
        intent_id: str,
        error: SloError,
        *,
        source_path: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            row = self._intent_row(intent_id)
            if row["status"] != "pending":
                return {"intent_id": intent_id, "status": row["status"], "report_written": False}
            now = self.now_ms()
            payload = json.loads(row["payload_json"] or "{}")
            report: dict[str, Any] | None = None
            with self._transaction():
                self.connection.execute(
                    "UPDATE intent_log SET status='aborted', error_json=?, updated_at_ms=? WHERE intent_id=?",
                    (dumps_json(error.to_dict()), now, intent_id),
                )
                if row["kind"] in {"import", "snapshot_replace"} and row["batch_id"]:
                    state = self._snapshot_state_raw()
                    # Do not retain a reference to ``error.details`` inside the
                    # report: callers attach the report back to that same details
                    # object, which would otherwise create a JSON cycle.
                    error_payload = json.loads(dumps_json(error.to_dict()["error"]))
                    report = {
                        "batch_id": row["batch_id"],
                        "kind": row["kind"],
                        "status": "ABORTED",
                        "business_code": error.code,
                        "format": payload.get("format"),
                        "format_version": payload.get("format_version"),
                        "snapshot_sequence": payload.get("snapshot_sequence"),
                        "source_path": source_path or payload.get("source_path"),
                        "created_at_ms": now,
                        "atomic": True,
                        "counts": {
                            "inserted": 0,
                            "overwritten": 0,
                            "skipped": 0,
                            "rejected": 0,
                            "accepted": 0,
                        },
                        "decisions": [],
                        "degradations": [],
                        "rounding": {},
                        "previous_state": self._state_payload(state),
                        "result_state": self._state_payload(state),
                        "error": error_payload,
                    }
                    self.connection.execute(
                        """
                        INSERT INTO import_reports
                            (batch_id, kind, status, business_code, format,
                             format_version, snapshot_sequence, source_path,
                             created_at_ms, counts_json, report_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            report["batch_id"],
                            report["kind"],
                            report["status"],
                            report["business_code"],
                            report["format"],
                            report["format_version"],
                            report["snapshot_sequence"],
                            report["source_path"],
                            report["created_at_ms"],
                            dumps_json(report["counts"]),
                            dumps_json(report),
                        ),
                    )
            return {
                "intent_id": intent_id,
                "status": "aborted",
                "report_written": report is not None,
                "report": report,
            }

    def _snapshot_state_raw(self) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM snapshot_state WHERE id=1"
        ).fetchone()
        if row is None:
            raise SloError("E_INTERNAL", "snapshot_state row is missing")
        return row

    @staticmethod
    def _state_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "snapshot_sequence": row["snapshot_sequence"],
            "format": row["format"],
            "format_version": row["format_version"],
            "generated_at_ms": row["generated_at_ms"],
            "activated_at_ms": row["activated_at_ms"],
            "record_count": row["record_count"],
        }

    def active_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._state_payload(self._snapshot_state_raw())

    def window_records(self) -> tuple[WindowRecord, ...]:
        with self._lock:
            return sorted_records(self._index.values())

    def windows_for(self, service: str, sli_type: str) -> tuple[WindowRecord, ...]:
        with self._lock:
            values = [
                record
                for record in self._index.values()
                if record.service == service and record.sli_type == sli_type
            ]
            return sorted_records(values)

    def list_import_reports(self, *, limit: int = 100, kind: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if kind:
                rows = self.connection.execute(
                    "SELECT report_json FROM import_reports WHERE kind=? "
                    "ORDER BY created_at_ms DESC, batch_id DESC LIMIT ?",
                    (kind, limit),
                ).fetchall()
            else:
                rows = self.connection.execute(
                    "SELECT report_json FROM import_reports "
                    "ORDER BY created_at_ms DESC, batch_id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [json.loads(row["report_json"]) for row in rows]

    def get_import_report(self, batch_id: str) -> dict[str, Any]:
        with self._lock:
            row = self.connection.execute(
                "SELECT report_json FROM import_reports WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if row is None:
                raise SloError(
                    "E_NOT_FOUND",
                    "import report not found",
                    details={"batch_id": batch_id},
                )
            return json.loads(row["report_json"])

    def list_recovery_reports(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT * FROM recovery_reports ORDER BY report_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [
                {
                    "report_id": row["report_id"],
                    "intent_id": row["intent_id"],
                    "batch_id": row["batch_id"],
                    "action": row["action"],
                    "details": json.loads(row["details_json"]),
                    "created_at_ms": row["created_at_ms"],
                }
                for row in rows
            ]

    def list_intents(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT * FROM intent_log ORDER BY created_at_ms DESC, intent_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [
                {
                    "intent_id": row["intent_id"],
                    "kind": row["kind"],
                    "batch_id": row["batch_id"],
                    "status": row["status"],
                    "payload": json.loads(row["payload_json"]),
                    "error": json.loads(row["error_json"]) if row["error_json"] else None,
                    "created_at_ms": row["created_at_ms"],
                    "updated_at_ms": row["updated_at_ms"],
                }
                for row in rows
            ]

    def list_alerts(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT * FROM alerts ORDER BY created_at_ms DESC, alert_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [
                {
                    "alert_id": row["alert_id"],
                    "snapshot_sequence": row["snapshot_sequence"],
                    "service": row["service"],
                    "sli_type": row["sli_type"],
                    "as_of_ms": row["as_of_ms"],
                    "grade": row["grade"],
                    "result": json.loads(row["result_json"]),
                    "created_at_ms": row["created_at_ms"],
                }
                for row in rows
            ]

    def state_summary(self) -> dict[str, Any]:
        with self._lock:
            state = self._state_payload(self._snapshot_state_raw())
            versions = self.connection.execute(
                "SELECT * FROM snapshot_versions ORDER BY snapshot_sequence DESC LIMIT 20"
            ).fetchall()
            return {
                "active": state,
                "window_count": len(self._index),
                "recovered_intents_at_startup": self._recovered_intents,
                "recent_snapshot_versions": [
                    {
                        "snapshot_sequence": row["snapshot_sequence"],
                        "format": row["format"],
                        "format_version": row["format_version"],
                        "generated_at_ms": row["generated_at_ms"],
                        "loaded_at_ms": row["loaded_at_ms"],
                        "record_count": row["record_count"],
                        "checksum_sha256": row["checksum_sha256"],
                        "status": row["status"],
                    }
                    for row in versions
                ],
            }

    @staticmethod
    def _same_business_record(left: WindowRecord, right: WindowRecord) -> bool:
        return (
            left.window_length_ms == right.window_length_ms
            and left.good_events == right.good_events
            and left.total_events == right.total_events
            and left.late_count == right.late_count
        )

    def resolve_conflict(self, incoming: WindowRecord, existing: WindowRecord) -> tuple[str, str]:
        """Return ``(OVERWRITE|SKIP|REJECT, reason)``."""

        if incoming.format_version > existing.format_version:
            return "OVERWRITE", "higher_format_version"
        if incoming.format_version < existing.format_version:
            return "SKIP", "lower_format_version"
        if incoming.late_count > existing.late_count:
            return "OVERWRITE", "higher_late_count"
        if incoming.late_count < existing.late_count:
            return "SKIP", "lower_late_count"
        if incoming.last_write_ms > existing.last_write_ms:
            return "OVERWRITE", "later_last_write_ms"
        if incoming.last_write_ms < existing.last_write_ms:
            return "SKIP", "earlier_last_write_ms"
        if self._same_business_record(incoming, existing):
            return "SKIP", "idempotent_equal_record"
        return "REJECT", "all_tie_breakers_equal_but_record_differs"

    def apply_import(self, intent_id: str, parsed: ParsedBatch, *, source_path: str) -> dict[str, Any]:
        """Atomically merge a parsed batch, or leave window state untouched."""

        with self._lock:
            intent = self._intent_row(intent_id)
            if intent["status"] != "pending" or intent["kind"] != "import":
                raise SloError(
                    "E_INTERNAL",
                    "import requires a pending import intent",
                    details={"intent_id": intent_id, "status": intent["status"]},
                )
            batch_id = intent["batch_id"] or uuid.uuid4().hex
            now = self.now_ms()
            current_state = self._snapshot_state_raw()
            previous_state = self._state_payload(current_state)
            target_sequence = current_state["snapshot_sequence"]
            seed_state = target_sequence == 0
            if seed_state:
                target_sequence = parsed.manifest.snapshot_sequence or 1

            decisions: list[dict[str, Any]] = []
            planned: dict[tuple[str, str, int], WindowRecord] = {}
            counts = {"inserted": 0, "overwritten": 0, "skipped": 0, "rejected": 0}
            for incoming in parsed.records:
                existing = self._index.get(incoming.key)
                if existing is None:
                    normalized = dataclasses.replace(
                        incoming,
                        last_write_ms=incoming.last_write_ms or parsed.manifest.generated_at_ms,
                        snapshot_sequence=target_sequence,
                    )
                    action = "INSERT"
                    reason = "no_existing_record"
                    counts["inserted"] += 1
                    planned[incoming.key] = normalized
                    decision = {
                        "action": action,
                        "reason": reason,
                        "key": list(incoming.key),
                        "incoming": incoming.persisted_dict(),
                        "existing": None,
                        "applied_record": normalized.persisted_dict(),
                    }
                else:
                    action, reason = self.resolve_conflict(incoming, existing)
                    if action == "OVERWRITE":
                        normalized = dataclasses.replace(
                            incoming,
                            last_write_ms=incoming.last_write_ms or parsed.manifest.generated_at_ms,
                            snapshot_sequence=target_sequence,
                        )
                        counts["overwritten"] += 1
                        planned[incoming.key] = normalized
                        applied_record = normalized.persisted_dict()
                    elif action == "SKIP":
                        counts["skipped"] += 1
                        applied_record = existing.persisted_dict()
                    else:
                        counts["rejected"] += 1
                        applied_record = None
                    decision = {
                        "action": action,
                        "reason": reason,
                        "key": list(incoming.key),
                        "incoming": incoming.persisted_dict(),
                        "existing": existing.persisted_dict(),
                        "applied_record": applied_record,
                        "tie_break": {
                            "format_version": {
                                "incoming": incoming.format_version,
                                "existing": existing.format_version,
                            },
                            "late_count": {
                                "incoming": incoming.late_count,
                                "existing": existing.late_count,
                            },
                            "last_write_ms": {
                                "incoming": incoming.last_write_ms,
                                "existing": existing.last_write_ms,
                            },
                        },
                    }
                decisions.append(decision)

            counts["accepted"] = counts["inserted"] + counts["overwritten"]
            rolled_back = counts["rejected"] > 0
            status = "ROLLED_BACK" if rolled_back else "APPLIED"
            business_code = "E_IMPORT_CONFLICT" if rolled_back else None

            new_index = dict(self._index)
            if not rolled_back:
                for key, record in planned.items():
                    new_index[key] = record
            result_state = {
                "snapshot_sequence": (
                    target_sequence if not rolled_back else current_state["snapshot_sequence"]
                ),
                "record_count": (
                    len(new_index) if not rolled_back else previous_state["record_count"]
                ),
            }
            report: dict[str, Any] = {
                "batch_id": batch_id,
                "kind": "import",
                "status": status,
                "business_code": business_code,
                "format": parsed.manifest.file_format,
                "format_version": parsed.manifest.format_version,
                "snapshot_sequence": parsed.manifest.snapshot_sequence,
                "source_path": source_path,
                "created_at_ms": now,
                "atomic": True,
                "counts": counts,
                "decisions": decisions,
                "degradations": list(parsed.degradations),
                "negotiation": parsed.negotiation.to_dict(),
                "rounding": {
                    "cumulative_error_us": parsed.cumulative_rounding_error_us,
                    "records": [item.to_dict() for item in parsed.rounding],
                },
                "previous_state": previous_state,
                "result_state": result_state,
                "error": (
                    {
                        "code": "E_IMPORT_CONFLICT",
                        "message": "one or more records could not be resolved deterministically",
                    }
                    if rolled_back
                    else None
                ),
            }

            with self._transaction():
                if not rolled_back:
                    for decision in decisions:
                        if decision["action"] not in {"INSERT", "OVERWRITE"}:
                            continue
                        record = planned[tuple(decision["key"])]
                        self.connection.execute(
                            """
                            INSERT INTO windows
                                (service, sli_type, window_start_ms, window_length_ms,
                                 good_events, total_events, late_count, format_version,
                                 last_write_ms, snapshot_sequence, source_format,
                                 import_batch_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(service, sli_type, window_start_ms) DO UPDATE SET
                                window_length_ms=excluded.window_length_ms,
                                good_events=excluded.good_events,
                                total_events=excluded.total_events,
                                late_count=excluded.late_count,
                                format_version=excluded.format_version,
                                last_write_ms=excluded.last_write_ms,
                                snapshot_sequence=excluded.snapshot_sequence,
                                source_format=excluded.source_format,
                                import_batch_id=excluded.import_batch_id
                            """,
                            (
                                record.service,
                                record.sli_type,
                                record.window_start_ms,
                                record.window_length_ms,
                                record.good_events,
                                record.total_events,
                                record.late_count,
                                record.format_version,
                                record.last_write_ms,
                                record.snapshot_sequence,
                                record.source_format,
                                batch_id,
                            ),
                        )
                    if seed_state:
                        self.connection.execute(
                            """
                            UPDATE snapshot_state
                               SET snapshot_sequence=?, format=?, format_version=?,
                                   generated_at_ms=?, activated_at_ms=?, record_count=?
                             WHERE id=1
                            """,
                            (
                                target_sequence,
                                parsed.manifest.file_format,
                                parsed.manifest.format_version,
                                parsed.manifest.generated_at_ms,
                                now,
                                len(new_index),
                            ),
                        )
                        self.connection.execute(
                            """
                            INSERT OR REPLACE INTO snapshot_versions
                                (snapshot_sequence, format, format_version,
                                 generated_at_ms, loaded_at_ms, record_count,
                                 checksum_sha256, status)
                            VALUES (?, ?, ?, ?, ?, ?, ?, 'import_seed')
                            """,
                            (
                                target_sequence,
                                parsed.manifest.file_format,
                                parsed.manifest.format_version,
                                parsed.manifest.generated_at_ms,
                                now,
                                len(new_index),
                                parsed.checksum_sha256,
                            ),
                        )
                    else:
                        self.connection.execute(
                            "UPDATE snapshot_state SET record_count=? WHERE id=1",
                            (len(new_index),),
                        )
                self.connection.execute(
                    """
                    INSERT INTO import_reports
                        (batch_id, kind, status, business_code, format,
                         format_version, snapshot_sequence, source_path,
                         created_at_ms, counts_json, report_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        report["batch_id"],
                        report["kind"],
                        report["status"],
                        report["business_code"],
                        report["format"],
                        report["format_version"],
                        report["snapshot_sequence"],
                        report["source_path"],
                        report["created_at_ms"],
                        dumps_json(report["counts"]),
                        dumps_json(report),
                    ),
                )
                self.connection.execute(
                    "UPDATE intent_log SET status=?, updated_at_ms=? WHERE intent_id=?",
                    ("rolled_back" if rolled_back else "committed", now, intent_id),
                )

            if not rolled_back:
                self._index = new_index
                self._derived_cache.clear()
            return report

    def apply_snapshot_replace(
        self,
        intent_id: str,
        parsed: ParsedBatch,
        *,
        source_path: str,
    ) -> dict[str, Any]:
        with self._lock:
            intent = self._intent_row(intent_id)
            if intent["status"] != "pending" or intent["kind"] != "snapshot_replace":
                raise SloError(
                    "E_INTERNAL",
                    "snapshot replacement requires a pending snapshot_replace intent",
                    details={"intent_id": intent_id, "status": intent["status"]},
                )
            batch_id = intent["batch_id"] or uuid.uuid4().hex
            now = self.now_ms()
            current_state = self._snapshot_state_raw()
            current_sequence = current_state["snapshot_sequence"]
            if parsed.manifest.snapshot_sequence <= current_sequence:
                error = SloError(
                    "E_SNAPSHOT_INVALID",
                    "snapshot sequence must be strictly increasing",
                    path=source_path,
                    details={
                        "incoming_sequence": parsed.manifest.snapshot_sequence,
                        "active_sequence": current_sequence,
                        "old_snapshot_preserved": True,
                    },
                )
                self.abort_write_intent(intent_id, error, source_path=source_path)
                raise error

            new_records = [
                dataclasses.replace(record, snapshot_sequence=parsed.manifest.snapshot_sequence)
                for record in parsed.records
            ]
            new_index = {record.key: record for record in new_records}
            previous_state = self._state_payload(current_state)
            report: dict[str, Any] = {
                "batch_id": batch_id,
                "kind": "snapshot_replace",
                "status": "APPLIED",
                "business_code": None,
                "format": parsed.manifest.file_format,
                "format_version": parsed.manifest.format_version,
                "snapshot_sequence": parsed.manifest.snapshot_sequence,
                "source_path": source_path,
                "created_at_ms": now,
                "atomic": True,
                "counts": {
                    "inserted": len(new_records),
                    "overwritten": 0,
                    "skipped": 0,
                    "rejected": 0,
                    "accepted": len(new_records),
                },
                "decisions": [],
                "degradations": list(parsed.degradations),
                "negotiation": parsed.negotiation.to_dict(),
                "rounding": {
                    "cumulative_error_us": parsed.cumulative_rounding_error_us,
                    "records": [item.to_dict() for item in parsed.rounding],
                },
                "previous_state": previous_state,
                "result_state": {
                    "snapshot_sequence": parsed.manifest.snapshot_sequence,
                    "record_count": len(new_records),
                },
                "error": None,
            }

            with self._transaction():
                current_seq_tx = self.connection.execute(
                    "SELECT snapshot_sequence FROM snapshot_state WHERE id=1"
                ).fetchone()["snapshot_sequence"]
                if parsed.manifest.snapshot_sequence <= current_seq_tx:
                    raise SloError(
                        "E_SNAPSHOT_INVALID",
                        "snapshot sequence became non-monotonic",
                        path=source_path,
                        details={
                            "incoming_sequence": parsed.manifest.snapshot_sequence,
                            "active_sequence": current_seq_tx,
                        },
                    )
                self.connection.execute(
                    "UPDATE snapshot_versions SET status='retired' WHERE status='active'"
                )
                self.connection.execute(
                    """
                    INSERT INTO snapshot_versions
                        (snapshot_sequence, format, format_version, generated_at_ms,
                         loaded_at_ms, record_count, checksum_sha256, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'active')
                    """,
                    (
                        parsed.manifest.snapshot_sequence,
                        parsed.manifest.file_format,
                        parsed.manifest.format_version,
                        parsed.manifest.generated_at_ms,
                        now,
                        len(new_records),
                        parsed.checksum_sha256,
                    ),
                )
                self.connection.execute("DELETE FROM windows")
                for record in new_records:
                    self.connection.execute(
                        """
                        INSERT INTO windows
                            (service, sli_type, window_start_ms, window_length_ms,
                             good_events, total_events, late_count, format_version,
                             last_write_ms, snapshot_sequence, source_format,
                             import_batch_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record.service,
                            record.sli_type,
                            record.window_start_ms,
                            record.window_length_ms,
                            record.good_events,
                            record.total_events,
                            record.late_count,
                            record.format_version,
                            record.last_write_ms,
                            record.snapshot_sequence,
                            record.source_format,
                            batch_id,
                        ),
                    )
                self.connection.execute(
                    """
                    UPDATE snapshot_state
                       SET snapshot_sequence=?, format=?, format_version=?,
                           generated_at_ms=?, activated_at_ms=?, record_count=?
                     WHERE id=1
                    """,
                    (
                        parsed.manifest.snapshot_sequence,
                        parsed.manifest.file_format,
                        parsed.manifest.format_version,
                        parsed.manifest.generated_at_ms,
                        now,
                        len(new_records),
                    ),
                )
                self.connection.execute(
                    """
                    INSERT INTO import_reports
                        (batch_id, kind, status, business_code, format,
                         format_version, snapshot_sequence, source_path,
                         created_at_ms, counts_json, report_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        report["batch_id"],
                        report["kind"],
                        report["status"],
                        report["business_code"],
                        report["format"],
                        report["format_version"],
                        report["snapshot_sequence"],
                        report["source_path"],
                        report["created_at_ms"],
                        dumps_json(report["counts"]),
                        dumps_json(report),
                    ),
                )
                self.connection.execute(
                    "UPDATE intent_log SET status='committed', updated_at_ms=? WHERE intent_id=?",
                    (now, intent_id),
                )

            # The lock remains held across the commit and memory swap. Readers
            # never observe a new SQLite snapshot with the old index.
            self._index = new_index
            self._derived_cache.clear()
            return report

    def persist_alert(
        self,
        intent_id: str,
        *,
        snapshot_sequence: int,
        service: str,
        sli_type: str,
        as_of_ms: int,
        grade: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        with self._lock:
            intent = self._intent_row(intent_id)
            if intent["status"] != "pending" or intent["kind"] != "alert":
                raise SloError(
                    "E_INTERNAL",
                    "alert persistence requires a pending alert intent",
                    details={"intent_id": intent_id, "status": intent["status"]},
                )
            now = self.now_ms()
            with self._transaction():
                self.connection.execute(
                    """
                    INSERT INTO alerts
                        (snapshot_sequence, service, sli_type, as_of_ms,
                         grade, result_json, created_at_ms)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(snapshot_sequence, service, sli_type, as_of_ms)
                    DO UPDATE SET
                        grade=excluded.grade,
                        result_json=excluded.result_json,
                        created_at_ms=excluded.created_at_ms
                    WHERE excluded.created_at_ms >= alerts.created_at_ms
                    """,
                    (
                        snapshot_sequence,
                        service,
                        sli_type,
                        as_of_ms,
                        grade,
                        dumps_json(result),
                        now,
                    ),
                )
                self.connection.execute(
                    "UPDATE intent_log SET status='committed', updated_at_ms=? WHERE intent_id=?",
                    (now, intent_id),
                )
            return {
                "snapshot_sequence": snapshot_sequence,
                "service": service,
                "sli_type": sli_type,
                "as_of_ms": as_of_ms,
                "grade": grade,
                "created_at_ms": now,
            }

    def export_material(self, file_format: str) -> tuple[tuple[WindowRecord, ...], SnapshotManifest]:
        with self._lock:
            state = self._snapshot_state_raw()
            sequence = state["snapshot_sequence"]
            generated = state["generated_at_ms"] or self.now_ms()
            records = sorted_records(self._index.values())
            manifest = SnapshotManifest(
                file_format=file_format,
                format_version=2,
                snapshot_sequence=sequence,
                generated_at_ms=generated,
                record_count=len(records),
            )
            return records, manifest
