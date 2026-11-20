"""Versioned capacity ledger orchestration.

Commit protocol:
1. validate the complete request and acquire the cross-process writer lock;
2. calculate the candidate ledger and reject budget violations;
3. persist a PREPARED intent in its own transaction and fsync the intent log;
4. invalidate and rebuild artifacts, write the ledger, commit record and mark
   the intent COMMITTED in one SQLite transaction.

A forced termination after step 3 leaves only an abortable intent; one before
step 3 has no ledger side effects at all.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .artifacts import (
    all_artifacts_full,
    build_artifact_values,
    build_catalog,
    direct_changes,
    propagate_invalidations,
    rebuild_scope,
    weighted_denominator_changed,
)
from .checksum import canonical_json, sha256_value
from .domain import calculate_metrics
from .errors import (
    CapacityError,
    conflicting_commit,
    ledger_corrupt,
    stale_base,
)
from .models import CapacityIncrement, CapacitySnapshot
from .storage import SQLiteStore


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "runtime" / "capacity_ledger.sqlite3"
DEFAULT_INTENT_DIR = PROJECT_ROOT / "runtime" / "intents"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _as_json(text: str) -> Any:
    return json.loads(text)


class CapacityLedger:
    def __init__(self, db_path: str | Path | None = None, intent_dir: str | Path | None = None):
        resolved_db = Path(
            db_path or os.environ.get("CAPACITY_DB_PATH", str(DEFAULT_DB_PATH))
        )
        resolved_intents = Path(
            intent_dir or os.environ.get("CAPACITY_INTENT_DIR", str(DEFAULT_INTENT_DIR))
        )
        self.store = SQLiteStore(resolved_db, resolved_intents)

    def _latest_row(self, connection: sqlite3.Connection) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM ledger_versions ORDER BY version DESC LIMIT 1"
        ).fetchone()

    def _row_version(self, connection: sqlite3.Connection, version: int) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM ledger_versions WHERE version = ?", (version,)
        ).fetchone()

    @staticmethod
    def _row_snapshot(row: sqlite3.Row) -> CapacitySnapshot:
        return CapacitySnapshot.from_payload(_as_json(row["snapshot_json"]))

    @staticmethod
    def _row_metrics(row: sqlite3.Row) -> dict[str, Any]:
        return _as_json(row["metrics_json"])

    def get_current(self) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            self._verify_integrity(connection)
            row = self._latest_row(connection)
            if row is None:
                return {
                    "ok": True,
                    "state": "EMPTY",
                    "version": 0,
                    "latest_complete_version": 0,
                }
            return self._query_response(row)

    def get_version(self, version: int) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            self._verify_integrity(connection)
            row = self._row_version(connection, version)
            if row is None:
                raise CapacityError(
                    "E_VERSION_NOT_FOUND",
                    "The requested committed version does not exist.",
                    details={"version": version},
                    http_status=404,
                )
            return self._query_response(row)

    def _query_response(self, row: sqlite3.Row) -> dict[str, Any]:
        result = _as_json(row["result_json"])
        result = dict(result)
        result["snapshot"] = _as_json(row["snapshot_json"])
        result["checksums"] = {
            "snapshot_checksum": row["snapshot_checksum"],
            "metrics_checksum": row["metrics_checksum"],
            "result_checksum": row["result_checksum"],
            "ledger_checksum": row["ledger_checksum"],
        }
        result["state"] = "CURRENT"
        return result

    def get_dependencies(self) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM artifact_catalog ORDER BY artifact_id"
            ).fetchall()
            artifacts = [
                {
                    "artifact_id": row["artifact_id"],
                    "artifact_kind": row["artifact_kind"],
                    "service_name": row["service_name"],
                    "dependencies": _as_json(row["dependencies_json"]),
                }
                for row in rows
            ]
            return {"ok": True, "artifact_count": len(artifacts), "artifacts": artifacts}

    def list_intents(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.store.read_transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM intents ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [
                {
                    "intent_id": row["intent_id"],
                    "commit_id": row["commit_id"],
                    "kind": row["kind"],
                    "base_version": row["base_version"],
                    "target_version": row["target_version"],
                    "state": row["state"],
                    "failure_code": row["failure_code"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]

    def commit_snapshot(self, payload: Any) -> dict[str, Any]:
        snapshot = CapacitySnapshot.from_payload(payload)
        digest = sha256_value({"kind": "snapshot-commit", "payload": payload})
        with self.store.writer_lock:
            with self.store.read_transaction() as connection:
                current_row = self._latest_row(connection)
                existing = self._lookup_commit(connection, snapshot.commit_id, digest)
                if existing is not None:
                    return self._resolve_existing_commit(existing)
                base_version = current_row["version"] if current_row is not None else 0
                expected_version = base_version + 1
                if snapshot.version != expected_version:
                    raise CapacityError(
                        "E_STALE_BASE",
                        "A full snapshot must advance exactly one version from the committed ledger.",
                        details={
                            "current_version": base_version,
                            "expected_version": expected_version,
                            "supplied_version": snapshot.version,
                            "persisted_state_changed": False,
                        },
                        http_status=409,
                    )
                previous_snapshot = (
                    self._row_snapshot(current_row) if current_row is not None else None
                )
                previous_metrics = (
                    self._row_metrics(current_row) if current_row is not None else None
                )

            changed: dict[str, tuple[str, ...]] = {}
            if previous_snapshot is not None:
                for name in sorted(snapshot.services):
                    old = previous_snapshot.services.get(name)
                    if old is None:
                        changed[name] = ("SERVICE_ADDED",)
                    else:
                        fields = snapshot.services[name].changed_fields(old)
                        if fields:
                            changed[name] = fields
                for name in sorted(set(previous_snapshot.services) - set(snapshot.services)):
                    changed[name] = ("SERVICE_REMOVED",)

            metrics = calculate_metrics(
                snapshot,
                prior_metrics=previous_metrics,
                changed_services=set(snapshot.services),
                force_full=True,
            )
            if metrics["verdict"]["violations"]:
                self._reject_budget(snapshot.commit_id, digest, snapshot.version, metrics)

            reason = (
                "Initial full snapshot." if current_row is None else "Explicit full snapshot submission."
            )
            catalog = build_catalog(sorted(snapshot.services))
            events = all_artifacts_full(catalog, "FULL_SNAPSHOT")
            scope = rebuild_scope(
                mode="full",
                reason=reason,
                catalog=catalog,
                events=events,
                changed=changed,
            )
            return self._persist_commit(
                intent_kind="snapshot",
                raw_payload=payload,
                input_checksum=snapshot.checksum,
                snapshot=snapshot,
                base_version=base_version,
                mode="full",
                mode_reason=reason,
                metrics=metrics,
                catalog=catalog,
                invalidations=events,
                scope=scope,
                request_digest=digest,
                replace_catalog=True,
            )

    def commit_increment(
        self,
        payload: Any,
        *,
        crash_point: str | None = None,
    ) -> dict[str, Any]:
        increment = CapacityIncrement.from_payload(payload)
        digest = sha256_value({"kind": "increment-commit", "payload": payload})
        with self.store.writer_lock:
            with self.store.read_transaction() as connection:
                current_row = self._latest_row(connection)
                existing = self._lookup_commit(connection, increment.commit_id, digest)
                if existing is not None:
                    return self._resolve_existing_commit(existing)
                base_version = current_row["version"] if current_row is not None else 0
                if increment.base_version != base_version:
                    raise stale_base(base_version, increment.base_version)
                current_snapshot = self._row_snapshot(current_row)
                current_metrics = self._row_metrics(current_row)

            changed = increment.changed_fields(current_snapshot)
            full_required = weighted_denominator_changed(changed)
            services = dict(current_snapshot.services)
            services.update(increment.services)
            candidate = current_snapshot.with_services(
                services,
                version=increment.version,
                generated_at=increment.generated_at,
                commit_id=increment.commit_id,
            )
            metrics = calculate_metrics(
                candidate,
                prior_metrics=current_metrics,
                changed_services=set(changed),
                force_full=full_required,
            )
            if metrics["verdict"]["violations"]:
                self._reject_budget(increment.commit_id, digest, increment.version, metrics)

            catalog = build_catalog(sorted(candidate.services))
            if full_required:
                mode = "full"
                mode_reason = (
                    "SLO weight changed the weighted-cost normalization denominator; "
                    "the global artifact set must be rebuilt."
                )
                events = all_artifacts_full(catalog, "GLOBAL_NORMALIZATION_BASE_CHANGED")
            else:
                mode = "incremental"
                mode_reason = (
                    "Only service-local leaves and dependency-derived aggregates changed; "
                    "the SLO normalization denominator is unchanged."
                )
                direct = direct_changes(changed)
                events = propagate_invalidations(catalog, direct)
            scope = rebuild_scope(
                mode=mode,
                reason=mode_reason,
                catalog=catalog,
                events=events,
                changed=changed,
            )
            return self._persist_commit(
                intent_kind="increment",
                raw_payload=payload,
                input_checksum=increment.checksum,
                snapshot=candidate,
                base_version=increment.base_version,
                mode=mode,
                mode_reason=mode_reason,
                metrics=metrics,
                catalog=catalog,
                invalidations=events,
                scope=scope,
                request_digest=digest,
                replace_catalog=False,
                crash_point=crash_point,
            )

    def _lookup_commit(
        self, connection: sqlite3.Connection, commit_id: str, request_digest: str
    ) -> sqlite3.Row | None:
        row = connection.execute(
            "SELECT * FROM commits WHERE commit_id = ?", (commit_id,)
        ).fetchone()
        if row is None:
            return None
        if row["request_digest"] != request_digest:
            raise conflicting_commit(commit_id)
        return row

    @staticmethod
    def _resolve_existing_commit(row: sqlite3.Row) -> dict[str, Any]:
        if row["status"] == "COMMITTED":
            return _as_json(row["response_json"])
        error_payload = _as_json(row["response_json"])["error"]
        raise CapacityError(
            error_payload["code"],
            error_payload["message"],
            details=error_payload.get("details", {}),
            http_status=int(row["http_status"]),
        )

    def _reject_budget(
        self,
        commit_id: str,
        request_digest: str,
        candidate_version: int,
        metrics: dict[str, Any],
    ) -> None:
        verdict = metrics["verdict"]
        error = CapacityError(
            "E_BUDGET_EXCEEDED",
            "The candidate capacity snapshot violates one or more budget constraints.",
            details={
                "candidate_version": candidate_version,
                "primary_constraint": verdict["primary_constraint"],
                "violations": verdict["violations"],
                "within_tolerance": verdict["within_tolerance"],
                "metrics": metrics,
                "persisted_state_changed": False,
            },
            http_status=409,
        )
        now = utc_now()
        payload = error.to_payload()
        with self.store.write_transaction() as connection:
            existing = self._lookup_commit(connection, commit_id, request_digest)
            if existing is not None:
                self._resolve_existing_commit(existing)
            connection.execute(
                """
                INSERT INTO commits (
                    commit_id, request_digest, kind, base_version, target_version,
                    status, http_status, response_json, created_at, updated_at
                ) VALUES (?, ?, 'rejected', ?, ?, 'REJECTED', ?, ?, ?, ?)
                """,
                (
                    commit_id,
                    request_digest,
                    candidate_version - 1,
                    candidate_version,
                    error.http_status,
                    canonical_json(payload),
                    now,
                    now,
                ),
            )
        raise error

    def _persist_commit(
        self,
        *,
        intent_kind: str,
        raw_payload: dict[str, Any],
        input_checksum: str,
        snapshot: CapacitySnapshot,
        base_version: int,
        mode: str,
        mode_reason: str,
        metrics: dict[str, Any],
        catalog: dict[str, dict[str, Any]],
        invalidations: list[dict[str, Any]],
        scope: dict[str, Any],
        request_digest: str,
        replace_catalog: bool,
        crash_point: str | None = None,
    ) -> dict[str, Any]:
        intent_id = f"intent-{uuid.uuid4().hex}"
        created_at = utc_now()
        intent_event = {
            "event": "PREPARED",
            "intent_id": intent_id,
            "commit_id": snapshot.commit_id,
            "kind": intent_kind,
            "base_version": base_version,
            "target_version": snapshot.version,
            "created_at": created_at,
        }
        with self.store.write_transaction() as connection:
            connection.execute(
                """
                INSERT INTO intents (
                    intent_id, commit_id, kind, base_version, target_version,
                    state, payload_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'PREPARED', ?, ?, ?)
                """,
                (
                    intent_id,
                    snapshot.commit_id,
                    intent_kind,
                    base_version,
                    snapshot.version,
                    canonical_json(raw_payload),
                    created_at,
                    created_at,
                ),
            )
        self.store.append_intent_log(intent_event)

        artifact_values = build_artifact_values(snapshot, metrics)
        metrics_json = canonical_json(metrics)
        metrics_checksum = sha256_value(metrics)
        previous_row: sqlite3.Row | None
        with self.store.read_transaction() as connection:
            previous_row = self._latest_row(connection)
        previous_version = previous_row["version"] if previous_row is not None else 0
        previous_ledger_checksum = (
            previous_row["ledger_checksum"] if previous_row is not None else ""
        )
        if previous_version != base_version:
            self._abort_intent(intent_id, "E_STALE_BASE")
            raise stale_base(base_version, previous_version)

        ledger_checksum = sha256_value(
            {
                "previous_ledger_checksum": previous_ledger_checksum,
                "version": snapshot.version,
                "snapshot_checksum": snapshot.checksum,
                "metrics_checksum": metrics_checksum,
            }
        )
        result: dict[str, Any] = {
            "ok": True,
            "commit_id": snapshot.commit_id,
            "version": snapshot.version,
            "base_version": base_version,
            "mode": mode,
            "mode_reason": mode_reason,
            "input_checksum": input_checksum,
            "snapshot_checksum": snapshot.checksum,
            "metrics_checksum": metrics_checksum,
            "ledger_checksum": ledger_checksum,
            "snapshot_generated_at": snapshot.generated_at,
            "committed_at": created_at,
            "metrics": metrics,
            "verdict": metrics["verdict"],
            "rebuild_scope": scope,
            "precision_contract": "integer_and_explicit_fixed_point_only",
        }
        result_checksum = sha256_value(result)
        result["result_checksum"] = result_checksum
        result_json = canonical_json(result)
        snapshot_json = canonical_json(snapshot.to_payload())

        with self.store.write_transaction() as connection:
            locked_latest = self._latest_row(connection)
            locked_version = locked_latest["version"] if locked_latest is not None else 0
            if locked_version != base_version:
                raise stale_base(base_version, locked_version)

            if replace_catalog:
                connection.execute("DELETE FROM artifact_current")
                connection.execute("DELETE FROM artifact_catalog")

            for artifact_id, record in sorted(catalog.items()):
                connection.execute(
                    """
                    INSERT INTO artifact_catalog (
                        artifact_id, artifact_kind, service_name, dependencies_json
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(artifact_id) DO UPDATE SET
                        artifact_kind = excluded.artifact_kind,
                        service_name = excluded.service_name,
                        dependencies_json = excluded.dependencies_json
                    """,
                    (
                        artifact_id,
                        record["artifact_kind"],
                        record["service_name"],
                        canonical_json(record["dependencies"]),
                    ),
                )

            for event in invalidations:
                connection.execute(
                    """
                    INSERT INTO invalidations (
                        version, artifact_id, wave, reason, cause
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.version,
                        event["artifact_id"],
                        event["wave"],
                        event["reason"],
                        event["cause"],
                    ),
                )

            crash_after = max(1, len(invalidations) // 2)
            for position, event in enumerate(invalidations):
                artifact_id = event["artifact_id"]
                value = artifact_values[artifact_id]
                value_json = canonical_json(value)
                value_checksum = sha256_value(value)
                connection.execute(
                    """
                    INSERT INTO artifact_current (
                        artifact_id, origin_version, value_json, value_checksum
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(artifact_id) DO UPDATE SET
                        origin_version = excluded.origin_version,
                        value_json = excluded.value_json,
                        value_checksum = excluded.value_checksum
                    """,
                    (artifact_id, snapshot.version, value_json, value_checksum),
                )
                connection.execute(
                    """
                    INSERT INTO artifact_versions (
                        version, artifact_id, value_json, value_checksum, event
                    ) VALUES (?, ?, ?, ?, 'REBUILT')
                    """,
                    (snapshot.version, artifact_id, value_json, value_checksum),
                )
                if crash_point == "after_partial_rebuild" and position + 1 >= crash_after:
                    os._exit(86)

            connection.execute(
                """
                INSERT INTO ledger_versions (
                    version, commit_id, base_version, mode, mode_reason,
                    snapshot_checksum, snapshot_json, metrics_checksum, metrics_json,
                    result_checksum, result_json, previous_ledger_checksum,
                    ledger_checksum, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.version,
                    snapshot.commit_id,
                    base_version,
                    mode,
                    mode_reason,
                    snapshot.checksum,
                    snapshot_json,
                    metrics_checksum,
                    metrics_json,
                    result_checksum,
                    result_json,
                    previous_ledger_checksum,
                    ledger_checksum,
                    created_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO commits (
                    commit_id, request_digest, kind, base_version, target_version,
                    status, http_status, response_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'COMMITTED', 200, ?, ?, ?)
                """,
                (
                    snapshot.commit_id,
                    request_digest,
                    intent_kind,
                    base_version,
                    snapshot.version,
                    result_json,
                    created_at,
                    created_at,
                ),
            )
            updated = connection.execute(
                """
                UPDATE intents
                SET state = 'COMMITTED', updated_at = ?
                WHERE intent_id = ? AND state = 'PREPARED'
                """,
                (utc_now(), intent_id),
            )
            if updated.rowcount != 1:
                raise CapacityError(
                    "E_LEDGER_CORRUPT",
                    "The prepared intent disappeared before commit.",
                    details={"intent_id": intent_id},
                    http_status=500,
                )

        try:
            self.store.append_intent_log(
                {
                    "event": "COMMITTED",
                    "intent_id": intent_id,
                    "commit_id": snapshot.commit_id,
                    "version": snapshot.version,
                    "committed_at": utc_now(),
                }
            )
        except OSError:
            pass
        return result

    def _abort_intent(self, intent_id: str, failure_code: str) -> None:
        with self.store.write_transaction() as connection:
            connection.execute(
                """
                UPDATE intents
                SET state = 'ABORTED', failure_code = ?, updated_at = ?
                WHERE intent_id = ? AND state = 'PREPARED'
                """,
                (failure_code, utc_now(), intent_id),
            )

    def recover(self) -> dict[str, Any]:
        aborted_log_events: list[dict[str, Any]] = []
        with self.store.writer_lock:
            with self.store.write_transaction() as connection:
                self._verify_integrity(connection)
                before_row = self._latest_row(connection)
                before = self._version_summary(before_row)
                prepared = connection.execute(
                    "SELECT * FROM intents WHERE state = 'PREPARED' ORDER BY created_at, intent_id"
                ).fetchall()
                reconciled_committed: list[str] = []
                aborted: list[dict[str, Any]] = []
                for intent in prepared:
                    committed_row = connection.execute(
                        "SELECT 1 FROM ledger_versions WHERE version = ? AND commit_id = ?",
                        (intent["target_version"], intent["commit_id"]),
                    ).fetchone()
                    if committed_row is not None:
                        connection.execute(
                            """
                            UPDATE intents SET state = 'COMMITTED', updated_at = ?
                            WHERE intent_id = ? AND state = 'PREPARED'
                            """,
                            (utc_now(), intent["intent_id"]),
                        )
                        reconciled_committed.append(intent["intent_id"])
                    else:
                        updated_at = utc_now()
                        connection.execute(
                            """
                            UPDATE intents
                            SET state = 'ABORTED', failure_code = 'E_PROCESS_TERMINATED',
                                updated_at = ?
                            WHERE intent_id = ? AND state = 'PREPARED'
                            """,
                            (updated_at, intent["intent_id"]),
                        )
                        record = {
                            "intent_id": intent["intent_id"],
                            "commit_id": intent["commit_id"],
                            "kind": intent["kind"],
                            "base_version": intent["base_version"],
                            "target_version": intent["target_version"],
                            "state": "ABORTED",
                            "failure_code": "E_PROCESS_TERMINATED",
                            "aborted_at": updated_at,
                        }
                        aborted.append(record)
                        aborted_log_events.append(
                            {"event": "ABORTED", **record}
                        )

                connection.execute(
                    "DELETE FROM artifact_versions WHERE version NOT IN (SELECT version FROM ledger_versions)"
                )
                connection.execute(
                    "DELETE FROM invalidations WHERE version NOT IN (SELECT version FROM ledger_versions)"
                )
                after_row = self._latest_row(connection)
                after = self._version_summary(after_row)
                self._verify_integrity(connection)

        for event in aborted_log_events:
            try:
                self.store.append_intent_log(event)
            except OSError:
                pass

        return {
            "ok": True,
            "report_basis": "PERSISTED_FACTS_ONLY_AFTER_RESTART",
            "forced_termination_exit_report_available": False,
            "limitation": (
                "A forcibly terminated process cannot emit a complete exit report. "
                "This report is reconstructed by a later process from durable intents, "
                "ledger rows and SQLite transaction state."
            ),
            "before": before,
            "after": after,
            "difference": {
                "version_changed": before["version"] != after["version"],
                "snapshot_checksum_changed": before["snapshot_checksum"]
                != after["snapshot_checksum"],
                "metrics_checksum_changed": before["metrics_checksum"]
                != after["metrics_checksum"],
                "ledger_checksum_changed": before["ledger_checksum"]
                != after["ledger_checksum"],
                "state_unchanged": before == after,
            },
            "intents_scanned": len(prepared),
            "reconciled_committed": reconciled_committed,
            "aborted_intents": aborted,
            "aborted_count": len(aborted),
            "ledger_rollback_required": False,
            "ledger_recovery_policy": "last_complete_committed_version",
            "integrity": "VALID",
        }

    @staticmethod
    def _version_summary(row: sqlite3.Row | None) -> dict[str, Any]:
        if row is None:
            return {
                "version": 0,
                "snapshot_checksum": None,
                "metrics_checksum": None,
                "ledger_checksum": None,
                "commit_id": None,
            }
        return {
            "version": row["version"],
            "snapshot_checksum": row["snapshot_checksum"],
            "metrics_checksum": row["metrics_checksum"],
            "ledger_checksum": row["ledger_checksum"],
            "commit_id": row["commit_id"],
        }

    def verify(self) -> dict[str, Any]:
        with self.store.read_transaction() as connection:
            summary = self._verify_integrity(connection)
            return {"ok": True, "status": "VALID", **summary}

    def _verify_integrity(self, connection: sqlite3.Connection) -> dict[str, Any]:
        rows = connection.execute(
            "SELECT * FROM ledger_versions ORDER BY version"
        ).fetchall()
        previous_ledger_checksum = ""
        previous_version = 0
        latest_metrics: dict[str, Any] | None = None
        latest_snapshot: CapacitySnapshot | None = None
        latest_result: dict[str, Any] | None = None
        for row in rows:
            version = row["version"]
            if version != previous_version + 1:
                raise ledger_corrupt(
                    "ledger_versions.version",
                    "version chain is not contiguous",
                    version=version,
                    previous_version=previous_version,
                )
            if row["base_version"] != previous_version:
                raise ledger_corrupt(
                    "ledger_versions.base_version",
                    "base version does not match the previous committed version",
                    version=version,
                )
            if row["previous_ledger_checksum"] != previous_ledger_checksum:
                raise ledger_corrupt(
                    "ledger_versions.previous_ledger_checksum",
                    "ledger checksum chain is broken",
                    version=version,
                )
            try:
                snapshot = CapacitySnapshot.from_payload(_as_json(row["snapshot_json"]))
            except Exception as exc:
                raise ledger_corrupt(
                    "ledger_versions.snapshot_json",
                    "stored snapshot is invalid",
                    version=version,
                ) from exc
            if snapshot.checksum != row["snapshot_checksum"]:
                raise ledger_corrupt(
                    "ledger_versions.snapshot_checksum",
                    "snapshot checksum column differs from the stored snapshot",
                    version=version,
                )
            try:
                expected_metrics = calculate_metrics(snapshot, force_full=True)
            except Exception as exc:
                raise ledger_corrupt(
                    "ledger_versions.metrics_json",
                    "metrics could not be recomputed from the stored snapshot",
                    version=version,
                ) from exc
            stored_metrics = _as_json(row["metrics_json"])
            if canonical_json(stored_metrics) != canonical_json(expected_metrics):
                raise ledger_corrupt(
                    "ledger_versions.metrics_json",
                    "stored metrics differ from exact recomputation",
                    version=version,
                )
            metrics_checksum = sha256_value(stored_metrics)
            if metrics_checksum != row["metrics_checksum"]:
                raise ledger_corrupt(
                    "ledger_versions.metrics_checksum",
                    "metrics checksum mismatch",
                    version=version,
                )
            result = _as_json(row["result_json"])
            result_without_checksum = {
                key: value for key, value in result.items() if key != "result_checksum"
            }
            if sha256_value(result_without_checksum) != row["result_checksum"]:
                raise ledger_corrupt(
                    "ledger_versions.result_checksum",
                    "result checksum mismatch",
                    version=version,
                )
            if result.get("version") != version or result.get("commit_id") != row["commit_id"]:
                raise ledger_corrupt(
                    "ledger_versions.result_json",
                    "result identity does not match the ledger row",
                    version=version,
                )
            expected_ledger_checksum = sha256_value(
                {
                    "previous_ledger_checksum": previous_ledger_checksum,
                    "version": version,
                    "snapshot_checksum": row["snapshot_checksum"],
                    "metrics_checksum": row["metrics_checksum"],
                }
            )
            if expected_ledger_checksum != row["ledger_checksum"]:
                raise ledger_corrupt(
                    "ledger_versions.ledger_checksum",
                    "ledger checksum mismatch",
                    version=version,
                )
            commit = connection.execute(
                "SELECT * FROM commits WHERE commit_id = ?", (row["commit_id"],)
            ).fetchone()
            if commit is None or commit["status"] != "COMMITTED":
                raise ledger_corrupt(
                    "commits",
                    "committed ledger version has no matching commit record",
                    version=version,
                )
            if canonical_json(_as_json(commit["response_json"])) != canonical_json(result):
                raise ledger_corrupt(
                    "commits.response_json",
                    "commit replay response differs from the ledger result",
                    version=version,
                )
            previous_ledger_checksum = row["ledger_checksum"]
            previous_version = version
            latest_metrics = stored_metrics
            latest_snapshot = snapshot
            latest_result = result

        if rows:
            assert latest_metrics is not None and latest_snapshot is not None and latest_result is not None
            expected_catalog = build_catalog(sorted(latest_snapshot.services))
            catalog_rows = connection.execute(
                "SELECT * FROM artifact_catalog ORDER BY artifact_id"
            ).fetchall()
            if {row["artifact_id"] for row in catalog_rows} != set(expected_catalog):
                raise ledger_corrupt(
                    "artifact_catalog",
                    "artifact catalog does not match the latest committed service set",
                )
            for row in catalog_rows:
                expected = expected_catalog[row["artifact_id"]]
                if (
                    row["artifact_kind"] != expected["artifact_kind"]
                    or row["service_name"] != expected["service_name"]
                    or _as_json(row["dependencies_json"]) != expected["dependencies"]
                ):
                    raise ledger_corrupt(
                        "artifact_catalog",
                        "artifact metadata mismatch",
                        artifact_id=row["artifact_id"],
                    )

            expected_values = build_artifact_values(latest_snapshot, latest_metrics)
            current_rows = connection.execute(
                "SELECT * FROM artifact_current"
            ).fetchall()
            current_by_id = {row["artifact_id"]: row for row in current_rows}
            if set(current_by_id) != set(expected_values):
                raise ledger_corrupt(
                    "artifact_current",
                    "materialized artifact set is incomplete or contains unknown entries",
                )
            latest_version = rows[-1]["version"]
            for artifact_id, value in expected_values.items():
                row = current_by_id[artifact_id]
                if row["origin_version"] > latest_version:
                    raise ledger_corrupt(
                        "artifact_current.origin_version",
                        "artifact origin version is newer than the committed ledger",
                        artifact_id=artifact_id,
                    )
                if canonical_json(_as_json(row["value_json"])) != canonical_json(value):
                    raise ledger_corrupt(
                        "artifact_current.value_json",
                        "materialized artifact value differs from exact recomputation",
                        artifact_id=artifact_id,
                    )
                if sha256_value(value) != row["value_checksum"]:
                    raise ledger_corrupt(
                        "artifact_current.value_checksum",
                        "materialized artifact checksum mismatch",
                        artifact_id=artifact_id,
                    )
            scope = latest_result["rebuild_scope"]
            invalidated = sorted(scope["invalidated_artifacts"])
            invalidation_rows = connection.execute(
                "SELECT artifact_id FROM invalidations WHERE version = ? ORDER BY artifact_id",
                (latest_version,),
            ).fetchall()
            if [row["artifact_id"] for row in invalidation_rows] != invalidated:
                raise ledger_corrupt(
                    "invalidations",
                    "latest invalidation log differs from the recorded rebuild scope",
                    version=latest_version,
                )
            version_rows = connection.execute(
                "SELECT artifact_id FROM artifact_versions WHERE version = ? ORDER BY artifact_id",
                (latest_version,),
            ).fetchall()
            if [row["artifact_id"] for row in version_rows] != invalidated:
                raise ledger_corrupt(
                    "artifact_versions",
                    "latest artifact rebuild log differs from the recorded rebuild scope",
                    version=latest_version,
                )

        return {
            "verified_versions": len(rows),
            "latest_version": previous_version,
            "latest_ledger_checksum": previous_ledger_checksum or None,
            "artifact_count": len(build_catalog(sorted(latest_snapshot.services)))
            if rows and latest_snapshot is not None
            else 0,
        }

