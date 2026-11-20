from __future__ import annotations

import json
import os
import sqlite3
import sys
import threading
import time
from pathlib import Path
from typing import Any

from app.db import (
    InterProcessFileLock,
    connect,
    create_intent,
    finish_intent,
    get_current_version,
    get_snapshot_row,
    immediate_transaction,
    initialize_database,
    latest_complete_version,
    mark_intent_aborted,
    snapshot_is_complete,
    utc_now,
)
from app.engine import calculate_metrics, serialized_metric_views
from app.errors import (
    BusinessError,
    duplicate_version,
    snapshot_invalid,
    stale_base,
    version_not_found,
)
from app.snapshot import Snapshot, canonical_json, parse_snapshot, parse_snapshot_text


def _json_loads(text: str) -> Any:
    return json.loads(text)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class ConsistencyService:
    """Single-writer, fixed-point snapshot coordinator."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.lock_path = self.db_path.with_suffix(self.db_path.suffix + ".writer.lock")
        self._writer_guard = threading.RLock()
        self._initialization_guard = threading.Lock()
        self._initialized = False
        self.fault_hook = None

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        with self._initialization_guard:
            if self._initialized:
                return
            initialize_database(self.db_path)
            self._initialized = True

    def initialize(self, *, recover: bool = True) -> dict[str, Any] | None:
        self._ensure_initialized()
        return self.recover_pending(reason="service_startup") if recover else None

    def _writer_lock(self) -> InterProcessFileLock:
        return InterProcessFileLock(self.lock_path)

    def _fault(self, point: str) -> None:
        delay_ms = int(os.environ.get("CONSISTENCY_RECOMPUTE_DELAY_MS", "0") or "0")
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        if self.fault_hook is not None:
            self.fault_hook(point)
        if os.environ.get("CONSISTENCY_FAULT_POINT") == point:
            print(f"fault injection: forced termination at {point}", flush=True)
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(91)

    def _parse_input(self, snapshot_input: bytes | str | dict[str, Any]) -> Snapshot:
        if isinstance(snapshot_input, dict):
            return parse_snapshot(snapshot_input)
        if isinstance(snapshot_input, bytes):
            snapshot_input = snapshot_input.decode("utf-8")
        return parse_snapshot_text(snapshot_input)

    def _response(
        self,
        snapshot: Snapshot,
        metrics: dict[str, Any],
        *,
        base_version: int | None,
        current_version: int,
        reused: bool,
        intent_id: int | None,
    ) -> dict[str, Any]:
        conclusion = metrics["conclusion"]
        weighted = metrics["weighted_consistency"]
        return {
            "snapshot_version": snapshot.snapshot_version,
            "checksum": snapshot.checksum,
            "base_version": base_version,
            "current_version": current_version,
            "reused": reused,
            "intent_id": intent_id,
            "component_count": len(snapshot.components),
            "status": conclusion["status"],
            "consistent": conclusion["consistent"],
            "critical_component": conclusion["critical_component"],
            "weighted_consistency": weighted["value"],
            "weighted_consistency_error": weighted["cumulative_error"],
            "all_error_bounds_covered": metrics["all_error_bounds_covered"],
            "metrics_endpoint": f"/api/v1/snapshots/{snapshot.snapshot_version}/metrics",
            "consistency_endpoint": f"/api/v1/snapshots/{snapshot.snapshot_version}/consistency",
            "errors_endpoint": f"/api/v1/snapshots/{snapshot.snapshot_version}/errors",
        }

    def _stored_response(self, connection: sqlite3.Connection, version: int) -> dict[str, Any] | None:
        row = connection.execute("SELECT response_json FROM commit_responses WHERE version = ?", (version,)).fetchone()
        return None if row is None else _json_loads(row["response_json"])

    def commit_snapshot(
        self,
        snapshot_input: bytes | str | dict[str, Any],
        *,
        base_version: int | None = None,
    ) -> dict[str, Any]:
        snapshot = self._parse_input(snapshot_input)
        self._ensure_initialized()

        with self._writer_guard:
            with self._writer_lock():
                connection = connect(self.db_path)
                intent_id: int | None = None
                try:
                    current_version = get_current_version(connection)
                    expected_base = current_version if current_version is not None else 0
                    if base_version is not None and base_version != expected_base:
                        raise stale_base(expected_base, base_version)

                    existing = get_snapshot_row(connection, snapshot.snapshot_version)
                    if existing is not None:
                        if existing["checksum"] == snapshot.checksum and snapshot_is_complete(
                            connection, snapshot.snapshot_version
                        ):
                            stored = self._stored_response(connection, snapshot.snapshot_version)
                            if stored is not None:
                                stored = dict(stored)
                                stored["reused"] = True
                                stored["base_version"] = base_version
                                stored["current_version"] = current_version
                                return stored
                        if existing["checksum"] != snapshot.checksum:
                            raise duplicate_version(
                                snapshot.snapshot_version,
                                "same version has a different checksum",
                            )
                        raise duplicate_version(
                            snapshot.snapshot_version,
                            "version row is not a complete committed snapshot",
                        )

                    if current_version is not None and snapshot.snapshot_version <= current_version:
                        raise snapshot_invalid(
                            "snapshot_version is not strictly monotonic",
                            snapshot_version=snapshot.snapshot_version,
                            current_version=current_version,
                        )

                    intent_id = create_intent(
                        connection, snapshot.snapshot_version, snapshot.checksum, operation="commit_snapshot"
                    )
                    self._fault("after_intent_committed")

                    calculated = calculate_metrics(snapshot)
                    metrics = calculated["metrics"]
                    explanation = calculated["explanation"]
                    self._fault("after_metrics_before_commit")

                    response = self._response(
                        snapshot,
                        metrics,
                        base_version=base_version,
                        current_version=snapshot.snapshot_version,
                        reused=False,
                        intent_id=intent_id,
                    )
                    now = utc_now()
                    response_json = _json_dumps(response)
                    payload_json = canonical_json(snapshot.payload)
                    metrics_json = _json_dumps(metrics)
                    explanation_json = _json_dumps(explanation)
                    intent_detail = _json_dumps(
                        {
                            "snapshot_version": snapshot.snapshot_version,
                            "checksum": snapshot.checksum,
                            "status": "committed",
                            "component_count": len(snapshot.components),
                        }
                    )

                    with immediate_transaction(connection):
                        current_now = get_current_version(connection)
                        expected_now = current_now if current_now is not None else 0
                        if base_version is not None and base_version != expected_now:
                            raise stale_base(expected_now, base_version)
                        if current_now is not None and snapshot.snapshot_version <= current_now:
                            raise snapshot_invalid(
                                "snapshot_version is not strictly monotonic at commit",
                                snapshot_version=snapshot.snapshot_version,
                                current_version=current_now,
                            )
                        existing_now = get_snapshot_row(connection, snapshot.snapshot_version)
                        if existing_now is not None:
                            raise duplicate_version(snapshot.snapshot_version, "version appeared during recompute")

                        connection.execute(
                            """
                            INSERT INTO snapshots(
                                version, checksum, generated_at, schema_version, component_count,
                                payload_json, status, committed_at
                            ) VALUES (?, ?, ?, ?, ?, ?, 'committed', ?)
                            """,
                            (
                                snapshot.snapshot_version,
                                snapshot.checksum,
                                snapshot.generated_at,
                                snapshot.schema_version,
                                len(snapshot.components),
                                payload_json,
                                now,
                            ),
                        )
                        for component_id, share, headroom, weight, metric_json in serialized_metric_views(metrics):
                            component = metrics["components"][component_id]
                            connection.execute(
                                """
                                INSERT INTO derived_metrics(
                                    version, component_id, quota_share_micros, headroom_ratio_micros,
                                    normalized_weight_micros, quota_share_error_micros,
                                    headroom_error_micros, metric_json
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    snapshot.snapshot_version,
                                    component_id,
                                    share,
                                    headroom,
                                    weight,
                                    component["quota_share"]["cumulative_error_micros"],
                                    component["headroom_ratio"]["cumulative_error_micros"],
                                    metric_json,
                                ),
                            )
                        connection.execute(
                            """
                            INSERT INTO consistency_results(
                                version, status, weighted_consistency_micros,
                                weighted_error_micros, result_json
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                snapshot.snapshot_version,
                                metrics["conclusion"]["status"],
                                metrics["weighted_consistency"]["micros"],
                                metrics["weighted_consistency"]["cumulative_error_micros"],
                                metrics_json,
                            ),
                        )
                        connection.execute(
                            "INSERT INTO error_explanations(version, explanation_json) VALUES (?, ?)",
                            (snapshot.snapshot_version, explanation_json),
                        )
                        connection.execute(
                            "INSERT INTO commit_responses(version, response_json) VALUES (?, ?)",
                            (snapshot.snapshot_version, response_json),
                        )
                        finish_intent(connection, intent_id, "committed", intent_detail)
                        connection.execute(
                            "UPDATE current_state SET current_version = ?, updated_at = ? WHERE id = 1",
                            (snapshot.snapshot_version, now),
                        )
                    return response
                except Exception as exc:
                    if intent_id is not None:
                        try:
                            mark_intent_aborted(
                                self.db_path,
                                intent_id,
                                f"{type(exc).__name__}: {exc}",
                            )
                        except Exception:
                            pass
                    raise
                finally:
                    connection.close()

    def _load_complete_metrics(self, connection: sqlite3.Connection, version: int) -> dict[str, Any] | None:
        if not snapshot_is_complete(connection, version):
            return None
        row = connection.execute("SELECT result_json FROM consistency_results WHERE version = ?", (version,)).fetchone()
        return None if row is None else _json_loads(row["result_json"])

    def get_metrics(self, version: int) -> dict[str, Any]:
        self._ensure_initialized()
        connection = connect(self.db_path)
        try:
            snapshot = get_snapshot_row(connection, version)
            metrics = self._load_complete_metrics(connection, version)
            if snapshot is None or metrics is None:
                raise version_not_found(version)
            current = get_current_version(connection)
            component_rows = connection.execute(
                "SELECT component_id, metric_json FROM derived_metrics WHERE version = ? ORDER BY component_id",
                (version,),
            ).fetchall()
            return {
                "snapshot_version": version,
                "checksum": snapshot["checksum"],
                "current": current == version,
                "precision_contract": metrics["precision_contract"],
                "components": {
                    row["component_id"]: _json_loads(row["metric_json"]) for row in component_rows
                },
                "weighted_consistency": metrics["weighted_consistency"],
                "conclusion": metrics["conclusion"],
                "all_error_bounds_covered": metrics["all_error_bounds_covered"],
            }
        finally:
            connection.close()

    def get_consistency(self, version: int) -> dict[str, Any]:
        self._ensure_initialized()
        connection = connect(self.db_path)
        try:
            metrics = self._load_complete_metrics(connection, version)
            if metrics is None:
                raise version_not_found(version)
            return {
                "snapshot_version": version,
                "checksum": metrics["checksum"],
                "current": get_current_version(connection) == version,
                "conclusion": metrics["conclusion"],
                "weighted_consistency": metrics["weighted_consistency"],
                "difference_report": metrics["difference_report"],
                "error_cover": metrics["error_cover"],
                "all_error_bounds_covered": metrics["all_error_bounds_covered"],
            }
        finally:
            connection.close()

    def get_errors(self, version: int) -> dict[str, Any]:
        self._ensure_initialized()
        connection = connect(self.db_path)
        try:
            if not snapshot_is_complete(connection, version):
                raise version_not_found(version)
            row = connection.execute(
                "SELECT explanation_json FROM error_explanations WHERE version = ?", (version,)
            ).fetchone()
            if row is None:
                raise version_not_found(version)
            explanation = _json_loads(row["explanation_json"])
            explanation["current"] = get_current_version(connection) == version
            return explanation
        finally:
            connection.close()

    def get_current(self) -> dict[str, Any]:
        self._ensure_initialized()
        connection = connect(self.db_path)
        try:
            current = get_current_version(connection)
            if current is None:
                return {"current_version": None, "complete": False}
            snapshot = get_snapshot_row(connection, current)
            complete = snapshot_is_complete(connection, current)
            return {
                "current_version": current,
                "complete": complete,
                "checksum": snapshot["checksum"] if snapshot is not None else None,
                "snapshot_version": current,
            }
        finally:
            connection.close()

    def get_recovery_report(self) -> dict[str, Any]:
        self._ensure_initialized()
        connection = connect(self.db_path)
        try:
            rows = connection.execute(
                "SELECT id, created_at, report_json FROM recovery_reports ORDER BY id DESC LIMIT 20"
            ).fetchall()
            if not rows:
                return {
                    "report_id": None,
                    "created_at": None,
                    "report": None,
                    "history": [],
                    "message": "no persisted recovery report exists",
                }
            history = []
            for row in rows:
                item = _json_loads(row["report_json"])
                item["report_id"] = row["id"]
                item["created_at"] = row["created_at"]
                history.append(item)
            report = dict(history[0])
            report["history"] = history
            return report
        finally:
            connection.close()

    def recover_pending(self, *, reason: str = "manual_scan") -> dict[str, Any]:
        self._ensure_initialized()
        with self._writer_guard:
            with self._writer_lock():
                connection = connect(self.db_path)
                try:
                    with immediate_transaction(connection):
                        current_before = get_current_version(connection)
                        started_rows = connection.execute(
                            "SELECT * FROM intents WHERE status = 'started' ORDER BY id"
                        ).fetchall()
                        observed: list[dict[str, Any]] = []
                        actions: list[dict[str, Any]] = []
                        aborted_versions: set[int] = set()
                        for intent in started_rows:
                            version = int(intent["version"])
                            snapshot = get_snapshot_row(connection, version)
                            observed.append(
                                {
                                    "intent_id": intent["id"],
                                    "snapshot_version": version,
                                    "checksum": intent["checksum"],
                                    "status_before": intent["status"],
                                    "started_at": intent["started_at"],
                                    "snapshot_row_present": snapshot is not None,
                                }
                            )
                            if snapshot is not None and snapshot_is_complete(connection, version):
                                finish_intent(
                                    connection,
                                    int(intent["id"]),
                                    "committed",
                                    _json_dumps(
                                        {
                                            "recovered": "complete snapshot was already durable",
                                            "snapshot_version": version,
                                        }
                                    ),
                                )
                                actions.append(
                                    {
                                        "intent_id": intent["id"],
                                        "snapshot_version": version,
                                        "action": "preserved_complete_snapshot",
                                        "details": "all snapshot, metric, conclusion, and response rows are durable",
                                    }
                                )
                                continue

                            if snapshot is not None:
                                connection.execute("DELETE FROM derived_metrics WHERE version = ?", (version,))
                                connection.execute("DELETE FROM consistency_results WHERE version = ?", (version,))
                                connection.execute("DELETE FROM error_explanations WHERE version = ?", (version,))
                                connection.execute("DELETE FROM commit_responses WHERE version = ?", (version,))
                                connection.execute("UPDATE snapshots SET status = 'aborted' WHERE version = ?", (version,))
                                aborted_versions.add(version)
                                action_name = "truncated_incomplete_snapshot_and_marked_aborted"
                            else:
                                action_name = "marked_intent_aborted_no_snapshot_rows"

                            finish_intent(
                                connection,
                                int(intent["id"]),
                                "recovered_aborted",
                                _json_dumps(
                                    {
                                        "recovered_on_restart": True,
                                        "action": action_name,
                                        "snapshot_version": version,
                                    }
                                ),
                            )
                            actions.append(
                                {
                                    "intent_id": intent["id"],
                                    "snapshot_version": version,
                                    "action": action_name,
                                    "details": "no uncommitted metric/conclusion rows are visible",
                                }
                            )

                        current_after = get_current_version(connection)
                        if current_after is not None and not snapshot_is_complete(connection, current_after):
                            fallback = latest_complete_version(connection, upper_bound=current_after - 1)
                            if fallback is None:
                                fallback = latest_complete_version(connection)
                            connection.execute(
                                "UPDATE current_state SET current_version = ?, updated_at = ? WHERE id = 1",
                                (fallback, utc_now()),
                            )
                            actions.append(
                                {
                                    "action": "rolled_back_current_pointer",
                                    "from_version": current_after,
                                    "to_version": fallback,
                                }
                            )
                            current_after = fallback

                        complete_versions = [
                            int(row["version"])
                            for row in connection.execute(
                                "SELECT version FROM snapshots WHERE status = 'committed' ORDER BY version"
                            ).fetchall()
                            if snapshot_is_complete(connection, int(row["version"]))
                        ]
                        report = {
                            "report_type": "durable_restart_recovery",
                            "created_at": utc_now(),
                            "reason": reason,
                            "started_intents_observed": observed,
                            "started_intent_count": len(observed),
                            "actions": actions,
                            "current_version_before": current_before,
                            "current_version_after": current_after,
                            "complete_versions": complete_versions,
                            "aborted_versions": sorted(aborted_versions),
                            "facts_only": True,
                            "limitations": [
                                "A forced process termination cannot produce a complete graceful-exit report.",
                                "This report was generated after restart and contains only durable SQLite observations.",
                                "It does not claim that uncommitted in-memory recomputation was completed.",
                            ],
                        }
                        cursor = connection.execute(
                            "INSERT INTO recovery_reports(created_at, report_json) VALUES (?, ?)",
                            (report["created_at"], _json_dumps(report)),
                        )
                        report_id = int(cursor.lastrowid)
                    report["report_id"] = report_id
                    return report
                finally:
                    connection.close()
