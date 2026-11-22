from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .db import Database
from .errors import BusinessError, SimulatedForcedTermination
from .policy import (
    CANDIDATE_RULE,
    cooldown_elapsed,
    normalize_snapshot,
    target_layer_id,
)
from .samples import load_samples
from .utils import canonical_json, heat_histogram, iso_z, parse_datetime, sha256_json, snapshot_checksum, utcnow


MIGRATION_STATES = ("PENDING", "MIGRATING", "COMMITTED", "FAILED", "ABORTED")
ALLOWED_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "PENDING": ("MIGRATING", "ABORTED"),
    "MIGRATING": ("COMMITTED", "FAILED", "ABORTED"),
    "COMMITTED": (),
    "FAILED": ("PENDING",),
    "ABORTED": (),
}
BATCH_TERMINAL_STATES = ("COMMITTED", "FAILED", "ABORTED")


class TieringService:
    """SQLite-backed tiering engine.

    The service intentionally uses short-lived SQLite connections. Every write
    transaction uses BEGIN IMMEDIATE, while every read uses one read transaction so
    all returned fields belong to one WAL snapshot.
    """

    def __init__(
        self,
        db_path: str | Path,
        *,
        samples_dir: str | Path | None = None,
        auto_seed: bool = True,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.db = Database(db_path)
        self.db_path = Path(db_path)
        self.samples_dir = Path(samples_dir) if samples_dir is not None else None
        self.auto_seed = auto_seed
        self._clock = clock or utcnow
        self._run_id: int | None = None
        self._latest_startup_recovery: dict[str, Any] | None = None
        self._fault_plan: dict[str, Any] | None = None
        self._safe_shutdown_completed = False
        self._started = False

    # ------------------------------------------------------------------
    # Lifecycle and metadata
    # ------------------------------------------------------------------
    def startup(self) -> None:
        if self._started:
            return
        self.db.initialize()
        with self.db.write() as conn:
            last_run = conn.execute(
                "SELECT * FROM lifecycle_runs ORDER BY run_id DESC LIMIT 1"
            ).fetchone()
            forced_previous_run = bool(last_run and last_run["status"] == "RUNNING")
            if forced_previous_run:
                self._latest_startup_recovery = self._recover_previous_run(conn, last_run)

            if self.auto_seed:
                self._seed_if_empty(conn)

            now = self._now()
            cursor = conn.execute(
                "INSERT INTO lifecycle_runs(pid, started_at, status) VALUES(?, ?, 'RUNNING')",
                (os.getpid(), iso_z(now)),
            )
            self._run_id = int(cursor.lastrowid)
            self._bump_epoch(conn)
        self._started = True

    def safe_shutdown(self, reason: str = "graceful_shutdown") -> dict[str, Any]:
        if not self._started:
            return {"recorded": False, "reason": "service_not_started"}
        with self.db.writer_lock:
            if self._safe_shutdown_completed:
                with self.db.read() as conn:
                    report = self._latest_report_view(conn, "SAFE_SHUTDOWN")
                return report or {"recorded": True, "reason": "already_safe"}

            with self.db.write() as conn:
                run = self._current_run_row(conn)
                if run is None:
                    return {"recorded": False, "reason": "run_not_found"}
                if run["status"] == "SAFE_EXITED":
                    self._safe_shutdown_completed = True
                    report_row = conn.execute(
                        "SELECT report_json FROM reports WHERE report_id=?",
                        (run["recovery_report_id"],),
                    ).fetchone()
                    return json.loads(report_row["report_json"]) if report_row else {
                        "recorded": True,
                        "reason": "already_safe",
                    }

                now = self._now()
                aborted = self._abort_unfinished_batches(
                    conn,
                    reason=f"SAFE_SHUTDOWN:{reason}",
                    phase="SAFE_SHUTDOWN_ABORTED",
                )
                snapshot = self._load_snapshot(conn)
                if snapshot is not None:
                    layers = self._rebuild_layer_state(conn, snapshot, now)
                else:
                    layers = []
                completed_rows = conn.execute(
                    "SELECT * FROM batches WHERE state='COMMITTED' ORDER BY finished_at, batch_id"
                ).fetchall()
                completed = []
                for batch in completed_rows:
                    result = json.loads(batch["result_json"]) if batch["result_json"] else {}
                    completed.append(
                        {
                            "batch_id": batch["batch_id"],
                            "snapshot_version": batch["snapshot_version"],
                            "finished_at": batch["finished_at"],
                            "moved_count": result.get("moved_count", 0),
                            "selected_count": result.get("selected_count", 0),
                        }
                    )
                epoch = self._bump_epoch(conn)
                report_id = f"safe-{uuid.uuid4().hex}"
                report = {
                    "report_id": report_id,
                    "report_type": "SAFE_SHUTDOWN",
                    "forced_termination_detected": False,
                    "generated_at": iso_z(now),
                    "run_id": run["run_id"],
                    "reason": reason,
                    "persisted_only": True,
                    "safe_exit_summary_available": True,
                    "completed_batches": completed,
                    "aborted_during_shutdown": aborted,
                    "layer_occupancy": layers,
                    "state_epoch": epoch,
                    "note": (
                        "This is a graceful shutdown summary produced by the service. It is "
                        "qualified separately from forced-termination recovery reports."
                    ),
                }
                conn.execute(
                    "INSERT INTO reports(report_id, report_type, created_at, "
                    "forced_termination_detected, run_id, report_json) VALUES(?,?,?,?,?,?)",
                    (
                        report_id,
                        "SAFE_SHUTDOWN",
                        iso_z(now),
                        0,
                        run["run_id"],
                        canonical_json(report),
                    ),
                )
                conn.execute(
                    "UPDATE lifecycle_runs SET ended_at=?, status='SAFE_EXITED', "
                    "safe_summary_json=?, recovery_report_id=? WHERE run_id=?",
                    (iso_z(now), canonical_json(report), report_id, run["run_id"]),
                )
                self._safe_shutdown_completed = True
                return report

    def _seed_if_empty(self, conn: sqlite3.Connection) -> None:
        snapshot_count = conn.execute("SELECT COUNT(*) AS n FROM policy_snapshots").fetchone()["n"]
        if snapshot_count:
            return
        shard_count = conn.execute("SELECT COUNT(*) AS n FROM shards").fetchone()["n"]
        if shard_count:
            raise BusinessError(
                "E_PERSISTED_STATE_INVALID",
                "shards exist without a policy snapshot",
                context={"shard_count": shard_count},
                status_code=500,
            )
        if self.samples_dir is None:
            return
        snapshot, shards = load_samples(self.samples_dir)
        now = self._now()
        self._assert_capacity_and_topology(conn, snapshot)
        conn.execute(
            "INSERT INTO policy_snapshots(version, generated_at, checksum, payload_json, applied_at) "
            "VALUES(?,?,?,?,?)",
            (
                snapshot["version"],
                snapshot["generated_at"],
                snapshot["checksum"],
                canonical_json(snapshot),
                iso_z(now),
            ),
        )
        for shard in shards:
            conn.execute(
                "INSERT INTO shards(shard_id, size_bytes, heat, layer_id, cooldown_until, "
                "last_batch_id, updated_at) VALUES(?,?,?,?,?,NULL,?)",
                (
                    shard["shard_id"],
                    shard["size_bytes"],
                    shard["heat"],
                    shard["layer_id"],
                    shard["cooldown_until"],
                    iso_z(now),
                ),
            )
        self._set_meta(conn, "current_snapshot_version", snapshot["version"])
        self._set_meta(conn, "cascade_generation", "1")
        self._rebuild_layer_state(conn, snapshot, now)
        self._assert_occupancy_equals_shard_sum(conn, snapshot, now)
        self._bump_epoch(conn)

    # ------------------------------------------------------------------
    # Policy snapshots
    # ------------------------------------------------------------------
    def submit_snapshot(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._ensure_operational()
        snapshot = normalize_snapshot(payload, verify_checksum=True)
        with self.db.write() as conn:
            current = self._load_snapshot(conn)
            current_version = current["version"] if current else 0
            if snapshot["version"] <= current_version:
                raise BusinessError(
                    "E_STALE_SNAPSHOT",
                    "snapshot version must be greater than the currently committed version",
                    context={
                        "submitted_version": snapshot["version"],
                        "current_version": current_version,
                    },
                )
            before_layers = self._layer_rows(conn)
            before_runtime = self._runtime_row(conn)
            now = self._now()
            after_layers = self._apply_snapshot(conn, snapshot, now)
            after_runtime = self._runtime_row(conn)
            epoch = self._bump_epoch(conn)
            return self._snapshot_mutation_response(
                snapshot=snapshot,
                previous_version=current_version,
                before_layers=before_layers,
                after_layers=after_layers,
                before_runtime=before_runtime,
                after_runtime=after_runtime,
                state_epoch=epoch,
                mutation="FULL_REPLACEMENT",
            )

    def adjust_snapshot(
        self,
        *,
        base_version: int,
        version: int,
        generated_at: str | None = None,
        layer_capacity_updates: dict[str, int] | None = None,
        heat_threshold: int | None = None,
        batch_limit: int | None = None,
        cooldown_seconds: int | None = None,
    ) -> dict[str, Any]:
        self._ensure_operational()
        with self.db.write() as conn:
            current = self._load_snapshot(conn)
            if current is None:
                raise BusinessError(
                    "E_NO_SNAPSHOT",
                    "no policy snapshot has been committed",
                    status_code=503,
                )
            if current["version"] != base_version:
                raise BusinessError(
                    "E_STALE_BASE",
                    "incremental adjustment is based on a stale policy version",
                    context={
                        "base_version": base_version,
                        "current_version": current["version"],
                    },
                )
            if version <= current["version"]:
                raise BusinessError(
                    "E_STALE_SNAPSHOT",
                    "new snapshot version must be greater than the current version",
                    context={"submitted_version": version, "current_version": current["version"]},
                )

            draft = {
                "version": version,
                "generated_at": generated_at or iso_z(self._now()),
                "layers": [
                    {"id": layer["id"], "capacity_bytes": layer["capacity_bytes"]}
                    for layer in current["layers"]
                ],
                "heat_threshold": (
                    heat_threshold if heat_threshold is not None else current["heat_threshold"]
                ),
                "batch_limit": batch_limit if batch_limit is not None else current["batch_limit"],
                "cooldown_seconds": (
                    cooldown_seconds
                    if cooldown_seconds is not None
                    else current["cooldown_seconds"]
                ),
            }
            layer_by_id = {layer["id"]: layer for layer in draft["layers"]}
            for layer_id, capacity in (layer_capacity_updates or {}).items():
                if layer_id not in layer_by_id:
                    raise BusinessError(
                        "E_SNAPSHOT_INVALID",
                        "incremental adjustment references an unknown layer",
                        context={"layer_id": layer_id, "known_layers": sorted(layer_by_id)},
                        status_code=400,
                    )
                layer_by_id[layer_id]["capacity_bytes"] = capacity
            draft["checksum"] = snapshot_checksum(draft)
            snapshot = normalize_snapshot(draft, verify_checksum=True)

            before_layers = self._layer_rows(conn)
            before_runtime = self._runtime_row(conn)
            now = self._now()
            after_layers = self._apply_snapshot(conn, snapshot, now)
            after_runtime = self._runtime_row(conn)
            epoch = self._bump_epoch(conn)
            return self._snapshot_mutation_response(
                snapshot=snapshot,
                previous_version=current["version"],
                before_layers=before_layers,
                after_layers=after_layers,
                before_runtime=before_runtime,
                after_runtime=after_runtime,
                state_epoch=epoch,
                mutation="INCREMENTAL_ADJUSTMENT",
            )

    def _apply_snapshot(
        self,
        conn: sqlite3.Connection,
        snapshot: dict[str, Any],
        now: datetime,
    ) -> list[dict[str, Any]]:
        self._assert_capacity_and_topology(conn, snapshot)
        conn.execute(
            "INSERT INTO policy_snapshots(version, generated_at, checksum, payload_json, applied_at) "
            "VALUES(?,?,?,?,?)",
            (
                snapshot["version"],
                snapshot["generated_at"],
                snapshot["checksum"],
                canonical_json(snapshot),
                iso_z(now),
            ),
        )
        cascade_generation = self._meta_int(conn, "cascade_generation", 0) + 1
        self._set_meta(conn, "current_snapshot_version", snapshot["version"])
        self._set_meta(conn, "cascade_generation", cascade_generation)
        layers = self._rebuild_layer_state(conn, snapshot, now)
        self._assert_occupancy_equals_shard_sum(conn, snapshot, now)
        return layers

    def _assert_capacity_and_topology(
        self,
        conn: sqlite3.Connection,
        snapshot: dict[str, Any],
    ) -> None:
        layer_ids = {layer["id"] for layer in snapshot["layers"]}
        usage_rows = conn.execute(
            "SELECT layer_id, COALESCE(SUM(size_bytes), 0) AS used_bytes, COUNT(*) AS shard_count "
            "FROM shards GROUP BY layer_id"
        ).fetchall()
        unknown_layers = [row["layer_id"] for row in usage_rows if row["layer_id"] not in layer_ids]
        if unknown_layers:
            raise BusinessError(
                "E_SNAPSHOT_INVALID",
                "snapshot removes layers that still contain shards",
                context={"layers_with_shards": sorted(unknown_layers)},
                status_code=400,
            )
        usage = {row["layer_id"]: row["used_bytes"] for row in usage_rows}
        overflows = []
        for layer in snapshot["layers"]:
            used = int(usage.get(layer["id"], 0))
            if used > layer["capacity_bytes"]:
                overflows.append(
                    {
                        "layer_id": layer["id"],
                        "capacity_bytes": layer["capacity_bytes"],
                        "current_used_bytes": used,
                        "overflow_bytes": used - layer["capacity_bytes"],
                    }
                )
        if overflows:
            raise BusinessError(
                "E_CAPACITY_OVERFLOW",
                "new layer capacity is below current occupancy",
                context={
                    "layers": overflows,
                    "total_overflow_bytes": sum(item["overflow_bytes"] for item in overflows),
                    "applied": False,
                },
            )

    def _snapshot_mutation_response(
        self,
        *,
        snapshot: dict[str, Any],
        previous_version: int,
        before_layers: list[dict[str, Any]],
        after_layers: list[dict[str, Any]],
        before_runtime: dict[str, Any] | None,
        after_runtime: dict[str, Any] | None,
        state_epoch: int,
        mutation: str,
    ) -> dict[str, Any]:
        return {
            "accepted": True,
            "mutation": mutation,
            "previous_version": previous_version,
            "snapshot": snapshot,
            "cascade": {
                "invalidated": True,
                "eligibility_changed": (
                    (before_runtime or {}).get("eligible_shards")
                    != (after_runtime or {}).get("eligible_shards")
                    or (before_runtime or {}).get("eligible_bytes")
                    != (after_runtime or {}).get("eligible_bytes")
                ),
                "runtime_before": before_runtime,
                "runtime_after": after_runtime,
                "layer_occupancy_before": before_layers,
                "layer_occupancy_after": after_layers,
                "candidate_inputs_recomputed_in_transaction": True,
            },
            "state_epoch": state_epoch,
        }

    def get_current_policy(self) -> dict[str, Any]:
        with self.db.read() as conn:
            snapshot = self._load_snapshot(conn)
            if snapshot is None:
                raise BusinessError(
                    "E_NO_SNAPSHOT",
                    "no policy snapshot has been committed",
                    status_code=503,
                )
            return {
                "snapshot": snapshot,
                "cascade_generation": self._meta_int(conn, "cascade_generation", 0),
                "runtime": self._runtime_row(conn),
                "state_epoch": self._state_epoch(conn),
            }

    # ------------------------------------------------------------------
    # Candidate construction and batches
    # ------------------------------------------------------------------
    def preview_batch(self, snapshot_version: int) -> dict[str, Any]:
        with self.db.read() as conn:
            snapshot = self._load_snapshot(conn)
            if snapshot is None:
                raise BusinessError("E_NO_SNAPSHOT", "no policy snapshot has been committed", status_code=503)
            if snapshot["version"] != snapshot_version:
                raise BusinessError(
                    "E_STALE_SNAPSHOT",
                    "batch preview must use the currently committed snapshot",
                    context={
                        "requested_version": snapshot_version,
                        "current_version": snapshot["version"],
                    },
                )
            plan = self._build_batch_plan(conn, snapshot, self._now())
            plan["state_epoch"] = self._state_epoch(conn)
            return plan

    def execute_batch(self, batch_id: str, snapshot_version: int) -> dict[str, Any]:
        self._ensure_operational()
        request_hash = sha256_json({"batch_id": batch_id, "snapshot_version": snapshot_version})
        with self.db.writer_lock:
            existing = self._lookup_batch_by_id(batch_id)
            if existing is not None:
                self._assert_same_batch_request(existing, request_hash, batch_id)
                if existing["state"] not in BATCH_TERMINAL_STATES:
                    existing = self._wait_for_batch_terminal(batch_id)
                with self.db.read() as conn:
                    return self._batch_view(conn, existing, replayed=True)

            prepared = False
            with self.db.write() as conn:
                existing = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
                if existing is not None:
                    self._assert_same_batch_request(existing, request_hash, batch_id)
                    with self.db.read() as read_conn:
                        return self._batch_view(read_conn, existing, replayed=True)

                snapshot = self._load_snapshot(conn)
                if snapshot is None:
                    raise BusinessError(
                        "E_NO_SNAPSHOT",
                        "no policy snapshot has been committed",
                        status_code=503,
                    )
                if snapshot["version"] != snapshot_version:
                    raise BusinessError(
                        "E_STALE_SNAPSHOT",
                        "batch execution must use the currently committed snapshot",
                        context={
                            "requested_version": snapshot_version,
                            "current_version": snapshot["version"],
                        },
                    )
                now = self._now()
                plan = self._build_batch_plan(conn, snapshot, now)
                cascade_generation = self._meta_int(conn, "cascade_generation", 0)
                conn.execute(
                    "INSERT INTO batches(batch_id, request_hash, snapshot_version, "
                    "cascade_generation, state, phase, created_at, started_at, plan_json) "
                    "VALUES(?,?,?,?,?,'PLANNED',?,?,?)",
                    (
                        batch_id,
                        request_hash,
                        snapshot_version,
                        cascade_generation,
                        "MIGRATING",
                        iso_z(now),
                        iso_z(now),
                        canonical_json(plan),
                    ),
                )
                for sequence, candidate in enumerate(plan["selected"]):
                    conn.execute(
                        "INSERT INTO batch_items(batch_id, shard_id, source_layer_id, "
                        "target_layer_id, size_bytes, heat, state, last_changed_at, reason, sequence) "
                        "VALUES(?,?,?,?,?,?,'PENDING',?,?,?)",
                        (
                            batch_id,
                            candidate["shard_id"],
                            candidate["source_layer_id"],
                            candidate["target_layer_id"],
                            candidate["size_bytes"],
                            candidate["heat"],
                            iso_z(now),
                            "BATCH_PREPARED",
                            sequence,
                        ),
                    )
                    self._insert_transition(
                        conn,
                        batch_id,
                        candidate["shard_id"],
                        None,
                        "PENDING",
                        iso_z(now),
                        "BATCH_PREPARED",
                        "PLANNED",
                    )
                    conn.execute(
                        "INSERT INTO migration_intents(batch_id, shard_id, snapshot_version, "
                        "cascade_generation, source_layer_id, target_layer_id, size_bytes, state, "
                        "created_at, updated_at, detail_json) VALUES(?,?,?,?,?,?,?,'PREPARED',?,?,?)",
                        (
                            batch_id,
                            candidate["shard_id"],
                            snapshot_version,
                            cascade_generation,
                            candidate["source_layer_id"],
                            candidate["target_layer_id"],
                            candidate["size_bytes"],
                            iso_z(now),
                            iso_z(now),
                            canonical_json(
                                {
                                    "source_release_bytes": candidate["size_bytes"],
                                    "target_alloc_bytes": candidate["size_bytes"],
                                }
                            ),
                        ),
                    )
                self._bump_epoch(conn)
                prepared = True

            if not prepared:
                raise BusinessError("E_BATCH_INTERNAL", "batch preparation failed", status_code=500)

            self._maybe_terminate("after_intent", batch_id)

            with self.db.write() as conn:
                conn.execute(
                    "UPDATE batches SET phase='TRANSFER_STARTED' WHERE batch_id=? AND state='MIGRATING'",
                    (batch_id,),
                )
                self._bump_epoch(conn)

            try:
                with self.db.write() as conn:
                    batch = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
                    if batch is None or batch["state"] != "MIGRATING":
                        raise BusinessError(
                            "E_BATCH_STATE",
                            "batch is not in an executable state",
                            context={"batch_id": batch_id},
                            status_code=500,
                        )
                    current_snapshot = self._load_snapshot(conn)
                    current_cascade = self._meta_int(conn, "cascade_generation", 0)
                    if (
                        current_snapshot is None
                        or current_snapshot["version"] != batch["snapshot_version"]
                        or current_cascade != batch["cascade_generation"]
                    ):
                        raise BusinessError(
                            "E_STALE_SNAPSHOT",
                            "policy changed between batch preparation and commit",
                            context={
                                "batch_snapshot_version": batch["snapshot_version"],
                                "current_snapshot_version": (
                                    current_snapshot["version"] if current_snapshot else None
                                ),
                                "batch_cascade_generation": batch["cascade_generation"],
                                "current_cascade_generation": current_cascade,
                            },
                        )
                    ordered_snapshot = snapshot
                    now = self._now()
                    items = conn.execute(
                        "SELECT * FROM batch_items WHERE batch_id=? ORDER BY sequence",
                        (batch_id,),
                    ).fetchall()
                    for item in items:
                        self._apply_item_move(conn, ordered_snapshot, item, now)
                    after_layers = self._rebuild_layer_state(conn, ordered_snapshot, now)
                    self._assert_occupancy_equals_shard_sum(conn, ordered_snapshot, now)
                    heats = [
                        shard_row["heat"]
                        for shard_row in conn.execute("SELECT heat FROM shards").fetchall()
                    ]
                    result = self._build_batch_result(
                        batch_id=batch_id,
                        snapshot=ordered_snapshot,
                        plan=plan,
                        items=items,
                        layer_before=plan["layer_before"],
                        layer_after=after_layers,
                        state_epoch=self._state_epoch(conn) + 1,
                        shard_heats=heats,
                        completed_at=now,
                    )
                    epoch = self._bump_epoch(conn)
                    result["state_epoch"] = epoch
                    conn.execute(
                        "UPDATE batches SET state='COMMITTED', phase='COMMITTED', finished_at=?, "
                        "result_json=?, committed_epoch=? WHERE batch_id=?",
                        (iso_z(now), canonical_json(result), epoch, batch_id),
                    )
                    self._maybe_terminate("after_transfer_before_commit", batch_id)
            except SimulatedForcedTermination:
                raise
            except BaseException as exc:
                self._record_failed_batch(batch_id, exc)
                raise

            self._maybe_terminate("after_commit_before_response", batch_id)
            with self.db.read() as conn:
                row = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
                return self._batch_view(conn, row, replayed=False)

    def _apply_item_move(
        self,
        conn: sqlite3.Connection,
        snapshot: dict[str, Any],
        item: sqlite3.Row,
        now: datetime,
    ) -> None:
        shard = conn.execute(
            "SELECT * FROM shards WHERE shard_id=?",
            (item["shard_id"],),
        ).fetchone()
        if shard is None:
            raise BusinessError(
                "E_PERSISTED_STATE_INVALID",
                "batch references a missing shard",
                context={"batch_id": item["batch_id"], "shard_id": item["shard_id"]},
                status_code=500,
            )
        if shard["layer_id"] != item["source_layer_id"]:
            raise BusinessError(
                "E_PERSISTED_STATE_INVALID",
                "shard source layer does not match the persisted migration intent",
                context={
                    "shard_id": item["shard_id"],
                    "expected_source_layer_id": item["source_layer_id"],
                    "actual_layer_id": shard["layer_id"],
                },
                status_code=500,
            )
        if item["state"] != "PENDING":
            raise BusinessError(
                "E_INVALID_TRANSITION",
                "batch item must be PENDING before transfer",
                context={
                    "batch_id": item["batch_id"],
                    "shard_id": item["shard_id"],
                    "from_state": item["state"],
                    "to_state": "MIGRATING",
                },
            )

        changed_at = iso_z(now)
        self._transition_item(
            conn,
            batch_id=item["batch_id"],
            shard_id=item["shard_id"],
            from_state="PENDING",
            to_state="MIGRATING",
            at=changed_at,
            reason="TRANSFER_STARTED",
            phase="TRANSFER",
            extra_sql="reason=?",
            extra_params=("TRANSFER_STARTED",),
        )

        cooldown_until = now + timedelta(seconds=snapshot["cooldown_seconds"])
        conn.execute(
            "UPDATE shards SET layer_id=?, cooldown_until=?, last_batch_id=?, updated_at=? "
            "WHERE shard_id=?",
            (
                item["target_layer_id"],
                iso_z(cooldown_until),
                item["batch_id"],
                changed_at,
                item["shard_id"],
            ),
        )
        conn.execute(
            "UPDATE layer_state SET used_bytes=used_bytes-?, shard_count=shard_count-1, "
            "updated_at=? WHERE layer_id=?",
            (item["size_bytes"], changed_at, item["source_layer_id"]),
        )
        conn.execute(
            "UPDATE layer_state SET used_bytes=used_bytes+?, shard_count=shard_count+1, "
            "updated_at=? WHERE layer_id=?",
            (item["size_bytes"], changed_at, item["target_layer_id"]),
        )

        self._transition_item(
            conn,
            batch_id=item["batch_id"],
            shard_id=item["shard_id"],
            from_state="MIGRATING",
            to_state="COMMITTED",
            at=changed_at,
            reason="TRANSFER_COMMITTED",
            phase="TRANSFER",
            extra_sql="source_release_bytes=?, target_alloc_bytes=?, reason=?",
            extra_params=(item["size_bytes"], item["size_bytes"], "TRANSFER_COMMITTED"),
        )
        conn.execute(
            "UPDATE migration_intents SET state='APPLIED', updated_at=? "
            "WHERE batch_id=? AND shard_id=?",
            (changed_at, item["batch_id"], item["shard_id"]),
        )

    def _transition_item(
        self,
        conn: sqlite3.Connection,
        *,
        batch_id: str,
        shard_id: str,
        from_state: str,
        to_state: str,
        at: str,
        reason: str,
        phase: str,
        extra_sql: str = "reason=?",
        extra_params: tuple[Any, ...] = (),
    ) -> None:
        self._validate_transition(from_state, to_state, batch_id=batch_id, shard_id=shard_id)
        self._insert_transition(conn, batch_id, shard_id, from_state, to_state, at, reason, phase)
        conn.execute(
            f"UPDATE batch_items SET state=?, last_changed_at=?, {extra_sql} "
            "WHERE batch_id=? AND shard_id=?",
            (to_state, at, *extra_params, batch_id, shard_id),
        )

    def _insert_transition(
        self,
        conn: sqlite3.Connection,
        batch_id: str,
        shard_id: str,
        from_state: str | None,
        to_state: str,
        at: str,
        reason: str,
        phase: str,
    ) -> None:
        conn.execute(
            "INSERT INTO transition_events(batch_id, shard_id, from_state, to_state, at, reason, "
            "persisted_phase) VALUES(?,?,?,?,?,?,?)",
            (batch_id, shard_id, from_state, to_state, at, reason, phase),
        )

    def _build_batch_plan(
        self,
        conn: sqlite3.Connection,
        snapshot: dict[str, Any],
        now: datetime,
    ) -> dict[str, Any]:
        layers = snapshot["layers"]
        layer_by_id = {layer["id"]: layer for layer in layers}
        rank_by_id = {layer["id"]: rank for rank, layer in enumerate(layers)}
        shards = conn.execute(
            "SELECT shard_id, size_bytes, heat, layer_id, cooldown_until FROM shards "
            "ORDER BY shard_id"
        ).fetchall()
        usage: dict[str, int] = {layer["id"]: 0 for layer in layers}
        counts: dict[str, int] = {layer["id"]: 0 for layer in layers}
        for shard in shards:
            if shard["layer_id"] not in layer_by_id:
                raise BusinessError(
                    "E_PERSISTED_STATE_INVALID",
                    "shard references a layer outside the current policy",
                    context={"shard_id": shard["shard_id"], "layer_id": shard["layer_id"]},
                    status_code=500,
                )
            usage[shard["layer_id"]] += shard["size_bytes"]
            counts[shard["layer_id"]] += 1

        candidates: list[dict[str, Any]] = []
        for shard in shards:
            target = target_layer_id(
                heat=shard["heat"],
                current_layer_id=shard["layer_id"],
                layers=layers,
                heat_threshold=snapshot["heat_threshold"],
            )
            if target is None:
                continue
            if not cooldown_elapsed(shard["cooldown_until"], now=now):
                continue
            candidates.append(
                {
                    "shard_id": shard["shard_id"],
                    "size_bytes": shard["size_bytes"],
                    "heat": shard["heat"],
                    "source_layer_id": shard["layer_id"],
                    "source_rank": rank_by_id[shard["layer_id"]],
                    "target_layer_id": target,
                    "target_rank": rank_by_id[target],
                }
            )
        candidates.sort(key=lambda item: (-item["heat"], item["shard_id"]))

        incoming: dict[str, int] = {layer["id"]: 0 for layer in layers}
        selected: list[dict[str, Any]] = []
        truncated: list[dict[str, Any]] = []
        selected_ids: set[str] = set()
        for candidate in candidates:
            if len(selected) >= snapshot["batch_limit"]:
                truncated.append(
                    {
                        **candidate,
                        "truncation_reason": "BATCH_LIMIT",
                        "detail": {
                            "batch_limit": snapshot["batch_limit"],
                            "selected_count": len(selected),
                        },
                    }
                )
                continue
            target = candidate["target_layer_id"]
            target_used = usage[target] + incoming[target]
            capacity = layer_by_id[target]["capacity_bytes"]
            if target_used + candidate["size_bytes"] > capacity:
                truncated.append(
                    {
                        **candidate,
                        "truncation_reason": "TARGET_CAPACITY",
                        "detail": {
                            "target_capacity_bytes": capacity,
                            "target_used_after_accepted_bytes": target_used,
                            "shard_size_bytes": candidate["size_bytes"],
                            "shortfall_bytes": (
                                target_used + candidate["size_bytes"] - capacity
                            ),
                        },
                    }
                )
                continue
            if candidate["shard_id"] in selected_ids:
                continue
            selected_ids.add(candidate["shard_id"])
            incoming[target] += candidate["size_bytes"]
            selected.append(candidate)

        return {
            "snapshot_version": snapshot["version"],
            "cascade_generation": self._meta_int(conn, "cascade_generation", 0),
            "generated_at": iso_z(now),
            "batch_limit": snapshot["batch_limit"],
            "heat_threshold": snapshot["heat_threshold"],
            "candidate_rule": CANDIDATE_RULE,
            "candidates_total": len(candidates),
            "selected_count": len(selected),
            "selected": selected,
            "truncated_count": len(truncated),
            "truncated": truncated,
            "empty": not selected,
            "layer_before": self._layer_rows(conn),
        }

    def _build_batch_result(
        self,
        *,
        batch_id: str,
        snapshot: dict[str, Any],
        plan: dict[str, Any],
        items: list[sqlite3.Row],
        layer_before: list[dict[str, Any]],
        layer_after: list[dict[str, Any]],
        state_epoch: int,
        shard_heats: list[int],
        completed_at: datetime,
    ) -> dict[str, Any]:
        moves = []
        for item in items:
            moves.append(
                {
                    "shard_id": item["shard_id"],
                    "source_layer_id": item["source_layer_id"],
                    "target_layer_id": item["target_layer_id"],
                    "source_release_bytes": item["size_bytes"],
                    "target_alloc_bytes": item["size_bytes"],
                    "heat": item["heat"],
                }
            )
        return {
            "batch_id": batch_id,
            "state": "COMMITTED",
            "snapshot_version": snapshot["version"],
            "cascade_generation": plan["cascade_generation"],
            "candidate_rule": CANDIDATE_RULE,
            "candidates_total": plan["candidates_total"],
            "selected_count": plan["selected_count"],
            "moved_count": len(moves),
            "empty": not moves,
            "moves": moves,
            "selected": plan["selected"],
            "truncated_count": plan["truncated_count"],
            "truncated": plan["truncated"],
            "layer_before": layer_before,
            "layer_after": layer_after,
            "heat_distribution_after": heat_histogram(shard_heats),
            "state_epoch": state_epoch,
            "completed_at": iso_z(completed_at),
        }

    def _record_failed_batch(self, batch_id: str, exc: BaseException) -> None:
        try:
            with self.db.write() as conn:
                batch = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
                if batch is None or batch["state"] in BATCH_TERMINAL_STATES:
                    return
                now = iso_z(self._now())
                items = conn.execute(
                    "SELECT * FROM batch_items WHERE batch_id=? ORDER BY sequence",
                    (batch_id,),
                ).fetchall()
                for item in items:
                    if item["state"] in ("PENDING", "MIGRATING"):
                        self._transition_item(
                            conn,
                            batch_id=batch_id,
                            shard_id=item["shard_id"],
                            from_state=item["state"],
                            to_state="ABORTED",
                            at=now,
                            reason="EXECUTION_ERROR_ROLLBACK",
                            phase="FAILED",
                            extra_sql="reason=?",
                            extra_params=("EXECUTION_ERROR_ROLLBACK",),
                        )
                conn.execute(
                    "UPDATE migration_intents SET state='ROLLED_BACK', updated_at=? "
                    "WHERE batch_id=? AND state='PREPARED'",
                    (now, batch_id),
                )
                result = {
                    "batch_id": batch_id,
                    "state": "FAILED",
                    "error": {"type": type(exc).__name__, "message": str(exc)},
                    "selected_count": json.loads(batch["plan_json"])["selected_count"],
                    "moved_count": 0,
                    "completed_at": now,
                }
                if batch["state"] == "MIGRATING":
                    try:
                        self._transition_batch_state(conn, batch_id, "MIGRATING", "FAILED", now)
                    except BusinessError:
                        pass
                conn.execute(
                    "UPDATE batches SET state='FAILED', phase='FAILED', finished_at=?, "
                    "result_json=?, error_json=? WHERE batch_id=?",
                    (now, canonical_json(result), canonical_json(result["error"]), batch_id),
                )
                self._bump_epoch(conn)
        except BaseException:
            # Never hide the original execution failure.
            return

    def _transition_batch_state(
        self,
        conn: sqlite3.Connection,
        batch_id: str,
        from_state: str,
        to_state: str,
        now: str,
    ) -> None:
        row = conn.execute("SELECT state FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None or row["state"] != from_state:
            raise BusinessError(
                "E_INVALID_TRANSITION",
                "batch state changed unexpectedly",
                context={"batch_id": batch_id, "from_state": from_state, "to_state": to_state},
            )
        conn.execute(
            "UPDATE batches SET state=?, finished_at=? WHERE batch_id=?",
            (to_state, now, batch_id),
        )

    def _assert_same_batch_request(
        self,
        existing: sqlite3.Row,
        request_hash: str,
        batch_id: str,
    ) -> None:
        if existing["request_hash"] != request_hash:
            raise BusinessError(
                "E_CONFLICTING_BATCH",
                "batch id was already used with different request content",
                context={
                    "batch_id": batch_id,
                    "existing_snapshot_version": existing["snapshot_version"],
                },
            )

    def _wait_for_batch_terminal(self, batch_id: str) -> sqlite3.Row:
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            row = self._lookup_batch_by_id(batch_id)
            if row is not None and row["state"] in BATCH_TERMINAL_STATES:
                return row
            time.sleep(0.05)
        raise BusinessError(
            "E_BATCH_IN_PROGRESS",
            "batch is still in progress",
            context={"batch_id": batch_id},
            status_code=409,
        )

    def _lookup_batch_by_id(self, batch_id: str) -> sqlite3.Row | None:
        with self.db.read() as conn:
            return conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()

    def _batch_view(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        replayed: bool,
    ) -> dict[str, Any]:
        items = conn.execute(
            "SELECT * FROM batch_items WHERE batch_id=? ORDER BY sequence",
            (row["batch_id"],),
        ).fetchall()
        events = conn.execute(
            "SELECT event_id, shard_id, from_state, to_state, at, reason, persisted_phase "
            "FROM transition_events WHERE batch_id=? ORDER BY event_id",
            (row["batch_id"],),
        ).fetchall()
        item_views = []
        for item in items:
            item_views.append(
                {
                    "shard_id": item["shard_id"],
                    "source_layer_id": item["source_layer_id"],
                    "target_layer_id": item["target_layer_id"],
                    "size_bytes": item["size_bytes"],
                    "heat": item["heat"],
                    "state": item["state"],
                    "last_changed_at": item["last_changed_at"],
                    "reason": item["reason"],
                    "source_release_bytes": item["source_release_bytes"],
                    "target_alloc_bytes": item["target_alloc_bytes"],
                }
            )
        event_views = [
            {
                "event_id": event["event_id"],
                "shard_id": event["shard_id"],
                "from_state": event["from_state"],
                "to_state": event["to_state"],
                "at": event["at"],
                "reason": event["reason"],
                "persisted_phase": event["persisted_phase"],
            }
            for event in events
        ]
        result = json.loads(row["result_json"]) if row["result_json"] else None
        conclusion_matches = self._batch_conclusion_matches(row["state"], item_views)
        return {
            "batch_id": row["batch_id"],
            "state": row["state"],
            "phase": row["phase"],
            "snapshot_version": row["snapshot_version"],
            "cascade_generation": row["cascade_generation"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "committed_epoch": row["committed_epoch"],
            "items": item_views,
            "transitions": event_views,
            "result": result,
            "conclusion_matches_items": conclusion_matches,
            "replayed": replayed,
            "state_epoch": self._state_epoch(conn),
        }

    def _batch_conclusion_matches(self, batch_state: str, items: list[dict[str, Any]]) -> bool:
        states = [item["state"] for item in items]
        if batch_state == "COMMITTED":
            return all(state == "COMMITTED" for state in states)
        if batch_state == "ABORTED":
            return all(state in ("ABORTED",) for state in states)
        if batch_state == "FAILED":
            return all(state in ("COMMITTED", "FAILED", "ABORTED") for state in states)
        if batch_state == "MIGRATING":
            return all(state in ("PENDING", "MIGRATING", "COMMITTED") for state in states)
        return True

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        with self.db.read() as conn:
            row = conn.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if row is None:
                raise BusinessError(
                    "E_BATCH_NOT_FOUND",
                    "batch was not found",
                    context={"batch_id": batch_id},
                    status_code=404,
                )
            return self._batch_view(conn, row, replayed=False)

    # ------------------------------------------------------------------
    # Read-side summaries
    # ------------------------------------------------------------------
    def get_occupancy(self) -> dict[str, Any]:
        with self.db.read() as conn:
            snapshot = self._load_snapshot(conn)
            if snapshot is None:
                raise BusinessError("E_NO_SNAPSHOT", "no policy snapshot has been committed", status_code=503)
            layers = self._layer_rows(conn)
            runtime = self._runtime_row(conn)
            shard_totals = conn.execute(
                "SELECT COUNT(*) AS shard_count, COALESCE(SUM(size_bytes), 0) AS used_bytes "
                "FROM shards"
            ).fetchone()
            return {
                "snapshot_version": snapshot["version"],
                "snapshot_checksum": snapshot["checksum"],
                "cascade_generation": self._meta_int(conn, "cascade_generation", 0),
                "state_epoch": self._state_epoch(conn),
                "layers": layers,
                "runtime": runtime,
                "totals": {
                    "capacity_bytes": sum(layer["capacity_bytes"] for layer in layers),
                    "used_bytes": int(shard_totals["used_bytes"]),
                    "shard_count": int(shard_totals["shard_count"]),
                    "hot_shards": sum(layer["hot_shards"] for layer in layers),
                    "hot_bytes": sum(layer["hot_bytes"] for layer in layers),
                    "cold_shards": sum(layer["cold_shards"] for layer in layers),
                    "cold_bytes": sum(layer["cold_bytes"] for layer in layers),
                },
                "invariant": "layer used_bytes equals the sum of shard bytes in that layer",
            }

    def get_heat_distribution(self) -> dict[str, Any]:
        occupancy = self.get_occupancy()
        return {
            "snapshot_version": occupancy["snapshot_version"],
            "state_epoch": occupancy["state_epoch"],
            "threshold": occupancy["runtime"]["heat_threshold"],
            "layers": [
                {
                    "layer_id": layer["layer_id"],
                    "heat_histogram": layer["heat_histogram"],
                    "hot_shards": layer["hot_shards"],
                    "hot_bytes": layer["hot_bytes"],
                    "cold_shards": layer["cold_shards"],
                    "cold_bytes": layer["cold_bytes"],
                }
                for layer in occupancy["layers"]
            ],
            "totals": {
                "hot_shards": occupancy["totals"]["hot_shards"],
                "hot_bytes": occupancy["totals"]["hot_bytes"],
                "cold_shards": occupancy["totals"]["cold_shards"],
                "cold_bytes": occupancy["totals"]["cold_bytes"],
            },
        }

    def list_shards(self, *, layer_id: str | None = None, limit: int = 1000) -> dict[str, Any]:
        with self.db.read() as conn:
            snapshot = self._load_snapshot(conn)
            if snapshot is None:
                raise BusinessError("E_NO_SNAPSHOT", "no policy snapshot has been committed", status_code=503)
            if layer_id is None:
                rows = conn.execute(
                    "SELECT s.*, p.candidate_eligible, p.target_layer_id, p.cooldown_eligible, "
                    "p.snapshot_version AS policy_snapshot_version FROM shards s "
                    "LEFT JOIN shard_policy_state p ON p.shard_id=s.shard_id "
                    "ORDER BY s.shard_id LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT s.*, p.candidate_eligible, p.target_layer_id, p.cooldown_eligible, "
                    "p.snapshot_version AS policy_snapshot_version FROM shards s "
                    "LEFT JOIN shard_policy_state p ON p.shard_id=s.shard_id "
                    "WHERE s.layer_id=? ORDER BY s.shard_id LIMIT ?",
                    (layer_id, limit),
                ).fetchall()
            return {
                "snapshot_version": snapshot["version"],
                "state_epoch": self._state_epoch(conn),
                "shards": [dict(row) for row in rows],
            }

    def get_shard(self, shard_id: str) -> dict[str, Any]:
        with self.db.read() as conn:
            shard = conn.execute("SELECT * FROM shards WHERE shard_id=?", (shard_id,)).fetchone()
            if shard is None:
                raise BusinessError(
                    "E_SHARD_NOT_FOUND",
                    "shard was not found",
                    context={"shard_id": shard_id},
                    status_code=404,
                )
            transitions = conn.execute(
                "SELECT * FROM transition_events WHERE shard_id=? ORDER BY event_id DESC LIMIT 50",
                (shard_id,),
            ).fetchall()
            policy_state = conn.execute(
                "SELECT * FROM shard_policy_state WHERE shard_id=?", (shard_id,)
            ).fetchone()
            return {
                "shard": dict(shard),
                "policy_state": dict(policy_state) if policy_state is not None else None,
                "transitions": [dict(row) for row in transitions],
                "snapshot_version": (self._load_snapshot(conn) or {}).get("version"),
                "state_epoch": self._state_epoch(conn),
            }

    # ------------------------------------------------------------------
    # State machine API and fault injection
    # ------------------------------------------------------------------
    def state_machine(self) -> dict[str, Any]:
        return {
            "states": list(MIGRATION_STATES),
            "terminal_states": ["COMMITTED", "ABORTED"],
            "transitions": {state: list(targets) for state, targets in ALLOWED_TRANSITIONS.items()},
            "note": (
                "COMMITTED resource movement is performed only by the batch executor. The "
                "validation endpoint never modifies persisted state."
            ),
        }

    def check_transition(self, from_state: str, to_state: str) -> dict[str, Any]:
        self._validate_transition(from_state, to_state)
        return {
            "allowed": True,
            "from_state": from_state,
            "to_state": to_state,
            "persisted_state_changed": False,
            "state_epoch": self.get_state_epoch(),
        }

    def _validate_transition(
        self,
        from_state: str,
        to_state: str,
        *,
        batch_id: str | None = None,
        shard_id: str | None = None,
    ) -> None:
        if from_state not in ALLOWED_TRANSITIONS or to_state not in ALLOWED_TRANSITIONS:
            raise BusinessError(
                "E_INVALID_TRANSITION",
                "transition references an unknown migration state",
                context={
                    "from_state": from_state,
                    "to_state": to_state,
                    "states": list(MIGRATION_STATES),
                },
            )
        if to_state not in ALLOWED_TRANSITIONS[from_state]:
            raise BusinessError(
                "E_INVALID_TRANSITION",
                "transition is not allowed by the migration state machine",
                context={
                    "from_state": from_state,
                    "to_state": to_state,
                    "allowed_next_states": list(ALLOWED_TRANSITIONS[from_state]),
                    "batch_id": batch_id,
                    "shard_id": shard_id,
                    "persisted_state_changed": False,
                },
            )

    def arm_fault(self, *, point: str, mode: str, batch_id: str | None) -> dict[str, Any]:
        if point not in (
            "after_intent",
            "after_transfer_before_commit",
            "after_commit_before_response",
        ):
            raise BusinessError(
                "E_INVALID_REQUEST",
                "unknown fault injection point",
                context={"point": point},
                status_code=422,
            )
        if mode not in ("raise", "hard_exit"):
            raise BusinessError(
                "E_INVALID_REQUEST",
                "unknown fault injection mode",
                context={"mode": mode},
                status_code=422,
            )
        self._fault_plan = {"point": point, "mode": mode, "batch_id": batch_id}
        return {
            "armed": True,
            "point": point,
            "mode": mode,
            "batch_id": batch_id,
            "note": (
                "hard_exit calls os._exit after the selected durable boundary. Restart the "
                "service to run startup recovery."
            ),
        }

    def _maybe_terminate(self, point: str, batch_id: str) -> None:
        plan = self._fault_plan
        if not plan or plan["point"] != point:
            return
        if plan.get("batch_id") not in (None, batch_id):
            return
        self._fault_plan = None
        if plan["mode"] == "hard_exit":
            os._exit(73)
        raise SimulatedForcedTermination(point, batch_id)

    # ------------------------------------------------------------------
    # Recovery and reports
    # ------------------------------------------------------------------
    def _recover_previous_run(
        self,
        conn: sqlite3.Connection,
        previous_run: sqlite3.Row,
    ) -> dict[str, Any]:
        now = self._now()
        before_layers = self._layer_rows(conn)
        prepared_intents_at_scan = conn.execute(
            "SELECT COUNT(*) AS n FROM migration_intents WHERE state='PREPARED'"
        ).fetchone()["n"]
        unfinished = conn.execute(
            "SELECT * FROM batches WHERE state IN ('PENDING','MIGRATING') "
            "ORDER BY created_at, batch_id"
        ).fetchall()
        recovered_batches = []
        for batch in unfinished:
            result = self._abort_one_unfinished_batch(
                conn,
                batch,
                reason="FORCED_TERMINATION_RECOVERY",
                phase="RECOVERY_ABORTED",
                now=now,
            )
            recovered_batches.append(result)

        snapshot = self._load_snapshot(conn)
        if snapshot is not None:
            after_layers = self._rebuild_layer_state(conn, snapshot, now)
            self._assert_occupancy_equals_shard_sum(conn, snapshot, now)
        else:
            after_layers = before_layers
        orphan_intent_cursor = conn.execute(
            "UPDATE migration_intents SET state='ROLLED_BACK', updated_at=? "
            "WHERE state='PREPARED' AND batch_id NOT IN "
            "(SELECT batch_id FROM batches WHERE state='COMMITTED')",
            (iso_z(now),),
        )
        orphan_intents_rolled_back = max(0, orphan_intent_cursor.rowcount)
        prepared_intents_after = conn.execute(
            "SELECT COUNT(*) AS n FROM migration_intents WHERE state='PREPARED'"
        ).fetchone()["n"]
        epoch = self._bump_epoch(conn)
        report_id = f"recovery-{uuid.uuid4().hex}"
        report = {
            "report_id": report_id,
            "report_type": "STARTUP_RECOVERY",
            "forced_termination_detected": True,
            "generated_at": iso_z(now),
            "previous_run": dict(previous_run),
            "unfinished_batch_count": len(unfinished),
            "recovered_batches": recovered_batches,
            "snapshot_version_used": snapshot["version"] if snapshot else None,
            "intent_scan": {
                "prepared_intents_at_scan": prepared_intents_at_scan,
                "orphan_intents_rolled_back": orphan_intents_rolled_back,
                "prepared_intents_after_recovery": prepared_intents_after,
            },
            "layer_occupancy_before": before_layers,
            "layer_occupancy_after": after_layers,
            "occupancy_difference": self._occupancy_difference(before_layers, after_layers),
            "persisted_only": True,
            "safe_exit_summary_available": False,
            "safe_exit_reports_are_separate": True,
            "state_epoch": epoch,
            "recovery_basis": (
                "Only SQLite rows committed before forced termination were used. Uncommitted "
                "WAL transactions are discarded by SQLite; batches without a committed terminal "
                "row are marked ABORTED and all layer usage is rebuilt from shard layer membership."
            ),
            "caveat": (
                "A forced termination cannot produce a complete graceful-exit report. This "
                "report is a startup reconstruction and must not be read as a safe shutdown."
            ),
        }
        conn.execute(
            "INSERT INTO reports(report_id, report_type, created_at, "
            "forced_termination_detected, run_id, report_json) VALUES(?,?,?,?,?,?)",
            (
                report_id,
                "STARTUP_RECOVERY",
                iso_z(now),
                1,
                previous_run["run_id"],
                canonical_json(report),
            ),
        )
        conn.execute(
            "UPDATE lifecycle_runs SET status='FORCED_RECOVERED', ended_at=?, recovery_report_id=? "
            "WHERE run_id=?",
            (iso_z(now), report_id, previous_run["run_id"]),
        )
        return report

    def _abort_unfinished_batches(
        self,
        conn: sqlite3.Connection,
        *,
        reason: str,
        phase: str,
    ) -> list[dict[str, Any]]:
        now = self._now()
        rows = conn.execute(
            "SELECT * FROM batches WHERE state IN ('PENDING','MIGRATING') "
            "ORDER BY created_at, batch_id"
        ).fetchall()
        return [
            self._abort_one_unfinished_batch(conn, row, reason=reason, phase=phase, now=now)
            for row in rows
        ]

    def _abort_one_unfinished_batch(
        self,
        conn: sqlite3.Connection,
        batch: sqlite3.Row,
        *,
        reason: str,
        phase: str,
        now: datetime,
    ) -> dict[str, Any]:
        now_text = iso_z(now)
        items = conn.execute(
            "SELECT * FROM batch_items WHERE batch_id=? ORDER BY sequence",
            (batch["batch_id"],),
        ).fetchall()
        item_results = []
        committed_items = 0
        for item in items:
            state = item["state"]
            if state == "COMMITTED":
                committed_items += 1
                item_results.append({"shard_id": item["shard_id"], "state": state, "preserved": True})
                continue
            if state in ("PENDING", "MIGRATING"):
                self._transition_item(
                    conn,
                    batch_id=batch["batch_id"],
                    shard_id=item["shard_id"],
                    from_state=state,
                    to_state="ABORTED",
                    at=now_text,
                    reason=reason,
                    phase=phase,
                    extra_sql="reason=?",
                    extra_params=(reason,),
                )
                item_results.append(
                    {
                        "shard_id": item["shard_id"],
                        "from_state": state,
                        "state": "ABORTED",
                        "preserved": False,
                    }
                )
            else:
                item_results.append({"shard_id": item["shard_id"], "state": state, "preserved": True})
        conn.execute(
            "UPDATE migration_intents SET state='ROLLED_BACK', updated_at=? "
            "WHERE batch_id=? AND state='PREPARED'",
            (now_text, batch["batch_id"]),
        )
        batch_result = {
            "batch_id": batch["batch_id"],
            "state": "ABORTED" if committed_items == 0 else "FAILED",
            "reason": reason,
            "committed_items_preserved": committed_items,
            "items": item_results,
            "recovered_at": now_text,
            "snapshot_version": batch["snapshot_version"],
        }
        final_state = batch_result["state"]
        conn.execute(
            "UPDATE batches SET state=?, phase=?, finished_at=?, result_json=?, "
            "error_json=?, recovery_reason=? WHERE batch_id=?",
            (
                final_state,
                phase,
                now_text,
                canonical_json(batch_result),
                canonical_json({"code": "E_FORCED_RECOVERY", "message": reason}),
                reason,
                batch["batch_id"],
            ),
        )
        return batch_result

    def get_recovery_report(self) -> dict[str, Any]:
        with self.db.read() as conn:
            latest_recovery = self._latest_report_view(conn, "STARTUP_RECOVERY")
            latest_safe = self._latest_report_view(conn, "SAFE_SHUTDOWN")
            lifecycle = [
                dict(row)
                for row in conn.execute(
                    "SELECT run_id, pid, started_at, ended_at, status, recovery_report_id "
                    "FROM lifecycle_runs ORDER BY run_id"
                ).fetchall()
            ]
            return {
                "state_epoch": self._state_epoch(conn),
                "persisted_only": True,
                "latest_startup_recovery": latest_recovery,
                "latest_safe_shutdown": latest_safe,
                "forced_recovery_available": latest_recovery is not None,
                "safe_exit_available": latest_safe is not None,
                "lifecycle_runs": lifecycle,
                "interpretation": (
                    "STARTUP_RECOVERY is reconstructed only from committed persistence facts "
                    "after a forced termination. SAFE_SHUTDOWN is a separate graceful-exit "
                    "summary. Absence of a safe shutdown summary is expected after a crash."
                ),
            }

    def list_reports(self, *, limit: int = 50) -> dict[str, Any]:
        with self.db.read() as conn:
            rows = conn.execute(
                "SELECT report_id, report_type, created_at, forced_termination_detected, run_id "
                "FROM reports ORDER BY created_at DESC, report_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return {
                "state_epoch": self._state_epoch(conn),
                "reports": [dict(row) for row in rows],
            }

    def _latest_report_view(
        self,
        conn: sqlite3.Connection,
        report_type: str,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT report_json FROM reports WHERE report_type=? "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (report_type,),
        ).fetchone()
        return json.loads(row["report_json"]) if row else None

    # ------------------------------------------------------------------
    # Derived state and helper functions
    # ------------------------------------------------------------------
    def _rebuild_layer_state(
        self,
        conn: sqlite3.Connection,
        snapshot: dict[str, Any],
        now: datetime,
    ) -> list[dict[str, Any]]:
        layers = snapshot["layers"]
        layer_by_id = {layer["id"]: layer for layer in layers}
        rank_by_id = {layer["id"]: rank for rank, layer in enumerate(layers)}
        shards = conn.execute(
            "SELECT shard_id, size_bytes, heat, layer_id, cooldown_until FROM shards "
            "ORDER BY shard_id"
        ).fetchall()
        aggregates = {
            layer["id"]: {
                "used_bytes": 0,
                "shard_count": 0,
                "hot_shards": 0,
                "hot_bytes": 0,
                "cold_shards": 0,
                "cold_bytes": 0,
                "eligible_shards": 0,
                "eligible_bytes": 0,
                "policy_states": [],
                "heats": [],
            }
            for layer in layers
        }
        for shard in shards:
            layer_id = shard["layer_id"]
            if layer_id not in aggregates:
                raise BusinessError(
                    "E_PERSISTED_STATE_INVALID",
                    "shard layer is not present in the current policy snapshot",
                    context={"shard_id": shard["shard_id"], "layer_id": layer_id},
                    status_code=500,
                )
            aggregate = aggregates[layer_id]
            aggregate["used_bytes"] += shard["size_bytes"]
            aggregate["shard_count"] += 1
            aggregate["heats"].append(shard["heat"])
            if shard["heat"] >= snapshot["heat_threshold"]:
                aggregate["hot_shards"] += 1
                aggregate["hot_bytes"] += shard["size_bytes"]
            else:
                aggregate["cold_shards"] += 1
                aggregate["cold_bytes"] += shard["size_bytes"]
            target = target_layer_id(
                heat=shard["heat"],
                current_layer_id=layer_id,
                layers=layers,
                heat_threshold=snapshot["heat_threshold"],
            )
            cooldown_ok = cooldown_elapsed(shard["cooldown_until"], now=now)
            if target is not None and cooldown_ok:
                aggregate["eligible_shards"] += 1
                aggregate["eligible_bytes"] += shard["size_bytes"]
            aggregate["policy_states"].append(
                {
                    "shard_id": shard["shard_id"],
                    "candidate_eligible": int(target is not None and cooldown_ok),
                    "target_layer_id": target,
                    "cooldown_eligible": int(cooldown_ok),
                    "heat_class": (
                        "HOT" if shard["heat"] >= snapshot["heat_threshold"] else "COLD"
                    ),
                }
            )

        conn.execute("DELETE FROM layer_state")
        conn.execute("DELETE FROM policy_runtime")
        conn.execute("DELETE FROM shard_policy_state")
        cascade_generation = self._meta_int(conn, "cascade_generation", 0)
        total_eligible = 0
        total_eligible_bytes = 0
        updated_at = iso_z(now)
        for layer in layers:
            data = aggregates[layer["id"]]
            total_eligible += data["eligible_shards"]
            total_eligible_bytes += data["eligible_bytes"]
            conn.execute(
                "INSERT INTO layer_state(layer_id, rank, capacity_bytes, used_bytes, shard_count, "
                "hot_shards, hot_bytes, cold_shards, cold_bytes, eligible_shards, eligible_bytes, "
                "heat_histogram_json, snapshot_version, updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    layer["id"],
                    rank_by_id[layer["id"]],
                    layer["capacity_bytes"],
                    data["used_bytes"],
                    data["shard_count"],
                    data["hot_shards"],
                    data["hot_bytes"],
                    data["cold_shards"],
                    data["cold_bytes"],
                    data["eligible_shards"],
                    data["eligible_bytes"],
                    canonical_json(heat_histogram(data["heats"])),
                    snapshot["version"],
                    updated_at,
                ),
            )
            for policy_state in data["policy_states"]:
                conn.execute(
                    "INSERT INTO shard_policy_state(shard_id, snapshot_version, "
                    "cascade_generation, candidate_eligible, target_layer_id, cooldown_eligible, "
                    "heat_class, computed_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        policy_state["shard_id"],
                        snapshot["version"],
                        cascade_generation,
                        policy_state["candidate_eligible"],
                        policy_state["target_layer_id"],
                        policy_state["cooldown_eligible"],
                        policy_state["heat_class"],
                        updated_at,
                    ),
                )
        conn.execute(
            "INSERT INTO policy_runtime(snapshot_version, cascade_generation, eligible_shards, "
            "eligible_bytes, candidate_rule, computed_at) VALUES(?,?,?,?,?,?)",
            (
                snapshot["version"],
                cascade_generation,
                total_eligible,
                total_eligible_bytes,
                CANDIDATE_RULE,
                updated_at,
            ),
        )
        return self._layer_rows(conn)

    def _assert_occupancy_equals_shard_sum(
        self,
        conn: sqlite3.Connection,
        snapshot: dict[str, Any],
        now: datetime,
    ) -> None:
        expected = {
            layer["id"]: {
                "used_bytes": 0,
                "shard_count": 0,
                "hot_shards": 0,
                "hot_bytes": 0,
                "cold_shards": 0,
                "cold_bytes": 0,
                "eligible_shards": 0,
                "eligible_bytes": 0,
                "heats": [],
            }
            for layer in snapshot["layers"]
        }
        layers = snapshot["layers"]
        for shard in conn.execute(
            "SELECT size_bytes, heat, layer_id, cooldown_until FROM shards"
        ).fetchall():
            layer_id = shard["layer_id"]
            if layer_id not in expected:
                raise BusinessError(
                    "E_OCCUPANCY_MISMATCH",
                    "shard references a layer outside the current snapshot",
                    context={"layer_id": layer_id},
                    status_code=500,
                )
            aggregate = expected[layer_id]
            aggregate["used_bytes"] += shard["size_bytes"]
            aggregate["shard_count"] += 1
            aggregate["heats"].append(shard["heat"])
            if shard["heat"] >= snapshot["heat_threshold"]:
                aggregate["hot_shards"] += 1
                aggregate["hot_bytes"] += shard["size_bytes"]
            else:
                aggregate["cold_shards"] += 1
                aggregate["cold_bytes"] += shard["size_bytes"]
            target = target_layer_id(
                heat=shard["heat"],
                current_layer_id=layer_id,
                layers=layers,
                heat_threshold=snapshot["heat_threshold"],
            )
            if target is not None and cooldown_elapsed(shard["cooldown_until"], now=now):
                aggregate["eligible_shards"] += 1
                aggregate["eligible_bytes"] += shard["size_bytes"]

        for aggregate in expected.values():
            aggregate["heat_histogram"] = heat_histogram(aggregate.pop("heats"))

        actual = {}
        for row in conn.execute("SELECT * FROM layer_state").fetchall():
            actual[row["layer_id"]] = {
                "used_bytes": row["used_bytes"],
                "shard_count": row["shard_count"],
                "hot_shards": row["hot_shards"],
                "hot_bytes": row["hot_bytes"],
                "cold_shards": row["cold_shards"],
                "cold_bytes": row["cold_bytes"],
                "eligible_shards": row["eligible_shards"],
                "eligible_bytes": row["eligible_bytes"],
                "heat_histogram": json.loads(row["heat_histogram_json"]),
            }
        if expected != actual:
            raise BusinessError(
                "E_OCCUPANCY_MISMATCH",
                "recomputed layer summary does not equal the per-shard aggregate",
                context={
                    "expected": expected,
                    "actual": actual,
                },
                status_code=500,
            )

    def _layer_rows(self, conn: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = conn.execute("SELECT * FROM layer_state ORDER BY rank").fetchall()
        return [
            {
                "layer_id": row["layer_id"],
                "rank": row["rank"],
                "capacity_bytes": row["capacity_bytes"],
                "used_bytes": row["used_bytes"],
                "free_bytes": max(0, row["capacity_bytes"] - row["used_bytes"]),
                "over_capacity_bytes": max(0, row["used_bytes"] - row["capacity_bytes"]),
                "shard_count": row["shard_count"],
                "hot_shards": row["hot_shards"],
                "hot_bytes": row["hot_bytes"],
                "cold_shards": row["cold_shards"],
                "cold_bytes": row["cold_bytes"],
                "eligible_shards": row["eligible_shards"],
                "eligible_bytes": row["eligible_bytes"],
                "heat_histogram": json.loads(row["heat_histogram_json"]),
                "snapshot_version": row["snapshot_version"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]

    def _runtime_row(self, conn: sqlite3.Connection) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT * FROM policy_runtime ORDER BY snapshot_version DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        snapshot = self._load_snapshot(conn, row["snapshot_version"])
        return {
            "snapshot_version": row["snapshot_version"],
            "cascade_generation": row["cascade_generation"],
            "heat_threshold": snapshot["heat_threshold"] if snapshot else None,
            "batch_limit": snapshot["batch_limit"] if snapshot else None,
            "cooldown_seconds": snapshot["cooldown_seconds"] if snapshot else None,
            "eligible_shards": row["eligible_shards"],
            "eligible_bytes": row["eligible_bytes"],
            "candidate_rule": row["candidate_rule"],
            "computed_at": row["computed_at"],
        }

    def _load_snapshot(
        self,
        conn: sqlite3.Connection,
        version: int | None = None,
    ) -> dict[str, Any] | None:
        if version is None:
            version = self._meta_int(conn, "current_snapshot_version", 0) or None
            if version is None:
                return None
        row = conn.execute(
            "SELECT * FROM policy_snapshots WHERE version=?", (version,)
        ).fetchone()
        if row is None:
            return None
        payload = json.loads(row["payload_json"])
        payload["applied_at"] = row["applied_at"]
        return payload

    def _current_run_row(self, conn: sqlite3.Connection) -> sqlite3.Row | None:
        if self._run_id is None:
            return None
        return conn.execute(
            "SELECT * FROM lifecycle_runs WHERE run_id=?", (self._run_id,)
        ).fetchone()

    def _state_epoch(self, conn: sqlite3.Connection) -> int:
        return self._meta_int(conn, "commit_epoch", 0)

    def get_state_epoch(self) -> int:
        with self.db.read() as conn:
            return self._state_epoch(conn)

    def _bump_epoch(self, conn: sqlite3.Connection) -> int:
        epoch = self._state_epoch(conn) + 1
        self._set_meta(conn, "commit_epoch", epoch)
        return epoch

    def _meta_int(self, conn: sqlite3.Connection, key: str, default: int) -> int:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return int(row["value"]) if row else default

    def _set_meta(self, conn: sqlite3.Connection, key: str, value: Any) -> None:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    def _occupancy_difference(
        self,
        before: list[dict[str, Any]],
        after: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        before_by_id = {layer["layer_id"]: layer for layer in before}
        after_by_id = {layer["layer_id"]: layer for layer in after}
        differences = []
        for layer_id in sorted(set(before_by_id) | set(after_by_id)):
            old = before_by_id.get(layer_id, {})
            new = after_by_id.get(layer_id, {})
            differences.append(
                {
                    "layer_id": layer_id,
                    "used_bytes_before": old.get("used_bytes"),
                    "used_bytes_after": new.get("used_bytes"),
                    "used_bytes_delta": (new.get("used_bytes") or 0) - (old.get("used_bytes") or 0),
                    "shard_count_before": old.get("shard_count"),
                    "shard_count_after": new.get("shard_count"),
                }
            )
        return differences

    def _ensure_operational(self) -> None:
        if not self._started:
            raise BusinessError(
                "E_SERVICE_NOT_READY",
                "service startup has not completed",
                status_code=503,
            )
        if self._safe_shutdown_completed:
            raise BusinessError(
                "E_SERVICE_STOPPING",
                "service has completed a safe shutdown and no longer accepts writes",
                status_code=503,
            )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(timezone.utc).replace(microsecond=0)
