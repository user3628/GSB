from __future__ import annotations

import json
import os
import sqlite3
import uuid
from threading import RLock
from typing import Any, Callable

from app.config import Settings
from app.domain import (
    ROUNDING_NOTE,
    UNIT_SCALE,
    CompensatedSum,
    TenantRecord,
    descendants,
    lineage,
    as_tenant_records,
    parse_tenant_specs,
    parse_tenant_values,
    policy_checksum,
    policy_document,
    validate_tenant_hierarchy,
    to_units,
    transition_allowed,
    units_to_str,
)
from app.engine import RuntimeState, evaluate_batch, refill
from app.errors import DomainError, spec_invalid, unknown_tenant
from app.models import (
    BatchIn,
    EventIn,
    PolicyPatchIn,
    PolicySnapshotIn,
    SimulationIn,
    TenantPatch,
    TenantPatchIn,
    TenantSpec,
    TenantStatus,
)
from app.store import Store


def _uuid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


class GatewayService:
    def __init__(self, store: Store, settings: Settings) -> None:
        self.store = store
        self.settings = settings
        self._operation_lock = RLock()

    # ------------------------------------------------------------------ reads
    def _policy_row(self) -> sqlite3.Row:
        row = self.store.current_policy()
        if row is None:
            raise spec_invalid("no policy snapshot has been submitted")
        return row

    def _tenants(self) -> dict[str, TenantRecord]:
        return {
            row["tenant_id"]: TenantRecord(
                tenant_id=row["tenant_id"],
                capacity_units=int(row["capacity_units"]),
                rps_units=int(row["rps_units"]),
                burst_units=int(row["burst_units"]),
                weight_units=int(row["weight_units"]),
                parent_id=row["parent_id"],
            )
            for row in self.store.rows("SELECT * FROM tenants ORDER BY tenant_id")
        }

    def _states(
        self,
        tenants: dict[str, TenantRecord] | None = None,
    ) -> dict[str, RuntimeState]:
        tenants = tenants or self._tenants()
        rows = self.store.state_rows()
        states: dict[str, RuntimeState] = {}
        policy_row = self.store.current_policy()
        baseline_ms = int(policy_row["generated_at_ms"]) if policy_row is not None else 0
        for tenant_id, tenant in tenants.items():
            row = rows.get(tenant_id)
            if row is None:
                states[tenant_id] = RuntimeState.initial(tenant, baseline_ms)
                continue
            states[tenant_id] = RuntimeState(
                tenant_id=row["tenant_id"],
                tokens_units=int(row["tokens_units"]),
                last_refill_ms=int(row["last_refill_ms"]),
                refill_remainder=int(row["refill_remainder"]),
                direct_used_bytes_units=int(row["direct_used_bytes_units"]),
                aggregate_used_bytes_units=int(row["aggregate_used_bytes_units"]),
                direct_used_request_units=int(row["direct_used_request_units"]),
                aggregate_used_request_units=int(row["aggregate_used_request_units"]),
                consecutive_rejections=int(row["consecutive_rejections"]),
                status=TenantStatus(row["status"]),
                last_reason=row["last_reason"],
                suspended_reason=row["suspended_reason"],
                updated_at_ms=int(row["updated_at_ms"]),
            )
        return states

    def _policy_public(self) -> dict[str, Any]:
        row = self._policy_row()
        document = json.loads(row["snapshot_json"])
        document["checksum"] = row["checksum"]
        return document

    def get_policy(self) -> dict[str, Any]:
        return self._policy_public()

    def get_state(self) -> dict[str, Any]:
        with self.store.read_transaction():
            tenants = self._tenants()
            states = self._states(tenants)
            policy_row = self._policy_row()
            last_event_ms = self.store.last_event_ms()
        public_tenants = []
        for tenant_id in sorted(tenants):
            tenant = tenants[tenant_id]
            state = states[tenant_id].as_public(tenant)
            state["policy_version"] = int(policy_row["version"])
            public_tenants.append(
                {
                    "tenant_id": tenant.tenant_id,
                    "parent_id": tenant.parent_id,
                    "capacity_bytes_units": tenant.capacity_units,
                    "capacity_bytes": units_to_str(tenant.capacity_units),
                    "requests_per_second_units": tenant.rps_units,
                    "requests_per_second": units_to_str(tenant.rps_units),
                    "burst_capacity_units": tenant.burst_units,
                    "burst_capacity": units_to_str(tenant.burst_units),
                    "weight_units": tenant.weight_units,
                    "weight": units_to_str(tenant.weight_units),
                    "state": state,
                }
            )
        return {
            "policy_version": int(policy_row["version"]),
            "policy_checksum": policy_row["checksum"],
            "generated_at_ms": int(policy_row["generated_at_ms"]),
            "last_event_ms": last_event_ms,
            "tenants": public_tenants,
            "accounting": {
                "scale": 6,
                "unit": "1e-6",
                "rounding": "ROUND_FLOOR",
                "note": ROUNDING_NOTE,
            },
        }

    def get_tenant(self, tenant_id: str) -> dict[str, Any]:
        with self.store.read_transaction():
            tenants = self._tenants()
            if tenant_id not in tenants:
                raise unknown_tenant(tenant_id)
            state = self._states(tenants)[tenant_id].as_public(tenants[tenant_id])
            policy_row = self._policy_row()
        state["policy_version"] = int(policy_row["version"])
        tenant = tenants[tenant_id]
        return {
            "tenant_id": tenant.tenant_id,
            "parent_id": tenant.parent_id,
            "capacity_bytes_units": tenant.capacity_units,
            "capacity_bytes": units_to_str(tenant.capacity_units),
            "requests_per_second_units": tenant.rps_units,
            "requests_per_second": units_to_str(tenant.rps_units),
            "burst_capacity_units": tenant.burst_units,
            "burst_capacity": units_to_str(tenant.burst_units),
            "weight_units": tenant.weight_units,
            "weight": units_to_str(tenant.weight_units),
            "lineage": lineage(tenant_id, tenants),
            "state": state,
            "policy_version": int(policy_row["version"]),
        }

    def list_intents(self, status: str | None = None) -> list[dict[str, Any]]:
        if status is None:
            rows = self.store.rows("SELECT * FROM intents ORDER BY created_at_ms, intent_id")
        else:
            rows = self.store.rows(
                "SELECT * FROM intents WHERE status=? ORDER BY created_at_ms, intent_id",
                (status,),
            )
        output = []
        for row in rows:
            item = dict(row)
            item["before_state_json"] = json.loads(item["before_state_json"])
            item["detail_json"] = json.loads(item["detail_json"] or "{}")
            output.append(item)
        return output

    def list_batches(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.store.rows(
            "SELECT * FROM batches ORDER BY created_at_ms DESC LIMIT ?", (limit,)
        )
        return [dict(row) for row in rows]

    def list_usage_ledger(self, tenant_id: str | None = None, limit: int = 500) -> list[dict[str, Any]]:
        if tenant_id is None:
            rows = self.store.rows(
                "SELECT * FROM usage_ledger ORDER BY ledger_id DESC LIMIT ?", (limit,)
            )
        else:
            rows = self.store.rows(
                "SELECT * FROM usage_ledger WHERE account_tenant_id=? ORDER BY ledger_id DESC LIMIT ?",
                (tenant_id, limit),
            )
        return [dict(row) for row in rows]

    # --------------------------------------------------------------- recovery
    def _policy_restore_snapshot(self) -> dict[str, Any]:
        policy_row = self.store.current_policy()
        return {
            "kind": "POLICY",
            "policy_row": dict(policy_row) if policy_row is not None else None,
            "tenants": [dict(row) for row in self.store.rows("SELECT * FROM tenants ORDER BY tenant_id")],
            "states": [dict(row) for row in self.store.rows("SELECT * FROM tenant_states ORDER BY tenant_id")],
            "last_event_ms": self.store.last_event_ms(),
            "simulation_statuses": {
                row["run_id"]: row["status"]
                for row in self.store.rows("SELECT run_id, status FROM simulation_runs")
            },
        }

    def _restore_policy_snapshot(self, connection: sqlite3.Connection, before: dict[str, Any]) -> int | None:
        policy_row = before.get("policy_row")
        if policy_row is not None:
            connection.execute(
                "DELETE FROM policy_snapshots WHERE version > ?",
                (int(policy_row["version"]),),
            )
            connection.execute(
                """
                INSERT INTO policy_snapshots(version, generated_at_ms, checksum, snapshot_json, created_at_ms)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(version) DO UPDATE SET
                  generated_at_ms=excluded.generated_at_ms,
                  checksum=excluded.checksum,
                  snapshot_json=excluded.snapshot_json,
                  created_at_ms=excluded.created_at_ms
                """,
                (
                    int(policy_row["version"]),
                    int(policy_row["generated_at_ms"]),
                    policy_row["checksum"],
                    policy_row["snapshot_json"],
                    int(policy_row["created_at_ms"]),
                ),
            )
        connection.execute("DELETE FROM tenants")
        for row in before.get("tenants", []):
            connection.execute(
                """
                INSERT INTO tenants(tenant_id, policy_version, capacity_units, rps_units,
                                    burst_units, weight_units, parent_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["tenant_id"], row["policy_version"], row["capacity_units"],
                    row["rps_units"], row["burst_units"], row["weight_units"], row["parent_id"],
                ),
            )
        connection.execute("DELETE FROM tenant_states")
        for row in before.get("states", []):
            self._insert_state_row(connection, row)
        connection.execute(
            "UPDATE clock_state SET last_event_ms=? WHERE singleton_id=1",
            (int(before.get("last_event_ms", 0)),),
        )
        for run_id, status in before.get("simulation_statuses", {}).items():
            connection.execute(
                "UPDATE simulation_runs SET status=? WHERE run_id=?",
                (status, run_id),
            )
        return int(policy_row["version"]) if policy_row is not None else None

    def _recover_pending_locked(self, trigger: str) -> dict[str, Any]:
        pending = self.store.rows(
            "SELECT * FROM intents WHERE status='PENDING' ORDER BY created_at_ms, intent_id"
        )
        if not pending:
            return {
                "report_id": None,
                "trigger": trigger,
                "pending_intents": [],
                "rolled_back_batches": [],
                "aborted_batches": [],
                "restored_policy_versions": [],
                "basis": "sqlite durable intent facts",
                "message": "no pending intents",
            }

        report_pending: list[dict[str, Any]] = []
        rolled_back_batches: list[str] = []
        aborted_batches: set[str] = set()
        restored_versions: list[int] = []
        skipped: list[dict[str, Any]] = []

        with self.store.transaction() as connection:
            for intent in pending:
                before = json.loads(intent["before_state_json"])
                kind = intent["kind"]
                batch_id = intent["batch_id"]
                action = "rollback_applied"
                report_pending.append(
                    {
                        "intent_id": intent["intent_id"],
                        "kind": kind,
                        "batch_id": batch_id,
                        "created_at_ms": int(intent["created_at_ms"]),
                        "status_before_recovery": "PENDING",
                    }
                )
                if kind == "BATCH":
                    current_clock = self.store.last_event_ms()
                    before_clock = int(before.get("last_event_ms", 0))
                    if current_clock > before_clock:
                        skipped.append(
                            {
                                "intent_id": intent["intent_id"],
                                "reason": "later_committed_activity_detected",
                                "current_clock": current_clock,
                                "before_clock": before_clock,
                            }
                        )
                        connection.execute(
                            "UPDATE intents SET status='ABORTED', completed_at_ms=?, error=? WHERE intent_id=?",
                            (current_clock, "later committed activity; recovery did not overwrite", intent["intent_id"]),
                        )
                        continue
                    connection.execute("DELETE FROM events WHERE batch_id=?", (batch_id,))
                    connection.execute("DELETE FROM usage_ledger WHERE batch_id=?", (batch_id,))
                    connection.execute("DELETE FROM batches WHERE batch_id=?", (batch_id,))
                    for state_row in before.get("states", []):
                        self._insert_state_row(connection, state_row)
                    connection.execute(
                        "UPDATE clock_state SET last_event_ms=? WHERE singleton_id=1",
                        (before_clock,),
                    )
                    rolled_back_batches.append(batch_id or intent["intent_id"])
                    if batch_id:
                        aborted_batches.add(batch_id)
                elif kind == "POLICY":
                    restored = self._restore_policy_snapshot(connection, before)
                    if restored is not None:
                        restored_versions.append(restored)
                    action = "policy_restored"
                else:
                    for state_row in before.get("states", []):
                        self._insert_state_row(connection, state_row)
                    action = "state_restored"

                connection.execute(
                    "UPDATE batches SET status='ABORTED', completed_at_ms=? WHERE batch_id=? AND status!='COMPLETED'",
                    (self.store.last_event_ms(), batch_id),
                )
                batch_row = connection.execute(
                    "SELECT status FROM batches WHERE batch_id=?", (batch_id,)
                ).fetchone() if batch_id else None
                if batch_row is not None and batch_row["status"] == "ABORTED":
                    aborted_batches.add(batch_id)
                connection.execute(
                    "UPDATE intents SET status='ABORTED', completed_at_ms=?, error=?, detail_json=? WHERE intent_id=?",
                    (
                        self.store.last_event_ms(),
                        "process terminated before intent completion",
                        json.dumps({"recovery_action": action}, sort_keys=True),
                        intent["intent_id"],
                    ),
                )

            report = {
                "trigger": trigger,
                "recovered_at_ms": self.store.last_event_ms(),
                "pending_intents": report_pending,
                "rolled_back_batches": rolled_back_batches,
                "aborted_batches": sorted(aborted_batches),
                "restored_policy_versions": sorted(set(restored_versions)),
                "skipped": skipped,
                "basis": "sqlite durable intent, batch, event, ledger and state facts",
                "message": "unfinished intents were rolled back and marked aborted",
            }
            cursor = connection.execute(
                "INSERT INTO recovery_reports(recovered_at_ms, report_json) VALUES (?, ?)",
                (self.store.last_event_ms(), json.dumps(report, sort_keys=True)),
            )
            report["report_id"] = cursor.lastrowid
        return report

    def recover_pending(self, trigger: str = "manual") -> dict[str, Any]:
        with self._operation_lock:
            return self._recover_pending_locked(trigger)

    def latest_recovery_report(self) -> dict[str, Any]:
        row = self.store.one(
            "SELECT * FROM recovery_reports ORDER BY report_id DESC LIMIT 1"
        )
        if row is None:
            return {
                "report_id": None,
                "message": "no recovery report persisted",
                "pending_intents": [],
            }
        report = json.loads(row["report_json"])
        report["report_id"] = int(row["report_id"])
        report["persisted_recovered_at_ms"] = int(row["recovered_at_ms"])
        return report

    # ------------------------------------------------------------ intent helpers
    def _create_intent(
        self,
        kind: str,
        before: dict[str, Any],
        batch_id: str | None = None,
        detail: dict[str, Any] | None = None,
        at_ms: int = 0,
    ) -> str:
        intent_id = _uuid("intent")
        with self.store.transaction() as connection:
            connection.execute(
                """
                INSERT INTO intents(intent_id, kind, batch_id, status, before_state_json,
                                    detail_json, created_at_ms)
                VALUES (?, ?, ?, 'PENDING', ?, ?, ?)
                """,
                (
                    intent_id,
                    kind,
                    batch_id,
                    json.dumps(before, sort_keys=True),
                    json.dumps(detail or {}, sort_keys=True),
                    at_ms,
                ),
            )
        return intent_id

    @staticmethod
    def _complete_intent(
        connection: sqlite3.Connection,
        intent_id: str,
        at_ms: int,
        detail: dict[str, Any] | None = None,
    ) -> None:
        connection.execute(
            "UPDATE intents SET status='COMPLETE', completed_at_ms=?, detail_json=? WHERE intent_id=?",
            (at_ms, json.dumps(detail or {}, sort_keys=True), intent_id),
        )

    @staticmethod
    def _insert_state_row(connection: sqlite3.Connection, row: dict[str, Any]) -> None:
        connection.execute(
            """
            INSERT INTO tenant_states(
                tenant_id, tokens_units, last_refill_ms, refill_remainder,
                direct_used_bytes_units, aggregate_used_bytes_units,
                direct_used_request_units, aggregate_used_request_units,
                consecutive_rejections, status, last_reason, suspended_reason, updated_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tenant_id) DO UPDATE SET
                tokens_units=excluded.tokens_units,
                last_refill_ms=excluded.last_refill_ms,
                refill_remainder=excluded.refill_remainder,
                direct_used_bytes_units=excluded.direct_used_bytes_units,
                aggregate_used_bytes_units=excluded.aggregate_used_bytes_units,
                direct_used_request_units=excluded.direct_used_request_units,
                aggregate_used_request_units=excluded.aggregate_used_request_units,
                consecutive_rejections=excluded.consecutive_rejections,
                status=excluded.status,
                last_reason=excluded.last_reason,
                suspended_reason=excluded.suspended_reason,
                updated_at_ms=excluded.updated_at_ms
            """,
            (
                row["tenant_id"], int(row["tokens_units"]), int(row["last_refill_ms"]),
                int(row["refill_remainder"]), int(row["direct_used_bytes_units"]),
                int(row["aggregate_used_bytes_units"]), int(row["direct_used_request_units"]),
                int(row["aggregate_used_request_units"]), int(row["consecutive_rejections"]),
                str(row["status"]), row.get("last_reason"), row.get("suspended_reason"),
                int(row["updated_at_ms"]),
            ),
        )

    # ------------------------------------------------------------ policy writes
    def submit_policy(self, payload: PolicySnapshotIn) -> dict[str, Any]:
        with self._operation_lock:
            self._recover_pending_locked("pre_submit_policy")
            if payload.version < 1:
                raise spec_invalid("policy version must be a positive integer", version=payload.version)
            if payload.generated_at_ms < 0:
                raise spec_invalid("generated_at_ms must be non-negative")
            tenant_values = parse_tenant_values(payload.tenants)
            tenant_candidates = as_tenant_records(tenant_values)
            tenant_docs = [
                tenant_candidates[key].as_policy_dict() for key in sorted(tenant_candidates)
            ]
            computed = policy_checksum(payload.version, payload.generated_at_ms, tenant_docs)
            if payload.checksum is not None and payload.checksum != computed:
                raise spec_invalid(
                    "policy checksum does not match canonical snapshot",
                    provided_checksum=payload.checksum,
                    computed_checksum=computed,
                )
            current_version = self.store.current_version()
            if payload.version <= current_version:
                raise DomainError(
                    "E_STALE_SNAPSHOT",
                    "policy version must be strictly greater than the current version",
                    {
                        "submitted_version": payload.version,
                        "current_version": current_version,
                    },
                    409,
                )

            tenants = validate_tenant_hierarchy(tenant_candidates)
            document = policy_document(payload.version, payload.generated_at_ms, tenants)
            before = self._policy_restore_snapshot()
            at_ms = max(payload.generated_at_ms, self.store.last_event_ms())
            intent_id = self._create_intent(
                "POLICY",
                before,
                detail={"operation": "submitted_policy", "version": payload.version},
                at_ms=at_ms,
            )
            try:
                with self.store.transaction() as connection:
                    connection.execute(
                        """
                        INSERT INTO policy_snapshots(version, generated_at_ms, checksum, snapshot_json, created_at_ms)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            payload.version,
                            payload.generated_at_ms,
                            document["checksum"],
                            json.dumps(document, sort_keys=True),
                            at_ms,
                        ),
                    )
                    connection.execute("DELETE FROM tenants")
                    connection.execute("DELETE FROM tenant_states")
                    for tenant_id in sorted(tenants):
                        tenant = tenants[tenant_id]
                        connection.execute(
                            """
                            INSERT INTO tenants(tenant_id, policy_version, capacity_units, rps_units,
                                                burst_units, weight_units, parent_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                tenant.tenant_id, payload.version, tenant.capacity_units,
                                tenant.rps_units, tenant.burst_units, tenant.weight_units,
                                tenant.parent_id,
                            ),
                        )
                        state = RuntimeState.initial(tenant, at_ms).as_row_dict()
                        self._insert_state_row(connection, state)
                    connection.execute(
                        """
                        INSERT INTO clock_state(singleton_id, last_event_ms) VALUES (1, ?)
                        ON CONFLICT(singleton_id) DO UPDATE SET last_event_ms=excluded.last_event_ms
                        """,
                        (at_ms,),
                    )
                    connection.execute("UPDATE simulation_runs SET status='STALE' WHERE status='CURRENT'")
                    self._complete_intent(connection, intent_id, at_ms, {"version": payload.version})
            except BaseException:
                raise
            return {
                "policy": document,
                "previous_version": current_version,
                "intent_id": intent_id,
                "state_reset": True,
            }

    def patch_policy(self, payload: PolicyPatchIn) -> dict[str, Any]:
        with self._operation_lock:
            self._recover_pending_locked("pre_patch_policy")
            # Spec checks come before stale-base checks by contract.
            if payload.base_version < 1:
                raise spec_invalid("base_version must be positive")
            self._validate_patch_numeric_values(payload)
            if payload.faults.version_rewind:
                raise DomainError(
                    "E_STALE_SNAPSHOT",
                    "injected version rollback",
                    {
                        "fault": "version_rewind",
                        "base_version": payload.base_version,
                        "current_version": self.store.current_version(),
                    },
                    409,
                )
            current_version = self.store.current_version()
            if current_version == 0:
                raise spec_invalid("no current policy to patch")
            if payload.base_version != current_version:
                raise DomainError(
                    "E_STALE_BASE",
                    "incremental edit base version is not current",
                    {
                        "base_version": payload.base_version,
                        "current_version": current_version,
                    },
                    409,
                )
            if not (
                payload.tenant_patches
                or payload.upsert_tenants
                or payload.remove_tenant_ids
            ):
                raise spec_invalid("incremental patch must contain at least one change")

            old_tenants = self._tenants()
            raw: dict[str, dict[str, Any]] = {
                tenant_id: {
                    "tenant_id": tenant.tenant_id,
                    "capacity_bytes": units_to_str(tenant.capacity_units),
                    "requests_per_second": units_to_str(tenant.rps_units),
                    "burst_capacity": units_to_str(tenant.burst_units),
                    "weight": units_to_str(tenant.weight_units),
                    "parent_id": tenant.parent_id,
                }
                for tenant_id, tenant in old_tenants.items()
            }
            changed_ids: set[str] = set()
            for patch in payload.tenant_patches:
                if patch.tenant_id not in old_tenants:
                    raise unknown_tenant(patch.tenant_id, operation="patch_policy")
                item = raw[patch.tenant_id]
                fields = patch.model_fields_set
                if "capacity_bytes" in fields and patch.capacity_bytes is not None:
                    item["capacity_bytes"] = units_to_str(
                        to_units(patch.capacity_bytes, f"tenant_patches[{patch.tenant_id}].capacity_bytes")
                    )
                if "requests_per_second" in fields and patch.requests_per_second is not None:
                    item["requests_per_second"] = units_to_str(
                        to_units(patch.requests_per_second, f"tenant_patches[{patch.tenant_id}].requests_per_second")
                    )
                if "burst_capacity" in fields and patch.burst_capacity is not None:
                    item["burst_capacity"] = units_to_str(
                        to_units(patch.burst_capacity, f"tenant_patches[{patch.tenant_id}].burst_capacity")
                    )
                if "weight" in fields and patch.weight is not None:
                    item["weight"] = units_to_str(
                        to_units(patch.weight, f"tenant_patches[{patch.tenant_id}].weight")
                    )
                if "parent_id" in fields:
                    item["parent_id"] = patch.parent_id
                changed_ids.add(patch.tenant_id)
            for spec in payload.upsert_tenants:
                raw[spec.tenant_id] = json.loads(spec.model_dump_json())
                changed_ids.add(spec.tenant_id)
            for tenant_id in payload.remove_tenant_ids:
                if tenant_id not in old_tenants:
                    raise unknown_tenant(tenant_id, operation="remove_tenant")
                raw.pop(tenant_id, None)
                changed_ids.add(tenant_id)
            if not changed_ids:
                raise spec_invalid("incremental patch did not affect any tenant")

            specs = [TenantSpec(**value) for value in raw.values()]
            new_tenants = parse_tenant_specs(specs)
            new_version = current_version + 1
            current_policy = self._policy_row()
            generated_at_ms = payload.generated_at_ms
            if generated_at_ms is None:
                generated_at_ms = max(
                    int(current_policy["generated_at_ms"]),
                    self.store.last_event_ms(),
                )
            if generated_at_ms < 0:
                raise spec_invalid("generated_at_ms must be non-negative")
            document = policy_document(new_version, generated_at_ms, new_tenants)
            before = self._policy_restore_snapshot()
            old_states = self._states(old_tenants)
            rebuild_ms = max(generated_at_ms, self.store.last_event_ms())
            intent_id = self._create_intent(
                "POLICY",
                before,
                detail={
                    "operation": "incremental_policy_patch",
                    "base_version": current_version,
                    "target_version": new_version,
                    "changed_tenant_ids": sorted(changed_ids),
                },
                at_ms=rebuild_ms,
            )
            affected = self._affected_closure(changed_ids, old_tenants, new_tenants)
            rebuild_states = self._rebuild_states(
                old_tenants=old_tenants,
                new_tenants=new_tenants,
                old_states=old_states,
                rebuild_ms=rebuild_ms,
                new_version=new_version,
            )
            try:
                with self.store.transaction() as connection:
                    connection.execute(
                        """
                        INSERT INTO policy_snapshots(version, generated_at_ms, checksum, snapshot_json, created_at_ms)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            new_version,
                            generated_at_ms,
                            document["checksum"],
                            json.dumps(document, sort_keys=True),
                            rebuild_ms,
                        ),
                    )
                    connection.execute("DELETE FROM tenants")
                    for tenant_id in sorted(new_tenants):
                        tenant = new_tenants[tenant_id]
                        connection.execute(
                            """
                            INSERT INTO tenants(tenant_id, policy_version, capacity_units, rps_units,
                                                burst_units, weight_units, parent_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                tenant.tenant_id, new_version, tenant.capacity_units,
                                tenant.rps_units, tenant.burst_units, tenant.weight_units,
                                tenant.parent_id,
                            ),
                        )
                    connection.execute("DELETE FROM tenant_states")
                    for tenant_id in sorted(rebuild_states):
                        self._insert_state_row(connection, rebuild_states[tenant_id].as_row_dict())
                    connection.execute("UPDATE simulation_runs SET status='STALE' WHERE status='CURRENT'")
                    connection.execute(
                        """
                        INSERT INTO clock_state(singleton_id, last_event_ms) VALUES (1, ?)
                        ON CONFLICT(singleton_id) DO UPDATE SET last_event_ms=excluded.last_event_ms
                        """,
                        (rebuild_ms,),
                    )
                    if payload.faults.crash_during_rebuild:
                        if self.settings.allow_chaos:
                            os._exit(92)
                        raise DomainError(
                            "E_SPEC_INVALID",
                            "rebuild crash injection is disabled outside the chaos worker",
                            {"fault": "crash_during_rebuild", "intent_id": intent_id},
                            422,
                        )
                    if payload.faults.rebuild_failure:
                        raise DomainError(
                            "E_REBUILD_FAILED",
                            "injected rebuild failure; transaction must roll back",
                            {
                                "intent_id": intent_id,
                                "base_version": current_version,
                                "target_version": new_version,
                            },
                            500,
                        )
                    self._complete_intent(
                        connection,
                        intent_id,
                        rebuild_ms,
                        {
                            "version": new_version,
                            "changed_tenant_ids": sorted(changed_ids),
                            "rebuild_scope": sorted(affected),
                        },
                    )
            except BaseException:
                raise
            return {
                "policy": document,
                "base_version": current_version,
                "new_version": new_version,
                "changed_tenant_ids": sorted(changed_ids),
                "affected_tenant_ids": sorted(affected),
                "rebuild": {
                    "usage_views_rebuilt": True,
                    "states_rebuilt": True,
                    "simulations_invalidated": True,
                    "atomic": True,
                },
                "intent_id": intent_id,
            }

    def patch_tenant(self, tenant_id: str, payload: TenantPatchIn) -> dict[str, Any]:
        fields = payload.model_fields_set
        patch = TenantPatch(tenant_id=tenant_id)
        updates: dict[str, Any] = {}
        for name in ("capacity_bytes", "requests_per_second", "burst_capacity", "weight", "parent_id"):
            if name in fields:
                updates[name] = getattr(payload, name)
        patch = TenantPatch(tenant_id=tenant_id, **updates)
        response = self.patch_policy(
            PolicyPatchIn(
                base_version=payload.base_version,
                generated_at_ms=payload.generated_at_ms,
                tenant_patches=[patch],
                faults=payload.faults,
            )
        )
        response["tenant_id"] = tenant_id
        return response

    @staticmethod
    def _validate_patch_numeric_values(payload: PolicyPatchIn) -> None:
        for patch in payload.tenant_patches:
            if not patch.tenant_id.strip():
                raise spec_invalid("tenant_id must not be empty")
            for field in ("capacity_bytes", "requests_per_second", "burst_capacity", "weight"):
                value = getattr(patch, field)
                if value is not None:
                    to_units(value, f"tenant_patches[{patch.tenant_id}].{field}")
            if (
                "parent_id" in patch.model_fields_set
                and patch.parent_id is not None
                and not patch.parent_id.strip()
            ):
                raise spec_invalid(
                    "parent_id must be null or non-empty",
                    tenant_id=patch.tenant_id,
                )
        for spec in payload.upsert_tenants:
            if not spec.tenant_id.strip():
                raise spec_invalid("upsert tenant_id must not be empty")
            for field in ("capacity_bytes", "requests_per_second", "burst_capacity", "weight"):
                to_units(getattr(spec, field), f"upsert_tenants[{spec.tenant_id}].{field}")
        for tenant_id in payload.remove_tenant_ids:
            if not tenant_id.strip():
                raise spec_invalid("remove_tenant_ids must contain non-empty ids")

    @staticmethod
    def _affected_closure(
        changed_ids: set[str],
        old_tenants: dict[str, TenantRecord],
        new_tenants: dict[str, TenantRecord],
    ) -> set[str]:
        affected = set(changed_ids)
        for tenant_id in list(changed_ids):
            affected.update(descendants(tenant_id, old_tenants))
            affected.update(descendants(tenant_id, new_tenants))
        for tenant_id in list(affected):
            if tenant_id in new_tenants:
                affected.update(lineage(tenant_id, new_tenants))
        return affected

    @staticmethod
    def _rebuild_states(
        old_tenants: dict[str, TenantRecord],
        new_tenants: dict[str, TenantRecord],
        old_states: dict[str, RuntimeState],
        rebuild_ms: int,
        new_version: int,
    ) -> dict[str, RuntimeState]:
        direct_bytes = {
            tenant_id: old_states[tenant_id].direct_used_bytes_units
            for tenant_id in new_tenants
            if tenant_id in old_states
        }
        direct_requests = {
            tenant_id: old_states[tenant_id].direct_used_request_units
            for tenant_id in new_tenants
            if tenant_id in old_states
        }
        hierarchy_reset_ids: set[str] = set()
        for tenant_id in sorted(set(old_tenants) & set(new_tenants)):
            if old_tenants[tenant_id].parent_id != new_tenants[tenant_id].parent_id:
                hierarchy_reset_ids.add(tenant_id)
                hierarchy_reset_ids.update(descendants(tenant_id, old_tenants))
                hierarchy_reset_ids.update(descendants(tenant_id, new_tenants))
                hierarchy_reset_ids.update(lineage(tenant_id, old_tenants))
                hierarchy_reset_ids.update(lineage(tenant_id, new_tenants))
        for tenant_id in sorted(set(old_tenants) - set(new_tenants)):
            hierarchy_reset_ids.update(descendants(tenant_id, old_tenants))
            hierarchy_reset_ids.update(lineage(tenant_id, old_tenants))
        rebuilt: dict[str, RuntimeState] = {}
        for tenant_id in sorted(new_tenants):
            tenant = new_tenants[tenant_id]
            subtree = {tenant_id} | descendants(tenant_id, new_tenants)
            byte_sum = CompensatedSum()
            request_sum = CompensatedSum()
            for item in sorted(subtree):
                byte_sum.add(direct_bytes.get(item, 0))
                request_sum.add(direct_requests.get(item, 0))
            aggregate_bytes = byte_sum.value
            aggregate_requests = request_sum.value
            old_state = old_states.get(tenant_id)
            token_parameters_changed = (
                old_state is None
                or tenant_id not in old_tenants
                or old_tenants[tenant_id].rps_units != tenant.rps_units
                or old_tenants[tenant_id].burst_units != tenant.burst_units
                or tenant_id in hierarchy_reset_ids
            )
            if old_state is None:
                state = RuntimeState.initial(tenant, rebuild_ms)
            else:
                state = old_state.copy()
                state.aggregate_used_bytes_units = aggregate_bytes
                state.aggregate_used_request_units = aggregate_requests
                state.updated_at_ms = rebuild_ms
                if token_parameters_changed:
                    state.tokens_units = tenant.burst_units
                    state.refill_remainder = 0
                    state.last_refill_ms = rebuild_ms
                elif rebuild_ms >= state.last_refill_ms:
                    refill(state, tenant, rebuild_ms)
            state.direct_used_bytes_units = direct_bytes.get(tenant_id, 0)
            state.direct_used_request_units = direct_requests.get(tenant_id, 0)
            state.aggregate_used_bytes_units = aggregate_bytes
            state.aggregate_used_request_units = aggregate_requests
            state.consecutive_rejections = 0
            state.last_reason = f"REBUILT_POLICY_v{new_version}"
            if state.status == TenantStatus.SUSPENDED:
                pass
            elif aggregate_bytes >= tenant.capacity_units:
                state.status = TenantStatus.QUOTA_BLOCKED
            elif state.tokens_units <= 0 and tenant.burst_units > 0:
                state.status = TenantStatus.THROTTLED
            else:
                state.status = TenantStatus.ACTIVE
            rebuilt[tenant_id] = state
        return rebuilt

    # --------------------------------------------------------------- admission
    def _validate_events(
        self,
        events: list[EventIn],
        faults: Any | None = None,
        check_global_clock: bool = True,
    ) -> None:
        self._policy_row()
        if not events:
            raise spec_invalid("an event batch must contain at least one event")
        if faults is not None and getattr(faults, "invalid_payload", False):
            raise DomainError(
                "E_SPEC_INVALID",
                "injected invalid payload",
                {
                    "fault": "invalid_payload",
                    "event_index": 0,
                    "field": "payload_bytes",
                    "reason": "payload_bytes must be non-negative",
                },
                422,
            )
        for index, event in enumerate(events):
            to_units(event.payload_bytes, f"events[{index}].payload_bytes")
            to_units(event.request_weight, f"events[{index}].request_weight")
            if event.event_time_ms < 0:
                raise spec_invalid(
                    "event_time_ms must be non-negative",
                    event_index=index,
                )
            if not event.tenant_id:
                raise spec_invalid("tenant_id must not be empty", event_index=index)

        tenants = self._tenants()
        for index, event in enumerate(events):
            if event.tenant_id not in tenants:
                raise unknown_tenant(event.tenant_id, event_index=index)
        if faults is not None and getattr(faults, "unknown_tenant", False):
            raise unknown_tenant("injected-unknown-tenant", event_index=0, fault="unknown_tenant")

        previous_time = self.store.last_event_ms() if check_global_clock else 0
        for index, event in enumerate(events):
            if event.event_time_ms < previous_time:
                raise DomainError(
                    "E_CLOCK_REWIND",
                    "event time moved backwards",
                    {
                        "event_index": index,
                        "event_time_ms": event.event_time_ms,
                        "previous_time_ms": previous_time,
                    },
                    409,
                )
            previous_time = event.event_time_ms
        if faults is not None and getattr(faults, "clock_rewind", False):
            raise DomainError(
                "E_CLOCK_REWIND",
                "injected batch clock rewind",
                {
                    "fault": "clock_rewind",
                    "event_index": min(1, len(events) - 1),
                    "event_time_ms": events[0].event_time_ms - 1,
                    "previous_time_ms": events[0].event_time_ms,
                },
                409,
            )

    def process_batch(
        self,
        payload: BatchIn,
        crash_hook: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        with self._operation_lock:
            self._recover_pending_locked("pre_process_batch")
            batch_id = payload.batch_id or _uuid("batch")
            if not batch_id.strip():
                raise spec_invalid("batch_id must not be empty")
            existing = self.store.one("SELECT * FROM batches WHERE batch_id=?", (batch_id,))
            if existing is not None:
                return self._batch_response(batch_id, idempotent_replay=True)
            self._validate_events(payload.events, payload.faults)
            policy_row = self._policy_row()
            policy_version = int(policy_row["version"])
            tenants = self._tenants()
            affected_ids: set[str] = set()
            for event in payload.events:
                affected_ids.update(lineage(event.tenant_id, tenants))
            before = self.store.state_snapshot(sorted(affected_ids))
            at_ms = payload.events[-1].event_time_ms
            intent_id = self._create_intent(
                "BATCH",
                before,
                batch_id=batch_id,
                detail={
                    "event_count": len(payload.events),
                    "policy_version": policy_version,
                    "faults": payload.faults.model_dump(mode="json"),
                },
                at_ms=at_ms,
            )
            if payload.faults.abort_midway:
                raise DomainError(
                    "E_BATCH_ABORTED",
                    "batch was intentionally aborted after intent persistence",
                    {
                        "fault": "abort_midway",
                        "batch_id": batch_id,
                        "intent_id": intent_id,
                        "committed_events": 0,
                        "pending_recovery_required": True,
                    },
                    409,
                )
            if payload.faults.crash_after_intent:
                if crash_hook is not None:
                    crash_hook(intent_id)
                if not self.settings.allow_chaos:
                    raise DomainError(
                        "E_SPEC_INVALID",
                        "crash injection is disabled; set ALLOW_CHAOS=1 for the local worker only",
                        {"fault": "crash_after_intent", "intent_id": intent_id},
                        422,
                    )
                os._exit(91)

            states = self._states(tenants)
            results, summary = evaluate_batch(
                states=states,
                tenants=tenants,
                events=payload.events,
                last_event_ms=self.store.last_event_ms(),
                throttle_threshold=self.settings.throttle_rejection_threshold,
            )
            summary["policy_version"] = policy_version
            completed_at_ms = payload.events[-1].event_time_ms
            try:
                with self.store.transaction() as connection:
                    connection.execute(
                        """
                        INSERT INTO batches(batch_id, policy_version, status, event_count,
                                            accepted_count, rejected_count, created_at_ms,
                                            completed_at_ms, faults_json, summary_json)
                        VALUES (?, ?, 'COMPLETED', ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            batch_id, policy_version, len(payload.events),
                            summary["accepted_count"], summary["rejected_count"],
                            completed_at_ms, completed_at_ms,
                            json.dumps(payload.faults.model_dump(mode="json"), sort_keys=True),
                            json.dumps(summary, sort_keys=True),
                        ),
                    )
                    for result in results:
                        event = payload.events[result["index"]]
                        connection.execute(
                            """
                            INSERT INTO events(batch_id, event_index, event_time_ms, tenant_id,
                                               meter_mode, io_direction, payload_units,
                                               request_weight_units, accepted, code,
                                               context_json, result_json, policy_version)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                batch_id, result["index"], event.event_time_ms, event.tenant_id,
                                str(event.meter_mode), str(event.io_direction),
                                to_units(event.payload_bytes, "payload_bytes"),
                                to_units(event.request_weight, "request_weight"),
                                int(result["accepted"]), result["code"],
                                json.dumps(result["context"], sort_keys=True),
                                json.dumps(result, sort_keys=True), policy_version,
                            ),
                        )
                        if result["accepted"]:
                            for account in result["lineage"]:
                                connection.execute(
                                    """
                                    INSERT INTO usage_ledger(batch_id, event_index, account_tenant_id,
                                                             source_tenant_id, capacity_units, request_units,
                                                             event_time_ms, policy_version)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        batch_id, result["index"], account["tenant_id"], event.tenant_id,
                                        int(result["charged"]["capacity_bytes_units"]),
                                        int(result["charged"]["request_tokens_units"]),
                                        event.event_time_ms, policy_version,
                                    ),
                                )
                    for tenant_id in sorted(affected_ids):
                        state = states[tenant_id]
                        connection.execute(
                            """
                            UPDATE tenant_states
                            SET tokens_units=?, last_refill_ms=?, refill_remainder=?,
                                direct_used_bytes_units=?, aggregate_used_bytes_units=?,
                                direct_used_request_units=?, aggregate_used_request_units=?,
                                consecutive_rejections=?, status=?, last_reason=?,
                                suspended_reason=?, updated_at_ms=?
                            WHERE tenant_id=?
                            """,
                            (
                                state.tokens_units, state.last_refill_ms, state.refill_remainder,
                                state.direct_used_bytes_units, state.aggregate_used_bytes_units,
                                state.direct_used_request_units, state.aggregate_used_request_units,
                                state.consecutive_rejections, str(state.status), state.last_reason,
                                state.suspended_reason, state.updated_at_ms, tenant_id,
                            ),
                        )
                    connection.execute(
                        """
                        INSERT INTO clock_state(singleton_id, last_event_ms) VALUES (1, ?)
                        ON CONFLICT(singleton_id) DO UPDATE SET last_event_ms=excluded.last_event_ms
                        """,
                        (completed_at_ms,),
                    )
                    self._complete_intent(
                        connection,
                        intent_id,
                        completed_at_ms,
                        {"batch_id": batch_id, "policy_version": policy_version},
                    )
            except BaseException:
                raise
            response = self._batch_response(batch_id)
            response["intent_id"] = intent_id
            return response

    def _batch_response(self, batch_id: str, idempotent_replay: bool = False) -> dict[str, Any]:
        with self.store.read_transaction():
            batch = self.store.one("SELECT * FROM batches WHERE batch_id=?", (batch_id,))
            if batch is None:
                raise DomainError(
                    "E_UNKNOWN_BATCH",
                    "batch is not present in durable storage",
                    {"batch_id": batch_id},
                    404,
                )
            event_rows = self.store.rows(
                "SELECT result_json FROM events WHERE batch_id=? ORDER BY event_index",
                (batch_id,),
            )
        events = [json.loads(row["result_json"]) for row in event_rows]
        summary = json.loads(batch["summary_json"])
        summary["policy_version"] = int(batch["policy_version"])
        return {
            "batch_id": batch_id,
            "status": batch["status"],
            "policy_version": int(batch["policy_version"]),
            "events": events,
            "summary": summary,
            "idempotent_replay": idempotent_replay,
            "accounting": {
                "scale": 6,
                "rounding": "ROUND_FLOOR",
                "note": ROUNDING_NOTE,
                "compensated_aggregation": (
                    "Usage aggregates are rebuilt from integer 1e-6-unit direct facts; "
                    "integer exactness removes cumulative floating-point drift. No binary floating "
                    "point is used for quotas or tokens."
                ),
            },
        }

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        return self._batch_response(batch_id)

    # --------------------------------------------------------------- simulation
    def simulate(self, payload: SimulationIn) -> dict[str, Any]:
        with self._operation_lock:
            policy_row = self._policy_row()
            self._validate_events(
                payload.events,
                check_global_clock=payload.baseline == "CURRENT",
            )
            policy_version = int(policy_row["version"])
            tenants = self._tenants()
            if payload.baseline == "POLICY":
                baseline_ms = int(policy_row["generated_at_ms"])
                states = {
                    tenant_id: RuntimeState.initial(tenant, baseline_ms)
                    for tenant_id, tenant in tenants.items()
                }
            else:
                baseline_ms = self.store.last_event_ms()
                states = {
                    tenant_id: state.copy()
                    for tenant_id, state in self._states(tenants).items()
                }
            state_before = {
                tenant_id: state.as_row_dict() for tenant_id, state in states.items()
            }
            results, summary = evaluate_batch(
                states=states,
                tenants=tenants,
                events=payload.events,
                last_event_ms=baseline_ms,
                throttle_threshold=self.settings.throttle_rejection_threshold,
            )
            state_after = {
                tenant_id: state.as_row_dict() for tenant_id, state in states.items()
            }
            run_id = _uuid("sim")
            result = {
                "run_id": run_id,
                "status": "CURRENT",
                "policy_version": policy_version,
                "baseline": payload.baseline,
                "baseline_ms": baseline_ms,
                "events": results,
                "summary": summary,
                "hypothetical_state_changed": state_after != state_before,
                "actual_state_mutated": False,
                "accounting": {
                    "scale": 6,
                    "rounding": "ROUND_FLOOR",
                    "note": ROUNDING_NOTE,
                },
            }
            at_ms = max(int(policy_row["generated_at_ms"]), self.store.last_event_ms())
            with self.store.transaction() as connection:
                connection.execute(
                    """
                    INSERT INTO simulation_runs(run_id, policy_version, status, label,
                                                 request_json, result_json, created_at_ms)
                    VALUES (?, ?, 'CURRENT', ?, ?, ?, ?)
                    """,
                    (
                        run_id, policy_version, payload.label,
                        json.dumps(payload.model_dump(mode="json"), sort_keys=True),
                        json.dumps(result, sort_keys=True), at_ms,
                    ),
                )
            return result

    def get_simulation(self, run_id: str) -> dict[str, Any]:
        row = self.store.one("SELECT * FROM simulation_runs WHERE run_id=?", (run_id,))
        if row is None:
            raise DomainError(
                "E_UNKNOWN_SIMULATION",
                "simulation run is not present",
                {"run_id": run_id},
                404,
            )
        current_version = self.store.current_version()
        if row["status"] == "STALE" or int(row["policy_version"]) != current_version:
            raise DomainError(
                "E_SIMULATION_STALE",
                "simulation result was invalidated by a newer policy version",
                {
                    "run_id": run_id,
                    "simulation_policy_version": int(row["policy_version"]),
                    "current_policy_version": current_version,
                },
                409,
            )
        result = json.loads(row["result_json"])
        result["status"] = row["status"]
        return result

    # ------------------------------------------------------------ state machine
    def suspend_tenant(self, tenant_id: str, reason: str) -> dict[str, Any]:
        with self._operation_lock:
            self._recover_pending_locked("pre_suspend")
            tenants = self._tenants()
            if tenant_id not in tenants:
                raise unknown_tenant(tenant_id)
            states = self._states(tenants)
            current = states[tenant_id]
            if not transition_allowed(current.status, TenantStatus.SUSPENDED):
                raise DomainError(
                    "E_INVALID_TRANSITION",
                    "requested tenant state transition is not legal",
                    {
                        "tenant_id": tenant_id,
                        "from": str(current.status),
                        "to": str(TenantStatus.SUSPENDED),
                        "legal_targets": sorted(
                            str(target)
                            for target in (
                                TenantStatus.THROTTLED,
                                TenantStatus.QUOTA_BLOCKED,
                                TenantStatus.SUSPENDED,
                            )
                            if transitions_from(current.status, target)
                        ),
                    },
                    409,
                )
            at_ms = max(int(current.updated_at_ms), self.store.last_event_ms())
            before = self.store.state_snapshot([tenant_id])
            intent_id = self._create_intent(
                "STATE",
                before,
                detail={"operation": "suspend", "tenant_id": tenant_id},
                at_ms=at_ms,
            )
            try:
                with self.store.transaction() as connection:
                    connection.execute(
                        """
                        UPDATE tenant_states
                        SET status='SUSPENDED', suspended_reason=?, updated_at_ms=?, last_reason='MANUAL_SUSPEND'
                        WHERE tenant_id=?
                        """,
                        (reason, at_ms, tenant_id),
                    )
                    self._complete_intent(connection, intent_id, at_ms, {"tenant_id": tenant_id})
            except BaseException:
                raise
            return {
                "tenant_id": tenant_id,
                "previous_status": str(current.status),
                "status": "SUSPENDED",
                "intent_id": intent_id,
            }

    def resume_tenant(self, tenant_id: str, at_ms: int | None) -> dict[str, Any]:
        with self._operation_lock:
            self._recover_pending_locked("pre_resume")
            tenants = self._tenants()
            if tenant_id not in tenants:
                raise unknown_tenant(tenant_id)
            states = self._states(tenants)
            current = states[tenant_id]
            if current.status != TenantStatus.SUSPENDED:
                raise DomainError(
                    "E_INVALID_TRANSITION",
                    "only a SUSPENDED tenant can be resumed",
                    {
                        "tenant_id": tenant_id,
                        "from": str(current.status),
                        "to": "ACTIVE",
                    },
                    409,
                )
            resume_at = at_ms if at_ms is not None else max(
                int(current.last_refill_ms), self.store.last_event_ms()
            )
            if resume_at < self.store.last_event_ms():
                raise DomainError(
                    "E_CLOCK_REWIND",
                    "resume timestamp is earlier than the persisted event clock",
                    {
                        "resume_at_ms": resume_at,
                        "last_event_ms": self.store.last_event_ms(),
                    },
                    409,
                )
            chain = lineage(tenant_id, tenants)
            working = {account_id: states[account_id].copy() for account_id in chain}
            for account_id in chain:
                refill(working[account_id], tenants[account_id], resume_at)
            suspended_ancestor = next(
                (
                    account_id
                    for account_id in chain
                    if account_id != tenant_id
                    and working[account_id].status == TenantStatus.SUSPENDED
                ),
                None,
            )
            remaining = (
                tenants[tenant_id].capacity_units
                - working[tenant_id].aggregate_used_bytes_units
            )
            required_tokens = min(UNIT_SCALE, tenants[tenant_id].burst_units)
            reasons: list[str] = []
            if suspended_ancestor is not None:
                reasons.append("ANCESTOR_SUSPENDED")
            if remaining <= 0:
                reasons.append("NO_CAPACITY_REMAINING")
            if required_tokens <= 0 or working[tenant_id].tokens_units < required_tokens:
                reasons.append("TOKEN_RECOVERY_THRESHOLD_NOT_MET")
            if reasons:
                raise DomainError(
                    "E_RECOVERY_CONDITION_NOT_MET",
                    "declared recovery conditions are not satisfied",
                    {
                        "tenant_id": tenant_id,
                        "resume_at_ms": resume_at,
                        "reasons": reasons,
                        "remaining_bytes": units_to_str(max(0, remaining)),
                        "tokens_remaining": units_to_str(working[tenant_id].tokens_units),
                        "recovery_condition": (
                            "no suspended ancestor, remaining capacity > 0, and tokens >= "
                            "min(1 token, burst)"
                        ),
                    },
                    409,
                )
            before = self.store.state_snapshot(chain)
            intent_id = self._create_intent(
                "STATE",
                before,
                detail={"operation": "resume", "tenant_id": tenant_id},
                at_ms=resume_at,
            )
            try:
                with self.store.transaction() as connection:
                    for account_id in chain:
                        state = working[account_id]
                        if account_id == tenant_id:
                            state.status = TenantStatus.ACTIVE
                            state.consecutive_rejections = 0
                            state.last_reason = "MANUAL_RESUME"
                            state.suspended_reason = None
                            state.updated_at_ms = resume_at
                        connection.execute(
                            """
                            UPDATE tenant_states
                            SET tokens_units=?, last_refill_ms=?, refill_remainder=?,
                                status=?, consecutive_rejections=?, last_reason=?,
                                suspended_reason=?, updated_at_ms=?
                            WHERE tenant_id=?
                            """,
                            (
                                state.tokens_units, state.last_refill_ms, state.refill_remainder,
                                str(state.status), state.consecutive_rejections, state.last_reason,
                                state.suspended_reason, state.updated_at_ms, account_id,
                            ),
                        )
                    connection.execute(
                        """
                        INSERT INTO clock_state(singleton_id, last_event_ms) VALUES (1, ?)
                        ON CONFLICT(singleton_id) DO UPDATE SET last_event_ms=excluded.last_event_ms
                        """,
                        (resume_at,),
                    )
                    self._complete_intent(connection, intent_id, resume_at, {"tenant_id": tenant_id})
            except BaseException:
                raise
            return {
                "tenant_id": tenant_id,
                "previous_status": "SUSPENDED",
                "status": "ACTIVE",
                "recovery_evidence": {
                    "resume_at_ms": resume_at,
                    "remaining_bytes": units_to_str(max(0, remaining)),
                    "tokens_remaining": units_to_str(working[tenant_id].tokens_units),
                },
                "intent_id": intent_id,
            }


def transitions_from(source: TenantStatus, target: TenantStatus) -> bool:
    return transition_allowed(source, target)
