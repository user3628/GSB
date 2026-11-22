"""Transactional governance service.

Write path:
    writer lock -> persist intent (own transaction) -> BEGIN IMMEDIATE
    -> check base revision -> mutate business state -> complete intent -> COMMIT

Read path:
    BEGIN DEFERRED -> resolve pinned revision -> read policy/runtime/decisions
    -> COMMIT

A business revision is allocated once per committed write. Both a runtime-state
snapshot and the write's intent-completion marker live in that same transaction.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

from .errors import AppError, ErrorCode
from .migration import checksum_value, plan_migration
from .policy import (
    SCHEMA_V1,
    SCHEMA_V2,
    TOKEN_SCALE,
    Lifecycle,
    PolicySnapshot,
    TenantPolicy,
    build_snapshot,
    deep_merge,
    parse_hierarchical_document,
    parse_policy_document,
    require_int,
    require_str,
    verify_checksum,
    with_checksum,
)
from .storage import (
    Database,
    current_revision,
    current_snapshot_version,
    dumps,
    get_runtime_row,
    list_runtime_rows,
    loads,
    runtime_snapshot_at_revision,
    set_meta,
    snapshot_at_revision,
)

ACTIVE = "ACTIVE"
THROTTLED = "THROTTLED"
QUOTA_BLOCKED = "QUOTA_BLOCKED"
SUSPENDED = "SUSPENDED"
MIGRATING = "MIGRATING"
STATES = {ACTIVE, THROTTLED, QUOTA_BLOCKED, SUSPENDED, MIGRATING}

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    ACTIVE: {THROTTLED, QUOTA_BLOCKED, SUSPENDED, MIGRATING},
    THROTTLED: {ACTIVE, QUOTA_BLOCKED, SUSPENDED, MIGRATING},
    QUOTA_BLOCKED: {ACTIVE, THROTTLED, SUSPENDED, MIGRATING},
    SUSPENDED: {ACTIVE, MIGRATING},
    MIGRATING: {ACTIVE, SUSPENDED},
}

BLOCKING_STATE_CODES = {
    SUSPENDED: ErrorCode.E_TENANT_SUSPENDED,
    MIGRATING: ErrorCode.E_TENANT_MIGRATING,
}


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class DecisionExecution:
    decision: dict[str, Any]
    replayed: bool


class GovernanceService:
    def __init__(
        self,
        db_path: str,
        *,
        crash_after_intent: bool = False,
    ):
        self.db = Database(db_path)
        self.writer_lock = threading.RLock()
        self.crash_after_intent = crash_after_intent

    # ------------------------------------------------------------------ setup
    def startup(self) -> dict[str, Any]:
        self.db.initialize()
        return self.recover_pending_intents()

    # --------------------------------------------------------------- intents
    def _create_intent(self, kind: str, payload: dict[str, Any]) -> tuple[str, int]:
        intent_id = uuid.uuid4().hex
        payload_json = dumps(payload)
        payload_checksum = checksum_value(payload)
        with self.db.write_tx() as conn:
            base_revision = current_revision(conn)
            conn.execute(
                "INSERT INTO intents("
                "intent_id, kind, base_revision, payload_json, payload_checksum, "
                "status, created_at_ms) VALUES(?, ?, ?, ?, ?, 'PENDING', ?)",
                (intent_id, kind, base_revision, payload_json, payload_checksum, now_ms()),
            )
        return intent_id, base_revision

    def _abort_intent(self, intent_id: str, error: AppError) -> None:
        try:
            with self.db.write_tx() as conn:
                conn.execute(
                    "UPDATE intents SET status='ABORTED', completed_at_ms=?, error_json=? "
                    "WHERE intent_id=? AND status='PENDING'",
                    (now_ms(), dumps(error.as_error()), intent_id),
                )
        except Exception:
            # Recovery will find any intent that could not be marked here.
            pass

    def _execute_write(
        self,
        kind: str,
        payload: dict[str, Any],
        callback: Callable[[sqlite3.Connection, int, str, int], Any],
    ) -> Any:
        with self.writer_lock:
            intent_id, base_revision = self._create_intent(kind, payload)
            if self.crash_after_intent:
                try:
                    os.sync() if hasattr(os, "sync") else None
                except Exception:
                    pass
                os._exit(91)
            try:
                with self.db.write_tx() as conn:
                    observed = current_revision(conn)
                    if observed != base_revision:
                        raise AppError(
                            ErrorCode.E_STALE_BASE,
                            "writer revision moved while the intent was pending",
                            {"base_revision": base_revision, "current_revision": observed},
                        )
                    revision = base_revision + 1
                    set_meta(conn, "current_revision", str(revision))
                    result = callback(conn, revision, intent_id, base_revision)
                    conn.execute(
                        "UPDATE intents SET status='COMMITTED', completed_at_ms=?, "
                        "revision=? WHERE intent_id=?",
                        (now_ms(), revision, intent_id),
                    )
                    return result
            except AppError as exc:
                self._abort_intent(intent_id, exc)
                raise
            except Exception as exc:  # pragma: no cover - defensive conversion
                app_error = AppError(
                    ErrorCode.E_INTERNAL,
                    "transaction failed before commit",
                    {"exception_type": type(exc).__name__},
                )
                self._abort_intent(intent_id, app_error)
                raise app_error from exc

    # ---------------------------------------------------------------- states
    @staticmethod
    def _runtime_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "tenant_id": row["tenant_id"],
            "snapshot_version": int(row["snapshot_version"]),
            "status": row["status"],
            "usage_cpu_millicores": int(row["usage_cpu_millicores"]),
            "usage_memory_bytes": int(row["usage_memory_bytes"]),
            "direct_cpu_millicores": int(row["direct_cpu_millicores"]),
            "direct_memory_bytes": int(row["direct_memory_bytes"]),
            "tokens_scaled": int(row["tokens_scaled"]),
            "last_refill_ms": int(row["last_refill_ms"]),
            "state_version": int(row["state_version"]),
            "updated_revision": int(row["updated_revision"]),
        }

    def _upsert_runtime(
        self,
        conn: sqlite3.Connection,
        runtime: dict[str, Any],
        revision: int,
    ) -> None:
        conn.execute(
            "INSERT INTO tenant_runtime("
            "tenant_id, snapshot_version, status, usage_cpu_millicores, "
            "usage_memory_bytes, direct_cpu_millicores, direct_memory_bytes, "
            "tokens_scaled, last_refill_ms, state_version, updated_revision) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(tenant_id) DO UPDATE SET "
            "snapshot_version=excluded.snapshot_version, status=excluded.status, "
            "usage_cpu_millicores=excluded.usage_cpu_millicores, "
            "usage_memory_bytes=excluded.usage_memory_bytes, "
            "direct_cpu_millicores=excluded.direct_cpu_millicores, "
            "direct_memory_bytes=excluded.direct_memory_bytes, "
            "tokens_scaled=excluded.tokens_scaled, last_refill_ms=excluded.last_refill_ms, "
            "state_version=excluded.state_version, updated_revision=excluded.updated_revision",
            (
                runtime["tenant_id"],
                runtime["snapshot_version"],
                runtime["status"],
                runtime["usage_cpu_millicores"],
                runtime["usage_memory_bytes"],
                runtime["direct_cpu_millicores"],
                runtime["direct_memory_bytes"],
                runtime["tokens_scaled"],
                runtime["last_refill_ms"],
                runtime["state_version"],
                revision,
            ),
        )

    def _save_runtime_snapshot(
        self,
        conn: sqlite3.Connection,
        revision: int,
        snapshot_version: int,
        created_at_ms: int,
    ) -> dict[str, Any]:
        entries = [self._runtime_dict(row) for row in list_runtime_rows(conn)]
        payload = {
            "revision": revision,
            "snapshot_version": snapshot_version,
            "tenants": entries,
        }
        digest = checksum_value(payload)
        conn.execute(
            "INSERT INTO runtime_snapshots(revision, snapshot_version, checksum, "
            "state_json, created_at_ms) VALUES(?, ?, ?, ?, ?)",
            (revision, snapshot_version, digest, dumps(payload), created_at_ms),
        )
        return {"checksum": digest, "state": payload}

    def _load_snapshot_row(
        self,
        conn: sqlite3.Connection,
        revision: int,
    ) -> PolicySnapshot:
        row = snapshot_at_revision(conn, revision)
        if row is None:
            raise AppError(
                ErrorCode.E_NO_SNAPSHOT,
                "no policy snapshot has been committed",
                {"revision": revision},
            )
        document = loads(row["document_json"])
        return parse_hierarchical_document(document, verify=True)

    # ------------------------------------------------------- snapshot commits
    def _prepare_snapshot_for_activation(
        self,
        parsed: PolicySnapshot,
        source_checksum: str,
    ) -> PolicySnapshot:
        canonical = build_snapshot(
            parsed.tenants.values(),
            version=parsed.version,
            generated_at_ms=parsed.generated_at_ms,
            source_schema=parsed.source_schema,
        )
        return canonical

    def _activate_snapshot(
        self,
        conn: sqlite3.Connection,
        snapshot: PolicySnapshot,
        *,
        revision: int,
        base_version: int | None,
        source_checksum: str,
        source_schema: str,
        created_at_ms: int,
    ) -> dict[str, Any]:
        current_version = current_snapshot_version(conn)
        if current_version is not None and snapshot.version <= current_version:
            raise AppError(
                ErrorCode.E_VERSION_CONFLICT,
                "snapshot version must be strictly greater than the active version",
                {"current_version": current_version, "submitted_version": snapshot.version},
            )

        existing = {
            row["tenant_id"]: self._runtime_dict(row)
            for row in list_runtime_rows(conn)
        }
        new_ids = set(snapshot.tenants)
        for tenant_id, runtime in existing.items():
            if tenant_id not in new_ids:
                if runtime["direct_cpu_millicores"] or runtime["direct_memory_bytes"]:
                    raise AppError(
                        ErrorCode.E_SPEC_INVALID,
                        "cannot remove a tenant that still owns quota",
                        {
                            "tenant_id": tenant_id,
                            "direct_cpu_millicores": runtime["direct_cpu_millicores"],
                            "direct_memory_bytes": runtime["direct_memory_bytes"],
                        },
                    )
                conn.execute("DELETE FROM tenant_runtime WHERE tenant_id = ?", (tenant_id,))

        document = snapshot.to_dict()
        conn.execute(
            "INSERT INTO policy_snapshots("
            "version, base_version, schema_version, source_schema, generated_at_ms, "
            "source_checksum, checksum, document_json, created_revision, created_at_ms) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                snapshot.version,
                base_version,
                snapshot.schema_version,
                source_schema,
                snapshot.generated_at_ms,
                source_checksum,
                snapshot.checksum,
                dumps(document),
                revision,
                created_at_ms,
            ),
        )

        reconciled: dict[str, dict[str, Any]] = {}
        for tenant_id in snapshot.tenant_ids:
            policy = snapshot.tenants[tenant_id]
            prior = existing.get(tenant_id)
            if prior is None:
                runtime = {
                    "tenant_id": tenant_id,
                    "snapshot_version": snapshot.version,
                    "status": ACTIVE,
                    "usage_cpu_millicores": 0,
                    "usage_memory_bytes": 0,
                    "direct_cpu_millicores": 0,
                    "direct_memory_bytes": 0,
                    "tokens_scaled": policy.rate_limit.initial_tokens * TOKEN_SCALE,
                    "last_refill_ms": max(policy.lifecycle.start_ms, snapshot.generated_at_ms),
                    "state_version": 0,
                }
            else:
                runtime = dict(prior)
                runtime["snapshot_version"] = snapshot.version
                runtime["tokens_scaled"] = min(
                    policy.rate_limit.capacity * TOKEN_SCALE,
                    max(0, runtime["tokens_scaled"]),
                )
                runtime["last_refill_ms"] = max(
                    runtime["last_refill_ms"], snapshot.generated_at_ms
                )
            reconciled[tenant_id] = runtime

        # Rebuild ancestor aggregates from direct ownership under the new hierarchy.
        for runtime in reconciled.values():
            runtime["usage_cpu_millicores"] = 0
            runtime["usage_memory_bytes"] = 0
        for source_id, runtime in reconciled.items():
            for ancestor_id in snapshot.chain(source_id):
                ancestor = reconciled[ancestor_id]
                ancestor["usage_cpu_millicores"] += runtime["direct_cpu_millicores"]
                ancestor["usage_memory_bytes"] += runtime["direct_memory_bytes"]

        for runtime in reconciled.values():
            self._upsert_runtime(conn, runtime, revision)
        runtime_snapshot = self._save_runtime_snapshot(
            conn, revision, snapshot.version, created_at_ms
        )
        return {
            "activation": "REPLACED",
            "version": snapshot.version,
            "base_version": base_version,
            "checksum": snapshot.checksum,
            "source_checksum": source_checksum,
            "source_schema": source_schema,
            "revision": revision,
            "runtime_checksum": runtime_snapshot["checksum"],
        }

    def load_snapshot(
        self,
        document: dict[str, Any],
        *,
        parent_map: dict[str, str | None] | None = None,
    ) -> dict[str, Any]:
        parsed = parse_policy_document(document, parent_map=parent_map, verify=True)
        source_checksum = parsed.checksum
        source_schema = parsed.source_schema
        snapshot = self._prepare_snapshot_for_activation(parsed, source_checksum)
        created_at = now_ms()

        def apply(conn: sqlite3.Connection, revision: int, intent_id: str, base_revision: int):
            active = current_snapshot_version(conn)
            if active is not None and snapshot.version <= active:
                raise AppError(
                    ErrorCode.E_VERSION_CONFLICT,
                    "snapshot version must be greater than the active snapshot",
                    {"current_version": active, "submitted_version": snapshot.version},
                )
            result = self._activate_snapshot(
                conn,
                snapshot,
                revision=revision,
                base_version=active,
                source_checksum=source_checksum,
                source_schema=source_schema,
                created_at_ms=created_at,
            )
            result["snapshot"] = snapshot.to_dict()
            return result

        return self._execute_write(
            "SNAPSHOT_LOAD",
            {"version": snapshot.version, "checksum": snapshot.checksum, "source_schema": source_schema},
            apply,
        )

    def submit_incremental(self, body: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise AppError(ErrorCode.E_SPEC_INVALID, "incremental body must be an object")
        base_version = require_int(body.get("base_version"), "base_version", minimum=1)
        generated_at = require_int(
            body.get("generated_at_ms", now_ms()), "generated_at_ms", minimum=0
        )
        upserts = body.get("upsert", [])
        removals = body.get("remove", [])
        if not isinstance(upserts, list):
            raise AppError(ErrorCode.E_SPEC_INVALID, "upsert must be an array")
        if not isinstance(removals, list) or any(not isinstance(x, str) for x in removals):
            raise AppError(ErrorCode.E_SPEC_INVALID, "remove must be an array of tenant ids")
        if not upserts and not removals:
            raise AppError(ErrorCode.E_SPEC_INVALID, "incremental patch is empty")
        for fragment in upserts:
            if not isinstance(fragment, dict):
                raise AppError(ErrorCode.E_SPEC_INVALID, "upsert entries must be objects")
            require_str(fragment.get("tenant_id"), "tenant_id")

        with self.writer_lock:
            with self.db.read_tx() as conn:
                active_row = snapshot_at_revision(conn, current_revision(conn))
                if active_row is None:
                    raise AppError(ErrorCode.E_NO_SNAPSHOT, "cannot patch without a base snapshot")
                if int(active_row["version"]) != base_version:
                    raise AppError(
                        ErrorCode.E_STALE_BASE,
                        "incremental base_version is not the active snapshot",
                        {
                            "base_version": base_version,
                            "current_version": int(active_row["version"]),
                        },
                    )

            created_at = now_ms()

            def apply(conn: sqlite3.Connection, revision: int, intent_id: str, base_revision: int):
                active_row = snapshot_at_revision(conn, revision)
                if active_row is None:
                    raise AppError(ErrorCode.E_NO_SNAPSHOT, "no active snapshot")
                if int(active_row["version"]) != base_version:
                    raise AppError(
                        ErrorCode.E_STALE_BASE,
                        "incremental base_version became stale",
                        {
                            "base_version": base_version,
                            "current_version": int(active_row["version"]),
                        },
                    )
                current_doc = loads(active_row["document_json"])
                raw_tenants = {
                    item["tenant_id"]: item for item in current_doc["tenants"]
                }
                for fragment in upserts:
                    tenant_id = fragment["tenant_id"]
                    if tenant_id in raw_tenants:
                        raw_tenants[tenant_id] = deep_merge(raw_tenants[tenant_id], fragment)
                    else:
                        raw_tenants[tenant_id] = dict(fragment)
                for tenant_id in removals:
                    if tenant_id not in raw_tenants:
                        raise AppError(
                            ErrorCode.E_SPEC_INVALID,
                            "remove references a tenant that is not in the snapshot",
                            {"tenant_id": tenant_id},
                        )
                    raw_tenants.pop(tenant_id)
                new_document = {
                    "schema_version": SCHEMA_V2,
                    "version": base_version + 1,
                    "generated_at_ms": generated_at,
                    "tenants": [raw_tenants[tid] for tid in sorted(raw_tenants)],
                }
                new_document = with_checksum(new_document)
                parsed = parse_hierarchical_document(new_document, verify=True)
                activated = self._activate_snapshot(
                    conn,
                    parsed,
                    revision=revision,
                    base_version=base_version,
                    source_checksum=parsed.checksum,
                    source_schema=SCHEMA_V2,
                    created_at_ms=created_at,
                )
                activated["snapshot"] = parsed.to_dict()
                return activated

            return self._execute_write(
                "INCREMENTAL_SNAPSHOT",
                {
                    "base_version": base_version,
                    "generated_at_ms": generated_at,
                    "upsert": upserts,
                    "remove": removals,
                },
                apply,
            )

    # -------------------------------------------------------------- decisions
    @staticmethod
    def _normalize_application(payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise AppError(ErrorCode.E_SPEC_INVALID, "application must be an object")
        request_id = require_str(payload.get("request_id"), "request_id")
        tenant_id = require_str(payload.get("tenant_id"), "tenant_id")
        namespace = payload.get("namespace", "shared")
        namespace = require_str(namespace, "namespace")
        arrival_ms = require_int(payload.get("arrival_ms"), "arrival_ms", minimum=0)
        cpu = require_int(payload.get("cpu_millicores"), "cpu_millicores", minimum=0)
        memory = require_int(payload.get("memory_bytes"), "memory_bytes", minimum=0)
        token_cost = require_int(payload.get("token_cost", 1), "token_cost", minimum=1)
        if cpu == 0 and memory == 0:
            raise AppError(
                ErrorCode.E_SPEC_INVALID,
                "application must request CPU or memory",
                {"request_id": request_id},
            )
        return {
            "request_id": request_id,
            "tenant_id": tenant_id,
            "namespace": namespace,
            "arrival_ms": arrival_ms,
            "cpu_millicores": cpu,
            "memory_bytes": memory,
            "token_cost": token_cost,
        }

    @staticmethod
    def _quota_details(
        tenant: TenantPolicy,
        runtime: dict[str, Any],
        cpu: int,
        memory: int,
    ) -> dict[str, Any]:
        remaining_cpu = max(0, tenant.quota.cpu_limit - runtime["usage_cpu_millicores"])
        remaining_mem = max(0, tenant.quota.memory_limit - runtime["usage_memory_bytes"])
        short_cpu = max(0, cpu - remaining_cpu)
        short_mem = max(0, memory - remaining_mem)
        return {
            "tenant_id": tenant.tenant_id,
            "resources": {
                "cpu_millicores": {
                    "requested": cpu,
                    "used": runtime["usage_cpu_millicores"],
                    "base_limit": tenant.quota.cpu_millicores,
                    "burst_limit": tenant.quota.burst_cpu_millicores,
                    "effective_limit": tenant.quota.cpu_limit,
                    "remaining": remaining_cpu,
                    "shortfall": short_cpu,
                },
                "memory_bytes": {
                    "requested": memory,
                    "used": runtime["usage_memory_bytes"],
                    "base_limit": tenant.quota.memory_bytes,
                    "burst_limit": tenant.quota.burst_memory_bytes,
                    "effective_limit": tenant.quota.memory_limit,
                    "remaining": remaining_mem,
                    "shortfall": short_mem,
                },
            },
            "exceeded": short_cpu > 0 or short_mem > 0,
        }

    @staticmethod
    def _token_details(
        tenant: TenantPolicy,
        runtime: dict[str, Any],
        available_scaled: int,
        token_cost: int,
        *,
        scope: str,
        ancestor_chain: tuple[str, ...],
    ) -> dict[str, Any]:
        required = token_cost * TOKEN_SCALE
        return {
            "tenant_id": tenant.tenant_id,
            "scope": scope,
            "ancestor_chain": list(ancestor_chain),
            "tokens": {
                "required_units": required,
                "required": token_cost,
                "available_units": available_scaled,
                "available": available_scaled // TOKEN_SCALE,
                "capacity": tenant.rate_limit.capacity,
                "shortfall_units": max(0, required - available_scaled),
                "shortfall": max(
                    0, (required - available_scaled + TOKEN_SCALE - 1) // TOKEN_SCALE
                ),
                "refill_per_second": tenant.rate_limit.refill_per_second,
                "last_refill_ms": runtime["last_refill_ms"],
            },
        }

    @staticmethod
    def _status_transition_for_rejection(code: str) -> str | None:
        if code in (ErrorCode.E_QUOTA_EXCEEDED, ErrorCode.E_PARENT_QUOTA_EXCEEDED):
            return QUOTA_BLOCKED
        if code == ErrorCode.E_RATE_LIMITED:
            return THROTTLED
        return None

    def _insert_decision(
        self,
        conn: sqlite3.Connection,
        *,
        app: dict[str, Any],
        batch_id: str | None,
        request_checksum: str,
        snapshot_version: int,
        revision: int,
        decision: dict[str, Any],
        created_at_ms: int,
    ) -> dict[str, Any]:
        cursor = conn.execute(
            "INSERT INTO decisions("
            "request_id, request_checksum, batch_id, tenant_id, namespace, "
            "cpu_millicores, memory_bytes, arrival_ms, token_cost, snapshot_version, "
            "decision, code, http_status, result_json, revision, created_at_ms) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?)",
            (
                app["request_id"],
                request_checksum,
                batch_id,
                app["tenant_id"],
                app["namespace"],
                app["cpu_millicores"],
                app["memory_bytes"],
                app["arrival_ms"],
                app["token_cost"],
                snapshot_version,
                decision["decision"],
                decision["code"],
                decision["http_status"],
                revision,
                created_at_ms,
            ),
        )
        decision["decision_id"] = int(cursor.lastrowid)
        conn.execute(
            "UPDATE decisions SET result_json=? WHERE id=?",
            (dumps(decision), decision["decision_id"]),
        )
        return decision

    def _decision_response(
        self,
        *,
        app: dict[str, Any],
        batch_id: str | None,
        snapshot_version: int,
        snapshot_checksum: str,
        request_checksum: str,
        runtime_checksum: str,
        revision: int,
        accepted: bool,
        code: str,
        message: str,
        details: dict[str, Any],
        state_before: str,
        state_after: str,
        token_advances: list[dict[str, Any]],
        usage_changes: list[dict[str, Any]],
        created_at_ms: int,
    ) -> dict[str, Any]:
        return {
            "request_id": app["request_id"],
            "batch_id": batch_id,
            "tenant_id": app["tenant_id"],
            "namespace": app["namespace"],
            "arrival_ms": app["arrival_ms"],
            "request": {
                "cpu_millicores": app["cpu_millicores"],
                "memory_bytes": app["memory_bytes"],
                "token_cost": app["token_cost"],
            },
            "decision": "ACCEPTED" if accepted else "REJECTED",
            "code": "OK" if accepted else code,
            "message": message,
            "details": details,
            "http_status": (
                200 if accepted else AppError(code, message).http_status
            ),
            "snapshot_version": snapshot_version,
            "snapshot_checksum": snapshot_checksum,
            "request_checksum": request_checksum,
            "runtime_checksum": runtime_checksum,
            "revision": revision,
            "state_before": state_before,
            "state_after": state_after,
            "token_advances": token_advances,
            "usage_changes": usage_changes,
            "created_at_ms": created_at_ms,
        }

    def submit_decision(
        self,
        body: dict[str, Any],
        *,
        batch_id: str | None = None,
    ) -> DecisionExecution:
        app = self._normalize_application(body)
        request_checksum = checksum_value(app)

        with self.writer_lock:
            with self.db.read_tx() as conn:
                existing = conn.execute(
                    "SELECT request_checksum, result_json FROM decisions WHERE request_id=?",
                    (app["request_id"],),
                ).fetchone()
            if existing is not None:
                if existing["request_checksum"] != request_checksum:
                    raise AppError(
                        ErrorCode.E_CONFLICTING_REQUEST,
                        "request_id was already used with different content",
                        {
                            "request_id": app["request_id"],
                            "existing_checksum": existing["request_checksum"],
                            "submitted_checksum": request_checksum,
                        },
                    )
                return DecisionExecution(loads(existing["result_json"]), True)

            created_at = now_ms()

            def apply(conn: sqlite3.Connection, revision: int, intent_id: str, base_revision: int):
                snapshot = self._load_snapshot_row(conn, revision)
                try:
                    chain = snapshot.chain(app["tenant_id"])
                except AppError:
                    raise
                runtime = {
                    tenant_id: self._runtime_dict(get_runtime_row(conn, tenant_id))
                    for tenant_id in chain
                    if get_runtime_row(conn, tenant_id) is not None
                }
                missing = [tenant_id for tenant_id in chain if tenant_id not in runtime]
                if missing:
                    raise AppError(
                        ErrorCode.E_SPEC_INVALID,
                        "runtime state is missing for a policy tenant",
                        {"tenant_ids": missing, "snapshot_version": snapshot.version},
                    )

                direct_runtime = runtime[app["tenant_id"]]
                state_before = direct_runtime["status"]

                # State and lifecycle gates are evaluated across the complete chain.
                blocking_state: tuple[str, str] | None = None
                inactive: tuple[str, Lifecycle] | None = None
                for tenant_id in chain:
                    policy = snapshot.tenants[tenant_id]
                    row = runtime[tenant_id]
                    if row["status"] in BLOCKING_STATE_CODES:
                        blocking_state = (tenant_id, row["status"])
                        break
                    if not policy.lifecycle.contains(app["arrival_ms"]):
                        inactive = (tenant_id, policy.lifecycle)
                        break

                invalid_time = next(
                    (
                        tenant_id
                        for tenant_id in chain
                        if app["arrival_ms"] < runtime[tenant_id]["last_refill_ms"]
                    ),
                    None,
                )

                advances: dict[str, dict[str, Any]] = {}
                if blocking_state is None and inactive is None and invalid_time is None:
                    for tenant_id in chain:
                        policy = snapshot.tenants[tenant_id]
                        row = runtime[tenant_id]
                        delta_ms = app["arrival_ms"] - row["last_refill_ms"]
                        refill_units = policy.rate_limit.refill_per_second * delta_ms
                        available = min(
                            policy.rate_limit.capacity * TOKEN_SCALE,
                            row["tokens_scaled"] + refill_units,
                        )
                        advances[tenant_id] = {
                            "tenant_id": tenant_id,
                            "before_units": row["tokens_scaled"],
                            "refill_units": refill_units,
                            "available_units": available,
                            "capacity_units": policy.rate_limit.capacity * TOKEN_SCALE,
                            "delta_ms": delta_ms,
                        }

                code: str | None = None
                message = ""
                details: dict[str, Any] = {}
                if blocking_state is not None:
                    blocking_tenant, status = blocking_state
                    code = BLOCKING_STATE_CODES[status]
                    message = f"tenant {blocking_tenant!r} is {status}"
                    details = {
                        "tenant_id": blocking_tenant,
                        "status": status,
                        "ancestor_chain": list(chain),
                    }
                elif inactive is not None:
                    tenant_id, lifecycle = inactive
                    code = ErrorCode.E_TENANT_INACTIVE
                    message = f"tenant {tenant_id!r} is outside its lifecycle at arrival_ms"
                    details = {
                        "tenant_id": tenant_id,
                        "arrival_ms": app["arrival_ms"],
                        "lifecycle": lifecycle.to_dict(),
                        "ancestor_chain": list(chain),
                    }
                elif invalid_time is not None:
                    code = ErrorCode.E_TIMESTAMP_REGRESSION
                    message = "arrival_ms precedes a token bucket's last refill timestamp"
                    details = {
                        "tenant_id": invalid_time,
                        "arrival_ms": app["arrival_ms"],
                        "last_refill_ms": runtime[invalid_time]["last_refill_ms"],
                    }
                else:
                    direct_quota = self._quota_details(
                        snapshot.tenants[app["tenant_id"]],
                        direct_runtime,
                        app["cpu_millicores"],
                        app["memory_bytes"],
                    )
                    if direct_quota["exceeded"]:
                        code = ErrorCode.E_QUOTA_EXCEEDED
                        message = "tenant quota is insufficient"
                        details = dict(direct_quota, scope="tenant")
                    else:
                        for ancestor_id in chain[1:]:
                            ancestor_quota = self._quota_details(
                                snapshot.tenants[ancestor_id],
                                runtime[ancestor_id],
                                app["cpu_millicores"],
                                app["memory_bytes"],
                            )
                            if ancestor_quota["exceeded"]:
                                code = ErrorCode.E_PARENT_QUOTA_EXCEEDED
                                message = (
                                    f"ancestor {ancestor_id!r} quota is insufficient"
                                )
                                details = dict(
                                    ancestor_quota,
                                    scope="ancestor",
                                    blocking_tenant_id=ancestor_id,
                                    ancestor_chain=list(chain),
                                )
                                break
                    if code is None:
                        direct_tokens = self._token_details(
                            snapshot.tenants[app["tenant_id"]],
                            direct_runtime,
                            advances[app["tenant_id"]]["available_units"],
                            app["token_cost"],
                            scope="tenant",
                            ancestor_chain=chain,
                        )
                        required = app["token_cost"] * TOKEN_SCALE
                        if advances[app["tenant_id"]]["available_units"] < required:
                            code = ErrorCode.E_RATE_LIMITED
                            message = "tenant token bucket is insufficient"
                            details = direct_tokens
                        else:
                            for ancestor_id in chain[1:]:
                                ancestor_tokens = self._token_details(
                                    snapshot.tenants[ancestor_id],
                                    runtime[ancestor_id],
                                    advances[ancestor_id]["available_units"],
                                    app["token_cost"],
                                    scope="ancestor",
                                    ancestor_chain=chain,
                                )
                                if advances[ancestor_id]["available_units"] < required:
                                    code = ErrorCode.E_RATE_LIMITED
                                    message = (
                                        f"ancestor {ancestor_id!r} token bucket is insufficient"
                                    )
                                    details = dict(
                                        ancestor_tokens,
                                        blocking_tenant_id=ancestor_id,
                                    )
                                    break

                accepted = code is None
                state_after = state_before
                usage_changes: list[dict[str, Any]] = []

                if blocking_state is None and inactive is None and invalid_time is None:
                    # Token advancement is persisted for both accepted and quota/rate
                    # rejected requests. Rejections never deduct quota or tokens.
                    if accepted:
                        for tenant_id in chain:
                            runtime[tenant_id]["tokens_scaled"] = (
                                advances[tenant_id]["available_units"]
                                - app["token_cost"] * TOKEN_SCALE
                            )
                            runtime[tenant_id]["last_refill_ms"] = app["arrival_ms"]
                            runtime[tenant_id]["usage_cpu_millicores"] += app["cpu_millicores"]
                            runtime[tenant_id]["usage_memory_bytes"] += app["memory_bytes"]
                            if tenant_id == app["tenant_id"]:
                                runtime[tenant_id]["direct_cpu_millicores"] += app["cpu_millicores"]
                                runtime[tenant_id]["direct_memory_bytes"] += app["memory_bytes"]
                                if runtime[tenant_id]["status"] in (THROTTLED, QUOTA_BLOCKED):
                                    state_after = ACTIVE
                                    runtime[tenant_id]["status"] = ACTIVE
                                    runtime[tenant_id]["state_version"] += 1
                                    conn.execute(
                                        "INSERT INTO state_events("
                                        "tenant_id, from_state, to_state, reason, revision, created_at_ms) "
                                        "VALUES(?, ?, ?, ?, ?, ?)",
                                        (
                                            tenant_id,
                                            state_before,
                                            ACTIVE,
                                            "admission recovered blocked/throttled state",
                                            revision,
                                            created_at_ms,
                                        ),
                                    )
                            usage_changes.append(
                                {
                                    "tenant_id": tenant_id,
                                    "cpu_before": runtime[tenant_id]["usage_cpu_millicores"]
                                    - app["cpu_millicores"],
                                    "cpu_after": runtime[tenant_id]["usage_cpu_millicores"],
                                    "memory_before": runtime[tenant_id]["usage_memory_bytes"]
                                    - app["memory_bytes"],
                                    "memory_after": runtime[tenant_id]["usage_memory_bytes"],
                                }
                            )
                    else:
                        for tenant_id in chain:
                            runtime[tenant_id]["tokens_scaled"] = advances[tenant_id][
                                "available_units"
                            ]
                            runtime[tenant_id]["last_refill_ms"] = app["arrival_ms"]
                        target_state = self._status_transition_for_rejection(code or "")
                        if target_state is not None and state_before != target_state:
                            if target_state in ALLOWED_TRANSITIONS.get(state_before, set()):
                                runtime[app["tenant_id"]]["status"] = target_state
                                runtime[app["tenant_id"]]["state_version"] += 1
                                state_after = target_state
                                conn.execute(
                                    "INSERT INTO state_events("
                                    "tenant_id, from_state, to_state, reason, revision, created_at_ms) "
                                    "VALUES(?, ?, ?, ?, ?, ?)",
                                    (
                                        app["tenant_id"],
                                        state_before,
                                        target_state,
                                        code,
                                        revision,
                                        created_at,
                                    ),
                                )
                    for tenant_id in chain:
                        self._upsert_runtime(conn, runtime[tenant_id], revision)
                else:
                    # Blocking state/lifetime/timestamp decisions do not advance buckets.
                    for tenant_id in chain:
                        self._upsert_runtime(conn, runtime[tenant_id], revision)

                runtime_snapshot = self._save_runtime_snapshot(
                    conn, revision, snapshot.version, created_at
                )

                token_advances = []
                for tenant_id in chain:
                    if tenant_id in advances:
                        after_units = (
                            runtime[tenant_id]["tokens_scaled"]
                            if blocking_state is None and inactive is None and invalid_time is None
                            else advances[tenant_id]["available_units"]
                        )
                        token_advances.append(
                            {
                                "tenant_id": tenant_id,
                                "before_units": advances[tenant_id]["before_units"],
                                "refill_units": advances[tenant_id]["refill_units"],
                                "available_units": advances[tenant_id]["available_units"],
                                "after_units": after_units,
                                "capacity_units": advances[tenant_id]["capacity_units"],
                            }
                        )

                decision = self._decision_response(
                    app=app,
                    batch_id=batch_id,
                    snapshot_version=snapshot.version,
                    snapshot_checksum=snapshot.checksum,
                    request_checksum=request_checksum,
                    runtime_checksum=runtime_snapshot["checksum"],
                    revision=revision,
                    accepted=accepted,
                    code=code or "OK",
                    message=message or "admission accepted",
                    details=details,
                    state_before=state_before,
                    state_after=state_after,
                    token_advances=token_advances,
                    usage_changes=usage_changes,
                    created_at_ms=created_at,
                )
                return self._insert_decision(
                    conn,
                    app=app,
                    batch_id=batch_id,
                    request_checksum=request_checksum,
                    snapshot_version=snapshot.version,
                    revision=revision,
                    decision=decision,
                    created_at_ms=created_at,
                )

            decision = self._execute_write(
                "DECISION",
                {"application": app, "batch_id": batch_id},
                apply,
            )
            return DecisionExecution(decision, False)

    def submit_batch(self, body: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise AppError(ErrorCode.E_SPEC_INVALID, "batch body must be an object")
        batch_id = require_str(body.get("batch_id"), "batch_id")
        applications = body.get("applications")
        if not isinstance(applications, list) or not applications:
            raise AppError(ErrorCode.E_SPEC_INVALID, "applications must be a non-empty array")
        normalized = [self._normalize_application(item) for item in applications]
        items: list[dict[str, Any]] = []
        accepted = 0
        for app in normalized:
            execution = self.submit_decision(app, batch_id=batch_id)
            if execution.decision["decision"] == "ACCEPTED":
                accepted += 1
            items.append(
                {
                    "decision": execution.decision,
                    "replayed": execution.replayed,
                }
            )
        return {
            "batch_id": batch_id,
            "total": len(items),
            "accepted": accepted,
            "rejected": len(items) - accepted,
            "items": items,
        }

    # ----------------------------------------------------------- state machine
    def transition_state(
        self,
        tenant_id: str,
        target_state: str,
        *,
        release: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        tenant_id = require_str(tenant_id, "tenant_id")
        if not isinstance(target_state, str) or target_state not in STATES:
            raise AppError(
                ErrorCode.E_INVALID_TRANSITION,
                "unknown target state",
                {"target_state": target_state, "allowed_states": sorted(STATES)},
            )
        release = release or {}
        if not isinstance(release, dict):
            raise AppError(ErrorCode.E_SPEC_INVALID, "release must be an object")
        release_cpu = require_int(
            release.get("cpu_millicores", 0), "release.cpu_millicores", minimum=0
        )
        release_mem = require_int(
            release.get("memory_bytes", 0), "release.memory_bytes", minimum=0
        )
        created_at = now_ms()

        def apply(conn: sqlite3.Connection, revision: int, intent_id: str, base_revision: int):
            snapshot = self._load_snapshot_row(conn, revision)
            if tenant_id not in snapshot.tenants:
                raise AppError(
                    ErrorCode.E_TENANT_NOT_FOUND,
                    "tenant is not present in the active snapshot",
                    {"tenant_id": tenant_id},
                )
            row = get_runtime_row(conn, tenant_id)
            if row is None:
                raise AppError(
                    ErrorCode.E_TENANT_NOT_FOUND,
                    "tenant runtime state is not present",
                    {"tenant_id": tenant_id},
                )
            runtime = self._runtime_dict(row)
            from_state = runtime["status"]
            if target_state == from_state or target_state not in ALLOWED_TRANSITIONS.get(
                from_state, set()
            ):
                raise AppError(
                    ErrorCode.E_INVALID_TRANSITION,
                    f"transition {from_state} -> {target_state} is not allowed",
                    {
                        "tenant_id": tenant_id,
                        "from_state": from_state,
                        "target_state": target_state,
                        "allowed": sorted(ALLOWED_TRANSITIONS.get(from_state, set())),
                    },
                )
            chain = snapshot.chain(tenant_id)
            chain_rows = {
                tid: self._runtime_dict(get_runtime_row(conn, tid)) for tid in chain
            }
            if release_cpu or release_mem:
                if runtime["direct_cpu_millicores"] < release_cpu or (
                    runtime["direct_memory_bytes"] < release_mem
                ):
                    raise AppError(
                        ErrorCode.E_SPEC_INVALID,
                        "release exceeds the tenant's direct usage",
                        {
                            "tenant_id": tenant_id,
                            "direct_cpu_millicores": runtime["direct_cpu_millicores"],
                            "direct_memory_bytes": runtime["direct_memory_bytes"],
                            "release_cpu_millicores": release_cpu,
                            "release_memory_bytes": release_mem,
                        },
                    )
                for tid in chain:
                    if (
                        chain_rows[tid]["usage_cpu_millicores"] < release_cpu
                        or chain_rows[tid]["usage_memory_bytes"] < release_mem
                    ):
                        raise AppError(
                            ErrorCode.E_SPEC_INVALID,
                            "release would make an ancestor aggregate negative",
                            {"tenant_id": tid, "ancestor_chain": list(chain)},
                        )
                runtime["direct_cpu_millicores"] -= release_cpu
                runtime["direct_memory_bytes"] -= release_mem
                for tid in chain:
                    chain_rows[tid]["usage_cpu_millicores"] -= release_cpu
                    chain_rows[tid]["usage_memory_bytes"] -= release_mem
                    self._upsert_runtime(conn, chain_rows[tid], revision)
                # Keep the direct tenant's single working object authoritative;
                # otherwise the final status upsert would overwrite the release.
                runtime = chain_rows[tenant_id]

            runtime["status"] = target_state
            runtime["state_version"] += 1
            self._upsert_runtime(conn, runtime, revision)
            conn.execute(
                "INSERT INTO state_events("
                "tenant_id, from_state, to_state, reason, revision, created_at_ms) "
                "VALUES(?, ?, ?, ?, ?, ?)",
                (tenant_id, from_state, target_state, reason, revision, created_at),
            )
            runtime_snapshot = self._save_runtime_snapshot(
                conn, revision, snapshot.version, created_at
            )
            return {
                "tenant_id": tenant_id,
                "from_state": from_state,
                "to_state": target_state,
                "state_version": runtime["state_version"],
                "released": {
                    "cpu_millicores": release_cpu,
                    "memory_bytes": release_mem,
                },
                "snapshot_version": snapshot.version,
                "snapshot_checksum": snapshot.checksum,
                "runtime_checksum": runtime_snapshot["checksum"],
                "revision": revision,
            }

        return self._execute_write(
            "STATE_TRANSITION",
            {
                "tenant_id": tenant_id,
                "target_state": target_state,
                "release": release,
                "reason": reason,
            },
            apply,
        )

    # --------------------------------------------------------------- migration
    def migrate_legacy(self, body: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise AppError(ErrorCode.E_SPEC_INVALID, "migration body must be an object")
        legacy = body.get("legacy")
        if not isinstance(legacy, dict):
            raise AppError(ErrorCode.E_SPEC_INVALID, "legacy must be a policy document")
        parent_map = body.get("parent_map") or {}
        if not isinstance(parent_map, dict):
            raise AppError(ErrorCode.E_SPEC_INVALID, "parent_map must be an object")
        confirm_overlap = body.get("confirm_lifecycle_overlap", False)
        if not isinstance(confirm_overlap, bool):
            raise AppError(
                ErrorCode.E_SPEC_INVALID, "confirm_lifecycle_overlap must be boolean"
            )
        generated_at = require_int(
            body.get("generated_at_ms", legacy.get("generated_at_ms", now_ms())),
            "generated_at_ms",
            minimum=0,
        )
        requested_base = body.get("base_version")
        if requested_base is not None:
            requested_base = require_int(requested_base, "base_version", minimum=0)

        with self.writer_lock:
            with self.db.read_tx() as conn:
                current_row = snapshot_at_revision(conn, current_revision(conn))
                active_version = int(current_row["version"]) if current_row is not None else 0
                if requested_base is not None and requested_base != active_version:
                    raise AppError(
                        ErrorCode.E_STALE_BASE,
                        "migration base_version is not the active snapshot",
                        {"base_version": requested_base, "current_version": active_version},
                    )
                current_snapshot = (
                    parse_hierarchical_document(loads(current_row["document_json"]), verify=True)
                    if current_row is not None
                    else None
                )
            plan = plan_migration(
                legacy,
                current_snapshot=current_snapshot,
                parent_map=parent_map,
                confirm_lifecycle_overlap=confirm_overlap,
                generated_at_ms=generated_at,
            )
            migration_id = uuid.uuid4().hex
            input_checksum = legacy.get("checksum") or checksum_value(legacy)
            created_at = now_ms()

            def apply(conn: sqlite3.Connection, revision: int, intent_id: str, base_revision: int):
                current_at_commit = current_snapshot_version(conn)
                expected_current = current_snapshot.version if current_snapshot else None
                if current_at_commit != expected_current:
                    raise AppError(
                        ErrorCode.E_STALE_BASE,
                        "snapshot changed while migration intent was pending",
                        {
                            "expected_version": expected_current,
                            "current_version": current_at_commit,
                        },
                    )
                output_version = None
                if plan.status == "APPLIED":
                    assert plan.target_tenants is not None
                    assert plan.output_version is not None
                    target_snapshot = build_snapshot(
                        plan.target_tenants.values(),
                        version=plan.output_version,
                        generated_at_ms=generated_at,
                        source_schema=SCHEMA_V1,
                    )
                    self._activate_snapshot(
                        conn,
                        target_snapshot,
                        revision=revision,
                        base_version=current_at_commit,
                        source_checksum=legacy["checksum"],
                        source_schema=SCHEMA_V1,
                        created_at_ms=created_at,
                    )
                    output_version = target_snapshot.version
                else:
                    self._save_runtime_snapshot(
                        conn,
                        revision,
                        current_at_commit or 0,
                        created_at,
                    )

                report = dict(plan.report)
                report.pop("report_checksum", None)
                report["migration_id"] = migration_id
                report["revision"] = revision
                report["output_version"] = output_version
                report["report_checksum"] = checksum_value(report)
                report_json = dumps(report)
                conn.execute(
                    "INSERT INTO migration_records("
                    "id, base_version, status, code, input_checksum, input_json, "
                    "output_version, report_json, report_checksum, revision, created_at_ms) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        migration_id,
                        current_snapshot.version if current_snapshot else 0,
                        plan.status,
                        plan.code,
                        input_checksum,
                        dumps(legacy),
                        output_version,
                        report_json,
                        report["report_checksum"],
                        revision,
                        created_at,
                    ),
                )
                return report

            report = self._execute_write(
                "MIGRATION",
                {
                    "migration_id": migration_id,
                    "base_version": current_snapshot.version if current_snapshot else 0,
                    "confirm_lifecycle_overlap": confirm_overlap,
                    "legacy_checksum": legacy.get("checksum"),
                },
                apply,
            )
            return report

    # ------------------------------------------------------------------- reads
    def _read_revision(self, conn: sqlite3.Connection, revision: int | None) -> int:
        return current_revision(conn) if revision is None else revision

    def current_snapshot(self) -> dict[str, Any]:
        with self.db.read_tx() as conn:
            revision = current_revision(conn)
            row = snapshot_at_revision(conn, revision)
            if row is None:
                raise AppError(ErrorCode.E_NO_SNAPSHOT, "no policy snapshot has been committed")
            runtime_row = runtime_snapshot_at_revision(conn, revision)
            return {
                "revision": revision,
                "version": int(row["version"]),
                "base_version": row["base_version"],
                "schema_version": row["schema_version"],
                "source_schema": row["source_schema"],
                "source_checksum": row["source_checksum"],
                "checksum": row["checksum"],
                "created_revision": int(row["created_revision"]),
                "runtime_checksum": runtime_row["checksum"] if runtime_row else None,
                "snapshot": loads(row["document_json"]),
            }

    def list_tenants(self, *, revision: int | None = None) -> dict[str, Any]:
        with self.db.read_tx() as conn:
            pinned = self._read_revision(conn, revision)
            runtime_row = runtime_snapshot_at_revision(conn, pinned)
            snapshot_row = snapshot_at_revision(conn, pinned)
            if runtime_row is None or snapshot_row is None:
                if pinned == 0:
                    return {
                        "revision": 0,
                        "snapshot_version": None,
                        "tenants": [],
                    }
                raise AppError(
                    ErrorCode.E_REVISION_NOT_FOUND,
                    "no complete runtime snapshot exists at this revision",
                    {"revision": pinned},
                )
            payload = loads(runtime_row["state_json"])
            snapshot = parse_hierarchical_document(
                loads(snapshot_row["document_json"]), verify=True
            )
            runtime_by_id = {item["tenant_id"]: item for item in payload["tenants"]}
            tenants: list[dict[str, Any]] = []
            for tenant_id in snapshot.tenant_ids:
                policy = snapshot.tenants[tenant_id]
                runtime = runtime_by_id.get(tenant_id)
                if runtime is None:
                    raise AppError(
                        ErrorCode.E_INTERNAL,
                        "runtime snapshot is missing a policy tenant",
                        {"tenant_id": tenant_id, "revision": pinned},
                    )
                tenants.append(
                    {
                        "tenant_id": tenant_id,
                        "parent_id": policy.parent_id,
                        "status": runtime["status"],
                        "state_version": runtime["state_version"],
                        "quota": {
                            "cpu_millicores": policy.quota.cpu_millicores,
                            "memory_bytes": policy.quota.memory_bytes,
                            "burst_cpu_millicores": policy.quota.burst_cpu_millicores,
                            "burst_memory_bytes": policy.quota.burst_memory_bytes,
                            "effective_cpu_limit": policy.quota.cpu_limit,
                            "effective_memory_limit": policy.quota.memory_limit,
                        },
                        "usage": {
                            "cpu_millicores": runtime["usage_cpu_millicores"],
                            "memory_bytes": runtime["usage_memory_bytes"],
                            "direct_cpu_millicores": runtime["direct_cpu_millicores"],
                            "direct_memory_bytes": runtime["direct_memory_bytes"],
                        },
                        "remaining": {
                            "cpu_millicores": max(
                                0,
                                policy.quota.cpu_limit
                                - runtime["usage_cpu_millicores"],
                            ),
                            "memory_bytes": max(
                                0,
                                policy.quota.memory_limit
                                - runtime["usage_memory_bytes"],
                            ),
                        },
                        "token_bucket": {
                            "refill_per_second": policy.rate_limit.refill_per_second,
                            "capacity": policy.rate_limit.capacity,
                            "initial_tokens": policy.rate_limit.initial_tokens,
                            "tokens_scaled": runtime["tokens_scaled"],
                            "available_tokens": runtime["tokens_scaled"] // TOKEN_SCALE,
                            "last_refill_ms": runtime["last_refill_ms"],
                        },
                        "lifecycle": policy.lifecycle.to_dict(),
                    }
                )
            return {
                "revision": pinned,
                "snapshot_version": snapshot.version,
                "snapshot_checksum": snapshot.checksum,
                "runtime_checksum": runtime_row["checksum"],
                "tenants": tenants,
            }

    def list_decisions(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        revision: int | None = None,
    ) -> dict[str, Any]:
        page = require_int(page, "page", minimum=1)
        page_size = require_int(page_size, "page_size", minimum=1, maximum=1000)
        with self.db.read_tx() as conn:
            pinned = self._read_revision(conn, revision)
            total = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM decisions WHERE revision <= ?", (pinned,)
                ).fetchone()["n"]
            )
            offset = (page - 1) * page_size
            rows = list(
                conn.execute(
                    "SELECT id, result_json FROM decisions WHERE revision <= ? "
                    "ORDER BY id ASC LIMIT ? OFFSET ?",
                    (pinned, page_size, offset),
                )
            )
            snapshot_row = snapshot_at_revision(conn, pinned)
            return {
                "revision": pinned,
                "snapshot_version": (
                    int(snapshot_row["version"]) if snapshot_row is not None else None
                ),
                "page": page,
                "page_size": page_size,
                "total": total,
                "pages": (total + page_size - 1) // page_size,
                "items": [loads(row["result_json"]) for row in rows],
            }

    def get_decision(self, request_id: str, *, revision: int | None = None) -> dict[str, Any]:
        request_id = require_str(request_id, "request_id")
        with self.db.read_tx() as conn:
            pinned = self._read_revision(conn, revision)
            row = conn.execute(
                "SELECT result_json FROM decisions WHERE request_id=? AND revision<=?",
                (request_id, pinned),
            ).fetchone()
            if row is None:
                raise AppError(
                    ErrorCode.E_NOT_FOUND,
                    "decision was not found at the requested revision",
                    {"request_id": request_id, "revision": pinned},
                )
            return loads(row["result_json"])

    def get_migration(self, migration_id: str) -> dict[str, Any]:
        migration_id = require_str(migration_id, "migration_id")
        with self.db.read_tx() as conn:
            row = conn.execute(
                "SELECT * FROM migration_records WHERE id=?", (migration_id,)
            ).fetchone()
            if row is None:
                raise AppError(
                    ErrorCode.E_NOT_FOUND,
                    "migration record not found",
                    {"migration_id": migration_id},
                )
            report = loads(row["report_json"])
            check = dict(report)
            supplied = check.pop("report_checksum", None)
            actual = checksum_value(check)
            return {
                "migration_id": migration_id,
                "status": row["status"],
                "code": row["code"],
                "base_version": row["base_version"],
                "output_version": row["output_version"],
                "input_checksum": row["input_checksum"],
                "report_checksum": row["report_checksum"],
                "replayable": supplied == actual == row["report_checksum"],
                "revision": row["revision"],
                "report": report,
            }

    def replay_migration(self, migration_id: str) -> dict[str, Any]:
        """Audit replay: verify stored input/report digests, change no state."""
        migration_id = require_str(migration_id, "migration_id")
        with self.db.read_tx() as conn:
            row = conn.execute(
                "SELECT * FROM migration_records WHERE id=?", (migration_id,)
            ).fetchone()
            if row is None:
                raise AppError(
                    ErrorCode.E_NOT_FOUND,
                    "migration record not found",
                    {"migration_id": migration_id},
                )
            legacy = loads(row["input_json"])
            report = loads(row["report_json"])
            try:
                input_valid = verify_checksum(legacy, required=True) == row["input_checksum"]
            except AppError:
                input_valid = False
            check = dict(report)
            supplied = check.pop("report_checksum", None)
            report_valid = supplied == checksum_value(check) == row["report_checksum"]
            return {
                "migration_id": migration_id,
                "status": row["status"],
                "code": row["code"],
                "base_version": row["base_version"],
                "output_version": row["output_version"],
                "input_checksum": row["input_checksum"],
                "input_checksum_valid": input_valid,
                "report_checksum": row["report_checksum"],
                "report_checksum_valid": report_valid,
                "replayed": input_valid and report_valid,
                "replay_mode": "AUDIT_ONLY_NO_STATE_CHANGE",
                "revision": row["revision"],
                "report": report,
            }

    def list_migrations(self, *, limit: int = 50) -> dict[str, Any]:
        limit = require_int(limit, "limit", minimum=1, maximum=500)
        with self.db.read_tx() as conn:
            rows = list(
                conn.execute(
                    "SELECT id, status, code, base_version, output_version, revision, "
                    "created_at_ms FROM migration_records ORDER BY revision DESC LIMIT ?",
                    (limit,),
                )
            )
            return {"items": [dict(row) for row in rows]}

    def get_latest_recovery(self) -> dict[str, Any]:
        with self.db.read_tx() as conn:
            row = conn.execute(
                "SELECT * FROM recovery_runs ORDER BY finished_at_ms DESC, rowid DESC LIMIT 1"
            ).fetchone()
            if row is None:
                raise AppError(ErrorCode.E_NOT_FOUND, "no recovery run has been recorded")
            report = loads(row["report_json"])
            check = dict(report)
            supplied = check.pop("report_checksum", None)
            return {
                "recovery_id": row["id"],
                "report_checksum": row["report_checksum"],
                "replayable": supplied == checksum_value(check) == row["report_checksum"],
                "report": report,
            }

    # ---------------------------------------------------------------- recovery
    def recover_pending_intents(self) -> dict[str, Any]:
        started = now_ms()
        with self.writer_lock:
            with self.db.write_tx() as conn:
                revision = current_revision(conn)
                snap_version = current_snapshot_version(conn)
                pending = list(
                    conn.execute(
                        "SELECT * FROM intents WHERE status='PENDING' ORDER BY created_at_ms, rowid"
                    )
                )
                runtime_row = runtime_snapshot_at_revision(conn, revision)
                runtime_checksum = runtime_row["checksum"] if runtime_row is not None else None
                facts = []
                recovery_id = uuid.uuid4().hex
                for row in pending:
                    observed = current_revision(conn)
                    fact = {
                        "intent_id": row["intent_id"],
                        "kind": row["kind"],
                        "base_revision": int(row["base_revision"]),
                        "observed_revision": observed,
                        "payload_checksum": row["payload_checksum"],
                        "action": "MARKED_ABORTED",
                        "business_state_rollback_required": False,
                        "fact": (
                            "The intent marker was durable, but its business transaction "
                            "was not committed. SQLite atomicity leaves the last complete "
                            "revision intact."
                        ),
                    }
                    facts.append(fact)
                    conn.execute(
                        "UPDATE intents SET status='ABORTED', completed_at_ms=?, "
                        "error_json=?, recovery_run_id=? WHERE intent_id=?",
                        (
                            now_ms(),
                            dumps(
                                {
                                    "code": "E_RECOVERED_ABORTED",
                                    "message": "unfinished intent aborted during startup recovery",
                                }
                            ),
                            recovery_id,
                            row["intent_id"],
                        ),
                    )
                finished = now_ms()
                report: dict[str, Any] = {
                    "recovery_id": recovery_id,
                    "started_at_ms": started,
                    "finished_at_ms": finished,
                    "pending_found": len(pending),
                    "aborted_count": len(pending),
                    "last_complete_revision": revision,
                    "last_complete_snapshot_version": snap_version,
                    "last_complete_runtime_checksum": runtime_checksum,
                    "facts": facts,
                    "conclusion": (
                        "NO_PENDING_INTENTS"
                        if not pending
                        else "UNCOMMITTED_INTENTS_ABORTED_LAST_COMPLETE_REVISION_PRESERVED"
                    ),
                }
                report["report_checksum"] = checksum_value(report)
                conn.execute(
                    "INSERT INTO recovery_runs("
                    "id, started_at_ms, finished_at_ms, pending_found, aborted_count, "
                    "revision, snapshot_version, report_json, report_checksum) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        recovery_id,
                        started,
                        finished,
                        len(pending),
                        len(pending),
                        revision,
                        snap_version,
                        dumps(report),
                        report["report_checksum"],
                    ),
                )
                return report

    # ------------------------------------------------------------- misc query
    def meta(self) -> dict[str, Any]:
        with self.db.read_tx() as conn:
            return {
                "revision": current_revision(conn),
                "snapshot_version": current_snapshot_version(conn),
            }
