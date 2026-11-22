"""Quota admission, batching, scheduling, preemption and recovery service."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .accounting import (
    assert_consistent,
    find_inconsistencies,
    group_usage,
    group_usage_map,
    node_usage_map,
)
from .batching import RequestPlanInput, plan_group
from .db import Database, WriteContext, json_dumps, row_to_dict
from .errors import (
    E_BATCH_NOT_FOUND,
    E_BATCH_UNPLACEABLE,
    E_EXEC_QUEUE_FULL,
    E_INVALID_TRANSITION,
    E_MODEL_UNKNOWN,
    E_NODE_UNHEALTHY,
    E_NODE_UNKNOWN,
    E_QUOTA_EXCEEDED,
    E_QUOTA_INCONSISTENT,
    E_REQUEST_NOT_FOUND,
    E_ROUND_NOT_FOUND,
    E_SPEC_INVALID,
    E_STATE_CONFLICT,
    E_VALIDATION,
    SchedulerError,
    spec_error,
)
from .specs import (
    EngineConfig,
    ModelSpec,
    NodeSpec,
    QuotaGroupSpec,
    RequestSpec,
    load_bundle,
    parse_millis,
    request_from_payload,
    spec_json,
)
from .state_machine import (
    ADMITTED,
    BATCHED,
    BATCH_ABORTED,
    BATCH_COMPLETED,
    BATCH_PLANNED,
    BATCH_PREEMPTED,
    BATCH_QUEUED,
    BATCH_RUNNING,
    COMPLETED,
    PENDING,
    PREEMPTED,
    REJECTED,
    RUNNING,
    ensure_transition,
)
from .units import checked_product


class QuotaScheduler:
    def __init__(
        self,
        database_path: str | Path,
        config: Mapping[str, Any] | None = None,
        *,
        ensure_schema: bool = True,
    ) -> None:
        self.db = Database(database_path)
        self._config_overrides: dict[str, Any] = dict(config or {})
        if ensure_schema:
            self.db.ensure_schema()
        self.config = self._load_config()

    @property
    def database_path(self) -> str:
        return self.db.path

    def _load_config(self) -> EngineConfig:
        try:
            with self.db.reader() as conn:
                row = conn.execute(
                    "SELECT value FROM meta WHERE key = 'config_json'"
                ).fetchone()
        except sqlite3.Error:
            row = None
        persisted: Mapping[str, Any] | None = None
        if row is not None:
            try:
                loaded = json.loads(row["value"])
                persisted = loaded if isinstance(loaded, dict) else None
            except json.JSONDecodeError:
                persisted = None
        return EngineConfig.from_mapping(persisted).merged(self._config_overrides)

    def update_config(self, overrides: Mapping[str, Any]) -> dict[str, Any]:
        merged = self.config.merged(overrides).to_dict()
        with self.db.operation("UPDATE_CONFIG", payload=merged) as tx:
            tx.conn.execute(
                "INSERT INTO meta(key, value) VALUES ('config_json', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (json_dumps(merged),),
            )
            assert_consistent(tx.conn, context="update_config")
        self.config = EngineConfig.from_mapping(merged)
        return merged

    # ------------------------------------------------------------------
    # Sample loading and deterministic bootstrap
    # ------------------------------------------------------------------
    def bootstrap(
        self,
        sample_dir: str | Path,
        *,
        load_requests: bool = False,
        auto_admit: bool = False,
    ) -> dict[str, Any]:
        bundle = load_bundle(sample_dir)
        config = bundle.config.merged(self._config_overrides)
        errors: list[dict[str, Any]] = []
        with self.db.operation(
            "BOOTSTRAP",
            payload={
                "sample_dir": str(Path(sample_dir).resolve()),
                "groups": len(bundle.quota_groups),
                "nodes": len(bundle.nodes),
                "models": len(bundle.models),
            },
        ) as tx:
            self._upsert_specs(tx, bundle.quota_groups, bundle.nodes, bundle.models)
            tx.conn.execute(
                "INSERT INTO meta(key, value) VALUES ('config_json', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (json_dumps(config.to_dict()),),
            )
            assert_consistent(tx.conn, context="bootstrap")
        self.config = config
        loaded_requests: list[dict[str, Any]] = []
        if load_requests:
            for spec in bundle.requests:
                try:
                    loaded_requests.append(
                        self.submit_request(spec.raw, auto_admit=auto_admit)
                    )
                except SchedulerError as exc:
                    errors.append(
                        {
                            "request_id": spec.request_id,
                            **exc.to_dict(),
                        }
                    )
        return {
            "ok": not errors,
            "database": self.database_path,
            "sample_dir": str(Path(sample_dir).resolve()),
            "quota_groups": len(bundle.quota_groups),
            "nodes": len(bundle.nodes),
            "models": len(bundle.models),
            "requests_loaded": len(loaded_requests),
            "request_errors": errors,
            "config": self.config.to_dict(),
        }

    def _upsert_specs(
        self,
        tx: WriteContext,
        groups: Sequence[QuotaGroupSpec],
        nodes: Sequence[NodeSpec],
        models: Sequence[ModelSpec],
    ) -> None:
        conn = tx.conn
        for group in groups:
            row = conn.execute(
                "SELECT * FROM quota_groups WHERE group_id = ?", (group.group_id,)
            ).fetchone()
            if row is not None:
                if (
                    int(row["memory_limit_bytes"]) != group.memory_limit_bytes
                    or int(row["compute_limit_millis"]) != group.compute_limit_millis
                ):
                    raise SchedulerError(
                        E_STATE_CONFLICT,
                        f"quota group {group.group_id!r} already exists with different limits",
                        field=f"quota_groups[{group.group_id}]",
                        details={
                            "persisted": row_to_dict(row),
                            "sample": spec_json(group),
                        },
                    )
                continue
            conn.execute(
                "INSERT INTO quota_groups(group_id, memory_limit_bytes, compute_limit_millis) "
                "VALUES (?, ?, ?)",
                (group.group_id, group.memory_limit_bytes, group.compute_limit_millis),
            )
        for node in nodes:
            row = conn.execute("SELECT * FROM nodes WHERE node_id = ?", (node.node_id,)).fetchone()
            sample = spec_json(node)
            if row is not None:
                persisted = row_to_dict(row)
                expected = {
                    "node_id": node.node_id,
                    "total_memory_bytes": node.total_memory_bytes,
                    "available_compute_millis": node.available_compute_millis,
                    "occupied_memory_bytes": node.occupied_memory_bytes,
                    "occupied_compute_millis": node.occupied_compute_millis,
                    "quota_group": node.quota_group,
                    "healthy": int(node.healthy),
                }
                if any(persisted[key] != value for key, value in expected.items()):
                    raise SchedulerError(
                        E_STATE_CONFLICT,
                        f"node {node.node_id!r} already exists with a different specification",
                        field=f"nodes[{node.node_id}]",
                        details={"persisted": persisted, "sample": sample},
                    )
                continue
            conn.execute(
                "INSERT INTO nodes(node_id, total_memory_bytes, available_compute_millis, "
                "occupied_memory_bytes, occupied_compute_millis, quota_group, healthy) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    node.node_id,
                    node.total_memory_bytes,
                    node.available_compute_millis,
                    node.occupied_memory_bytes,
                    node.occupied_compute_millis,
                    node.quota_group,
                    int(node.healthy),
                ),
            )
        for model in models:
            row = conn.execute("SELECT * FROM models WHERE model_id = ?", (model.model_id,)).fetchone()
            if row is not None:
                if (
                    int(row["max_batch_samples"]) != model.max_batch_samples
                    or int(row["unit_sample_cost_millis"])
                    != model.unit_sample_cost_millis
                ):
                    raise SchedulerError(
                        E_STATE_CONFLICT,
                        f"model {model.model_id!r} already exists with a different specification",
                        field=f"models[{model.model_id}]",
                        details={"persisted": row_to_dict(row), "sample": spec_json(model)},
                    )
                continue
            conn.execute(
                "INSERT INTO models(model_id, max_batch_samples, unit_sample_cost_millis) "
                "VALUES (?, ?, ?)",
                (model.model_id, model.max_batch_samples, model.unit_sample_cost_millis),
            )

    def load_sample_requests(
        self,
        sample_dir: str | Path,
        *,
        auto_admit: bool = False,
    ) -> dict[str, Any]:
        bundle = load_bundle(sample_dir)
        results: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        for spec in bundle.requests:
            try:
                results.append(self.submit_request(spec.raw, auto_admit=auto_admit))
            except SchedulerError as exc:
                errors.append({"request_id": spec.request_id, **exc.to_dict()})
        return {
            "ok": not errors,
            "requests": results,
            "errors": errors,
        }

    # ------------------------------------------------------------------
    # Request submission and quota admission
    # ------------------------------------------------------------------
    def submit_request(
        self, payload: Mapping[str, Any], *, auto_admit: bool = False
    ) -> dict[str, Any]:
        if not isinstance(payload, Mapping):
            raise SchedulerError(E_VALIDATION, "request payload must be an object")
        with self.db.reader() as conn:
            groups, models, nodes = self._known_ids(conn)
        spec = request_from_payload(
            payload,
            known_groups=groups,
            known_models=models,
            known_nodes=nodes,
        )
        preemptions: list[dict[str, Any]] = []
        deferred_error: SchedulerError | None = None
        with self.db.operation(
            "SUBMIT_REQUEST",
            request_id=spec.request_id,
            payload={"request": dict(payload), "auto_admit": auto_admit},
        ) as tx:
            existing = tx.conn.execute(
                "SELECT request_id FROM requests WHERE request_id = ?",
                (spec.request_id,),
            ).fetchone()
            if existing is not None:
                result = self._request_view(tx.conn, spec.request_id)
                result["created"] = False
                result["idempotent"] = True
                return result
            self._validate_request_node(tx.conn, spec)
            model = self._model_row(tx.conn, spec.model_id)
            compute = spec.compute_required_millis
            if compute is None:
                compute = checked_product(
                    spec.samples_per_batch,
                    int(model["unit_sample_cost_millis"]),
                    f"requests[{spec.request_id}].samples_per_batch",
                    "request compute requirement",
                )
            if compute <= 0:
                raise spec_error(
                    f"requests[{spec.request_id}].compute_required",
                    "request compute requirement must be positive",
                )
            submission_seq = tx.next_counter("submission_seq")
            tx.conn.execute(
                "INSERT INTO requests(request_id, service_name, model_id, node_id, "
                "memory_bytes, samples_per_batch, expected_latency_millis, priority, "
                "quota_group, compute_millis, status, submission_seq, payload_json, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    spec.request_id,
                    spec.service_name,
                    spec.model_id,
                    spec.node_id,
                    spec.memory_bytes,
                    spec.samples_per_batch,
                    spec.expected_latency_millis,
                    spec.priority,
                    spec.quota_group,
                    compute,
                    PENDING,
                    submission_seq,
                    json_dumps(dict(payload)),
                    tx.now,
                    tx.now,
                ),
            )
            tx.record_initial_state(
                spec.request_id,
                PENDING,
                "request_submitted",
                {"submission_seq": submission_seq},
            )
            if auto_admit:
                tx.conn.execute("SAVEPOINT auto_admission")
                try:
                    preemptions = self._admit_in_tx(
                        tx,
                        spec.request_id,
                        allow_preemption=True,
                        reason="auto_admitted",
                    )["preemptions"]
                except SchedulerError as exc:
                    if exc.code != E_QUOTA_EXCEEDED:
                        raise
                    # Keep the submission and its PENDING state, but roll back
                    # every admission/preemption side effect to the savepoint.
                    tx.conn.execute("ROLLBACK TO auto_admission")
                    tx.conn.execute("RELEASE auto_admission")
                    tx.conn.execute(
                        "UPDATE requests SET last_reason = ?, updated_at = ? "
                        "WHERE request_id = ?",
                        (exc.code, tx.now, spec.request_id),
                    )
                    deferred_error = exc
                else:
                    tx.conn.execute("RELEASE auto_admission")
            assert_consistent(tx.conn, context="submit_request")
            result = self._request_view(tx.conn, spec.request_id)
            result["created"] = True
            result["idempotent"] = False
            result["auto_admitted"] = auto_admit
            result["preemptions"] = preemptions
        if deferred_error is not None:
            deferred_error.details.setdefault("request_id", spec.request_id)
            deferred_error.details.setdefault("request_status", PENDING)
            raise deferred_error
        return result

    def admit_request(
        self, request_id: str, *, allow_preemption: bool | None = None
    ) -> dict[str, Any]:
        if allow_preemption is None:
            allow_preemption = self.config.preemption_enabled
        with self.db.operation(
            "ADMIT_REQUEST",
            request_id=request_id,
            payload={"allow_preemption": bool(allow_preemption)},
        ) as tx:
            row = tx.conn.execute(
                "SELECT status FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None:
                raise SchedulerError(
                    E_REQUEST_NOT_FOUND,
                    f"request {request_id!r} does not exist",
                    details={"request_id": request_id},
                )
            status = str(row["status"])
            if status in {ADMITTED, BATCHED, RUNNING, COMPLETED}:
                result = self._request_view(tx.conn, request_id)
                result["admitted"] = False
                result["idempotent"] = True
                return result
            if status != PENDING:
                raise SchedulerError(
                    E_INVALID_TRANSITION,
                    f"request in state {status} cannot be admitted",
                    details={"request_id": request_id, "from_state": status,
                             "allowed_from": [PENDING]},
                )
            admission = self._admit_in_tx(
                tx,
                request_id,
                allow_preemption=bool(allow_preemption),
                reason="admission_granted",
            )
            assert_consistent(tx.conn, context="admit_request")
            result = self._request_view(tx.conn, request_id)
            result["admitted"] = True
            result["idempotent"] = False
            result["preemptions"] = admission["preemptions"]
            result["quota_after"] = admission["quota_after"]
            return result

    def _known_ids(
        self, conn: sqlite3.Connection
    ) -> tuple[set[str], set[str], set[str]]:
        groups = {
            str(row["group_id"])
            for row in conn.execute("SELECT group_id FROM quota_groups")
        }
        models = {
            str(row["model_id"])
            for row in conn.execute("SELECT model_id FROM models")
        }
        nodes = {
            str(row["node_id"]) for row in conn.execute("SELECT node_id FROM nodes")
        }
        return groups, models, nodes

    def _validate_request_node(
        self, conn: sqlite3.Connection, spec: RequestSpec
    ) -> None:
        if spec.node_id is None:
            return
        row = conn.execute(
            "SELECT node_id, quota_group FROM nodes WHERE node_id = ?",
            (spec.node_id,),
        ).fetchone()
        if row is None:
            raise SchedulerError(
                E_NODE_UNKNOWN,
                f"request references unknown node {spec.node_id!r}",
                field=f"requests[{spec.request_id}].node_id",
                details={"node_id": spec.node_id},
            )
        if str(row["quota_group"]) != spec.quota_group:
            raise spec_error(
                f"requests[{spec.request_id}].quota_group",
                "request quota group does not match its explicitly selected node",
                request_quota_group=spec.quota_group,
                node_quota_group=row["quota_group"],
                node_id=spec.node_id,
            )

    def _model_row(self, conn: sqlite3.Connection, model_id: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM models WHERE model_id = ?", (model_id,)
        ).fetchone()
        if row is None:
            raise SchedulerError(
                E_MODEL_UNKNOWN,
                f"model {model_id!r} does not exist",
                details={"model_id": model_id},
            )
        return row

    def _admit_in_tx(
        self,
        tx: WriteContext,
        request_id: str,
        *,
        allow_preemption: bool,
        reason: str = "quota_reserved",
    ) -> dict[str, Any]:
        conn = tx.conn
        request = conn.execute(
            "SELECT * FROM requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if request is None:
            raise SchedulerError(
                E_REQUEST_NOT_FOUND,
                f"request {request_id!r} does not exist",
                details={"request_id": request_id},
            )
        if str(request["status"]) != PENDING:
            return {"preemptions": [], "quota_after": group_usage(conn, request["quota_group"])}
        group_id = str(request["quota_group"])
        memory_required = int(request["memory_bytes"])
        compute_required = int(request["compute_millis"])
        usage_before = group_usage(conn, group_id)
        preemptions: list[dict[str, Any]] = []

        def over_limit(usage: Mapping[str, int]) -> bool:
            return (
                usage["memory_used_bytes"] + memory_required
                > usage["memory_limit_bytes"]
                or usage["compute_used_millis"] + compute_required
                > usage["compute_limit_millis"]
            )

        usage = usage_before
        if over_limit(usage):
            if allow_preemption and self.config.preemption_enabled:
                preemptions = self._preempt_for_admission(
                    tx,
                    group_id=group_id,
                    by_request_id=request_id,
                    incoming_priority=int(request["priority"]),
                    memory_required=memory_required,
                    compute_required=compute_required,
                    usage_before=usage_before,
                )
                usage = group_usage(conn, group_id)
            if over_limit(usage):
                raise SchedulerError(
                    E_QUOTA_EXCEEDED,
                    "quota group does not have enough unreserved capacity",
                    field=f"requests[{request_id}].quota_group",
                    details=self._quota_exceeded_details(
                        group_id,
                        usage_before,
                        usage,
                        memory_required,
                        compute_required,
                        preemptions,
                    ),
                )

        reservation = conn.execute(
            "SELECT request_id FROM reservations WHERE request_id = ?", (request_id,)
        ).fetchone()
        if reservation is None:
            conn.execute(
                "INSERT INTO reservations(request_id, quota_group, memory_bytes, "
                "compute_millis, active, created_op_id, created_at) "
                "VALUES (?, ?, ?, ?, 1, ?, ?)",
                (request_id, group_id, memory_required, compute_required, tx.op_id, tx.now),
            )
        else:
            conn.execute(
                "UPDATE reservations SET quota_group = ?, memory_bytes = ?, "
                "compute_millis = ?, active = 1, created_op_id = ?, created_at = ?, "
                "released_op_id = NULL, released_at = NULL, release_reason = NULL "
                "WHERE request_id = ?",
                (
                    group_id,
                    memory_required,
                    compute_required,
                    tx.op_id,
                    tx.now,
                    request_id,
                ),
            )
        tx.transition_request(
            request_id,
            ADMITTED,
            reason,
            {
                "memory_bytes": memory_required,
                "compute_millis": compute_required,
                "quota_group": group_id,
                "preemption_count": len(preemptions),
            },
        )
        usage_after = group_usage(conn, group_id)
        return {"preemptions": preemptions, "quota_after": usage_after}

    def _quota_exceeded_details(
        self,
        group_id: str,
        usage_before: Mapping[str, int],
        usage_after_preemption_attempt: Mapping[str, int],
        memory_required: int,
        compute_required: int,
        preemptions: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        return {
            "quota_group": group_id,
            "current_usage_bytes": usage_before["memory_used_bytes"],
            "current_occupancy_bytes": usage_before["node_occupancy_bytes"],
            "reserved_bytes": usage_before["reserved_bytes"],
            "request_bytes": memory_required,
            "limit_bytes": usage_before["memory_limit_bytes"],
            "current_usage_millis": usage_before["compute_used_millis"],
            "current_occupancy_millis": usage_before[
                "node_occupancy_compute_millis"
            ],
            "reserved_millis": usage_before["reserved_compute_millis"],
            "request_millis": compute_required,
            "limit_millis": usage_before["compute_limit_millis"],
            "memory": {
                "current_occupancy_bytes": usage_before["node_occupancy_bytes"],
                "reserved_bytes": usage_before["reserved_bytes"],
                "current_usage_bytes": usage_before["memory_used_bytes"],
                "request_bytes": memory_required,
                "limit_bytes": usage_before["memory_limit_bytes"],
                "deficit_bytes": max(
                    0,
                    usage_after_preemption_attempt["memory_used_bytes"]
                    + memory_required
                    - usage_before["memory_limit_bytes"],
                ),
            },
            "compute": {
                "current_occupancy_millis": usage_before[
                    "node_occupancy_compute_millis"
                ],
                "reserved_millis": usage_before["reserved_compute_millis"],
                "current_usage_millis": usage_before["compute_used_millis"],
                "request_millis": compute_required,
                "limit_millis": usage_before["compute_limit_millis"],
                "deficit_millis": max(
                    0,
                    usage_after_preemption_attempt["compute_used_millis"]
                    + compute_required
                    - usage_before["compute_limit_millis"],
                ),
            },
            "usage_after_preemption_attempt": dict(usage_after_preemption_attempt),
            "preemptions": list(preemptions),
        }

    def _preempt_for_admission(
        self,
        tx: WriteContext,
        *,
        group_id: str,
        by_request_id: str,
        incoming_priority: int,
        memory_required: int,
        compute_required: int,
        usage_before: Mapping[str, int],
    ) -> list[dict[str, Any]]:
        conn = tx.conn
        victims = conn.execute(
            "SELECT q.request_id, q.priority, q.submission_seq FROM requests q "
            "JOIN reservations r ON r.request_id = q.request_id AND r.active = 1 "
            "WHERE q.quota_group = ? AND q.priority < ? "
            "AND q.status IN ('ADMITTED', 'BATCHED', 'RUNNING') "
            "ORDER BY q.priority ASC, q.submission_seq DESC",
            (group_id, incoming_priority),
        ).fetchall()
        preemptions: list[dict[str, Any]] = []
        attempts: set[str] = set()
        for victim in victims:
            victim_id = str(victim["request_id"])
            if victim_id in attempts:
                continue
            attempts.add(victim_id)
            closure = self._request_closure(conn, {victim_id})
            if not closure["request_ids"]:
                continue
            priorities = {
                int(row["priority"])
                for row in conn.execute(
                    f"SELECT priority FROM requests WHERE request_id IN ({_placeholders(len(closure['request_ids']))})",
                    tuple(sorted(closure["request_ids"])),
                )
            }
            # Preemption is batch-atomic.  Never collateral-damage an equal or
            # higher priority co-member merely because it shares a batch.
            if any(priority >= incoming_priority for priority in priorities):
                continue
            detail = self._preempt_closure_in_tx(
                tx,
                initial_request_ids={victim_id},
                reason=f"preempted_by_priority_{incoming_priority}",
                by_request_id=by_request_id,
                batch_status=BATCH_PREEMPTED,
            )
            preemptions.extend(detail["requests"])
            usage = group_usage(conn, group_id)
            if (
                usage["memory_used_bytes"] + memory_required
                <= usage["memory_limit_bytes"]
                and usage["compute_used_millis"] + compute_required
                <= usage["compute_limit_millis"]
            ):
                break
        return preemptions

    # ------------------------------------------------------------------
    # Generic state transitions
    # ------------------------------------------------------------------
    def transition_request(
        self, request_id: str, to_state: str, reason: str
    ) -> dict[str, Any]:
        if not isinstance(reason, str) or not reason.strip():
            raise SchedulerError(E_VALIDATION, "transition reason must not be empty")
        with self.db.operation(
            "TRANSITION_REQUEST",
            request_id=request_id,
            payload={"to_state": to_state, "reason": reason},
        ) as tx:
            row = tx.conn.execute(
                "SELECT status FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None:
                raise SchedulerError(
                    E_REQUEST_NOT_FOUND,
                    f"request {request_id!r} does not exist",
                    details={"request_id": request_id},
                )
            from_state = str(row["status"])
            if from_state == to_state:
                result = self._request_view(tx.conn, request_id)
                result["transitioned"] = False
                result["idempotent"] = True
                return result
            ensure_transition(from_state, to_state)
            if to_state == ADMITTED:
                if from_state != PENDING:
                    raise SchedulerError(
                        E_INVALID_TRANSITION,
                        "ADMITTED transition requires a PENDING request and quota reservation",
                        details={"from_state": from_state, "to_state": to_state},
                    )
                admission = self._admit_in_tx(
                    tx, request_id, allow_preemption=True, reason=reason
                )
                result = self._request_view(tx.conn, request_id)
                result["transitioned"] = True
                result["preemptions"] = admission["preemptions"]
            elif to_state == PREEMPTED:
                detail = self._preempt_closure_in_tx(
                    tx,
                    initial_request_ids={request_id},
                    reason=reason,
                    by_request_id=None,
                    batch_status=BATCH_PREEMPTED,
                )
                result = self._request_view(tx.conn, request_id)
                result["transitioned"] = True
                result["preempted_requests"] = detail["requests"]
                result["preempted_batches"] = detail["batches"]
            elif to_state == REJECTED:
                if from_state not in {PENDING, ADMITTED}:
                    raise SchedulerError(
                        E_INVALID_TRANSITION,
                        "manual rejection is only safe before or at ADMITTED; use preemption/abort for active batches",
                        details={"from_state": from_state, "to_state": to_state},
                    )
                if from_state == ADMITTED:
                    self._release_reservation(tx, request_id, reason)
                tx.transition_request(request_id, REJECTED, reason)
                result = self._request_view(tx.conn, request_id)
                result["transitioned"] = True
            elif to_state == PENDING:
                if from_state != PREEMPTED:
                    raise SchedulerError(
                        E_INVALID_TRANSITION,
                        "only PREEMPTED requests may be returned to PENDING",
                        details={"from_state": from_state, "to_state": to_state},
                    )
                tx.transition_request(request_id, PENDING, reason)
                result = self._request_view(tx.conn, request_id)
                result["transitioned"] = True
            elif to_state == COMPLETED and from_state == RUNNING:
                batch_rows = tx.conn.execute(
                    "SELECT DISTINCT b.batch_id, b.status FROM batches b "
                    "JOIN batch_members bm ON bm.batch_id = b.batch_id "
                    "WHERE bm.request_id = ? AND b.status IN "
                    "('PLANNED', 'QUEUED', 'RUNNING') ORDER BY b.batch_id",
                    (request_id,),
                ).fetchall()
                if not batch_rows or any(
                    str(row["status"]) != RUNNING for row in batch_rows
                ):
                    raise SchedulerError(
                        E_INVALID_TRANSITION,
                        "a request can only be completed when all remaining batches are RUNNING",
                        details={
                            "request_id": request_id,
                            "batches": [row_to_dict(row) for row in batch_rows],
                        },
                    )
                completion = self._complete_batches_in_tx(
                    tx, [str(row["batch_id"]) for row in batch_rows]
                )
                result = self._request_view(tx.conn, request_id)
                result["transitioned"] = True
                result["completed_batches"] = [
                    str(row["batch_id"]) for row in batch_rows
                ]
                result["completed_requests"] = completion["completed_requests"]
            else:
                raise SchedulerError(
                    E_INVALID_TRANSITION,
                    f"{from_state} -> {to_state} is resource-driven and must use batching/scheduling/completion APIs",
                    details={"from_state": from_state, "to_state": to_state},
                )
            assert_consistent(tx.conn, context="transition_request")
            return result

    def _release_reservation(
        self, tx: WriteContext, request_id: str, reason: str
    ) -> None:
        tx.conn.execute(
            "UPDATE reservations SET active = 0, released_op_id = ?, released_at = ?, "
            "release_reason = ? WHERE request_id = ? AND active = 1",
            (tx.op_id, tx.now, reason, request_id),
        )

    # ------------------------------------------------------------------
    # Preemption closure and quota recycling
    # ------------------------------------------------------------------
    def _request_closure(
        self, conn: sqlite3.Connection, initial_request_ids: Iterable[str]
    ) -> dict[str, Any]:
        request_ids = {
            str(value)
            for value in initial_request_ids
            if value is not None
        }
        if not request_ids:
            return {"request_ids": [], "batch_ids": []}
        changed = True
        while changed:
            changed = False
            batch_rows = conn.execute(
                "SELECT DISTINCT b.batch_id, b.status FROM batches b "
                "JOIN batch_members bm ON bm.batch_id = b.batch_id "
                f"WHERE bm.request_id IN ({_placeholders(len(request_ids))}) "
                "AND b.status IN ('PLANNED', 'QUEUED', 'RUNNING')",
                tuple(sorted(request_ids)),
            ).fetchall()
            batch_ids = {str(row["batch_id"]) for row in batch_rows}
            if not batch_ids:
                break
            member_rows = conn.execute(
                "SELECT DISTINCT q.request_id FROM batch_members bm "
                "JOIN requests q ON q.request_id = bm.request_id "
                f"WHERE bm.batch_id IN ({_placeholders(len(batch_ids))}) "
                "AND q.status IN ('ADMITTED', 'BATCHED', 'RUNNING')",
                tuple(sorted(batch_ids)),
            ).fetchall()
            for row in member_rows:
                value = str(row["request_id"])
                if value not in request_ids:
                    request_ids.add(value)
                    changed = True
        batch_rows = conn.execute(
            "SELECT DISTINCT b.batch_id FROM batches b "
            "JOIN batch_members bm ON bm.batch_id = b.batch_id "
            f"WHERE bm.request_id IN ({_placeholders(len(request_ids))}) "
            "AND b.status IN ('PLANNED', 'QUEUED', 'RUNNING')",
            tuple(sorted(request_ids)),
        ).fetchall()
        return {
            "request_ids": sorted(request_ids),
            "batch_ids": sorted(str(row["batch_id"]) for row in batch_rows),
        }

    def _preempt_closure_in_tx(
        self,
        tx: WriteContext,
        *,
        initial_request_ids: Iterable[str],
        reason: str,
        by_request_id: str | None,
        batch_status: str = BATCH_PREEMPTED,
    ) -> dict[str, Any]:
        if batch_status not in {BATCH_PREEMPTED, BATCH_ABORTED}:
            raise SchedulerError(E_VALIDATION, "unsupported terminal batch status for recycling")
        conn = tx.conn
        closure = self._request_closure(conn, initial_request_ids)
        request_ids = closure["request_ids"]
        batch_ids = closure["batch_ids"]
        released_requests: list[dict[str, Any]] = []
        released_batches: list[dict[str, Any]] = []

        if batch_ids:
            batch_rows = conn.execute(
                f"SELECT batch_id, status FROM batches WHERE batch_id IN ({_placeholders(len(batch_ids))})",
                tuple(batch_ids),
            ).fetchall()
            conn.execute(
                f"UPDATE node_allocations SET active = 0, released_op_id = ?, released_at = ? "
                f"WHERE batch_id IN ({_placeholders(len(batch_ids))}) AND active = 1",
                (tx.op_id, tx.now, *batch_ids),
            )
            for batch in batch_rows:
                previous = str(batch["status"])
                if previous in {BATCH_COMPLETED, BATCH_PREEMPTED, BATCH_ABORTED}:
                    continue
                conn.execute(
                    "UPDATE batches SET status = ?, updated_at = ? WHERE batch_id = ?",
                    (batch_status, tx.now, batch["batch_id"]),
                )
                tx.batch_event(
                    str(batch["batch_id"]),
                    previous,
                    batch_status,
                    reason,
                    {"by_request_id": by_request_id, "request_ids": request_ids},
                )
                released_batches.append(
                    {
                        "batch_id": str(batch["batch_id"]),
                        "from_status": previous,
                        "to_status": batch_status,
                    }
                )

        for request_id in request_ids:
            row = conn.execute(
                "SELECT * FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None:
                continue
            status = str(row["status"])
            reservation = conn.execute(
                "SELECT active, memory_bytes, compute_millis FROM reservations "
                "WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if status in {ADMITTED, BATCHED, RUNNING}:
                tx.transition_request(
                    request_id,
                    PREEMPTED,
                    reason,
                    {
                        "by_request_id": by_request_id,
                        "released_memory_bytes": int(row["memory_bytes"]),
                        "released_compute_millis": int(row["compute_millis"]),
                        "batch_ids": batch_ids,
                    },
                )
            if reservation is not None and int(reservation["active"]) == 1:
                self._release_reservation(tx, request_id, reason)
                released_requests.append(
                    {
                        "request_id": request_id,
                        "priority": int(row["priority"]),
                        "previous_status": status,
                        "released_memory_bytes": int(reservation["memory_bytes"]),
                        "released_compute_millis": int(reservation["compute_millis"]),
                        "released_batch_ids": batch_ids,
                    }
                )
        return {"requests": released_requests, "batches": released_batches}

    def preempt_request(
        self,
        request_id: str,
        reason: str = "manual_preemption",
        *,
        by_request_id: str | None = None,
    ) -> dict[str, Any]:
        with self.db.operation(
            "PREEMPT_REQUEST",
            request_id=request_id,
            payload={"reason": reason, "by_request_id": by_request_id},
        ) as tx:
            row = tx.conn.execute(
                "SELECT * FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is None:
                raise SchedulerError(
                    E_REQUEST_NOT_FOUND,
                    f"request {request_id!r} does not exist",
                    details={"request_id": request_id},
                )
            if str(row["status"]) == PREEMPTED:
                result = self._request_view(tx.conn, request_id)
                result["preempted"] = False
                result["idempotent"] = True
                return result
            if str(row["status"]) in {COMPLETED, REJECTED}:
                raise SchedulerError(
                    E_INVALID_TRANSITION,
                    f"request in terminal state {row['status']} cannot be preempted",
                    details={"request_id": request_id, "from_state": row["status"]},
                )
            group_id = str(row["quota_group"])
            usage_before = group_usage(tx.conn, group_id)
            detail = self._preempt_closure_in_tx(
                tx,
                initial_request_ids={request_id},
                reason=reason,
                by_request_id=by_request_id,
                batch_status=BATCH_PREEMPTED,
            )
            assert_consistent(tx.conn, context="preempt_request")
            usage_after = group_usage(tx.conn, group_id)
            result = self._request_view(tx.conn, request_id)
            result["preempted"] = True
            result["idempotent"] = False
            result["released_requests"] = detail["requests"]
            result["released_batches"] = detail["batches"]
            result["quota_before"] = usage_before
            result["quota_after"] = usage_after
            return result

    # ------------------------------------------------------------------
    # Batch organization
    # ------------------------------------------------------------------
    def organize_batches(
        self,
        *,
        strategy: str | None = None,
        strict: bool | None = None,
        request_ids: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        requested_strategy = (strategy or self.config.batch_strategy).strip().lower()
        if requested_strategy == "submission":
            requested_strategy = "greedy"
        if requested_strategy == "memory":
            requested_strategy = "memory_descending"
        if requested_strategy not in {"auto", "greedy", "memory_descending"}:
            raise SchedulerError(
                E_VALIDATION,
                "strategy must be auto, greedy/submission, or memory/memory_descending",
                details={"strategy": strategy},
            )
        if strict is None:
            strict = self.config.strict_batching
        with self.db.operation(
            "ORGANIZE_BATCHES",
            payload={
                "strategy": requested_strategy,
                "strict": bool(strict),
                "request_ids": list(request_ids) if request_ids else None,
            },
        ) as tx:
            conn = tx.conn
            params: list[Any] = []
            sql = (
                "SELECT q.*, m.max_batch_samples, m.unit_sample_cost_millis "
                "FROM requests q JOIN models m ON m.model_id = q.model_id "
                "JOIN reservations r ON r.request_id = q.request_id AND r.active = 1 "
                "WHERE q.status = 'ADMITTED'"
            )
            if request_ids:
                sql += f" AND q.request_id IN ({_placeholders(len(request_ids))})"
                params.extend(request_ids)
            sql += " ORDER BY q.submission_seq ASC, q.request_id ASC"
            candidates = conn.execute(sql, tuple(params)).fetchall()
            if not candidates:
                return {
                    "ok": True,
                    "strategy_requested": requested_strategy,
                    "strategy_selected": [],
                    "strategy_candidates": [],
                    "repack_decisions": [],
                    "batches": [],
                    "batch_count": 0,
                    "adjustments": [],
                    "errors": [],
                    "skipped_request_ids": [],
                    "metrics": {},
                }

            usage_by_node = node_usage_map(conn)
            remaining_memory = {
                node_id: value["remaining_memory_bytes"]
                for node_id, value in usage_by_node.items()
            }
            remaining_compute = {
                node_id: value["remaining_compute_millis"]
                for node_id, value in usage_by_node.items()
            }
            assignment_inputs: list[RequestPlanInput] = []
            assignment_errors: list[dict[str, Any]] = []
            assigned_node_by_request: dict[str, str] = {}
            for row in candidates:
                request_id = str(row["request_id"])
                group_id = str(row["quota_group"])
                memory = int(row["memory_bytes"])
                compute = int(row["compute_millis"])
                explicit = row["node_id"]
                selected_node: str | None = None
                if explicit is not None:
                    node_id = str(explicit)
                    node = usage_by_node.get(node_id)
                    if node is None:
                        assignment_errors.append(
                            {
                                "code": E_NODE_UNKNOWN,
                                "message": f"request references unknown node {node_id!r}",
                                "request_id": request_id,
                                "node_id": node_id,
                            }
                        )
                        continue
                    if node["quota_group"] != group_id:
                        assignment_errors.append(
                            {
                                "code": E_SPEC_INVALID,
                                "message": "request quota group does not match selected node",
                                "field": f"requests[{request_id}].quota_group",
                                "request_id": request_id,
                                "node_id": node_id,
                            }
                        )
                        continue
                    if not node["healthy"]:
                        assignment_errors.append(
                            {
                                "code": E_NODE_UNHEALTHY,
                                "message": "selected node is not healthy",
                                "request_id": request_id,
                                "node_id": node_id,
                            }
                        )
                        continue
                    if memory > remaining_memory[node_id] or compute > remaining_compute[node_id]:
                        assignment_errors.append(
                            {
                                "code": E_BATCH_UNPLACEABLE,
                                "message": "request does not fit on its selected node",
                                "request_id": request_id,
                                "node_id": node_id,
                                "required_memory_bytes": memory,
                                "remaining_memory_bytes": remaining_memory[node_id],
                                "required_compute_millis": compute,
                                "remaining_compute_millis": remaining_compute[node_id],
                            }
                        )
                        continue
                    selected_node = node_id
                else:
                    options = [
                        (remaining_memory[node_id], node_id)
                        for node_id, node in usage_by_node.items()
                        if node["healthy"]
                        and node["quota_group"] == group_id
                        and remaining_memory[node_id] >= memory
                        and remaining_compute[node_id] >= compute
                    ]
                    if not options:
                        attempted = [
                            {
                                "node_id": node_id,
                                "healthy": node["healthy"],
                                "remaining_memory_bytes": remaining_memory[node_id],
                                "remaining_compute_millis": remaining_compute[node_id],
                            }
                            for node_id, node in sorted(usage_by_node.items())
                            if node["quota_group"] == group_id
                        ]
                        any_healthy = any(node["healthy"] for node in attempted)
                        assignment_errors.append(
                            {
                                "code": E_BATCH_UNPLACEABLE
                                if any_healthy
                                else E_NODE_UNHEALTHY,
                                "message": (
                                    "no healthy member node has enough remaining resources"
                                    if any_healthy
                                    else "quota group has no healthy member node"
                                ),
                                "request_id": request_id,
                                "quota_group": group_id,
                                "required_memory_bytes": memory,
                                "required_compute_millis": compute,
                                "fragmentation_attempt": "checked_all_healthy_member_nodes",
                                "nodes_checked": attempted,
                            }
                        )
                        continue
                    selected_node = min(options)[1]
                remaining_memory[selected_node] -= memory
                remaining_compute[selected_node] -= compute
                assigned_node_by_request[request_id] = selected_node
                assignment_inputs.append(
                    RequestPlanInput(
                        request_id=request_id,
                        model_id=str(row["model_id"]),
                        node_id=selected_node,
                        quota_group=group_id,
                        memory_bytes=memory,
                        compute_millis=compute,
                        samples_per_batch=int(row["samples_per_batch"]),
                        expected_latency_millis=int(row["expected_latency_millis"]),
                        priority=int(row["priority"]),
                        submission_seq=int(row["submission_seq"]),
                    )
                )

            groups: list[list[RequestPlanInput]] = []
            current_key: tuple[str, str] | None = None
            current_group: list[RequestPlanInput] = []
            for request in assignment_inputs:
                key = (request.model_id, request.node_id)
                if current_key is not None and key != current_key:
                    groups.append(current_group)
                    current_group = []
                current_key = key
                current_group.append(request)
            if current_group:
                groups.append(current_group)

            batch_plans = []
            plan_adjustments: list[dict[str, Any]] = []
            plan_errors: list[dict[str, Any]] = []
            strategy_selected: list[str] = []
            plan_metrics: list[dict[str, Any]] = []
            repack_decisions: list[dict[str, Any]] = []
            planned_memory_by_node: dict[str, int] = defaultdict(int)
            planned_compute_by_node: dict[str, int] = defaultdict(int)
            for group in groups:
                first = group[0]
                node = usage_by_node[first.node_id]
                model = conn.execute(
                    "SELECT max_batch_samples, unit_sample_cost_millis FROM models "
                    "WHERE model_id = ?",
                    (first.model_id,),
                ).fetchone()
                if model is None:
                    plan_errors.append(
                        {
                            "code": E_MODEL_UNKNOWN,
                            "message": f"model {first.model_id!r} does not exist",
                            "model_id": first.model_id,
                        }
                    )
                    continue
                auto_threshold = self.config.batch_strategy_threshold
                if requested_strategy == "auto" and len(assignment_inputs) > auto_threshold:
                    auto_threshold = 0
                planned = plan_group(
                    group,
                    model_id=first.model_id,
                    max_batch_samples=int(model["max_batch_samples"]),
                    unit_sample_cost_millis=int(model["unit_sample_cost_millis"]),
                    node_memory_left=node["remaining_memory_bytes"]
                    - planned_memory_by_node[first.node_id],
                    node_compute_left=node["remaining_compute_millis"]
                    - planned_compute_by_node[first.node_id],
                    strategy=requested_strategy,
                    auto_strategy_threshold=auto_threshold,
                    batch_sample_target=self.config.batch_sample_target,
                )
                candidate_rows = planned.metrics.get("strategy_candidates") or [
                    {"strategy": planned.strategy, **planned.metrics}
                ]
                selected_metric = next(
                    (
                        candidate
                        for candidate in candidate_rows
                        if candidate.get("strategy") == planned.strategy
                    ),
                    {"strategy": planned.strategy, **planned.metrics},
                )
                alternatives = [
                    candidate
                    for candidate in candidate_rows
                    if candidate.get("strategy") != planned.strategy
                ]
                if alternatives:
                    alt = alternatives[0]
                    if int(selected_metric.get("batch_count", 0)) < int(
                        alt.get("batch_count", 0)
                    ):
                        repack_reason = "fewer_batches_after_deterministic_repack"
                    elif int(selected_metric.get("unplanned_request_count", 0)) < int(
                        alt.get("unplanned_request_count", 0)
                    ):
                        repack_reason = "fewer_unplanned_requests_after_deterministic_repack"
                    else:
                        repack_reason = "deterministic_tiebreak_greedy_submission"
                else:
                    repack_reason = "single_strategy_below_auto_threshold"
                repack_decisions.append(
                    {
                        "model_id": first.model_id,
                        "node_id": first.node_id,
                        "request_count": len(group),
                        "selected_strategy": planned.strategy,
                        "reason": repack_reason,
                        "candidates": [dict(candidate) for candidate in candidate_rows],
                        "node_remaining_memory_bytes_before_plan": node[
                            "remaining_memory_bytes"
                        ]
                        - (planned_memory_by_node[first.node_id]),
                        "node_remaining_compute_millis_before_plan": node[
                            "remaining_compute_millis"
                        ]
                        - planned_compute_by_node[first.node_id],
                    }
                )
                planned_memory_by_node[first.node_id] += sum(
                    batch.memory_bytes for batch in planned.batches
                )
                planned_compute_by_node[first.node_id] += sum(
                    batch.compute_millis for batch in planned.batches
                )
                batch_plans.extend(planned.batches)
                plan_adjustments.extend(planned.adjustments)
                plan_errors.extend(planned.errors)
                strategy_selected.append(planned.strategy)
                candidate_rows = planned.metrics.get("strategy_candidates")
                if not candidate_rows:
                    candidate_rows = [
                        {"strategy": planned.strategy, **planned.metrics}
                    ]
                for candidate_row in candidate_rows:
                    metric = dict(candidate_row)
                    metric["model_id"] = planned.batches[0].model_id if planned.batches else None
                    metric["node_id"] = planned.batches[0].node_id if planned.batches else None
                    plan_metrics.append(metric)

            all_errors = assignment_errors + plan_errors
            if strict and all_errors:
                first_error = all_errors[0]
                raise SchedulerError(
                    str(first_error.get("code", E_BATCH_UNPLACEABLE)),
                    str(first_error.get("message", "batch organization failed")),
                    field=first_error.get("field"),
                    details={"errors": all_errors},
                )

            inserted_batches: list[dict[str, Any]] = []
            transitioned_requests: set[str] = set()
            for plan in batch_plans:
                batch_id = f"batch-{tx.next_counter('batch_seq'):06d}"
                reasons = {
                    "strategy": plan.strategy,
                    "reasons": plan.reasons,
                    "adjustments": [
                        adjustment
                        for adjustment in plan_adjustments
                        if adjustment.get("request_id")
                        in {member.request_id for member in plan.members}
                    ],
                }
                conn.execute(
                    "INSERT INTO batches(batch_id, model_id, node_id, quota_group, "
                    "strategy, status, total_samples, memory_bytes, compute_millis, "
                    "estimated_latency_millis, priority, first_submission_seq, "
                    "created_op_id, reason_json, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        batch_id,
                        plan.model_id,
                        plan.node_id,
                        plan.quota_group,
                        plan.strategy,
                        BATCH_PLANNED,
                        plan.total_samples,
                        plan.memory_bytes,
                        plan.compute_millis,
                        plan.estimated_latency_millis,
                        plan.priority,
                        plan.first_submission_seq,
                        tx.op_id,
                        json_dumps(reasons),
                        tx.now,
                        tx.now,
                    ),
                )
                tx.batch_event(
                    batch_id,
                    None,
                    BATCH_PLANNED,
                    "batch_organized",
                    {"strategy": plan.strategy, "member_count": len(plan.members)},
                )
                for member in plan.members:
                    conn.execute(
                        "INSERT INTO batch_members(batch_id, request_id, piece_index, "
                        "samples, memory_bytes, compute_millis) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            batch_id,
                            member.request_id,
                            member.piece_index,
                            member.samples,
                            member.memory_bytes,
                            member.compute_millis,
                        ),
                    )
                    allocation_id = f"alloc-{tx.next_counter('allocation_seq'):08d}"
                    conn.execute(
                        "INSERT INTO node_allocations(allocation_id, request_id, batch_id, "
                        "node_id, memory_bytes, compute_millis, active, created_op_id, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)",
                        (
                            allocation_id,
                            member.request_id,
                            batch_id,
                            plan.node_id,
                            member.memory_bytes,
                            member.compute_millis,
                            tx.op_id,
                            tx.now,
                        ),
                    )
                    if member.request_id not in transitioned_requests:
                        tx.transition_request(
                            member.request_id,
                            BATCHED,
                            "batch_organized",
                            {
                                "batch_id": batch_id,
                                "node_id": plan.node_id,
                                "strategy": plan.strategy,
                            },
                        )
                        transitioned_requests.add(member.request_id)
                inserted_batches.append(self._batch_view(conn, batch_id))

            for request_id, node_id in assigned_node_by_request.items():
                conn.execute(
                    "UPDATE requests SET node_id = COALESCE(node_id, ?), updated_at = ? "
                    "WHERE request_id = ?",
                    (node_id, tx.now, request_id),
                )
            for error in all_errors:
                if error.get("request_id"):
                    conn.execute(
                        "UPDATE requests SET last_reason = ?, updated_at = ? "
                        "WHERE request_id = ? AND status = 'ADMITTED'",
                        (str(error.get("code")), tx.now, error["request_id"]),
                    )
            assert_consistent(tx.conn, context="organize_batches")
            return {
                "ok": not all_errors,
                "strategy_requested": requested_strategy,
                "strategy_selected": sorted(set(strategy_selected)),
                "strategy_candidates": [metrics for metrics in plan_metrics],
                "repack_decisions": repack_decisions,
                "batches": inserted_batches,
                "batch_count": len(inserted_batches),
                "adjustments": plan_adjustments,
                "errors": all_errors,
                "skipped_request_ids": sorted(
                    {
                        str(error["request_id"])
                        for error in all_errors
                        if error.get("request_id")
                    }
                ),
                "quota_after": group_usage_map(conn),
                "nodes_after": node_usage_map(conn),
            }

    # ------------------------------------------------------------------
    # Parallel execution scheduler
    # ------------------------------------------------------------------
    def schedule_round(
        self,
        *,
        round_no: int | None = None,
        max_parallel_batches: int | None = None,
        time_budget_millis: int | str | None = None,
        queue_capacity: int | None = None,
    ) -> dict[str, Any]:
        max_parallel = (
            self.config.global_max_parallel_batches
            if max_parallel_batches is None
            else max_parallel_batches
        )
        budget = (
            self.config.round_time_budget_millis
            if time_budget_millis is None
            else parse_millis(time_budget_millis, "time_budget_millis")
        )
        capacity = (
            self.config.execution_queue_capacity
            if queue_capacity is None
            else queue_capacity
        )
        for value, label, allow_zero in (
            (max_parallel, "max_parallel_batches", False),
            (capacity, "queue_capacity", True),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise SchedulerError(E_VALIDATION, f"{label} must be an integer")
            if value < 0 or (value == 0 and not allow_zero):
                raise SchedulerError(
                    E_VALIDATION,
                    f"{label} must be {'>= 0' if allow_zero else 'positive'}",
                )
        if budget <= 0:
            raise SchedulerError(E_VALIDATION, "time_budget_millis must be positive")
        if round_no is not None:
            if isinstance(round_no, bool) or not isinstance(round_no, int) or round_no <= 0:
                raise SchedulerError(E_VALIDATION, "round_no must be a positive integer")

        with self.db.operation(
            "SCHEDULE_ROUND",
            payload={
                "round_no": round_no,
                "max_parallel_batches": max_parallel,
                "time_budget_millis": budget,
                "queue_capacity": capacity,
            },
        ) as tx:
            conn = tx.conn
            if round_no is not None:
                existing = conn.execute(
                    "SELECT round_no FROM rounds WHERE round_no = ?", (round_no,)
                ).fetchone()
                if existing is not None:
                    result = self._round_view(conn, round_no)
                    result["ok"] = True
                    result["replayed"] = True
                    return result

            rows = conn.execute(
                "SELECT * FROM batches WHERE status IN ('PLANNED', 'QUEUED') "
                "ORDER BY priority DESC, estimated_latency_millis ASC, "
                "first_submission_seq ASC, batch_id ASC"
            ).fetchall()
            running_count = int(
                conn.execute(
                    "SELECT COUNT(*) AS count FROM batches WHERE status = 'RUNNING'"
                ).fetchone()["count"]
            )
            queued_after = {
                str(row["batch_id"])
                for row in conn.execute(
                    "SELECT batch_id FROM batches WHERE status = 'QUEUED'"
                )
            }
            if round_no is None:
                round_no = tx.next_counter("round_seq")
            else:
                current_seq = int(
                    conn.execute("SELECT value FROM meta WHERE key = 'round_seq'").fetchone()["value"]
                )
                if round_no > current_seq:
                    conn.execute(
                        "UPDATE meta SET value = ? WHERE key = 'round_seq'",
                        (str(round_no),),
                    )

            ordered = self._scheduler_order(rows, round_no)
            slots = max(0, max_parallel - running_count)
            time_used = 0
            selected: list[dict[str, Any]] = []
            queued_entries: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []
            overflow: list[dict[str, Any]] = []
            selected_ids: set[str] = set()
            for row in ordered:
                batch_id = str(row["batch_id"])
                latency = int(row["estimated_latency_millis"])
                can_select = slots > 0 and time_used + latency <= budget
                if can_select:
                    selected_ids.add(batch_id)
                    selected.append(
                        {
                            "batch_id": batch_id,
                            "priority": int(row["priority"]),
                            "quota_group": str(row["quota_group"]),
                            "estimated_latency_millis": latency,
                            "reason": "SELECTED_PRIORITY_GROUP_ROTATION",
                        }
                    )
                    queued_after.discard(batch_id)
                    slots -= 1
                    time_used += latency
                    continue
                reason = (
                    "CONCURRENCY_LIMIT" if slots <= 0 else "TIME_BUDGET"
                )
                if batch_id not in queued_after:
                    if len(queued_after) >= capacity:
                        overflow.append(
                            {
                                "batch_id": batch_id,
                                "reason": "EXEC_QUEUE_FULL",
                                "queue_capacity": capacity,
                                "queue_length": len(queued_after),
                            }
                        )
                    else:
                        queued_after.add(batch_id)
                queued_entries.append(
                    {
                        "batch_id": batch_id,
                        "priority": int(row["priority"]),
                        "quota_group": str(row["quota_group"]),
                        "estimated_latency_millis": latency,
                        "reason": f"QUEUED_{reason}",
                    }
                )
                skipped.append(
                    {
                        "batch_id": batch_id,
                        "reason": f"QUEUED_{reason}",
                        "estimated_latency_millis": latency,
                    }
                )

            if len(queued_after) > capacity:
                overflow.extend(
                    {
                        "batch_id": batch_id,
                        "reason": "EXEC_QUEUE_FULL",
                        "queue_capacity": capacity,
                        "queue_length": len(queued_after),
                    }
                    for batch_id in sorted(queued_after)
                    if batch_id not in {entry["batch_id"] for entry in overflow}
                )

            if overflow:
                raise SchedulerError(
                    E_EXEC_QUEUE_FULL,
                    "execution wait queue is full",
                    details={
                        "queue_capacity": capacity,
                        "selected_before_failure": selected,
                        "queued": queued_entries,
                        "overflow": overflow,
                        "skipped": skipped,
                        "time_budget_millis": budget,
                        "max_parallel_batches": max_parallel,
                    },
                )

            # Persist the round before dispatches so the FK and audit trail are
            # complete even when no batch is selected.
            conn.execute(
                "INSERT INTO rounds(round_no, time_budget_millis, max_parallel_batches, "
                "status, selected_json, queued_json, skipped_json, created_at, completed_at) "
                "VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?, NULL)",
                (
                    round_no,
                    budget,
                    max_parallel,
                    json_dumps(selected),
                    json_dumps(queued_entries),
                    json_dumps(skipped),
                    tx.now,
                ),
            )
            for entry in queued_entries:
                batch_id = entry["batch_id"]
                current = conn.execute(
                    "SELECT status FROM batches WHERE batch_id = ?", (batch_id,)
                ).fetchone()
                if current is not None and str(current["status"]) == BATCH_PLANNED:
                    conn.execute(
                        "UPDATE batches SET status = ?, updated_at = ? WHERE batch_id = ?",
                        (BATCH_QUEUED, tx.now, batch_id),
                    )
                    tx.batch_event(
                        batch_id,
                        BATCH_PLANNED,
                        BATCH_QUEUED,
                        entry["reason"],
                        {"round_no": round_no},
                    )
            for entry in selected:
                batch_id = entry["batch_id"]
                current = conn.execute(
                    "SELECT status FROM batches WHERE batch_id = ?", (batch_id,)
                ).fetchone()
                if current is None:
                    continue
                previous = str(current["status"])
                if previous not in {BATCH_PLANNED, BATCH_QUEUED}:
                    skipped.append(
                        {"batch_id": batch_id, "reason": "NOT_READY_AT_COMMIT"}
                    )
                    continue
                conn.execute(
                    "UPDATE batches SET status = ?, updated_at = ? WHERE batch_id = ?",
                    (BATCH_RUNNING, tx.now, batch_id),
                )
                tx.batch_event(
                    batch_id,
                    previous,
                    BATCH_RUNNING,
                    "round_dispatched",
                    {
                        "round_no": round_no,
                        "priority": entry["priority"],
                        "group_rotation_reason": entry["reason"],
                    },
                )
                conn.execute(
                    "INSERT INTO round_dispatches(round_no, batch_id, dispatched_at) "
                    "VALUES (?, ?, ?)",
                    (round_no, batch_id, tx.now),
                )
                member_ids = [
                    str(row["request_id"])
                    for row in conn.execute(
                        "SELECT DISTINCT request_id FROM batch_members "
                        "WHERE batch_id = ? ORDER BY request_id",
                        (batch_id,),
                    )
                ]
                for request_id in member_ids:
                    request = conn.execute(
                        "SELECT status FROM requests WHERE request_id = ?",
                        (request_id,),
                    ).fetchone()
                    if request is not None and str(request["status"]) == BATCHED:
                        tx.transition_request(
                            request_id,
                            RUNNING,
                            "batch_dispatched",
                            {"batch_id": batch_id, "round_no": round_no},
                        )
            conn.execute(
                "UPDATE rounds SET status = 'COMPLETED', completed_at = ? "
                "WHERE round_no = ?",
                (tx.now, round_no),
            )
            assert_consistent(tx.conn, context="schedule_round")
            result = self._round_view(conn, round_no)
            result["ok"] = True
            result["replayed"] = False
            result["running_before_round"] = running_count
            result["time_used_millis"] = time_used
            return result

    def _scheduler_order(
        self, rows: Sequence[sqlite3.Row], round_no: int
    ) -> list[sqlite3.Row]:
        if not rows:
            return []
        groups = sorted({str(row["quota_group"]) for row in rows})
        if groups:
            offset = (round_no - 1) % len(groups)
            groups = groups[offset:] + groups[:offset]
        priorities = sorted({int(row["priority"]) for row in rows}, reverse=True)
        result: list[sqlite3.Row] = []
        for priority in priorities:
            for group in groups:
                tier = [
                    row
                    for row in rows
                    if int(row["priority"]) == priority
                    and str(row["quota_group"]) == group
                ]
                tier.sort(
                    key=lambda row: (
                        int(row["estimated_latency_millis"]),
                        int(row["first_submission_seq"]),
                        str(row["batch_id"]),
                    )
                )
                result.extend(tier)
        return result

    # ------------------------------------------------------------------
    # Completion and resource return
    # ------------------------------------------------------------------
    def complete_batch(self, batch_id: str) -> dict[str, Any]:
        with self.db.operation(
            "COMPLETE_BATCH", payload={"batch_id": batch_id}
        ) as tx:
            row = tx.conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                raise SchedulerError(
                    E_BATCH_NOT_FOUND,
                    f"batch {batch_id!r} does not exist",
                    details={"batch_id": batch_id},
                )
            if str(row["status"]) == BATCH_COMPLETED:
                result = self._batch_view(tx.conn, batch_id)
                result["completed"] = False
                result["idempotent"] = True
                return result
            detail = self._complete_batches_in_tx(tx, [batch_id])
            assert_consistent(tx.conn, context="complete_batch")
            result = self._batch_view(tx.conn, batch_id)
            result["completed"] = True
            result["idempotent"] = False
            result["completed_requests"] = detail["completed_requests"]
            result["released_reservations"] = detail["released_reservations"]
            return result

    def complete_round(self, round_no: int) -> dict[str, Any]:
        if isinstance(round_no, bool) or not isinstance(round_no, int) or round_no <= 0:
            raise SchedulerError(E_VALIDATION, "round_no must be a positive integer")
        with self.db.operation(
            "COMPLETE_ROUND", payload={"round_no": round_no}
        ) as tx:
            round_row = tx.conn.execute(
                "SELECT status FROM rounds WHERE round_no = ?", (round_no,)
            ).fetchone()
            if round_row is None:
                raise SchedulerError(
                    E_ROUND_NOT_FOUND,
                    f"round {round_no} does not exist",
                    details={"round_no": round_no},
                )
            rows = tx.conn.execute(
                "SELECT b.batch_id FROM batches b JOIN round_dispatches d "
                "ON d.batch_id = b.batch_id WHERE d.round_no = ? AND b.status = 'RUNNING' "
                "ORDER BY b.batch_id",
                (round_no,),
            ).fetchall()
            batch_ids = [str(row["batch_id"]) for row in rows]
            if not batch_ids:
                return {
                    "ok": True,
                    "round_no": round_no,
                    "completed_batches": [],
                    "completed_requests": [],
                    "released_reservations": [],
                    "idempotent": True,
                }
            detail = self._complete_batches_in_tx(tx, batch_ids)
            assert_consistent(tx.conn, context="complete_round")
            return {
                "ok": True,
                "round_no": round_no,
                "completed_batches": batch_ids,
                "completed_requests": detail["completed_requests"],
                "released_reservations": detail["released_reservations"],
                "idempotent": False,
            }

    def _complete_batches_in_tx(
        self, tx: WriteContext, batch_ids: Sequence[str]
    ) -> dict[str, Any]:
        conn = tx.conn
        completed_requests: list[str] = []
        released_reservations: list[dict[str, Any]] = []
        affected_requests: set[str] = set()
        for batch_id in batch_ids:
            row = conn.execute(
                "SELECT status FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if row is None:
                raise SchedulerError(
                    E_BATCH_NOT_FOUND,
                    f"batch {batch_id!r} does not exist",
                    details={"batch_id": batch_id},
                )
            previous = str(row["status"])
            if previous != BATCH_RUNNING:
                raise SchedulerError(
                    E_INVALID_TRANSITION,
                    "only RUNNING batches may be completed",
                    details={"batch_id": batch_id, "batch_status": previous},
                )
            members = [
                str(member["request_id"])
                for member in conn.execute(
                    "SELECT DISTINCT request_id FROM batch_members WHERE batch_id = ?",
                    (batch_id,),
                )
            ]
            affected_requests.update(members)
            conn.execute(
                "UPDATE batches SET status = ?, updated_at = ? WHERE batch_id = ?",
                (BATCH_COMPLETED, tx.now, batch_id),
            )
            conn.execute(
                "UPDATE node_allocations SET active = 0, released_op_id = ?, released_at = ? "
                "WHERE batch_id = ? AND active = 1",
                (tx.op_id, tx.now, batch_id),
            )
            tx.batch_event(
                batch_id,
                previous,
                BATCH_COMPLETED,
                "batch_completed",
                {"request_ids": members},
            )
        for request_id in sorted(affected_requests):
            request = conn.execute(
                "SELECT status FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if request is None:
                continue
            remaining = int(
                conn.execute(
                    "SELECT COUNT(*) AS count FROM batch_members bm "
                    "JOIN batches b ON b.batch_id = bm.batch_id "
                    "WHERE bm.request_id = ? AND b.status IN ('PLANNED', 'QUEUED', 'RUNNING')",
                    (request_id,),
                ).fetchone()["count"]
            )
            if remaining == 0:
                if str(request["status"]) != RUNNING:
                    raise SchedulerError(
                        E_QUOTA_INCONSISTENT,
                        "all request batches completed but the request is not RUNNING",
                        details={"request_id": request_id, "request_status": request["status"]},
                    )
                tx.transition_request(
                    request_id,
                    COMPLETED,
                    "all_batches_completed",
                    {"completed_batch_ids": list(batch_ids)},
                )
                reservation = conn.execute(
                    "SELECT active, memory_bytes, compute_millis FROM reservations "
                    "WHERE request_id = ?",
                    (request_id,),
                ).fetchone()
                if reservation is not None and int(reservation["active"]) == 1:
                    self._release_reservation(tx, request_id, "request_completed")
                    released_reservations.append(
                        {
                            "request_id": request_id,
                            "memory_bytes": int(reservation["memory_bytes"]),
                            "compute_millis": int(reservation["compute_millis"]),
                        }
                    )
                completed_requests.append(request_id)
        return {
            "completed_requests": completed_requests,
            "released_reservations": released_reservations,
        }

    # ------------------------------------------------------------------
    # Recovery based only on durable facts
    # ------------------------------------------------------------------
    def recover(self, *, abort_running: bool = False) -> dict[str, Any]:
        with self.db.operation(
            "RECOVERY", payload={"abort_running": bool(abort_running)}
        ) as tx:
            conn = tx.conn
            prepared_rows = conn.execute(
                "SELECT op_id, op_seq, kind, request_id, payload_json, created_at "
                "FROM intents WHERE status = 'PREPARED' AND op_id != ? "
                "ORDER BY op_seq",
                (tx.op_id,),
            ).fetchall()
            prepared_before: list[dict[str, Any]] = []
            effects_warnings: list[dict[str, Any]] = []
            resolved: list[dict[str, Any]] = []
            for row in prepared_rows:
                op_id = str(row["op_id"])
                effects = {
                    "state_transitions": int(
                        conn.execute(
                            "SELECT COUNT(*) AS count FROM state_transitions WHERE op_id = ?",
                            (op_id,),
                        ).fetchone()["count"]
                    ),
                    "batches": int(
                        conn.execute(
                            "SELECT COUNT(*) AS count FROM batches WHERE created_op_id = ?",
                            (op_id,),
                        ).fetchone()["count"]
                    ),
                    "allocations": int(
                        conn.execute(
                            "SELECT COUNT(*) AS count FROM node_allocations WHERE created_op_id = ?",
                            (op_id,),
                        ).fetchone()["count"]
                    ),
                }
                record = {
                    "op_id": op_id,
                    "op_seq": int(row["op_seq"]),
                    "kind": str(row["kind"]),
                    "request_id": row["request_id"],
                    "payload": _json_load(str(row["payload_json"]), {}),
                    "created_at": str(row["created_at"]),
                    "effects": effects,
                    "resolution": "ABORTED",
                }
                prepared_before.append(record)
                if any(effects.values()):
                    effects_warnings.append(record)
                conn.execute(
                    "UPDATE intents SET status = 'ABORTED', error_json = ?, "
                    "finished_at = ? WHERE op_id = ? AND status = 'PREPARED'",
                    (
                        json_dumps(
                            {
                                "code": "E_RECOVERY_ORPHAN_INTENT",
                                "message": "intent was prepared but its business transaction was not committed",
                                "recovery_op_id": tx.op_id,
                                "effects": effects,
                            }
                        ),
                        tx.now,
                        op_id,
                    ),
                )
                resolved.append(
                    {"op_id": op_id, "kind": row["kind"], "resolution": "ABORTED"}
                )
                try:
                    self.db.append_intent_journal(
                        {
                            "event": "ABORTED",
                            "op_id": op_id,
                            "kind": row["kind"],
                            "recovery_op_id": tx.op_id,
                            "reason": "orphan_prepared_intent",
                        }
                    )
                except OSError:
                    pass

            aborted_running: list[dict[str, Any]] = []
            if abort_running:
                running_rows = conn.execute(
                    "SELECT b.batch_id, bm.request_id FROM batches b "
                    "JOIN batch_members bm ON bm.batch_id = b.batch_id "
                    "WHERE b.status = 'RUNNING' ORDER BY b.batch_id, bm.request_id"
                ).fetchall()
                running_batch_ids = sorted({str(row["batch_id"]) for row in running_rows})
                running_request_ids = {
                    str(row["request_id"]) for row in running_rows
                }
                if running_request_ids:
                    detail = self._preempt_closure_in_tx(
                        tx,
                        initial_request_ids=running_request_ids,
                        reason="recovery_aborted_running_batch",
                        by_request_id=None,
                        batch_status=BATCH_ABORTED,
                    )
                    aborted_running = detail["batches"]
                conn.execute(
                    "UPDATE rounds SET status = 'ABORTED', completed_at = ? "
                    "WHERE status = 'OPEN'",
                    (tx.now,),
                )

            issues = find_inconsistencies(conn)
            if issues:
                raise SchedulerError(
                    E_QUOTA_INCONSISTENT,
                    "recovery found durable quota inconsistencies",
                    details={
                        "issues": issues,
                        "prepared_intents": prepared_before,
                        "aborted_running_batches": aborted_running,
                    },
                )
            report = self._recovery_report_in_tx(
                conn,
                recovery_op_id=tx.op_id,
                prepared_before=prepared_before,
                resolved=resolved,
                aborted_running=aborted_running,
                effects_warnings=effects_warnings,
                abort_running=abort_running,
            )
            conn.execute(
                "INSERT INTO recovery_runs(op_id, report_json, created_at) VALUES (?, ?, ?)",
                (tx.op_id, json_dumps(report), tx.now),
            )
            return report

    def _recovery_report_in_tx(
        self,
        conn: sqlite3.Connection,
        *,
        recovery_op_id: str,
        prepared_before: Sequence[Mapping[str, Any]],
        resolved: Sequence[Mapping[str, Any]],
        aborted_running: Sequence[Mapping[str, Any]],
        effects_warnings: Sequence[Mapping[str, Any]],
        abort_running: bool,
    ) -> dict[str, Any]:
        request_counts = {
            str(row["status"]): int(row["count"])
            for row in conn.execute(
                "SELECT status, COUNT(*) AS count FROM requests GROUP BY status "
                "ORDER BY status"
            )
        }
        batch_counts = {
            str(row["status"]): int(row["count"])
            for row in conn.execute(
                "SELECT status, COUNT(*) AS count FROM batches GROUP BY status "
                "ORDER BY status"
            )
        }
        intent_counts = {
            str(row["status"]): int(row["count"])
            for row in conn.execute(
                "SELECT status, COUNT(*) AS count FROM intents GROUP BY status "
                "ORDER BY status"
            )
        }
        active_reservations = int(
            conn.execute(
                "SELECT COUNT(*) AS count FROM reservations WHERE active = 1"
            ).fetchone()["count"]
        )
        active_allocations = int(
            conn.execute(
                "SELECT COUNT(*) AS count FROM node_allocations WHERE active = 1"
            ).fetchone()["count"]
        )
        return {
            "ok": True,
            "recovery_op_id": recovery_op_id,
            "source": "sqlite-durable-facts-only",
            "intent_journal": {
                "path": self.db.intent_journal_path,
                "exists": Path(self.db.intent_journal_path).is_file(),
                "size_bytes": Path(self.db.intent_journal_path).stat().st_size
                if Path(self.db.intent_journal_path).is_file()
                else 0,
            },
            "abort_running_requested": abort_running,
            "prepared_intent_count": len(prepared_before),
            "prepared_intents": list(prepared_before),
            "resolved_intents": list(resolved),
            "effects_warnings": list(effects_warnings),
            "aborted_running_batches": list(aborted_running),
            "durable_counts": {
                "requests_by_status": request_counts,
                "batches_by_status": batch_counts,
                "intents_by_status": intent_counts,
                "active_reservations": active_reservations,
                "active_node_allocations": active_allocations,
            },
            "quota_usage": group_usage_map(conn),
            "node_usage": node_usage_map(conn),
            "invariants": {"ok": True, "issues": []},
        }

    def recovery_status(self) -> dict[str, Any]:
        with self.db.reader() as conn:
            row = conn.execute(
                "SELECT recovery_id, op_id, report_json, created_at FROM recovery_runs "
                "ORDER BY recovery_id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return {"ok": True, "exists": False, "report": None}
            return {
                "ok": True,
                "exists": True,
                "recovery_id": int(row["recovery_id"]),
                "op_id": str(row["op_id"]),
                "created_at": str(row["created_at"]),
                "report": _json_load(str(row["report_json"]), {}),
            }

    # ------------------------------------------------------------------
    # Read-only diagnostics
    # ------------------------------------------------------------------
    def _request_view(
        self, conn: sqlite3.Connection, request_id: str
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT q.*, m.max_batch_samples, m.unit_sample_cost_millis "
            "FROM requests q JOIN models m ON m.model_id = q.model_id "
            "WHERE q.request_id = ?",
            (request_id,),
        ).fetchone()
        if row is None:
            raise SchedulerError(
                E_REQUEST_NOT_FOUND,
                f"request {request_id!r} does not exist",
                details={"request_id": request_id},
            )
        reservation = conn.execute(
            "SELECT * FROM reservations WHERE request_id = ?", (request_id,)
        ).fetchone()
        batch_rows = conn.execute(
            "SELECT b.batch_id, b.status, b.node_id, b.strategy, b.total_samples, "
            "b.memory_bytes, b.compute_millis, b.estimated_latency_millis, "
            "bm.piece_index, bm.samples, bm.memory_bytes AS member_memory_bytes, "
            "bm.compute_millis AS member_compute_millis "
            "FROM batches b JOIN batch_members bm ON bm.batch_id = b.batch_id "
            "WHERE bm.request_id = ? ORDER BY b.batch_id, bm.piece_index",
            (request_id,),
        ).fetchall()
        transitions = conn.execute(
            "SELECT transition_id, from_state, to_state, reason, op_seq, op_id, "
            "metadata_json, created_at FROM state_transitions WHERE request_id = ? "
            "ORDER BY transition_id",
            (request_id,),
        ).fetchall()
        result: dict[str, Any] = {
            "request_id": str(row["request_id"]),
            "service_name": str(row["service_name"]),
            "model_id": str(row["model_id"]),
            "node_id": row["node_id"],
            "quota_group": str(row["quota_group"]),
            "memory_bytes": int(row["memory_bytes"]),
            "samples_per_batch": int(row["samples_per_batch"]),
            "expected_latency_millis": int(row["expected_latency_millis"]),
            "priority": int(row["priority"]),
            "compute_millis": int(row["compute_millis"]),
            "model_max_batch_samples": int(row["max_batch_samples"]),
            "unit_sample_cost_millis": int(row["unit_sample_cost_millis"]),
            "status": str(row["status"]),
            "submission_seq": int(row["submission_seq"]),
            "last_reason": row["last_reason"],
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "payload": _json_load(str(row["payload_json"]), {}),
            "reservation": None if reservation is None else {
                "active": bool(reservation["active"]),
                "memory_bytes": int(reservation["memory_bytes"]),
                "compute_millis": int(reservation["compute_millis"]),
                "quota_group": str(reservation["quota_group"]),
                "created_op_id": str(reservation["created_op_id"]),
                "created_at": str(reservation["created_at"]),
                "released_op_id": reservation["released_op_id"],
                "released_at": reservation["released_at"],
                "release_reason": reservation["release_reason"],
            },
            "batches": [
                {
                    "batch_id": str(batch["batch_id"]),
                    "status": str(batch["status"]),
                    "node_id": str(batch["node_id"]),
                    "strategy": str(batch["strategy"]),
                    "piece_index": int(batch["piece_index"]),
                    "samples": int(batch["samples"]),
                    "memory_bytes": int(batch["member_memory_bytes"]),
                    "compute_millis": int(batch["member_compute_millis"]),
                    "batch_total_samples": int(batch["total_samples"]),
                    "batch_memory_bytes": int(batch["memory_bytes"]),
                    "batch_estimated_latency_millis": int(
                        batch["estimated_latency_millis"]
                    ),
                }
                for batch in batch_rows
            ],
            "state_history": [
                {
                    "transition_id": int(item["transition_id"]),
                    "from_state": item["from_state"],
                    "to_state": str(item["to_state"]),
                    "reason": str(item["reason"]),
                    "op_seq": int(item["op_seq"]),
                    "op_id": str(item["op_id"]),
                    "metadata": _json_load(str(item["metadata_json"]), {}),
                    "created_at": str(item["created_at"]),
                }
                for item in transitions
            ],
        }
        return result

    def _batch_view(
        self, conn: sqlite3.Connection, batch_id: str
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if row is None:
            raise SchedulerError(
                E_BATCH_NOT_FOUND,
                f"batch {batch_id!r} does not exist",
                details={"batch_id": batch_id},
            )
        members = conn.execute(
            "SELECT bm.request_id, bm.piece_index, bm.samples, bm.memory_bytes, "
            "bm.compute_millis, q.status AS request_status, q.priority, "
            "q.samples_per_batch "
            "FROM batch_members bm JOIN requests q ON q.request_id = bm.request_id "
            "WHERE bm.batch_id = ? ORDER BY bm.piece_index, bm.request_id",
            (batch_id,),
        ).fetchall()
        dispatches = conn.execute(
            "SELECT round_no, dispatched_at FROM round_dispatches WHERE batch_id = ? "
            "ORDER BY round_no",
            (batch_id,),
        ).fetchall()
        events = conn.execute(
            "SELECT event_id, from_status, to_status, reason, op_seq, op_id, "
            "metadata_json, created_at FROM batch_events WHERE batch_id = ? "
            "ORDER BY event_id",
            (batch_id,),
        ).fetchall()
        return {
            "batch_id": str(row["batch_id"]),
            "model_id": str(row["model_id"]),
            "node_id": str(row["node_id"]),
            "quota_group": str(row["quota_group"]),
            "strategy": str(row["strategy"]),
            "status": str(row["status"]),
            "total_samples": int(row["total_samples"]),
            "memory_bytes": int(row["memory_bytes"]),
            "compute_millis": int(row["compute_millis"]),
            "estimated_latency_millis": int(row["estimated_latency_millis"]),
            "priority": int(row["priority"]),
            "first_submission_seq": int(row["first_submission_seq"]),
            "created_round": row["created_round"],
            "reason": _json_load(str(row["reason_json"]), {}),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
            "members": [
                {
                    "request_id": str(member["request_id"]),
                    "piece_index": int(member["piece_index"]),
                    "samples": int(member["samples"]),
                    "memory_bytes": int(member["memory_bytes"]),
                    "compute_millis": int(member["compute_millis"]),
                    "request_status": str(member["request_status"]),
                    "priority": int(member["priority"]),
                    "request_samples_per_batch": int(member["samples_per_batch"]),
                }
                for member in members
            ],
            "dispatches": [
                {"round_no": int(item["round_no"]), "dispatched_at": str(item["dispatched_at"])}
                for item in dispatches
            ],
            "events": [
                {
                    "event_id": int(item["event_id"]),
                    "from_status": item["from_status"],
                    "to_status": str(item["to_status"]),
                    "reason": str(item["reason"]),
                    "op_seq": int(item["op_seq"]),
                    "op_id": str(item["op_id"]),
                    "metadata": _json_load(str(item["metadata_json"]), {}),
                    "created_at": str(item["created_at"]),
                }
                for item in events
            ],
        }

    def _round_view(self, conn: sqlite3.Connection, round_no: int) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM rounds WHERE round_no = ?", (round_no,)
        ).fetchone()
        if row is None:
            raise SchedulerError(
                E_ROUND_NOT_FOUND,
                f"round {round_no} does not exist",
                details={"round_no": round_no},
            )
        dispatches = conn.execute(
            "SELECT d.batch_id, d.dispatched_at, b.status FROM round_dispatches d "
            "JOIN batches b ON b.batch_id = d.batch_id WHERE d.round_no = ? "
            "ORDER BY d.batch_id",
            (round_no,),
        ).fetchall()
        return {
            "round_no": int(row["round_no"]),
            "status": str(row["status"]),
            "time_budget_millis": int(row["time_budget_millis"]),
            "max_parallel_batches": int(row["max_parallel_batches"]),
            "selected": _json_load(str(row["selected_json"]), []),
            "queued": _json_load(str(row["queued_json"]), []),
            "skipped": _json_load(str(row["skipped_json"]), []),
            "dispatches": [
                {
                    "batch_id": str(item["batch_id"]),
                    "batch_status": str(item["status"]),
                    "dispatched_at": str(item["dispatched_at"]),
                }
                for item in dispatches
            ],
            "created_at": str(row["created_at"]),
            "completed_at": row["completed_at"],
        }

    def get_request(self, request_id: str) -> dict[str, Any]:
        with self.db.reader() as conn:
            return self._request_view(conn, request_id)

    def list_requests(self, status: str | None = None) -> dict[str, Any]:
        with self.db.reader() as conn:
            if status is None:
                rows = conn.execute(
                    "SELECT request_id FROM requests ORDER BY submission_seq, request_id"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT request_id FROM requests WHERE status = ? "
                    "ORDER BY submission_seq, request_id",
                    (status,),
                ).fetchall()
            return {
                "ok": True,
                "count": len(rows),
                "requests": [self._request_view(conn, str(row["request_id"])) for row in rows],
            }

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        with self.db.reader() as conn:
            return self._batch_view(conn, batch_id)

    def list_batches(self, status: str | None = None) -> dict[str, Any]:
        with self.db.reader() as conn:
            if status is None:
                rows = conn.execute(
                    "SELECT batch_id FROM batches ORDER BY batch_id"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT batch_id FROM batches WHERE status = ? ORDER BY batch_id",
                    (status,),
                ).fetchall()
            return {
                "ok": True,
                "count": len(rows),
                "batches": [self._batch_view(conn, str(row["batch_id"])) for row in rows],
            }

    def get_round(self, round_no: int) -> dict[str, Any]:
        with self.db.reader() as conn:
            return self._round_view(conn, round_no)

    def get_transitions(self, request_id: str) -> dict[str, Any]:
        view = self.get_request(request_id)
        return {
            "ok": True,
            "request_id": request_id,
            "transitions": view["state_history"],
        }

    def quota_snapshot(self) -> dict[str, Any]:
        with self.db.reader() as conn:
            return {
                "ok": True,
                "quota_groups": group_usage_map(conn),
                "nodes": node_usage_map(conn),
            }

    def list_intents(self, status: str | None = None, limit: int = 200) -> dict[str, Any]:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise SchedulerError(E_VALIDATION, "limit must be a positive integer")
        with self.db.reader() as conn:
            sql = (
                "SELECT op_id, op_seq, kind, request_id, payload_json, status, "
                "error_json, created_at, finished_at FROM intents"
            )
            params: list[Any] = []
            if status is not None:
                sql += " WHERE status = ?"
                params.append(status)
            sql += " ORDER BY op_seq DESC LIMIT ?"
            params.append(limit)
            rows = conn.execute(sql, tuple(params)).fetchall()
            return {
                "ok": True,
                "count": len(rows),
                "intents": [
                    {
                        "op_id": str(row["op_id"]),
                        "op_seq": int(row["op_seq"]),
                        "kind": str(row["kind"]),
                        "request_id": row["request_id"],
                        "payload": _json_load(str(row["payload_json"]), {}),
                        "status": str(row["status"]),
                        "error": _json_load(str(row["error_json"]), None)
                        if row["error_json"] is not None
                        else None,
                        "created_at": str(row["created_at"]),
                        "finished_at": row["finished_at"],
                    }
                    for row in rows
                ],
            }

    def state_summary(self) -> dict[str, Any]:
        with self.db.reader() as conn:
            request_counts = {
                str(row["status"]): int(row["count"])
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM requests GROUP BY status "
                    "ORDER BY status"
                )
            }
            batch_counts = {
                str(row["status"]): int(row["count"])
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS count FROM batches GROUP BY status "
                    "ORDER BY status"
                )
            }
            issue_list = find_inconsistencies(conn)
            return {
                "ok": not issue_list,
                "database": self.database_path,
                "config": self.config.to_dict(),
                "requests": request_counts,
                "batches": batch_counts,
                "quota_groups": group_usage_map(conn),
                "nodes": node_usage_map(conn),
                "invariants": {"ok": not issue_list, "issues": issue_list},
            }


def _placeholders(count: int) -> str:
    return ",".join("?" for _ in range(max(1, count)))


def _json_load(text: str, default: Any) -> Any:
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return default




