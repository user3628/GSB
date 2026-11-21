from __future__ import annotations

import json
import os
import sys
import threading
from collections import Counter
from typing import Any

from .db import Database
from .domain import PolicySnapshot, ServiceSpec, parse_policy, parse_round_request
from .engine import derive_batch
from .errors import (
    AppError,
    conflicting_round,
    insufficient_capacity,
    invalid_transition,
    observation_invalid,
    round_not_found,
    spec_invalid,
    stale_snapshot,
)
from .scheduler import build_initial_state, plan_round
from .state import NodeUsage, Placement, ServiceStatus, StateDraft, StateSnapshot
from .state_machine import ServiceState, plan_transition, transition
from .units import request_hash, require_int


def _counts_by_service(placements: list[Placement] | tuple[Placement, ...]) -> dict[str, int]:
    return dict(Counter(item.service_id for item in placements))


def _next_replica_index(placements: list[Placement] | tuple[Placement, ...], service_id: str) -> int:
    prefix = f"{service_id}-"
    maximum = 0
    for placement in placements:
        if placement.replica_id.startswith(prefix):
            suffix = placement.replica_id[len(prefix) :]
            if suffix.isdigit():
                maximum = max(maximum, int(suffix))
    return maximum


class ScalingControlService:
    def __init__(self, db: Database, *, recover_on_start: bool = True):
        self.db = db
        self._write_lock = threading.RLock()
        self.startup_recovery = db.recover_incomplete() if recover_on_start else []

    def initialize_policy(self, raw: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise spec_invalid("policy body must be a JSON object")
        submitted_version = require_int(raw.get("version"), "version", 1)
        with self._write_lock:
            current_version = self.db.current_policy_version()
            if submitted_version <= current_version:
                raise stale_snapshot(
                    "policy version must be strictly greater than the current version",
                    submitted_version=submitted_version,
                    current_version=current_version,
                )
            policy = parse_policy(raw)
            current_state = self.db.latest_state()
            if current_state is None:
                draft = build_initial_state(policy)
            else:
                draft = self._build_updated_policy_state(policy, current_state)
            intent_id = self.db.begin_policy_intent(policy.version, request_hash(raw))
            try:
                revision = self.db.commit_policy(policy, raw, draft, intent_id)
            except Exception:
                self.db.abort_intent(intent_id, "policy commit failed")
                raise
            return {
                "status": "COMMITTED",
                "policy_version": policy.version,
                "revision": revision,
                "state": self.get_state_response(revision),
            }

    def _build_updated_policy_state(
        self, policy: PolicySnapshot, base: StateSnapshot
    ) -> StateDraft:
        service_map = policy.service_map
        node_map = policy.node_map
        unknown_services = sorted(
            {item.service_id for item in base.placements} - set(service_map)
        )
        if unknown_services:
            raise spec_invalid(
                "a policy update cannot remove a service that still has replicas",
                service_ids=unknown_services,
            )
        unknown_nodes = sorted({item.node_id for item in base.placements} - set(node_map))
        if unknown_nodes:
            raise spec_invalid(
                "a policy update cannot remove a node that still hosts replicas",
                node_ids=unknown_nodes,
            )

        placements = list(base.placements)
        counts = _counts_by_service(placements)
        usage_memory = {node.id: node.reserved_memory_bytes for node in policy.nodes}
        usage_compute = {node.id: node.reserved_compute_milli for node in policy.nodes}
        for placement in placements:
            usage_memory[placement.node_id] += placement.memory_bytes
            usage_compute[placement.node_id] += placement.compute_milli

        explicit: dict[str, list[Any]] = {service.id: [] for service in policy.services}
        for item in policy.initial_placements:
            explicit[item.service_id].append(item)

        # Existing placements are authoritative.  Initial placements and
        # current_replicas are used only to bootstrap a service with no replicas.
        for service in sorted(policy.services, key=lambda item: (-item.priority, item.id)):
            if counts.get(service.id, 0) > 0 or service.current_replicas == 0:
                continue
            index = _next_replica_index(placements, service.id)
            explicit_count = sum(item.count for item in explicit[service.id])
            if explicit_count and explicit_count != service.current_replicas:
                raise spec_invalid(
                    "initial placement count must equal current_replicas",
                    service_id=service.id,
                    current_replicas=service.current_replicas,
                    initial_placements=explicit_count,
                )
            explicit_nodes = [
                (item.node_id, item.count) for item in explicit[service.id]
            ]
            while index < service.current_replicas:
                node_id: str | None = None
                if explicit_nodes:
                    candidate_node, candidate_count = explicit_nodes[0]
                    node_id = candidate_node
                    if candidate_count <= 1:
                        explicit_nodes.pop(0)
                    else:
                        explicit_nodes[0] = (candidate_node, candidate_count - 1)
                else:
                    candidates: list[str] = []
                    for node in policy.nodes:
                        if not node.healthy:
                            continue
                        if (
                            node.memory_bytes - usage_memory[node.id]
                            >= service.replica_memory_bytes
                            and node.compute_milli - usage_compute[node.id]
                            >= service.replica_compute_milli
                        ):
                            candidates.append(node.id)
                    if not candidates:
                        raise insufficient_capacity(
                            "cannot bootstrap replicas for a new service",
                            service_id=service.id,
                            missing_replicas=service.current_replicas - index,
                            node_free=_node_free_details(policy, usage_memory, usage_compute),
                        )
                    candidates.sort(
                        key=lambda node_id: (
                            sum(
                                1
                                for placement in placements
                                if placement.service_id == service.id
                                and node_map[placement.node_id].fault_domain
                                == node_map[node_id].fault_domain
                            ),
                            usage_memory[node_id],
                            node_id,
                        )
                    )
                    node_id = candidates[0]
                index += 1
                placement = Placement(
                    replica_id=f"{service.id}-{index:04d}",
                    service_id=service.id,
                    node_id=node_id,
                    memory_bytes=service.replica_memory_bytes,
                    compute_milli=service.replica_compute_milli,
                )
                placements.append(placement)
                usage_memory[node_id] += service.replica_memory_bytes
                usage_compute[node_id] += service.replica_compute_milli

        for node in policy.nodes:
            if usage_memory[node.id] > node.memory_bytes:
                raise insufficient_capacity(
                    "policy update makes existing placements exceed node memory",
                    node_id=node.id,
                    used_memory_bytes=usage_memory[node.id],
                    memory_bytes=node.memory_bytes,
                    free_memory_bytes=node.memory_bytes - usage_memory[node.id],
                )
            if usage_compute[node.id] > node.compute_milli:
                raise insufficient_capacity(
                    "policy update makes existing placements exceed node compute",
                    node_id=node.id,
                    used_compute_milli=usage_compute[node.id],
                    compute_milli=node.compute_milli,
                    free_compute_milli=node.compute_milli - usage_compute[node.id],
                )

        counts = _counts_by_service(placements)
        previous = base.service_status_map
        statuses: list[ServiceStatus] = []
        for service in policy.services:
            current = counts.get(service.id, 0)
            old = previous.get(service.id)
            desired = old.desired_replicas if old is not None else current
            desired = min(max(desired, service.min_replicas), service.max_replicas)
            conflicts = _domain_conflicts(policy, placements, service)
            unhealthy_nodes = sorted(
                {
                    placement.node_id
                    for placement in placements
                    if placement.service_id == service.id
                    and not next(node for node in policy.nodes if node.id == placement.node_id).healthy
                }
            )
            if unhealthy_nodes:
                target_state = ServiceState.BLOCKED
                reason = "policy snapshot marks hosting node(s) unhealthy: " + ", ".join(
                    unhealthy_nodes
                )
            elif conflicts:
                target_state = ServiceState.DEGRADED
                reason = "policy snapshot violates fault-domain spread"
            elif current < desired:
                target_state = ServiceState.SCALING_UP
                reason = f"policy target remains above current replicas: {current} < {desired}"
            elif current > desired:
                target_state = ServiceState.SCALING_DOWN
                reason = f"policy target remains below current replicas: {current} > {desired}"
            else:
                target_state = ServiceState.STABLE
                reason = "policy snapshot committed"
            if old is not None:
                target_state = plan_transition(old.state, target_state)
            statuses.append(
                ServiceStatus(
                    service_id=service.id,
                    state=target_state,
                    current_replicas=current,
                    desired_replicas=desired,
                    reason=reason,
                    degraded_domains=tuple(conflicts),
                )
            )

        return StateDraft(
            placements=placements,
            nodes=[
                NodeUsage(
                    node_id=node.id,
                    healthy=node.healthy,
                    used_memory_bytes=usage_memory[node.id],
                    used_compute_milli=usage_compute[node.id],
                )
                for node in policy.nodes
            ],
            services=statuses,
        )

    def execute_round(self, raw: dict[str, Any]) -> dict[str, Any]:
        request = parse_round_request(raw)
        digest = request_hash(raw)
        with self._write_lock:
            existing = self.db.get_round(request.round_id)
            if existing is not None:
                if existing.request_hash != digest:
                    raise conflicting_round(
                        request.round_id,
                        existing_status=existing.status,
                        existing_request_hash=existing.request_hash,
                        submitted_request_hash=digest,
                    )
                if existing.status == "COMMITTED" and existing.result is not None:
                    result = dict(existing.result)
                    result["replayed"] = True
                    return result
                if existing.status == "ABORTED":
                    raise _error_from_stored(existing.error, replayed=True)
                if existing.status == "RUNNING":
                    self.db.recover_incomplete()
                    existing = self.db.get_round(request.round_id)
                    if existing is not None and existing.status == "ABORTED":
                        raise _error_from_stored(existing.error, replayed=True)
                    raise AppError(
                        "E_CONFLICTING_ROUND",
                        "round is still in progress",
                        {"round_id": request.round_id},
                        409,
                    )

            current_policy_version = self.db.current_policy_version()
            if request.policy_version != current_policy_version:
                raise stale_snapshot(
                    "round references a policy version that is not current",
                    submitted_policy_version=request.policy_version,
                    current_policy_version=current_policy_version,
                )
            policy = self.db.get_policy(request.policy_version)
            state = self.db.latest_state()
            if policy is None or state is None:
                raise round_not_found(request.round_id)

            if request.fault_injection is not None and os.environ.get(
                "SCALE_ENABLE_FAULT_INJECTION"
            ) != "1":
                raise spec_invalid(
                    "fault injection is disabled",
                    hint="start the server with SCALE_ENABLE_FAULT_INJECTION=1",
                )

            unknown_health_nodes = sorted(
                set(request.observations.node_health) - set(policy.node_map)
            )
            if unknown_health_nodes:
                raise observation_invalid(
                    "node_health references unknown node(s)",
                    node_ids=unknown_health_nodes,
                )

            current_counts = _counts_by_service(state.placements)
            derivations, _ = derive_batch(
                policy, request.observations, current_by_service=current_counts
            )
            base_revision = state.revision
            intent_id = self.db.start_round(
                request.round_id,
                request.policy_version,
                digest,
                json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                base_revision,
            )

            if request.fault_injection is not None:
                # This is an explicit, opt-in crash test.  The round and intent
                # are already durable; no revision has been written yet.
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(42)

            try:
                outcome = plan_round(
                    policy,
                    state,
                    request.observations,
                    derivations,
                    request.round_id,
                )
                result = {
                    "round_id": request.round_id,
                    "status": "COMMITTED",
                    "policy_version": request.policy_version,
                    "base_revision": base_revision,
                    "replayed": False,
                    "observation": {
                        "batch_id": request.observations.batch_id,
                        "sampled_at": request.observations.sampled_at,
                        "window_ms": request.observations.window_ms,
                    },
                    "derivations": [item.as_dict() for item in outcome.derivations],
                    "budget": outcome.budget,
                    "summary": outcome.summary,
                    "actions": outcome.actions,
                    "queue": outcome.queue,
                    "rejected": outcome.rejected,
                    "services": [item.as_dict() for item in outcome.draft.services],
                    "conflicts": outcome.conflicts,
                    "atomic": True,
                    "outcome": (
                        "PARTIAL"
                        if outcome.queue
                        or outcome.rejected
                        or any(
                            item.state != ServiceState.STABLE
                            for item in outcome.draft.services
                        )
                        else "COMPLETE"
                    ),
                    "business_codes": sorted(
                        {item["code"] for item in outcome.rejected}
                    ),
                }
                _, stored = self.db.commit_round(
                    request.round_id,
                    intent_id,
                    policy,
                    outcome.draft,
                    result,
                    base_revision,
                )
                return stored
            except AppError as exc:
                self.db.abort_round(request.round_id, intent_id, exc.as_dict())
                raise
            except Exception:
                error = AppError(
                    "E_ROUND_ABORTED",
                    "round failed before a complete revision could be committed",
                    {"round_id": request.round_id, "last_committed_revision": base_revision},
                    500,
                )
                self.db.abort_round(request.round_id, intent_id, error.as_dict())
                raise error

    def derive(self, raw: dict[str, Any]) -> dict[str, Any]:
        from .domain import parse_observation_batch
        from .units import require_int

        if not isinstance(raw, dict):
            raise spec_invalid("derive request must be an object")
        version = raw.get("policy_version")
        policy_version = require_int(version, "policy_version", 1) if version is not None else None
        policy = self.db.get_policy(policy_version)
        if policy is None:
            raise stale_snapshot(
                "requested policy version does not exist",
                policy_version=policy_version,
            )
        state = self.db.latest_state()
        current_counts = (
            _counts_by_service(state.placements)
            if state is not None and state.policy_version == policy.version
            else {service.id: service.current_replicas for service in policy.services}
        )
        batch = parse_observation_batch(raw.get("observations"))
        derivations, _ = derive_batch(policy, batch, current_by_service=current_counts)
        return {
            "policy_version": policy.version,
            "formula_version": derivations[0].formula_version if derivations else None,
            "deterministic": True,
            "derivations": [item.as_dict() for item in derivations],
        }

    def get_state_response(self, revision: int | None = None) -> dict[str, Any]:
        state = self.db.get_state(revision)
        if state is None:
            raise AppError(
                "E_REVISION_NOT_FOUND",
                "requested committed revision does not exist",
                {"revision": revision},
                404,
            )
        policy = self.db.get_policy(state.policy_version)
        if policy is None:
            raise AppError(
                "E_REVISION_NOT_FOUND",
                "policy for committed revision does not exist",
                {"revision": state.revision, "policy_version": state.policy_version},
                404,
            )
        status_map = state.service_status_map
        placements_by_service: dict[str, list[dict[str, Any]]] = {}
        for placement in state.placements:
            placements_by_service.setdefault(placement.service_id, []).append(
                placement.as_dict()
            )
        usage_map = state.node_usage_map
        services = []
        for service in policy.services:
            status = status_map.get(service.id)
            if status is None:
                status = ServiceStatus(
                    service_id=service.id,
                    state=ServiceState.BLOCKED,
                    current_replicas=0,
                    desired_replicas=0,
                    reason="service status missing from committed revision",
                )
            services.append(
                {
                    "id": service.id,
                    "model": service.model,
                    "state": status.state.value,
                    "current_replicas": status.current_replicas,
                    "desired_replicas": status.desired_replicas,
                    "reason": status.reason,
                    "degraded_domains": list(status.degraded_domains),
                    "min_replicas": service.min_replicas,
                    "max_replicas": service.max_replicas,
                    "target_concurrency": service.target_concurrency,
                    "latency_budget_ms": service.latency_budget_ms,
                    "replica_memory_bytes": service.replica_memory_bytes,
                    "replica_compute_milli": service.replica_compute_milli,
                    "priority": service.priority,
                    "max_replicas_per_fault_domain": service.max_replicas_per_fault_domain,
                    "placements": sorted(
                        placements_by_service.get(service.id, []),
                        key=lambda item: item["replica_id"],
                    ),
                }
            )
        nodes = []
        for node in policy.nodes:
            usage = usage_map.get(node.id)
            used_memory = usage.used_memory_bytes if usage is not None else node.reserved_memory_bytes
            used_compute = usage.used_compute_milli if usage is not None else node.reserved_compute_milli
            nodes.append(
                {
                    "id": node.id,
                    "fault_domain": node.fault_domain,
                    "healthy": usage.healthy if usage is not None else node.healthy,
                    "memory_bytes": node.memory_bytes,
                    "used_memory_bytes": used_memory,
                    "free_memory_bytes": node.memory_bytes - used_memory,
                    "reserved_memory_bytes": node.reserved_memory_bytes,
                    "compute_milli": node.compute_milli,
                    "used_compute_milli": used_compute,
                    "free_compute_milli": node.compute_milli - used_compute,
                    "reserved_compute_milli": node.reserved_compute_milli,
                }
            )
        return {
            "revision": state.revision,
            "policy_version": state.policy_version,
            "kind": state.kind,
            "round_id": state.round_id,
            "created_at": state.created_at,
            "consistent_snapshot": True,
            "services": services,
            "nodes": nodes,
            "placements": [item.as_dict() for item in state.placements],
        }

    def get_round_response(self, round_id: str) -> dict[str, Any]:
        record = self.db.get_round(round_id)
        if record is None:
            raise round_not_found(round_id)
        return record.as_dict()

    def transition_service(
        self,
        service_id: str,
        target_state_raw: Any,
        expected_revision: int | None,
        reason: str = "operator transition",
    ) -> dict[str, Any]:
        with self._write_lock:
            state = self.db.latest_state()
            if state is None:
                raise AppError(
                    "E_REVISION_NOT_FOUND", "no committed state exists", {}, 404
                )
            policy = self.db.get_policy(state.policy_version)
            if policy is None:
                raise AppError(
                    "E_REVISION_NOT_FOUND",
                    "policy for current revision does not exist",
                    {"revision": state.revision},
                    404,
                )
            if service_id not in policy.service_map:
                raise AppError(
                    "E_SERVICE_NOT_FOUND",
                    "service does not exist",
                    {"service_id": service_id},
                    404,
                )
            if expected_revision is not None and expected_revision != state.revision:
                raise stale_snapshot(
                    "expected_revision is not the latest committed revision",
                    expected_revision=expected_revision,
                    current_revision=state.revision,
                )
            try:
                target = ServiceState(target_state_raw)
            except (TypeError, ValueError) as exc:
                raise invalid_transition(
                    "unknown target service state",
                    target_state=target_state_raw,
                    allowed=[item.value for item in ServiceState],
                ) from exc
            current = state.service_status_map[service_id]
            transition(current.state, target)
            statuses = list(state.services)
            for index, status in enumerate(statuses):
                if status.service_id == service_id:
                    statuses[index] = ServiceStatus(
                        service_id=status.service_id,
                        state=target,
                        current_replicas=status.current_replicas,
                        desired_replicas=status.desired_replicas,
                        reason=reason,
                        degraded_domains=status.degraded_domains,
                    )
                    break
            draft = StateDraft(
                placements=list(state.placements),
                nodes=list(state.nodes),
                services=statuses,
            )
            revision = self.db.commit_manual_state(
                policy,
                draft,
                reason,
                {
                    "service_id": service_id,
                    "from_state": current.state.value,
                    "to_state": target.value,
                    "expected_revision": expected_revision,
                },
            )
            return {
                "status": "COMMITTED",
                "revision": revision,
                "service_id": service_id,
                "from_state": current.state.value,
                "to_state": target.value,
            }


def _node_free_details(
    policy: PolicySnapshot,
    usage_memory: dict[str, int],
    usage_compute: dict[str, int],
) -> list[dict[str, Any]]:
    return [
        {
            "node_id": node.id,
            "healthy": node.healthy,
            "free_memory_bytes": node.memory_bytes - usage_memory[node.id],
            "free_compute_milli": node.compute_milli - usage_compute[node.id],
        }
        for node in policy.nodes
    ]


def _domain_conflicts(
    policy: PolicySnapshot,
    placements: list[Placement],
    service: ServiceSpec,
) -> list[dict[str, Any]]:
    node_map = policy.node_map
    domains: dict[str, list[str]] = {}
    for placement in placements:
        if placement.service_id != service.id:
            continue
        domain = node_map[placement.node_id].fault_domain
        domains.setdefault(domain, []).append(placement.node_id)
    conflicts = []
    for domain, node_ids in sorted(domains.items()):
        if len(node_ids) > service.max_replicas_per_fault_domain:
            conflicts.append(
                {
                    "fault_domain": domain,
                    "max_allowed": service.max_replicas_per_fault_domain,
                    "actual": len(node_ids),
                    "node_ids": sorted(set(node_ids)),
                }
            )
    return conflicts


def _error_from_stored(stored: dict[str, Any] | None, *, replayed: bool) -> AppError:
    if not stored or "error" not in stored:
        return AppError(
            "E_ROUND_ABORTED",
            "round was aborted",
            {"replayed": replayed},
            409,
        )
    error = stored["error"]
    code = str(error.get("code", "E_ROUND_ABORTED"))
    details = dict(error.get("details") or {})
    details["replayed"] = replayed
    status = {
        "E_SCALE_QUEUE_FULL": 429,
        "E_INSUFFICIENT_CAPACITY": 409,
        "E_CONFLICTING_ROUND": 409,
        "E_STALE_SNAPSHOT": 409,
        "E_ROUND_NOT_FOUND": 404,
    }.get(code, 409)
    return AppError(code, str(error.get("message", "round was aborted")), details, status)
