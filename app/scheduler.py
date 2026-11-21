from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .domain import (
    InitialPlacement,
    NodeSpec,
    ObservationBatch,
    PolicySnapshot,
    ServiceSpec,
)
from .engine import Derivation
from .errors import observation_invalid, scale_queue_full, spec_invalid
from .state import NodeUsage, Placement, ServiceStatus, StateDraft, StateSnapshot
from .state_machine import ServiceState, plan_transition


@dataclass
class _NodeRuntime:
    spec: NodeSpec
    healthy: bool
    used_memory_bytes: int
    used_compute_milli: int

    @property
    def free_memory_bytes(self) -> int:
        return self.spec.memory_bytes - self.used_memory_bytes

    @property
    def free_compute_milli(self) -> int:
        return self.spec.compute_milli - self.used_compute_milli

    def can_fit(self, service: ServiceSpec) -> bool:
        return (
            self.healthy
            and self.free_memory_bytes >= service.replica_memory_bytes
            and self.free_compute_milli >= service.replica_compute_milli
        )

    def as_usage(self) -> NodeUsage:
        return NodeUsage(
            node_id=self.spec.id,
            healthy=self.healthy,
            used_memory_bytes=self.used_memory_bytes,
            used_compute_milli=self.used_compute_milli,
        )


@dataclass
class PlanOutcome:
    draft: StateDraft
    derivations: list[Derivation]
    actions: list[dict[str, Any]]
    queue: list[dict[str, Any]]
    rejected: list[dict[str, Any]]
    summary: dict[str, Any]
    budget: dict[str, Any]
    conflicts: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


def _domain_key(service_id: str, domain: str) -> tuple[str, str]:
    return service_id, domain


class _Planner:
    def __init__(
        self,
        policy: PolicySnapshot,
        state: StateSnapshot,
        health_overrides: dict[str, bool],
    ) -> None:
        unknown_health = sorted(set(health_overrides) - set(policy.node_map))
        if unknown_health:
            raise observation_invalid(
                "node_health references unknown node(s)", node_ids=unknown_health
            )
        current_nodes = state.node_usage_map
        self.nodes: dict[str, _NodeRuntime] = {}
        for node in policy.nodes:
            usage = current_nodes.get(node.id)
            self.nodes[node.id] = _NodeRuntime(
                spec=node,
                healthy=health_overrides.get(
                    node.id, usage.healthy if usage is not None else node.healthy
                ),
                used_memory_bytes=(
                    usage.used_memory_bytes if usage is not None else node.reserved_memory_bytes
                ),
                used_compute_milli=(
                    usage.used_compute_milli if usage is not None else node.reserved_compute_milli
                ),
            )
        self.placements: list[Placement] = list(state.placements)
        self.services = policy.service_map
        self.policy = policy
        self.domain_counts: dict[tuple[str, str], int] = {}
        for placement in self.placements:
            domain = self.nodes[placement.node_id].spec.fault_domain
            key = _domain_key(placement.service_id, domain)
            self.domain_counts[key] = self.domain_counts.get(key, 0) + 1
        self.max_replica_index: dict[str, int] = {}
        for placement in self.placements:
            if placement.service_id not in self.services:
                continue
            prefix = f"{placement.service_id}-"
            if placement.replica_id.startswith(prefix):
                suffix = placement.replica_id[len(prefix) :]
                if suffix.isdigit():
                    self.max_replica_index[placement.service_id] = max(
                        self.max_replica_index.get(placement.service_id, 0), int(suffix)
                    )

    def service_placements(self, service_id: str) -> list[Placement]:
        return [item for item in self.placements if item.service_id == service_id]

    def current_count(self, service_id: str) -> int:
        return sum(1 for item in self.placements if item.service_id == service_id)

    def _next_replica_id(self, service_id: str) -> str:
        index = self.max_replica_index.get(service_id, 0) + 1
        while any(item.replica_id == f"{service_id}-{index:04d}" for item in self.placements):
            index += 1
        self.max_replica_index[service_id] = index
        return f"{service_id}-{index:04d}"

    def _count_for_domain(
        self, service: ServiceSpec, domain: str, source: Placement | None = None
    ) -> int:
        count = self.domain_counts.get(_domain_key(service.id, domain), 0)
        if source is not None:
            source_domain = self.nodes[source.node_id].spec.fault_domain
            if source_domain == domain:
                count -= 1
        return count

    def find_target(
        self, service: ServiceSpec, source: Placement | None = None
    ) -> tuple[str | None, bool, list[dict[str, Any]]]:
        candidates: list[tuple[tuple[int, int, int, str], str, bool]] = []
        for node_id, node in self.nodes.items():
            if source is not None and node_id == source.node_id:
                continue
            if not node.can_fit(service):
                continue
            domain = node.spec.fault_domain
            count = self._count_for_domain(service, domain, source)
            within_limit = count < service.max_replicas_per_fault_domain
            key = (
                0 if within_limit else 1,
                count,
                -(node.free_memory_bytes // max(service.replica_memory_bytes, 1)),
                node_id,
            )
            candidates.append((key, node_id, not within_limit))
        if not candidates:
            details = []
            for node_id, node in sorted(self.nodes.items()):
                details.append(
                    {
                        "node_id": node_id,
                        "healthy": node.healthy,
                        "free_memory_bytes": node.free_memory_bytes,
                        "free_compute_milli": node.free_compute_milli,
                    }
                )
            return None, False, details
        candidates.sort(key=lambda item: item[0])
        return candidates[0][1], candidates[0][2], []

    def reserve(
        self, service: ServiceSpec, node_id: str, replica_id: str | None = None
    ) -> Placement:
        node = self.nodes[node_id]
        if not node.can_fit(service):
            raise AssertionError("reserve called without capacity")
        node.used_memory_bytes += service.replica_memory_bytes
        node.used_compute_milli += service.replica_compute_milli
        placement = Placement(
            replica_id=replica_id or self._next_replica_id(service.id),
            service_id=service.id,
            node_id=node_id,
            memory_bytes=service.replica_memory_bytes,
            compute_milli=service.replica_compute_milli,
        )
        self.placements.append(placement)
        key = _domain_key(service.id, node.spec.fault_domain)
        self.domain_counts[key] = self.domain_counts.get(key, 0) + 1
        return placement

    def release(self, placement: Placement) -> None:
        node = self.nodes[placement.node_id]
        node.used_memory_bytes -= placement.memory_bytes
        node.used_compute_milli -= placement.compute_milli
        if node.used_memory_bytes < 0 or node.used_compute_milli < 0:
            raise AssertionError("node usage became negative")
        self.placements = [item for item in self.placements if item.replica_id != placement.replica_id]
        key = _domain_key(placement.service_id, node.spec.fault_domain)
        self.domain_counts[key] = max(0, self.domain_counts.get(key, 0) - 1)

    def migrate(self, placement: Placement, target_node_id: str) -> Placement:
        service = self.services[placement.service_id]
        old_node = self.nodes[placement.node_id]
        old_domain = old_node.spec.fault_domain
        new_node = self.nodes[target_node_id]
        new_domain = new_node.spec.fault_domain
        if not new_node.can_fit(service):
            raise AssertionError("migration target lacks capacity")
        new_node.used_memory_bytes += placement.memory_bytes
        new_node.used_compute_milli += placement.compute_milli
        old_node.used_memory_bytes -= placement.memory_bytes
        old_node.used_compute_milli -= placement.compute_milli
        old_key = _domain_key(service.id, old_domain)
        self.domain_counts[old_key] = max(0, self.domain_counts.get(old_key, 0) - 1)
        new_key = _domain_key(service.id, new_domain)
        self.domain_counts[new_key] = self.domain_counts.get(new_key, 0) + 1
        migrated = Placement(
            replica_id=placement.replica_id,
            service_id=placement.service_id,
            node_id=target_node_id,
            memory_bytes=placement.memory_bytes,
            compute_milli=placement.compute_milli,
        )
        self.placements = [
            migrated if item.replica_id == placement.replica_id else item
            for item in self.placements
        ]
        return migrated

    def conflict_details(self, service: ServiceSpec) -> list[dict[str, Any]]:
        domains: dict[str, list[str]] = {}
        for placement in self.service_placements(service.id):
            domain = self.nodes[placement.node_id].spec.fault_domain
            domains.setdefault(domain, []).append(placement.node_id)
        conflicts = []
        for domain, nodes in sorted(domains.items()):
            count = len(nodes)
            if count > service.max_replicas_per_fault_domain:
                conflicts.append(
                    {
                        "fault_domain": domain,
                        "max_allowed": service.max_replicas_per_fault_domain,
                        "actual": count,
                        "node_ids": sorted(set(nodes)),
                    }
                )
        return conflicts

    def unhealthy_placements(self, service_id: str) -> list[Placement]:
        return [
            placement
            for placement in self.service_placements(service_id)
            if not self.nodes[placement.node_id].healthy
        ]


def _scale_down(
    planner: _Planner,
    desired: dict[str, int],
    actions: list[dict[str, Any]],
) -> tuple[int, dict[str, int]]:
    removed_by_service: dict[str, int] = {}
    total = 0
    for service in sorted(planner.policy.services, key=lambda item: (-item.priority, item.id)):
        current = planner.current_count(service.id)
        remove_count = max(0, current - desired[service.id])
        if remove_count == 0:
            continue
        candidates = planner.service_placements(service.id)

        def order(placement: Placement) -> tuple[int, int, str]:
            node = planner.nodes[placement.node_id]
            domain_count = planner._count_for_domain(
                service, node.spec.fault_domain
            )
            return (0 if not node.healthy else 1, -domain_count, placement.replica_id)

        candidates.sort(key=order)
        for placement in candidates[:remove_count]:
            node = planner.nodes[placement.node_id]
            planner.release(placement)
            actions.append(
                {
                    "type": "SCALE_DOWN",
                    "service_id": service.id,
                    "replica_id": placement.replica_id,
                    "node_id": placement.node_id,
                    "fault_domain": node.spec.fault_domain,
                    "memory_bytes": placement.memory_bytes,
                    "compute_milli": placement.compute_milli,
                }
            )
            removed_by_service[service.id] = removed_by_service.get(service.id, 0) + 1
            total += 1
    return total, removed_by_service


def _final_statuses(
    planner: _Planner,
    desired: dict[str, int],
    previous: dict[str, ServiceStatus],
    blocked_reasons: dict[str, list[str]],
    granted_by_service: dict[str, int],
    queued_by_service: dict[str, int],
    rejected_by_service: dict[str, int],
    scaled_down_by_service: dict[str, int],
) -> list[ServiceStatus]:
    result: list[ServiceStatus] = []
    for service in planner.policy.services:
        current = planner.current_count(service.id)
        target = desired[service.id]
        conflicts = planner.conflict_details(service)
        unhealthy = planner.unhealthy_placements(service.id)
        if unhealthy:
            target_state = ServiceState.BLOCKED
            reason = (
                "replicas remain on unhealthy node(s): "
                + ", ".join(sorted({item.node_id for item in unhealthy}))
            )
        elif blocked_reasons.get(service.id):
            target_state = ServiceState.BLOCKED
            reason = "; ".join(blocked_reasons[service.id])
        elif conflicts:
            target_state = ServiceState.DEGRADED
            reason = "fault-domain spread constraint exceeded"
        elif current < target:
            target_state = ServiceState.SCALING_UP
            reason = (
                f"scale-up incomplete: current={current}, desired={target}, "
                f"granted={granted_by_service.get(service.id, 0)}, "
                f"queued={queued_by_service.get(service.id, 0)}, "
                f"rejected={rejected_by_service.get(service.id, 0)}"
            )
        elif current > target:
            target_state = ServiceState.SCALING_DOWN
            reason = f"scale-down incomplete: current={current}, desired={target}"
        else:
            target_state = ServiceState.STABLE
            if scaled_down_by_service.get(service.id, 0):
                reason = f"scale-down completed and reclaimed {scaled_down_by_service[service.id]} replica(s)"
            elif granted_by_service.get(service.id, 0):
                reason = f"scale-up completed with {granted_by_service[service.id]} granted replica(s)"
            else:
                reason = "desired replicas satisfied"

        old_state = previous.get(service.id, ServiceStatus(
            service_id=service.id,
            state=ServiceState.STABLE,
            current_replicas=current,
            desired_replicas=target,
            reason="initial",
        )).state
        target_state = plan_transition(old_state, target_state)
        result.append(
            ServiceStatus(
                service_id=service.id,
                state=target_state,
                current_replicas=current,
                desired_replicas=target,
                reason=reason,
                degraded_domains=tuple(conflicts),
            )
        )
    return result


def plan_round(
    policy: PolicySnapshot,
    state: StateSnapshot,
    observations: ObservationBatch,
    derivations: list[Derivation],
    round_id: str,
) -> PlanOutcome:
    planner = _Planner(policy, state, observations.node_health)
    actions: list[dict[str, Any]] = []
    queue: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    desired = {item.service_id: item.desired_replicas for item in derivations}
    arrival_order = {
        item.service_id: index for index, item in enumerate(observations.services)
    }
    previous = state.service_status_map

    requested_by_service = {
        service.id: max(0, desired[service.id] - planner.current_count(service.id))
        for service in policy.services
    }
    total_requested = sum(requested_by_service.values())
    scaled_down_total, scaled_down_by_service = _scale_down(planner, desired, actions)

    blocked_reasons: dict[str, list[str]] = {}
    granted_by_service: dict[str, int] = {}
    queued_by_service: dict[str, int] = {}
    rejected_by_service: dict[str, int] = {}
    migrated_by_service: dict[str, int] = {}
    queued_migrations_by_service: dict[str, int] = {}
    rejected_migrations_by_service: dict[str, int] = {}

    tasks: list[dict[str, Any]] = []
    for service in policy.services:
        for placement in planner.unhealthy_placements(service.id):
            tasks.append(
                {
                    "kind": "MIGRATION",
                    "service": service,
                    "placement": placement,
                    "arrival": arrival_order[service.id],
                    "sort": (
                        -service.priority,
                        arrival_order[service.id],
                        0,
                        service.id,
                        placement.replica_id,
                    ),
                }
            )
        deficit = max(0, desired[service.id] - planner.current_count(service.id))
        if deficit:
            tasks.append(
                {
                    "kind": "SCALE_UP",
                    "service": service,
                    "count": deficit,
                    "arrival": arrival_order[service.id],
                    "sort": (
                        -service.priority,
                        arrival_order[service.id],
                        1,
                        service.id,
                        "",
                    ),
                }
            )
    tasks.sort(key=lambda task: task["sort"])

    slots = policy.runtime.slot_count
    consumed = 0
    admission_sequence = 0

    def limiting_reason() -> str:
        return "ROUND_TIME_BUDGET_EXHAUSTED"

    def wave_for(sequence: int) -> int:
        return (sequence - 1) // policy.runtime.max_parallel_scales + 1

    for task in tasks:
        service: ServiceSpec = task["service"]
        if task["kind"] == "MIGRATION":
            if consumed >= slots:
                queue.append(
                    {
                        "service_id": service.id,
                        "task_type": "MIGRATION",
                        "replicas": 1,
                        "reason": limiting_reason(),
                        "priority": service.priority,
                        "arrival_order": task["arrival"],
                        "replica_ids": [task["placement"].replica_id],
                    }
                )
                queued_migrations_by_service[service.id] = (
                    queued_migrations_by_service.get(service.id, 0) + 1
                )
                continue
            target, degraded, free_details = planner.find_target(
                service, source=task["placement"]
            )
            consumed += 1
            admission_sequence += 1
            if target is None:
                blocked_reasons.setdefault(service.id, []).append(
                    "migration could not find target capacity"
                )
                rejected_migrations_by_service[service.id] = (
                    rejected_migrations_by_service.get(service.id, 0) + 1
                )
                rejected.append(
                    {
                        "service_id": service.id,
                        "task_type": "MIGRATION",
                        "replicas": 1,
                        "code": "E_INSUFFICIENT_CAPACITY",
                        "reason": "unhealthy replica has no feasible migration target",
                        "replica_ids": [task["placement"].replica_id],
                        "source_node_id": task["placement"].node_id,
                        "node_free": free_details,
                    }
                )
                continue
            migrated = planner.migrate(task["placement"], target)
            migrated_by_service[service.id] = migrated_by_service.get(service.id, 0) + 1
            node = planner.nodes[target]
            actions.append(
                {
                    "type": "MIGRATE",
                    "service_id": service.id,
                    "replica_id": migrated.replica_id,
                    "from_node_id": task["placement"].node_id,
                    "to_node_id": target,
                    "to_fault_domain": node.spec.fault_domain,
                    "domain_degraded": degraded,
                    "admission_sequence": admission_sequence,
                    "parallel_wave": wave_for(admission_sequence),
                }
            )
            continue

        remaining = int(task["count"])
        while remaining > 0 and consumed < slots:
            target, degraded, free_details = planner.find_target(service)
            consumed += 1
            admission_sequence += 1
            if target is None:
                blocked_reasons.setdefault(service.id, []).append(
                    "insufficient node capacity for scale-up"
                )
                rejected_by_service[service.id] = (
                    rejected_by_service.get(service.id, 0) + remaining
                )
                rejected.append(
                    {
                        "service_id": service.id,
                        "task_type": "SCALE_UP",
                        "replicas": remaining,
                        "code": "E_INSUFFICIENT_CAPACITY",
                        "reason": "no healthy node has enough memory and compute",
                        "node_free": free_details,
                    }
                )
                remaining = 0
                break
            placement = planner.reserve(service, target)
            node = planner.nodes[target]
            actions.append(
                {
                    "type": "SCALE_UP",
                    "service_id": service.id,
                    "replica_id": placement.replica_id,
                    "node_id": target,
                    "fault_domain": node.spec.fault_domain,
                    "memory_bytes": placement.memory_bytes,
                    "compute_milli": placement.compute_milli,
                    "domain_degraded": degraded,
                    "admission_sequence": admission_sequence,
                    "parallel_wave": wave_for(admission_sequence),
                }
            )
            granted_by_service[service.id] = granted_by_service.get(service.id, 0) + 1
            remaining -= 1
        if remaining > 0:
            queue.append(
                {
                    "service_id": service.id,
                    "task_type": "SCALE_UP",
                    "replicas": remaining,
                    "reason": limiting_reason(),
                    "priority": service.priority,
                    "arrival_order": task["arrival"],
                }
            )
            queued_by_service[service.id] = (
                queued_by_service.get(service.id, 0) + remaining
            )

    queued_total = sum(item["replicas"] for item in queue)
    if queued_total > policy.runtime.wait_queue_limit:
        raise scale_queue_full(
            "bounded scale wait queue is full; round was not committed",
            queued_replicas=queued_total,
            wait_queue_limit=policy.runtime.wait_queue_limit,
            grant_slots=slots,
            max_parallel_scales=policy.runtime.max_parallel_scales,
            time_budget_ms=policy.runtime.round_time_budget_ms,
            placement_cost_ms=policy.runtime.placement_cost_ms,
            queue=queue,
        )

    statuses = _final_statuses(
        planner,
        desired,
        previous,
        blocked_reasons,
        granted_by_service,
        queued_by_service,
        rejected_by_service,
        scaled_down_by_service,
    )
    status_map = {item.service_id: item for item in statuses}
    for service in policy.services:
        if service.id in queued_migrations_by_service:
            status = status_map[service.id]
            if status.state != ServiceState.BLOCKED:
                status_map[service.id] = ServiceStatus(
                    service_id=status.service_id,
                    state=plan_transition(status.state, ServiceState.BLOCKED),
                    current_replicas=status.current_replicas,
                    desired_replicas=status.desired_replicas,
                    reason="unhealthy replica migration is queued by the round budget",
                    degraded_domains=status.degraded_domains,
                )
    statuses = [status_map[service.id] for service in policy.services]

    granted_total = sum(granted_by_service.values())
    rejected_total = sum(rejected_by_service.values())
    migrated_total = sum(migrated_by_service.values())
    rejected_migrations_total = sum(rejected_migrations_by_service.values())
    queued_migrations_total = sum(queued_migrations_by_service.values())
    scale_up_queued_total = sum(queued_by_service.values())
    summary = {
        "scale_up_requested": total_requested,
        "scale_up_granted": granted_total,
        "scale_up_queued": scale_up_queued_total,
        "scale_up_rejected": rejected_total,
        "scale_down_removed": scaled_down_total,
        "migrations_completed": migrated_total,
        "migrations_queued": queued_migrations_total,
        "migrations_rejected": rejected_migrations_total,
        "queue_total_replicas": queued_total,
        "admission_slots_used": consumed,
        "admission_slots_available": slots,
    }
    budget = {
        "max_parallel_scales": policy.runtime.max_parallel_scales,
        "round_time_budget_ms": policy.runtime.round_time_budget_ms,
        "placement_cost_ms": policy.runtime.placement_cost_ms,
        "time_budget_slots": policy.runtime.round_time_budget_ms // policy.runtime.placement_cost_ms,
        "available_time_waves": policy.runtime.wave_count,
        "grant_slots": slots,
        "wait_queue_limit": policy.runtime.wait_queue_limit,
        "waves_used": (
            (consumed + policy.runtime.max_parallel_scales - 1)
            // policy.runtime.max_parallel_scales
        ),
        "deterministic_virtual_time_ms": (
            (consumed + policy.runtime.max_parallel_scales - 1)
            // policy.runtime.max_parallel_scales
        )
        * policy.runtime.placement_cost_ms,
    }
    conflicts = {
        service.id: planner.conflict_details(service) for service in policy.services
    }
    draft = StateDraft(
        placements=list(planner.placements),
        nodes=[node.as_usage() for node in planner.nodes.values()],
        services=statuses,
    )
    return PlanOutcome(
        draft=draft,
        derivations=derivations,
        actions=actions,
        queue=queue,
        rejected=rejected,
        summary=summary,
        budget=budget,
        conflicts=conflicts,
    )


def _initial_replica_id(service_id: str, index: int) -> str:
    return f"{service_id}-{index:04d}"


def build_initial_state(policy: PolicySnapshot) -> StateDraft:
    """Build the first committed revision from normalized policy facts."""

    usage_memory = {node.id: node.reserved_memory_bytes for node in policy.nodes}
    usage_compute = {node.id: node.reserved_compute_milli for node in policy.nodes}
    healthy = {node.id: node.healthy for node in policy.nodes}
    node_map = policy.node_map
    placements: list[Placement] = []

    explicit: dict[str, list[InitialPlacement]] = {
        service.id: [] for service in policy.services
    }
    for item in policy.initial_placements:
        explicit[item.service_id].append(item)

    for service in policy.services:
        index = 0
        for item in explicit[service.id]:
            for _ in range(item.count):
                index += 1
                node = node_map[item.node_id]
                placements.append(
                    Placement(
                        replica_id=_initial_replica_id(service.id, index),
                        service_id=service.id,
                        node_id=node.id,
                        memory_bytes=service.replica_memory_bytes,
                        compute_milli=service.replica_compute_milli,
                    )
                )
                usage_memory[node.id] += service.replica_memory_bytes
                usage_compute[node.id] += service.replica_compute_milli

        # If a policy omits explicit placements, deterministically spread its
        # current replicas across healthy nodes.  Explicit samples provide exact
        # initial topology and therefore bypass this convenience path.
        while index < service.current_replicas:
            candidates = []
            for node in policy.nodes:
                if not node.healthy:
                    continue
                free_memory = node.memory_bytes - usage_memory[node.id]
                free_compute = node.compute_milli - usage_compute[node.id]
                if (
                    free_memory >= service.replica_memory_bytes
                    and free_compute >= service.replica_compute_milli
                ):
                    candidates.append(node.id)
            if not candidates:
                raise spec_invalid(
                    "cannot place initial replicas within node capacity",
                    service_id=service.id,
                    missing_replicas=service.current_replicas - index,
                )
            candidates.sort(
                key=lambda node_id: (
                    sum(1 for p in placements if p.service_id == service.id and node_map[p.node_id].fault_domain == node_map[node_id].fault_domain),
                    usage_memory[node_id],
                    node_id,
                )
            )
            node_id = candidates[0]
            index += 1
            placements.append(
                Placement(
                    replica_id=_initial_replica_id(service.id, index),
                    service_id=service.id,
                    node_id=node_id,
                    memory_bytes=service.replica_memory_bytes,
                    compute_milli=service.replica_compute_milli,
                )
            )
            usage_memory[node_id] += service.replica_memory_bytes
            usage_compute[node_id] += service.replica_compute_milli

    domain_counts: dict[tuple[str, str], int] = {}
    for placement in placements:
        key = (placement.service_id, node_map[placement.node_id].fault_domain)
        domain_counts[key] = domain_counts.get(key, 0) + 1
    statuses: list[ServiceStatus] = []
    for service in policy.services:
        current = sum(1 for item in placements if item.service_id == service.id)
        conflicts = []
        for domain in sorted({node.fault_domain for node in policy.nodes}):
            count = domain_counts.get((service.id, domain), 0)
            if count > service.max_replicas_per_fault_domain:
                conflicts.append(
                    {
                        "fault_domain": domain,
                        "max_allowed": service.max_replicas_per_fault_domain,
                        "actual": count,
                        "node_ids": sorted(
                            {
                                p.node_id
                                for p in placements
                                if p.service_id == service.id
                                and node_map[p.node_id].fault_domain == domain
                            }
                        ),
                    }
                )
        unhealthy_nodes = sorted(
            {
                p.node_id
                for p in placements
                if p.service_id == service.id and not healthy[p.node_id]
            }
        )
        desired = min(max(current, service.min_replicas), service.max_replicas)
        if unhealthy_nodes:
            state = ServiceState.BLOCKED
            reason = "initial replicas are on unhealthy node(s): " + ", ".join(unhealthy_nodes)
        elif conflicts:
            state = ServiceState.DEGRADED
            reason = "fault-domain spread constraint exceeded"
        elif current < desired:
            state = ServiceState.SCALING_UP
            reason = f"initial replicas below configured minimum: {current} < {desired}"
        elif current > desired:
            state = ServiceState.SCALING_DOWN
            reason = f"initial replicas above configured maximum: {current} > {desired}"
        else:
            state = ServiceState.STABLE
            reason = "initial committed state"
        statuses.append(
            ServiceStatus(
                service_id=service.id,
                state=state,
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
                healthy=healthy[node.id],
                used_memory_bytes=usage_memory[node.id],
                used_compute_milli=usage_compute[node.id],
            )
            for node in policy.nodes
        ],
        services=statuses,
    )
