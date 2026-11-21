from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .errors import observation_invalid, spec_invalid
from .units import (
    parse_compute_milli,
    parse_memory,
    parse_timestamp,
    require_int,
    require_text,
)


@dataclass(frozen=True)
class ServiceSpec:
    id: str
    model: str
    current_replicas: int
    min_replicas: int
    max_replicas: int
    replica_memory_bytes: int
    replica_compute_milli: int
    target_concurrency: int
    latency_budget_ms: int
    priority: int
    max_replicas_per_fault_domain: int


@dataclass(frozen=True)
class NodeSpec:
    id: str
    fault_domain: str
    memory_bytes: int
    compute_milli: int
    healthy: bool
    reserved_memory_bytes: int
    reserved_compute_milli: int


@dataclass(frozen=True)
class RuntimeBudget:
    max_parallel_scales: int
    round_time_budget_ms: int
    placement_cost_ms: int
    wait_queue_limit: int

    @property
    def slot_count(self) -> int:
        waves = self.round_time_budget_ms // self.placement_cost_ms
        return max(0, self.max_parallel_scales * waves)

    @property
    def wave_count(self) -> int:
        return self.round_time_budget_ms // self.placement_cost_ms


@dataclass(frozen=True)
class InitialPlacement:
    service_id: str
    node_id: str
    count: int


@dataclass(frozen=True)
class PolicySnapshot:
    version: int
    generated_at: str
    services: tuple[ServiceSpec, ...]
    nodes: tuple[NodeSpec, ...]
    runtime: RuntimeBudget
    initial_placements: tuple[InitialPlacement, ...]

    @property
    def service_map(self) -> dict[str, ServiceSpec]:
        return {service.id: service for service in self.services}

    @property
    def node_map(self) -> dict[str, NodeSpec]:
        return {node.id: node for node in self.nodes}


@dataclass(frozen=True)
class ServiceObservation:
    service_id: str
    sampled_at: str
    arrivals: int
    queue_duration_ms: int
    p99_latency_ms: int


@dataclass(frozen=True)
class ObservationBatch:
    batch_id: str
    sampled_at: str
    window_ms: int
    services: tuple[ServiceObservation, ...]
    node_health: dict[str, bool] = field(default_factory=dict)

    @property
    def service_map(self) -> dict[str, ServiceObservation]:
        return {observation.service_id: observation for observation in self.services}


@dataclass(frozen=True)
class FaultInjection:
    crash_after: str


@dataclass(frozen=True)
class RoundRequest:
    round_id: str
    policy_version: int
    observations: ObservationBatch
    fault_injection: FaultInjection | None = None


def _list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise spec_invalid(f"{field_name} must be a list", field=field_name)
    return value


def _object(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise spec_invalid(f"{field_name} must be an object", field=field_name)
    return value


def _boolean(value: Any, field_name: str, default: bool | None = None) -> bool:
    if value is None and default is not None:
        return default
    if not isinstance(value, bool):
        raise spec_invalid(f"{field_name} must be a boolean", field=field_name)
    return value


def parse_runtime(value: Any) -> RuntimeBudget:
    raw = _object(value, "runtime")
    budget = RuntimeBudget(
        max_parallel_scales=require_int(
            raw.get("max_parallel_scales"), "runtime.max_parallel_scales", 1
        ),
        round_time_budget_ms=require_int(
            raw.get("round_time_budget_ms"), "runtime.round_time_budget_ms", 0
        ),
        placement_cost_ms=require_int(
            raw.get("placement_cost_ms"), "runtime.placement_cost_ms", 1
        ),
        wait_queue_limit=require_int(
            raw.get("wait_queue_limit"), "runtime.wait_queue_limit", 0
        ),
    )
    return budget


def parse_policy(raw_value: Any) -> PolicySnapshot:
    raw = _object(raw_value, "policy")
    version = require_int(raw.get("version"), "version", 1)
    generated_at = parse_timestamp(raw.get("generated_at"), "generated_at")
    runtime = parse_runtime(raw.get("runtime"))

    services: list[ServiceSpec] = []
    service_ids: set[str] = set()
    for index, item in enumerate(_list(raw.get("services"), "services")):
        entry = _object(item, f"services[{index}]")
        service_id = require_text(entry.get("id"), f"services[{index}].id")
        if service_id in service_ids:
            raise spec_invalid("duplicate service id", service_id=service_id)
        service_ids.add(service_id)

        minimum = require_int(entry.get("min_replicas"), f"services[{index}].min_replicas", 0)
        maximum = require_int(entry.get("max_replicas"), f"services[{index}].max_replicas", 0)
        if maximum < minimum:
            raise spec_invalid(
                "max_replicas must be >= min_replicas",
                service_id=service_id,
                min_replicas=minimum,
                max_replicas=maximum,
            )
        current = require_int(
            entry.get("current_replicas"), f"services[{index}].current_replicas", 0
        )
        # Zero is structurally valid but makes a target uncomputable; the round
        # reports E_OBSERVATION_INVALID at derivation time as required.
        target_concurrency = require_int(
            entry.get("target_concurrency"), f"services[{index}].target_concurrency", 0
        )
        latency_budget = require_int(
            entry.get("latency_budget_ms"), f"services[{index}].latency_budget_ms", 0
        )
        priority = require_int(entry.get("priority", 0), f"services[{index}].priority")
        max_per_domain = require_int(
            entry.get("max_replicas_per_fault_domain", max(maximum, 1)),
            f"services[{index}].max_replicas_per_fault_domain",
            1,
        )
        replica_memory = parse_memory(
            entry.get("replica_memory"), f"services[{index}].replica_memory"
        )
        replica_compute = parse_compute_milli(
            entry.get("replica_compute"), f"services[{index}].replica_compute"
        )
        if replica_memory == 0:
            raise spec_invalid("replica_memory must be > 0", service_id=service_id)
        if replica_compute == 0:
            raise spec_invalid("replica_compute must be > 0", service_id=service_id)

        services.append(
            ServiceSpec(
                id=service_id,
                model=require_text(entry.get("model"), f"services[{index}].model"),
                current_replicas=current,
                min_replicas=minimum,
                max_replicas=maximum,
                replica_memory_bytes=replica_memory,
                replica_compute_milli=replica_compute,
                target_concurrency=target_concurrency,
                latency_budget_ms=latency_budget,
                priority=priority,
                max_replicas_per_fault_domain=max_per_domain,
            )
        )
    if not services:
        raise spec_invalid("policy must contain at least one service")

    nodes: list[NodeSpec] = []
    node_ids: set[str] = set()
    for index, item in enumerate(_list(raw.get("nodes"), "nodes")):
        entry = _object(item, f"nodes[{index}]")
        node_id = require_text(entry.get("id"), f"nodes[{index}].id")
        if node_id in node_ids:
            raise spec_invalid("duplicate node id", node_id=node_id)
        node_ids.add(node_id)
        memory = parse_memory(entry.get("memory"), f"nodes[{index}].memory")
        compute = parse_compute_milli(
            entry.get("compute_share"), f"nodes[{index}].compute_share"
        )
        if memory == 0 or compute == 0:
            raise spec_invalid(
                "node memory and compute_share must be > 0", node_id=node_id
            )
        reserved_memory = parse_memory(
            entry.get("reserved_memory", 0), f"nodes[{index}].reserved_memory"
        )
        reserved_compute = parse_compute_milli(
            entry.get("reserved_compute", 0), f"nodes[{index}].reserved_compute"
        )
        if reserved_memory > memory:
            raise spec_invalid(
                "reserved_memory exceeds node memory",
                node_id=node_id,
                reserved_memory_bytes=reserved_memory,
                memory_bytes=memory,
            )
        if reserved_compute > compute:
            raise spec_invalid(
                "reserved_compute exceeds node compute_share",
                node_id=node_id,
                reserved_compute_milli=reserved_compute,
                compute_milli=compute,
            )
        nodes.append(
            NodeSpec(
                id=node_id,
                fault_domain=require_text(
                    entry.get("fault_domain"), f"nodes[{index}].fault_domain"
                ),
                memory_bytes=memory,
                compute_milli=compute,
                healthy=_boolean(entry.get("healthy", True), f"nodes[{index}].healthy"),
                reserved_memory_bytes=reserved_memory,
                reserved_compute_milli=reserved_compute,
            )
        )
    if not nodes:
        raise spec_invalid("policy must contain at least one node")

    initial: list[InitialPlacement] = []
    seen_initial: set[tuple[str, str]] = set()
    for index, item in enumerate(_list(raw.get("initial_placements", []), "initial_placements")):
        entry = _object(item, f"initial_placements[{index}]")
        service_id = require_text(entry.get("service_id"), f"initial_placements[{index}].service_id")
        node_id = require_text(entry.get("node_id"), f"initial_placements[{index}].node_id")
        count = require_int(entry.get("count"), f"initial_placements[{index}].count", 1)
        if service_id not in service_ids:
            raise spec_invalid("initial placement references an unknown service", service_id=service_id)
        if node_id not in node_ids:
            raise spec_invalid("initial placement references an unknown node", node_id=node_id)
        key = (service_id, node_id)
        if key in seen_initial:
            raise spec_invalid(
                "duplicate service/node initial placement",
                service_id=service_id,
                node_id=node_id,
            )
        seen_initial.add(key)
        initial.append(InitialPlacement(service_id=service_id, node_id=node_id, count=count))

    service_map = {service.id: service for service in services}
    node_map = {node.id: node for node in nodes}
    per_service_initial: dict[str, int] = {service.id: 0 for service in services}
    usage_memory: dict[str, int] = {
        node.id: node.reserved_memory_bytes for node in nodes
    }
    usage_compute: dict[str, int] = {
        node.id: node.reserved_compute_milli for node in nodes
    }
    for placement in initial:
        service = service_map[placement.service_id]
        node = node_map[placement.node_id]
        per_service_initial[placement.service_id] += placement.count
        usage_memory[node.id] += service.replica_memory_bytes * placement.count
        usage_compute[node.id] += service.replica_compute_milli * placement.count
    for service in services:
        # A policy update may preserve already-committed placements and omit
        # initial_placements for existing services.  If a service explicitly
        # supplies bootstrap placements, they must account for all its current
        # replicas.
        if per_service_initial[service.id] and (
            per_service_initial[service.id] != service.current_replicas
        ):
            raise spec_invalid(
                "initial placement count must equal current_replicas",
                service_id=service.id,
                current_replicas=service.current_replicas,
                initial_placements=per_service_initial[service.id],
            )
    for node in nodes:
        if usage_memory[node.id] > node.memory_bytes:
            raise spec_invalid(
                "initial placements exceed node memory",
                node_id=node.id,
                used_memory_bytes=usage_memory[node.id],
                memory_bytes=node.memory_bytes,
            )
        if usage_compute[node.id] > node.compute_milli:
            raise spec_invalid(
                "initial placements exceed node compute share",
                node_id=node.id,
                used_compute_milli=usage_compute[node.id],
                compute_milli=node.compute_milli,
            )

    return PolicySnapshot(
        version=version,
        generated_at=generated_at,
        services=tuple(services),
        nodes=tuple(nodes),
        runtime=runtime,
        initial_placements=tuple(initial),
    )


def parse_observation_batch(raw_value: Any) -> ObservationBatch:
    raw = _object(raw_value, "observations")
    batch_id = require_text(raw.get("batch_id"), "observations.batch_id")
    sampled_at = parse_timestamp(raw.get("sampled_at"), "observations.sampled_at")
    window_ms = require_int(raw.get("window_ms"), "observations.window_ms", 1)

    observations: list[ServiceObservation] = []
    seen: set[str] = set()
    for index, item in enumerate(_list(raw.get("services"), "observations.services")):
        entry = _object(item, f"observations.services[{index}]")
        service_id = require_text(entry.get("service_id"), f"observations.services[{index}].service_id")
        if service_id in seen:
            raise observation_invalid(
                "duplicate service observation", service_id=service_id
            )
        seen.add(service_id)
        observations.append(
            ServiceObservation(
                service_id=service_id,
                sampled_at=parse_timestamp(
                    entry.get("sampled_at"), f"observations.services[{index}].sampled_at"
                ),
                arrivals=require_int(
                    entry.get("arrivals"), f"observations.services[{index}].arrivals", 0
                ),
                queue_duration_ms=require_int(
                    entry.get("queue_duration_ms"),
                    f"observations.services[{index}].queue_duration_ms",
                    0,
                ),
                p99_latency_ms=require_int(
                    entry.get("p99_latency_ms"),
                    f"observations.services[{index}].p99_latency_ms",
                    0,
                ),
            )
        )

    node_health_raw = raw.get("node_health", {})
    node_health_obj = _object(node_health_raw, "observations.node_health")
    node_health: dict[str, bool] = {}
    for node_id, healthy in node_health_obj.items():
        if not isinstance(node_id, str) or not node_id.strip():
            raise observation_invalid("node_health keys must be node ids")
        node_health[node_id] = _boolean(healthy, f"observations.node_health.{node_id}")

    return ObservationBatch(
        batch_id=batch_id,
        sampled_at=sampled_at,
        window_ms=window_ms,
        services=tuple(observations),
        node_health=node_health,
    )


def parse_round_request(raw_value: Any) -> RoundRequest:
    raw = _object(raw_value, "round")
    round_id = require_text(raw.get("round_id"), "round_id")
    policy_version = require_int(raw.get("policy_version"), "policy_version", 1)
    observations = parse_observation_batch(raw.get("observations"))
    fault_injection = None
    fault_raw = raw.get("fault_injection")
    if fault_raw is not None:
        fault_obj = _object(fault_raw, "fault_injection")
        crash_after = require_text(fault_obj.get("crash_after"), "fault_injection.crash_after")
        if crash_after not in {"INTENT_COMMITTED"}:
            raise spec_invalid(
                "unsupported fault_injection.crash_after",
                allowed=["INTENT_COMMITTED"],
            )
        fault_injection = FaultInjection(crash_after=crash_after)
    return RoundRequest(
        round_id=round_id,
        policy_version=policy_version,
        observations=observations,
        fault_injection=fault_injection,
    )
