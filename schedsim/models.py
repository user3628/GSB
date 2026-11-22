"""Validated, normalized scheduler domain objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ResourceVector:
    cpu_millicores: int
    memory_bytes: int

    def to_dict(self) -> dict[str, int]:
        return {
            "cpu_millicores": self.cpu_millicores,
            "memory_bytes": self.memory_bytes,
        }

    def plus(self, other: "ResourceVector") -> "ResourceVector":
        return ResourceVector(
            self.cpu_millicores + other.cpu_millicores,
            self.memory_bytes + other.memory_bytes,
        )

    def minus(self, other: "ResourceVector") -> "ResourceVector":
        return ResourceVector(
            self.cpu_millicores - other.cpu_millicores,
            self.memory_bytes - other.memory_bytes,
        )

    def fits_within(self, other: "ResourceVector") -> bool:
        return (
            self.cpu_millicores <= other.cpu_millicores
            and self.memory_bytes <= other.memory_bytes
        )


@dataclass(frozen=True)
class LabelRequirement:
    key: str
    operator: str
    values: tuple[str, ...]
    source_text: str


@dataclass(frozen=True)
class LabelSelector:
    match_labels: dict[str, str] = field(default_factory=dict)
    expressions: tuple[LabelRequirement, ...] = ()
    source_text: str = "{}"


@dataclass(frozen=True)
class NodeSelectorTerm:
    expressions: tuple[LabelRequirement, ...]
    source_text: str


@dataclass(frozen=True)
class WeightedNodeTerm:
    weight: int
    term: NodeSelectorTerm


@dataclass(frozen=True)
class NodeAffinityRules:
    required_terms: tuple[NodeSelectorTerm, ...] = ()
    preferred_terms: tuple[WeightedNodeTerm, ...] = ()


@dataclass(frozen=True)
class PodAffinityTerm:
    selector: LabelSelector
    topology_key: str
    namespaces: tuple[str, ...]
    source_text: str


@dataclass(frozen=True)
class WeightedPodTerm:
    weight: int
    term: PodAffinityTerm


@dataclass(frozen=True)
class PodAffinityRules:
    required_terms: tuple[PodAffinityTerm, ...] = ()
    preferred_terms: tuple[WeightedPodTerm, ...] = ()


@dataclass(frozen=True)
class Toleration:
    key: str
    operator: str
    value: str
    effect: str | None
    source_text: str


@dataclass(frozen=True)
class Taint:
    key: str
    value: str
    effect: str
    source_text: str


@dataclass(frozen=True)
class NodeSpec:
    name: str
    allocatable: ResourceVector
    labels: dict[str, str]
    taints: tuple[Taint, ...]
    failure_domain: str | None
    healthy: bool
    raw_allocatable: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "allocatable": self.allocatable.to_dict(),
            "labels": dict(self.labels),
            "taints": [
                {
                    "key": taint.key,
                    "value": taint.value,
                    "effect": taint.effect,
                    "source_text": taint.source_text,
                }
                for taint in self.taints
            ],
            "failure_domain": self.failure_domain,
            "healthy": self.healthy,
        }


@dataclass(frozen=True)
class PodSpec:
    name: str
    namespace: str
    priority: int
    arrival_seq: int
    labels: dict[str, str]
    requests: ResourceVector
    limits: ResourceVector | None
    raw_requests: dict[str, str]
    node_selector: dict[str, str]
    node_affinity: NodeAffinityRules
    pod_affinity: PodAffinityRules
    pod_anti_affinity: PodAffinityRules
    tolerations: tuple[Toleration, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "namespace": self.namespace,
            "priority": self.priority,
            "arrival_seq": self.arrival_seq,
            "labels": dict(self.labels),
            "requests": self.requests.to_dict(),
            "limits": self.limits.to_dict() if self.limits else None,
            "node_selector": dict(self.node_selector),
            "tolerations": [toleration.source_text for toleration in self.tolerations],
        }


@dataclass(frozen=True)
class BudgetConfig:
    concurrency_limit: int = 2
    time_budget_ms: int = 1000
    per_node_round_limit: int = 2
    queue_capacity: int = 256
    node_check_cost_ms: int = 1
    score_tolerance: int = 5

    def to_dict(self) -> dict[str, int]:
        return {
            "concurrency_limit": self.concurrency_limit,
            "time_budget_ms": self.time_budget_ms,
            "per_node_round_limit": self.per_node_round_limit,
            "queue_capacity": self.queue_capacity,
            "node_check_cost_ms": self.node_check_cost_ms,
            "score_tolerance": self.score_tolerance,
        }


@dataclass(frozen=True)
class BatchSpec:
    batch_id: str
    nodes: tuple[NodeSpec, ...]
    pods: tuple[PodSpec, ...]
    budget: BudgetConfig

    @property
    def node_by_name(self) -> dict[str, NodeSpec]:
        return {node.name: node for node in self.nodes}

    @property
    def pod_by_name(self) -> dict[str, PodSpec]:
        return {pod.name: pod for pod in self.pods}


@dataclass(frozen=True)
class PlacementFact:
    placement_id: int
    batch_id: str
    round_no: int
    sequence: int
    pod_name: str
    node_name: str
    requests: ResourceVector
    remaining_before: ResourceVector
    remaining_after: ResourceVector
    score: int
    score_breakdown: dict[str, Any]


@dataclass
class MaterializedBatch:
    batch: BatchSpec
    remaining: dict[str, ResourceVector]
    placements: list[PlacementFact]
    placed_pod_names: set[str]
    round_node_counts: dict[str, int]

    def remaining_snapshot(self) -> dict[str, dict[str, int]]:
        return {name: resources.to_dict() for name, resources in self.remaining.items()}

