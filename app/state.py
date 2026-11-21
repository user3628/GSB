from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .state_machine import ServiceState


@dataclass(frozen=True)
class Placement:
    replica_id: str
    service_id: str
    node_id: str
    memory_bytes: int
    compute_milli: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "replica_id": self.replica_id,
            "service_id": self.service_id,
            "node_id": self.node_id,
            "memory_bytes": self.memory_bytes,
            "compute_milli": self.compute_milli,
        }


@dataclass(frozen=True)
class NodeUsage:
    node_id: str
    healthy: bool
    used_memory_bytes: int
    used_compute_milli: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "healthy": self.healthy,
            "used_memory_bytes": self.used_memory_bytes,
            "used_compute_milli": self.used_compute_milli,
        }


@dataclass(frozen=True)
class ServiceStatus:
    service_id: str
    state: ServiceState
    current_replicas: int
    desired_replicas: int
    reason: str
    degraded_domains: tuple[dict[str, Any], ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "service_id": self.service_id,
            "state": self.state.value,
            "current_replicas": self.current_replicas,
            "desired_replicas": self.desired_replicas,
            "reason": self.reason,
            "degraded_domains": list(self.degraded_domains),
        }


@dataclass(frozen=True)
class StateSnapshot:
    revision: int
    policy_version: int
    kind: str
    parent_revision: int | None
    round_id: str | None
    created_at: str
    placements: tuple[Placement, ...]
    nodes: tuple[NodeUsage, ...]
    services: tuple[ServiceStatus, ...]

    @property
    def placement_map(self) -> dict[str, Placement]:
        return {placement.replica_id: placement for placement in self.placements}

    @property
    def node_usage_map(self) -> dict[str, NodeUsage]:
        return {node.node_id: node for node in self.nodes}

    @property
    def service_status_map(self) -> dict[str, ServiceStatus]:
        return {service.service_id: service for service in self.services}


@dataclass
class StateDraft:
    placements: list[Placement]
    nodes: list[NodeUsage]
    services: list[ServiceStatus]

    def clone(self) -> "StateDraft":
        return StateDraft(
            placements=list(self.placements),
            nodes=list(self.nodes),
            services=list(self.services),
        )


@dataclass
class RoundRecord:
    round_id: str
    policy_version: int
    request_hash: str
    status: str
    started_at: str
    completed_at: str | None
    result: dict[str, Any] | None = field(default=None)
    error: dict[str, Any] | None = field(default=None)
    revision: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "round_id": self.round_id,
            "policy_version": self.policy_version,
            "request_hash": self.request_hash,
            "status": self.status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "result": self.result,
            "error": self.error,
            "revision": self.revision,
        }
