from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Taint:
    key: str
    value: str
    effect: str


@dataclass(frozen=True)
class Toleration:
    key: str
    operator: str = "Equal"
    value: str = ""
    effect: str = ""


@dataclass(frozen=True)
class Node:
    name: str
    cpu_milli: int
    memory_bytes: int
    drain_requested: bool
    labels: dict[str, str]
    taints: tuple[Taint, ...] = ()


@dataclass(frozen=True)
class Pod:
    name: str
    workload_name: str
    controller_type: str
    node: str
    phase: str
    eviction_state: str
    terminating: bool
    labels: dict[str, str]
    cpu_milli: int
    memory_bytes: int
    node_selector: dict[str, str]
    tolerations: tuple[Toleration, ...]
    ordinal: int

    @property
    def healthy(self) -> bool:
        return (
            self.phase.lower() == "running"
            and self.eviction_state.lower() in {"active", "healthy", "running"}
            and not self.terminating
        )


@dataclass(frozen=True)
class Workload:
    name: str
    controller_type: str
    desired_replicas: int
    pod_names: tuple[str, ...]


@dataclass(frozen=True)
class PDB:
    name: str
    selector: dict[str, Any]
    controller_type: str
    controller_name: str | None
    budget_kind: str
    budget_input: Any
    is_percentage: bool
    desired_replicas: int
    rounding: str
    calculated_integer: int
    max_unavailable: int | None
    required_available: int
    initial_available: int
    matched_pods: tuple[str, ...]

    def semantics(self) -> dict[str, Any]:
        if self.budget_kind == "minAvailable":
            formula = (
                f"ceil({self.desired_replicas} * {self.budget_input[:-1]} / 100) "
                f"= {self.calculated_integer}"
                if self.is_percentage
                else f"required_available = {self.calculated_integer}"
            )
            budget = {
                "kind": "minAvailable",
                "input": self.budget_input,
                "unit": "percentage" if self.is_percentage else "integer",
                "rounding": self.rounding,
                "calculated_integer": self.calculated_integer,
                "required_available": self.required_available,
                "formula": formula,
            }
        else:
            formula = (
                f"{self.desired_replicas} - floor({self.desired_replicas} * "
                f"{self.budget_input[:-1]} / 100) = {self.required_available}"
                if self.is_percentage
                else f"{self.desired_replicas} - {self.calculated_integer} = "
                f"{self.required_available}"
            )
            budget = {
                "kind": "maxUnavailable",
                "input": self.budget_input,
                "unit": "percentage" if self.is_percentage else "integer",
                "rounding": self.rounding,
                "calculated_integer": self.calculated_integer,
                "max_unavailable": self.max_unavailable,
                "required_available": self.required_available,
                "formula": formula,
            }
        return {
            "name": self.name,
            "controller": {
                "type": self.controller_type,
                "name": self.controller_name,
            },
            "selector": self.selector,
            "desired_replicas": self.desired_replicas,
            "matched_pods": list(self.matched_pods),
            "initial_available": self.initial_available,
            "budget": budget,
        }


@dataclass(frozen=True)
class Scenario:
    name: str
    nodes: dict[str, Node]
    workloads: dict[str, Workload]
    pods: dict[str, Pod]
    ordered_pods: tuple[Pod, ...]
    pdbs: dict[str, PDB]
    source_path: str | None = None

    def pods_for_node(self, node_name: str) -> list[Pod]:
        return [pod for pod in self.ordered_pods if pod.node == node_name]


@dataclass
class NodeUsage:
    cpu_milli: int = 0
    memory_bytes: int = 0
    workload_counts: dict[str, int] = field(default_factory=dict)

    def copy(self) -> "NodeUsage":
        return NodeUsage(
            cpu_milli=self.cpu_milli,
            memory_bytes=self.memory_bytes,
            workload_counts=dict(self.workload_counts),
        )

    def add(self, cpu_milli: int, memory_bytes: int, workload_name: str) -> None:
        self.cpu_milli += cpu_milli
        self.memory_bytes += memory_bytes
        self.workload_counts[workload_name] = self.workload_counts.get(workload_name, 0) + 1

    def remove(self, cpu_milli: int, memory_bytes: int, workload_name: str) -> None:
        self.cpu_milli = max(0, self.cpu_milli - cpu_milli)
        self.memory_bytes = max(0, self.memory_bytes - memory_bytes)
        count = self.workload_counts.get(workload_name, 0) - 1
        if count > 0:
            self.workload_counts[workload_name] = count
        else:
            self.workload_counts.pop(workload_name, None)

