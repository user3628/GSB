from __future__ import annotations

from typing import Any


DEFAULT_BUDGET: dict[str, int] = {
    "concurrency_limit": 10,
    "time_budget_ms": 1000,
    "per_node_round_limit": 100,
    "queue_capacity": 100,
    "node_check_cost_ms": 1,
    "score_tolerance": 5,
}


def make_node(
    name: str,
    *,
    cpu: str = "1000m",
    memory: str = "1Gi",
    zone: str = "zone-a",
    labels: dict[str, str] | None = None,
    taints: list[dict[str, str]] | None = None,
    healthy: bool = True,
) -> dict[str, Any]:
    final_labels = {
        "kubernetes.io/hostname": name,
        "topology.kubernetes.io/zone": zone,
        "disktype": "ssd",
    }
    if labels:
        final_labels.update(labels)
    return {
        "name": name,
        "allocatable": {"cpu": cpu, "memory": memory},
        "labels": final_labels,
        "taints": taints or [],
        "failure_domain": zone,
        "healthy": healthy,
    }


def make_pod(
    name: str,
    *,
    cpu: str = "100m",
    memory: str = "64Mi",
    priority: int = 0,
    arrival_seq: int = 0,
    labels: dict[str, str] | None = None,
    namespace: str = "default",
    node_selector: dict[str, str] | None = None,
    affinity: dict[str, Any] | None = None,
    tolerations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "name": name,
        "namespace": namespace,
        "priority": priority,
        "arrival_seq": arrival_seq,
        "labels": labels or {"app": name},
        "requests": {"cpu": cpu, "memory": memory},
    }
    if node_selector:
        result["nodeSelector"] = node_selector
    if affinity:
        result["affinity"] = affinity
    if tolerations:
        result["tolerations"] = tolerations
    return result


def make_payload(
    nodes: list[dict[str, Any]],
    pods: list[dict[str, Any]],
    *,
    batch_id: str = "batch-1",
    budget: dict[str, int] | None = None,
) -> dict[str, Any]:
    final_budget = dict(DEFAULT_BUDGET)
    if budget:
        final_budget.update(budget)
    return {
        "batch_id": batch_id,
        "nodes": nodes,
        "pods": pods,
        "budget": final_budget,
    }


def run_to_completion(engine: Any, batch_id: str, *, max_rounds: int = 30) -> list[dict[str, Any]]:
    rounds: list[dict[str, Any]] = []
    for _ in range(max_rounds):
        status = engine.get_batch_summary(batch_id)["status"]
        if status in {"COMPLETED", "ABORTED"}:
            return rounds
        rounds.append(engine.run_round(batch_id))
    raise AssertionError(f"batch {batch_id} did not complete in {max_rounds} rounds")

