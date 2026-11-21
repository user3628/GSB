from __future__ import annotations

from .models import Node, NodeUsage, Pod, Scenario
from .selectors import node_selector_matches, untolerated_taints


def _score_candidate(
    node: Node, pod: Pod, used_cpu: int, used_memory: int, same_workload_count: int
) -> dict:
    cpu_after = (used_cpu + pod.cpu_milli) / node.cpu_milli if node.cpu_milli else 0.0
    memory_after = (
        (used_memory + pod.memory_bytes) / node.memory_bytes if node.memory_bytes else 0.0
    )
    max_utilization = max(cpu_after, memory_after)
    headroom = max(0.0, 1.0 - max_utilization)
    spread = 1.0 / (1.0 + same_workload_count)
    balance = 1.0 - abs(cpu_after - memory_after)
    score = 50.0 * headroom + 30.0 * spread + 20.0 * balance
    return {
        "score": round(score, 6),
        "score_components": {
            "headroom": round(headroom, 6),
            "spread": round(spread, 6),
            "balance": round(balance, 6),
        },
        "utilization_after": {
            "cpu": round(cpu_after, 6),
            "memory": round(memory_after, 6),
        },
        "remaining_after": {
            "cpu_milli": node.cpu_milli - used_cpu - pod.cpu_milli,
            "memory_bytes": node.memory_bytes - used_memory - pod.memory_bytes,
        },
        "same_workload_replicas_after": same_workload_count + 1,
    }


def feasible_placements(
    scenario: Scenario,
    pod: Pod,
    source_node: str,
    usage: dict[str, NodeUsage],
) -> list[dict]:
    candidates: list[dict] = []
    for node_name in sorted(scenario.nodes):
        node = scenario.nodes[node_name]
        if node_name == source_node or node.drain_requested:
            continue
        if not node_selector_matches(pod, node):
            continue
        blocked_taints = untolerated_taints(pod, node)
        if blocked_taints:
            continue
        node_usage = usage.get(node_name, NodeUsage())
        if node_usage.cpu_milli + pod.cpu_milli > node.cpu_milli:
            continue
        if node_usage.memory_bytes + pod.memory_bytes > node.memory_bytes:
            continue
        same_workload = node_usage.workload_counts.get(pod.workload_name, 0)
        scored = _score_candidate(
            node, pod, node_usage.cpu_milli, node_usage.memory_bytes, same_workload
        )
        candidates.append(
            {
                "node": node_name,
                **scored,
                "request": {"cpu_milli": pod.cpu_milli, "memory_bytes": pod.memory_bytes},
            }
        )
    candidates.sort(key=lambda candidate: (-candidate["score"], candidate["node"]))
    return candidates


def choose_placement(
    scenario: Scenario,
    pod: Pod,
    source_node: str,
    usage: dict[str, NodeUsage],
) -> dict | None:
    candidates = feasible_placements(scenario, pod, source_node, usage)
    if not candidates:
        return None
    best = candidates[0]
    return {
        "target_node": best["node"],
        "score": best["score"],
        "score_components": best["score_components"],
        "utilization_after": best["utilization_after"],
        "remaining_after": best["remaining_after"],
        "same_workload_replicas_after": best["same_workload_replicas_after"],
        "evaluated_candidates": candidates,
        "scoring": (
            "score = 50*headroom + 30*spread + 20*balance; "
            "headroom=1-max(cpu_after,memory_after); spread=1/(1+same_workload_replicas); "
            "balance=1-abs(cpu_after-memory_after); ties resolve by node name"
        ),
    }


def add_pod_to_usage(usage: dict[str, NodeUsage], node_name: str, pod: Pod) -> None:
    node_usage = usage.setdefault(node_name, NodeUsage())
    node_usage.add(pod.cpu_milli, pod.memory_bytes, pod.workload_name)


def remove_pod_from_usage(
    usage: dict[str, NodeUsage], node_name: str, pod: Pod
) -> None:
    node_usage = usage.setdefault(node_name, NodeUsage())
    node_usage.remove(pod.cpu_milli, pod.memory_bytes, pod.workload_name)


def apply_move(
    usage: dict[str, NodeUsage], pod: Pod, source_node: str, target_node: str
) -> None:
    remove_pod_from_usage(usage, source_node, pod)
    add_pod_to_usage(usage, target_node, pod)

