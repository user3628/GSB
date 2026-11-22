"""Kubernetes-like label, taint, affinity, and fixed-point scoring semantics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .models import (
    LabelRequirement,
    LabelSelector,
    NodeSelectorTerm,
    NodeSpec,
    PodAffinityTerm,
    PodSpec,
    ResourceVector,
    Taint,
    Toleration,
)

NODE_PREFERRED_WEIGHT_UNIT = 1000
POD_PREFERRED_WEIGHT_UNIT = 1000
POD_ANTI_PREFERRED_WEIGHT_UNIT = 1000
RESOURCE_BALANCE_MAX = 1000
UNSATISFIED_TAINT_PENALTY = 500


def _integer_text_matches(value: str | None, comparison: str, operator: str) -> bool:
    if value is None:
        return False
    try:
        left = int(value, 10)
        right = int(comparison, 10)
    except (TypeError, ValueError):
        return False
    return left > right if operator == "Gt" else left < right


def requirement_matches(requirement: LabelRequirement, labels: dict[str, str]) -> bool:
    present = requirement.key in labels
    value = labels.get(requirement.key)
    operator = requirement.operator
    if operator == "In":
        return present and value in requirement.values
    if operator == "NotIn":
        return not present or value not in requirement.values
    if operator == "Exists":
        return present
    if operator == "DoesNotExist":
        return not present
    if operator in {"Gt", "Lt"}:
        return present and _integer_text_matches(value, requirement.values[0], operator)
    return False


def requirements_match(
    requirements: Iterable[LabelRequirement], labels: dict[str, str]
) -> bool:
    return all(requirement_matches(requirement, labels) for requirement in requirements)


def selector_matches(selector: LabelSelector, labels: dict[str, str]) -> bool:
    for key, expected in selector.match_labels.items():
        if labels.get(key) != expected:
            return False
    return requirements_match(selector.expressions, labels)


def node_selector_matches(pod: PodSpec, node: NodeSpec) -> bool:
    return all(node.labels.get(key) == value for key, value in pod.node_selector.items())


def node_term_matches(term: NodeSelectorTerm, node: NodeSpec) -> bool:
    if not term.expressions:
        return False
    return requirements_match(term.expressions, node.labels)


def node_required_affinity_matches(pod: PodSpec, node: NodeSpec) -> bool:
    if not pod.node_affinity.required_terms:
        return True
    return any(node_term_matches(term, node) for term in pod.node_affinity.required_terms)


def toleration_matches(toleration: Toleration, taint: Taint) -> bool:
    if toleration.effect is not None and toleration.effect != taint.effect:
        return False
    if toleration.operator == "Exists":
        if toleration.key == "":
            return True
        return toleration.key == taint.key
    return toleration.key == taint.key and toleration.value == taint.value


def is_tolerated(pod: PodSpec, taint: Taint) -> bool:
    return any(toleration_matches(toleration, taint) for toleration in pod.tolerations)


def untolerated_taints(
    pod: PodSpec, node: NodeSpec, effect: str | None = None
) -> list[Taint]:
    return [
        taint
        for taint in node.taints
        if (effect is None or taint.effect == effect) and not is_tolerated(pod, taint)
    ]


def topology_value(node: NodeSpec, topology_key: str) -> str | None:
    return node.labels.get(topology_key)


@dataclass(frozen=True)
class PlacedPodFact:
    pod_name: str
    namespace: str
    labels: dict[str, str]
    node_name: str


def _term_namespaces(term: PodAffinityTerm, scheduling_pod: PodSpec) -> set[str]:
    return set(term.namespaces) if term.namespaces else {scheduling_pod.namespace}


def matching_pods_in_topology(
    term: PodAffinityTerm,
    scheduling_pod: PodSpec,
    candidate_node: NodeSpec,
    placed_pods: Iterable[PlacedPodFact],
    node_by_name: dict[str, NodeSpec],
) -> list[PlacedPodFact]:
    """Return persisted Pods that satisfy a term in the candidate topology domain.

    A missing topology label on either node never creates a synthetic value or a
    default domain; such a pair is simply not in the same known domain.
    """

    candidate_value = topology_value(candidate_node, term.topology_key)
    if candidate_value is None:
        return []
    namespaces = _term_namespaces(term, scheduling_pod)
    matches: list[PlacedPodFact] = []
    for placed in placed_pods:
        if placed.namespace not in namespaces:
            continue
        if not selector_matches(term.selector, placed.labels):
            continue
        placed_node = node_by_name.get(placed.node_name)
        if placed_node is None:
            continue
        if topology_value(placed_node, term.topology_key) == candidate_value:
            matches.append(placed)
    return matches


def hard_pod_affinity_rejections(
    pod: PodSpec,
    candidate_node: NodeSpec,
    placed_pods: Iterable[PlacedPodFact],
    node_by_name: dict[str, NodeSpec],
) -> list[PodAffinityTerm]:
    placed = list(placed_pods)
    rejected: list[PodAffinityTerm] = []
    for term in pod.pod_affinity.required_terms:
        if topology_value(candidate_node, term.topology_key) is None:
            rejected.append(term)
            continue
        if not matching_pods_in_topology(term, pod, candidate_node, placed, node_by_name):
            rejected.append(term)
    return rejected


def hard_pod_anti_affinity_rejections(
    pod: PodSpec,
    candidate_node: NodeSpec,
    placed_pods: Iterable[PlacedPodFact],
    node_by_name: dict[str, NodeSpec],
) -> list[tuple[PodAffinityTerm, list[PlacedPodFact]]]:
    placed = list(placed_pods)
    rejected: list[tuple[PodAffinityTerm, list[PlacedPodFact]]] = []
    for term in pod.pod_anti_affinity.required_terms:
        if topology_value(candidate_node, term.topology_key) is None:
            rejected.append((term, []))
            continue
        matches = matching_pods_in_topology(term, pod, candidate_node, placed, node_by_name)
        if matches:
            rejected.append((term, matches))
    return rejected


def _ratio_basis_points(remaining: int, capacity: int) -> int:
    if capacity <= 0:
        return 1000 if remaining <= 0 else 0
    bounded = max(0, min(remaining, capacity))
    return (bounded * 1000) // capacity


def resource_balance_score(node: NodeSpec, remaining_after: ResourceVector) -> int:
    cpu_ratio = _ratio_basis_points(remaining_after.cpu_millicores, node.allocatable.cpu_millicores)
    memory_ratio = _ratio_basis_points(remaining_after.memory_bytes, node.allocatable.memory_bytes)
    return RESOURCE_BALANCE_MAX - abs(cpu_ratio - memory_ratio)


def score_candidate(
    pod: PodSpec,
    candidate_node: NodeSpec,
    remaining_after: ResourceVector,
    placed_pods: Iterable[PlacedPodFact],
    node_by_name: dict[str, NodeSpec],
    *,
    tie_tolerance: int,
) -> dict[str, object]:
    """Compute all score components using exact integers only."""

    placed = list(placed_pods)
    preferred_node_score = 0
    preferred_node_terms: list[dict[str, object]] = []
    for weighted in pod.node_affinity.preferred_terms:
        matched = node_term_matches(weighted.term, candidate_node)
        term_score = weighted.weight * NODE_PREFERRED_WEIGHT_UNIT if matched else 0
        preferred_node_score += term_score
        preferred_node_terms.append(
            {
                "weight": weighted.weight,
                "matched": matched,
                "score": term_score,
                "constraint": weighted.term.source_text,
            }
        )

    preferred_pod_score = 0
    preferred_pod_terms: list[dict[str, object]] = []
    for weighted in pod.pod_affinity.preferred_terms:
        matches = matching_pods_in_topology(
            weighted.term, pod, candidate_node, placed, node_by_name
        )
        term_score = weighted.weight * POD_PREFERRED_WEIGHT_UNIT if matches else 0
        preferred_pod_score += term_score
        preferred_pod_terms.append(
            {
                "weight": weighted.weight,
                "matched": bool(matches),
                "matched_pods": [match.pod_name for match in matches],
                "score": term_score,
                "constraint": weighted.term.source_text,
            }
        )

    preferred_anti_score = 0
    preferred_anti_terms: list[dict[str, object]] = []
    for weighted in pod.pod_anti_affinity.preferred_terms:
        matches = matching_pods_in_topology(
            weighted.term, pod, candidate_node, placed, node_by_name
        )
        term_score = -weighted.weight * POD_ANTI_PREFERRED_WEIGHT_UNIT if matches else 0
        preferred_anti_score += term_score
        preferred_anti_terms.append(
            {
                "weight": weighted.weight,
                "matched": bool(matches),
                "matched_pods": [match.pod_name for match in matches],
                "score": term_score,
                "constraint": weighted.term.source_text,
            }
        )

    balance_score = resource_balance_score(candidate_node, remaining_after)
    pref_taints = untolerated_taints(pod, candidate_node, effect="PreferNoSchedule")
    taint_penalty = -UNSATISFIED_TAINT_PENALTY * len(pref_taints)
    total = (
        preferred_node_score
        + preferred_pod_score
        + preferred_anti_score
        + balance_score
        + taint_penalty
    )
    return {
        "total": total,
        "fixed_point": True,
        "tie_tolerance": tie_tolerance,
        "components": {
            "preferred_node_affinity": preferred_node_score,
            "preferred_pod_affinity": preferred_pod_score,
            "preferred_pod_anti_affinity": preferred_anti_score,
            "resource_balance": balance_score,
            "prefer_no_schedule_taint_penalty": taint_penalty,
        },
        "preferred_node_terms": preferred_node_terms,
        "preferred_pod_terms": preferred_pod_terms,
        "preferred_pod_anti_terms": preferred_anti_terms,
        "prefer_no_schedule_taints": [taint.source_text for taint in pref_taints],
    }

