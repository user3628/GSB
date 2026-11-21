from __future__ import annotations

from functools import lru_cache
from typing import Any, Iterable

from .models import NodeUsage, PDB, Pod, Scenario
from .placement import apply_move, choose_placement, feasible_placements
from .selectors import pod_matches_pdb


def affected_pdbs(scenario: Scenario, pod: Pod) -> list[PDB]:
    return [
        pdb
        for pdb in sorted(scenario.pdbs.values(), key=lambda candidate: candidate.name)
        if pod_matches_pdb(pod, pdb)
    ]


def evaluate_pdb_eviction(
    pdb: PDB, current_available: int, pod: Pod
) -> dict[str, Any]:
    decrement = 1 if pod.healthy else 0
    after = current_available - decrement
    gap = max(0, pdb.required_available - after)
    return {
        "pdb": pdb.name,
        "budget_kind": pdb.budget_kind,
        "budget_input": pdb.budget_input,
        "rounding": pdb.rounding,
        "desired_replicas": pdb.desired_replicas,
        "current_available": current_available,
        "after_available": after,
        "required_available": pdb.required_available,
        "gap": gap,
        "satisfied": gap == 0,
        "pod_counted_as_healthy": pod.healthy,
    }


def pdb_blocking_reasons(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "code": "E_PDB_BLOCKED",
            "pdb": check["pdb"],
            "current_available": check["current_available"],
            "required_available": check["required_available"],
            "after_available": check["after_available"],
            "gap": check["gap"],
            "message": (
                f"evicting would leave {check['pdb']} with {check['after_available']} "
                f"available replicas, below required {check['required_available']}"
            ),
        }
        for check in checks
        if not check["satisfied"]
    ]


def build_usage(scenario: Scenario, movement_steps: Iterable[dict[str, Any]]) -> dict[str, NodeUsage]:
    usage: dict[str, NodeUsage] = {
        node_name: NodeUsage() for node_name in scenario.nodes
    }
    for pod in scenario.ordered_pods:
        usage[pod.node].add(pod.cpu_milli, pod.memory_bytes, pod.workload_name)
    for step in movement_steps:
        if step.get("outcome") != "ALLOWED" or step.get("status") != "COMMITTED":
            continue
        pod = scenario.pods.get(step.get("pod_name"))
        if pod is None:
            continue
        source = step.get("source_node")
        target = step.get("target_node")
        if source and target:
            apply_move(usage, pod, source, target)
    return usage


def effective_locations(
    scenario: Scenario, movement_steps: Iterable[dict[str, Any]]
) -> dict[str, str]:
    locations = {pod.name: pod.node for pod in scenario.ordered_pods}
    for step in movement_steps:
        if step.get("outcome") == "ALLOWED" and step.get("status") == "COMMITTED":
            pod_name = step.get("pod_name")
            target = step.get("target_node")
            if pod_name in locations and target:
                locations[pod_name] = target
    return locations


def adjudicate_single(
    scenario: Scenario,
    pod: Pod,
    source_node: str,
    pdb_available: dict[str, int],
    movement_steps: list[dict[str, Any]],
) -> dict[str, Any]:
    pdbs = affected_pdbs(scenario, pod)
    checks = [
        evaluate_pdb_eviction(pdb, pdb_available[pdb.name], pod)
        for pdb in pdbs
    ]
    reasons = pdb_blocking_reasons(checks)
    decision: dict[str, Any] = {
        "pod": pod.name,
        "source_node": source_node,
        "workload": pod.workload_name,
        "healthy": pod.healthy,
        "affected_pdbs": checks,
        "blocking_reasons": reasons,
        "placement": None,
    }
    if reasons:
        decision.update(
            {
                "decision": "BLOCKED",
                "code": "E_PDB_BLOCKED",
                "message": "one or more PDB constraints would be violated",
            }
        )
        return decision

    usage = build_usage(scenario, movement_steps)
    placement = choose_placement(scenario, pod, source_node, usage)
    if placement is None:
        message = "no other node satisfies capacity, selector and taint constraints"
        decision.update(
            {
                "decision": "BLOCKED",
                "code": "E_NO_FEASIBLE_NODE",
                "message": message,
                "blocking_reasons": [
                    {
                        "code": "E_NO_FEASIBLE_NODE",
                        "pod": pod.name,
                        "source_node": source_node,
                        "message": message,
                    }
                ],
            }
        )
        return decision
    decision.update({"decision": "ALLOWED", "code": "ALLOWED", "placement": placement})
    return decision


def preview_sequence(
    scenario: Scenario,
    ordered_pods: list[Pod],
    source_node: str,
    pdb_available: dict[str, int],
    movement_steps: list[dict[str, Any]],
) -> dict[str, Any]:
    availability = dict(pdb_available)
    usage = build_usage(scenario, movement_steps)
    steps: list[dict[str, Any]] = []
    allowed = 0
    for sequence, pod in enumerate(ordered_pods, start=1):
        pdbs = affected_pdbs(scenario, pod)
        checks = [
            evaluate_pdb_eviction(pdb, availability[pdb.name], pod) for pdb in pdbs
        ]
        reasons = pdb_blocking_reasons(checks)
        placement = None
        if not reasons:
            placement = choose_placement(scenario, pod, source_node, usage)
            if placement is None:
                reasons = [
                    {
                        "code": "E_NO_FEASIBLE_NODE",
                        "pod": pod.name,
                        "source_node": source_node,
                        "message": "no feasible target node after earlier simulated moves",
                    }
                ]
        before = {
            check["pdb"]: check["current_available"] for check in checks
        }
        step: dict[str, Any] = {
            "sequence": sequence,
            "pod": pod.name,
            "source_node": source_node,
            "affected_pdbs": checks,
            "blocking_reasons": reasons,
            "placement": placement,
            "availability_before": before,
        }
        if reasons:
            step.update(
                {
                    "decision": "BLOCKED",
                    "code": reasons[0]["code"],
                    "availability_after": dict(before),
                }
            )
        else:
            if pod.healthy:
                for check in checks:
                    availability[check["pdb"]] -= 1
            if placement is not None:
                apply_move(usage, pod, source_node, placement["target_node"])
            allowed += 1
            step.update(
                {
                    "decision": "ALLOWED",
                    "code": "ALLOWED",
                    "availability_after": {
                        pdbs_name: availability[pdbs_name] for pdbs_name in before
                    },
                }
            )
        steps.append(step)
    return {
        "order": [pod.name for pod in ordered_pods],
        "allowed_count": allowed,
        "blocked_count": len(steps) - allowed,
        "steps": steps,
    }


def _adaptive_order(
    scenario: Scenario,
    pods: list[Pod],
    source_node: str,
    pdb_available: dict[str, int],
    movement_steps: list[dict[str, Any]],
) -> list[Pod]:
    remaining = list(pods)
    ordered: list[Pod] = []
    availability = dict(pdb_available)
    usage = build_usage(scenario, movement_steps)
    while remaining:
        eligible: list[tuple[tuple[Any, ...], Pod, list[dict[str, Any]], dict | None]] = []
        for pod in remaining:
            pdbs = affected_pdbs(scenario, pod)
            checks = [
                evaluate_pdb_eviction(pdb, availability[pdb.name], pod) for pdb in pdbs
            ]
            if any(not check["satisfied"] for check in checks):
                continue
            placement = choose_placement(scenario, pod, source_node, usage)
            if placement is None:
                continue
            slack = min(
                (check["after_available"] - check["required_available"] for check in checks),
                default=10**9,
            )
            key = (
                len(pdbs),
                slack,
                -placement["score"],
                pod.ordinal,
            )
            eligible.append((key, pod, checks, placement))
        if not eligible:
            ordered.extend(sorted(remaining, key=lambda pod: (len(affected_pdbs(scenario, pod)), pod.ordinal)))
            break
        _, pod, checks, placement = min(eligible, key=lambda item: item[0])
        ordered.append(pod)
        remaining.remove(pod)
        if pod.healthy:
            for check in checks:
                availability[check["pdb"]] -= 1
        if placement is not None:
            apply_move(usage, pod, source_node, placement["target_node"])
    return ordered


def _maximum_pdb_order(
    scenario: Scenario,
    pods: list[Pod],
    pdb_available: dict[str, int],
) -> list[Pod] | None:
    """Exact maximum-cardinality PDB subset for small local drain batches.

    PDB capacity is monotone: if a final subset respects every lower bound, any
    order of that subset also respects each intermediate lower bound. The exact
    search is deliberately limited to small batches; larger batches still use
    the deterministic heuristic orders, and every selected order is replayed
    through the same declarative checks.
    """

    if len(pods) > 20:
        return None
    pdb_names = sorted(scenario.pdbs)
    pdb_index = {name: index for index, name in enumerate(pdb_names)}
    capacities = {
        name: max(0, pdb_available[name] - scenario.pdbs[name].required_available)
        for name in pdb_names
    }
    pod_bindings: list[tuple[Pod, tuple[tuple[int, int], ...]]] = []
    for pod in pods:
        bindings: list[tuple[int, int]] = []
        if pod.healthy:
            for pdb in affected_pdbs(scenario, pod):
                bindings.append((pdb_index[pdb.name], 1))
        pod_bindings.append((pod, tuple(bindings)))

    @lru_cache(maxsize=None)
    def solve(index: int, remaining: tuple[int, ...]) -> tuple[int, tuple[int, ...]]:
        if index == len(pod_bindings):
            return 0, ()
        pod, bindings = pod_bindings[index]
        excluded_count, excluded_selected = solve(index + 1, remaining)
        can_include = all(remaining[pdb_id] >= cost for pdb_id, cost in bindings)
        if can_include:
            updated = list(remaining)
            for pdb_id, cost in bindings:
                updated[pdb_id] -= cost
            rest_count, rest_selected = solve(index + 1, tuple(updated))
            included_count = rest_count + 1
            included_selected = (index,) + rest_selected
            if included_count > excluded_count or (
                included_count == excluded_count and included_selected < excluded_selected
            ):
                return included_count, included_selected
        return excluded_count, excluded_selected

    _, selected_indices = solve(0, tuple(capacities[name] for name in pdb_names))
    selected_set = set(selected_indices)
    selected = [pod_bindings[index][0] for index in selected_indices]
    remaining = [
        pod for index, (pod, _) in enumerate(pod_bindings) if index not in selected_set
    ]
    return sorted(
        selected, key=lambda pod: (len(affected_pdbs(scenario, pod)), pod.ordinal)
    ) + sorted(remaining, key=lambda pod: (len(affected_pdbs(scenario, pod)), pod.ordinal))


def choose_optimized_plan(
    scenario: Scenario,
    pods: list[Pod],
    source_node: str,
    pdb_available: dict[str, int],
    movement_steps: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    naive_order = sorted(pods, key=lambda pod: pod.ordinal)
    fewest_constraints = sorted(
        pods, key=lambda pod: (len(affected_pdbs(scenario, pod)), pod.ordinal)
    )
    scarce_first = sorted(
        pods,
        key=lambda pod: (
            min(
                (
                    pdb_available[pdb.name] - pdb.required_available
                    for pdb in affected_pdbs(scenario, pod)
                ),
                default=10**9,
            ),
            len(affected_pdbs(scenario, pod)),
            pod.ordinal,
        ),
    )
    adaptive = _adaptive_order(
        scenario, pods, source_node, pdb_available, movement_steps
    )

    maximum_pdb_order = _maximum_pdb_order(scenario, pods, pdb_available)

    candidates: list[tuple[str, list[Pod], dict[str, Any]]] = []
    for label, order in (
        ("naive_input_order", naive_order),
        ("fewest_pdb_memberships_first", fewest_constraints),
        ("scarce_budget_first", scarce_first),
        ("adaptive_least_constraint", adaptive),
    ):
        candidates.append(
            (
                label,
                order,
                preview_sequence(
                    scenario, order, source_node, pdb_available, movement_steps
                ),
            )
        )
    if maximum_pdb_order is not None:
        candidates.append(
            (
                "maximum_pdb_feasible_subset",
                maximum_pdb_order,
                preview_sequence(
                    scenario,
                    maximum_pdb_order,
                    source_node,
                    pdb_available,
                    movement_steps,
                ),
            )
        )

    selected_label, selected_order, selected_plan = min(
        candidates,
        key=lambda item: (
            -item[2]["allowed_count"],
            tuple(pod.name for pod in item[1]),
        ),
    )
    selected_plan["strategy"] = selected_label
    selected_plan["rule"] = (
        "simulate naive, fewest-PDB-membership, scarce-budget and adaptive orders; "
        "choose the highest admitted count using only the same declarative PDB checks; "
        "ties resolve by lexicographic pod order"
    )
    naive_plan = preview_sequence(
        scenario, naive_order, source_node, pdb_available, movement_steps
    )
    naive_plan["strategy"] = "naive_input_order"
    naive_plan["rule"] = "stable input order"
    return naive_plan, selected_plan

