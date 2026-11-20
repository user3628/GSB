from __future__ import annotations

import copy
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Any

from app.matcher import evaluate_term, matches_label_selector, topology_value, tolerance_matches
from app.models import BatchSpec, NodeSpec, PodSpec, ResourceAmount, Taint
from app.quantity import MAX_INT64
from app.scoring import SCORE_DECIMALS, SCORE_SCALE, format_fraction, score_node


@dataclass(frozen=True)
class PlacedPod:
    name: str
    labels: dict[str, str]
    node_name: str
    priority: int
    source: str
    preemptable: bool = False


@dataclass
class NodeRuntime:
    spec: NodeSpec
    used_cpu: int = 0
    used_memory: int = 0
    initial_ledger: list[dict[str, Any]] = field(default_factory=list)
    placement_ledger: list[dict[str, Any]] = field(default_factory=list)
    preemption_ledger: list[dict[str, Any]] = field(default_factory=list)
    accounting_overflow: bool = False
    overflow_details: dict[str, Any] | None = None
    assigned_status: dict[str, str] = field(default_factory=dict)


@dataclass
class World:
    nodes: dict[str, NodeRuntime]
    placed: list[PlacedPod]

    def clone(self) -> "World":
        return copy.deepcopy(self)


FILTER_ORDER = (
    "NodeAccounting",
    "NodeSelector",
    "NodeAffinity",
    "Taints",
    "PodAffinity",
    "PodAntiAffinity",
    "NodeResourcesFit",
)


def _resources_dict(resources: ResourceAmount) -> dict[str, int]:
    return {"cpuMillicores": resources.cpu_millicores, "memoryBytes": resources.memory_bytes}


def _remaining(node: NodeRuntime) -> dict[str, int]:
    return {
        "cpuMillicores": node.spec.allocatable.cpu_millicores - node.used_cpu,
        "memoryBytes": node.spec.allocatable.memory_bytes - node.used_memory,
    }


def _check_passed(plugin: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"plugin": plugin, "passed": True, "details": details or {}}


def _check_failed(
    plugin: str,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "plugin": plugin,
        "passed": False,
        "code": code,
        "message": message,
        "details": details or {},
    }


def initialize_world(batch: BatchSpec) -> World:
    nodes: dict[str, NodeRuntime] = {}
    placed: list[PlacedPod] = []
    for node_spec in batch.nodes:
        node = NodeRuntime(node_spec)
        for sequence, assigned in enumerate(node_spec.assigned_pods, start=1):
            before = _remaining(node)
            request = assigned.requests
            cpu_over = request.cpu_millicores > before["cpuMillicores"]
            memory_over = request.memory_bytes > before["memoryBytes"]
            accepted = not cpu_over and not memory_over
            entry: dict[str, Any] = {
                "sequence": sequence,
                "pod": assigned.name,
                "source": "assigned",
                "request": _resources_dict(request),
                "remainingBefore": before,
                "accepted": accepted,
            }
            if accepted:
                node.used_cpu += request.cpu_millicores
                node.used_memory += request.memory_bytes
                node.assigned_status[assigned.name] = "assigned"
                placed.append(
                    PlacedPod(
                        assigned.name,
                        dict(assigned.labels),
                        node_spec.name,
                        assigned.priority,
                        "assigned",
                        assigned.preemptable,
                    )
                )
                entry["remainingAfter"] = _remaining(node)
            else:
                node.accounting_overflow = True
                node.assigned_status[assigned.name] = "inconsistent"
                rejection = {
                    "code": "E_INSUFFICIENT_RESOURCES",
                    "message": "assigned Pod request exceeds node remaining capacity during ledger replay",
                    "resource": "cpu+memory",
                    "cpuShortfallMillicores": max(0, request.cpu_millicores - before["cpuMillicores"]),
                    "memoryShortfallBytes": max(0, request.memory_bytes - before["memoryBytes"]),
                    "remainingSnapshot": before,
                }
                entry["remainingAfter"] = before
                entry["rejection"] = rejection
                node.overflow_details = {"pod": assigned.name, "remainingSnapshot": before, "rejection": rejection}
            node.initial_ledger.append(entry)
        nodes[node_spec.name] = node
    return World(nodes, placed)


def _node_selector_check(pod: PodSpec, node: NodeSpec) -> dict[str, Any]:
    failures = []
    for key in sorted(pod.node_selector):
        expected = pod.node_selector[key]
        actual = node.labels.get(key)
        if actual != expected:
            failures.append({"key": key, "operator": "Equal", "expectedValue": expected, "labelValue": actual})
    if failures:
        first = failures[0]
        return _check_failed(
            "NodeSelector",
            "E_UNSATISFIED_AFFINITY",
            "node selector did not match node labels",
            {
                "firstViolationExpression": {
                    "source": "nodeSelector",
                    "key": first["key"],
                    "operator": "Equal",
                    "expectedValue": first["expectedValue"],
                    "labelValue": first["labelValue"],
                },
                "failures": failures,
                "nodeLabels": dict(sorted(node.labels.items())),
            },
        )
    return _check_passed("NodeSelector", {"requirements": dict(sorted(pod.node_selector.items()))})


def _node_affinity_check(pod: PodSpec, node: NodeSpec) -> dict[str, Any]:
    affinity = pod.affinity.node
    if not affinity.has_required_terms:
        return _check_passed("NodeAffinity", {"requiredTerms": 0})

    evaluations: list[dict[str, Any]] = []
    first_violation: dict[str, Any] | None = None
    matched = False
    for term_index, requirements in enumerate(affinity.required_terms):
        passed, expressions, failure = evaluate_term(node.labels, requirements)
        evaluations.append(
            {
                "termIndex": term_index,
                "matched": passed,
                "expressions": expressions,
                "firstFailure": failure,
            }
        )
        if passed:
            matched = True
        elif first_violation is None and failure is not None:
            first_violation = {
                "source": "requiredDuringSchedulingIgnoredDuringExecution",
                "termIndex": term_index,
                **failure,
            }
    if matched:
        return _check_passed("NodeAffinity", {"terms": evaluations})
    return _check_failed(
        "NodeAffinity",
        "E_UNSATISFIED_AFFINITY",
        "no required node affinity term matched",
        {
            "firstViolationExpression": first_violation,
            "terms": evaluations,
            "nodeLabels": dict(sorted(node.labels.items())),
        },
    )


def _taint_evaluation(
    taint: Taint,
    pod: PodSpec,
    *,
    now_seconds: int,
) -> tuple[bool, dict[str, Any]]:
    matching_tolerations = [
        toleration
        for toleration in pod.tolerations
        if tolerance_matches(taint, toleration, effect=taint.effect)
    ]
    evaluation: dict[str, Any] = {
        "taint": {"key": taint.key, "value": taint.value, "effect": taint.effect},
        "matchingTolerationCount": len(matching_tolerations),
        "tolerated": False,
    }
    if not matching_tolerations:
        evaluation["reason"] = "no matching toleration"
        return False, evaluation

    if taint.effect != "NoExecute":
        evaluation["tolerated"] = True
        evaluation["reason"] = "matching toleration found"
        return True, evaluation

    window_attempts: list[dict[str, Any]] = []
    for toleration in matching_tolerations:
        seconds = toleration.toleration_seconds
        if seconds is None:
            window_attempts.append({"tolerationSeconds": None, "valid": True, "reason": "unbounded toleration"})
            evaluation.update({"tolerated": True, "reason": "unbounded NoExecute toleration", "windowAttempts": window_attempts})
            return True, evaluation
        time_added = taint.time_added_seconds if taint.time_added_seconds is not None else now_seconds
        elapsed = max(0, now_seconds - time_added)
        remaining = seconds - elapsed
        window_attempts.append(
            {
                "tolerationSeconds": seconds,
                "taintTimeAddedSeconds": time_added,
                "nowSeconds": now_seconds,
                "elapsedSeconds": elapsed,
                "remainingSeconds": remaining,
                "valid": remaining > 0,
                "expiresAtSeconds": time_added + seconds,
            }
        )
        if remaining > 0:
            evaluation.update(
                {
                    "tolerated": True,
                    "reason": "NoExecute toleration time window is still open",
                    "windowAttempts": window_attempts,
                }
            )
            return True, evaluation

    evaluation.update(
        {
            "tolerated": False,
            "reason": "NoExecute toleration time window expired",
            "windowAttempts": window_attempts,
        }
    )
    return False, evaluation


def _taints_check(pod: PodSpec, node: NodeSpec, now_seconds: int) -> dict[str, Any]:
    evaluations: list[dict[str, Any]] = []
    hard_failures: list[dict[str, Any]] = []
    prefer_no_schedule = []
    for taint in node.taints:
        tolerated, evaluation = _taint_evaluation(taint, pod, now_seconds=now_seconds)
        evaluations.append(evaluation)
        if taint.effect == "PreferNoSchedule":
            if not tolerated:
                prefer_no_schedule.append(evaluation["taint"])
            continue
        if not tolerated:
            hard_failures.append(evaluation)
    if hard_failures:
        first = hard_failures[0]
        return _check_failed(
            "Taints",
            "E_TAINT_NOT_TOLERATED",
            f"untolerated {first['taint']['effect']} taint: {first['taint']['key']}",
            {
                "firstViolation": first,
                "taintEvaluations": evaluations,
                "preferNoSchedulePenalties": prefer_no_schedule,
            },
        )
    return _check_passed(
        "Taints",
        {
            "taintEvaluations": evaluations,
            "preferNoSchedulePenalties": prefer_no_schedule,
        },
    )


def _pod_affinity_check(pod: PodSpec, node: NodeSpec, world: World) -> dict[str, Any]:
    terms = pod.affinity.pod.required_terms
    if not terms:
        return _check_passed("PodAffinity", {"requiredTerms": 0})
    node_labels = {name: runtime.spec.labels for name, runtime in world.nodes.items()}
    evaluations = []
    for index, term in enumerate(terms):
        candidate_topology = topology_value(node.labels, term.topology_key)
        matching: list[dict[str, Any]] = []
        if candidate_topology is not None:
            for placed in world.placed:
                if not matches_label_selector(placed.labels, term.selector):
                    continue
                placed_topology = topology_value(node_labels.get(placed.node_name, {}), term.topology_key)
                if placed_topology is not None and placed_topology == candidate_topology:
                    matching.append({"name": placed.name, "node": placed.node_name, "topologyValue": placed_topology})
        evaluations.append(
            {
                "termIndex": index,
                "topologyKey": term.topology_key,
                "candidateTopologyValue": candidate_topology,
                "matchingPods": matching,
                "satisfied": bool(matching),
            }
        )
    if all(evaluation["satisfied"] for evaluation in evaluations):
        return _check_passed("PodAffinity", {"terms": evaluations})
    first = next(evaluation for evaluation in evaluations if not evaluation["satisfied"])
    return _check_failed(
        "PodAffinity",
        "E_POD_AFFINITY_UNSATISFIED",
        "required Pod affinity has no matching placed Pod in the target topology",
        {"firstViolation": first, "terms": evaluations},
    )


def _pod_anti_affinity_check(pod: PodSpec, node: NodeSpec, world: World) -> dict[str, Any]:
    terms = pod.affinity.pod_anti.required_terms
    if not terms:
        return _check_passed("PodAntiAffinity", {"requiredTerms": 0})
    node_labels = {name: runtime.spec.labels for name, runtime in world.nodes.items()}
    evaluations = []
    for index, term in enumerate(terms):
        candidate_topology = topology_value(node.labels, term.topology_key)
        conflicts: list[dict[str, Any]] = []
        if candidate_topology is not None:
            for placed in world.placed:
                if not matches_label_selector(placed.labels, term.selector):
                    continue
                placed_topology = topology_value(node_labels.get(placed.node_name, {}), term.topology_key)
                if placed_topology is not None and placed_topology == candidate_topology:
                    conflicts.append({"name": placed.name, "node": placed.node_name, "topologyValue": placed_topology})
        evaluations.append(
            {
                "termIndex": index,
                "topologyKey": term.topology_key,
                "candidateTopologyValue": candidate_topology,
                "conflicts": conflicts,
                "satisfied": not conflicts,
            }
        )
    if all(evaluation["satisfied"] for evaluation in evaluations):
        return _check_passed("PodAntiAffinity", {"terms": evaluations})
    first = next(evaluation for evaluation in evaluations if not evaluation["satisfied"])
    return _check_failed(
        "PodAntiAffinity",
        "E_POD_ANTI_AFFINITY_VIOLATION",
        "required Pod anti-affinity conflicts with a placed Pod in the same topology",
        {"firstViolation": first, "terms": evaluations},
    )


def _resource_check(pod: PodSpec, node: NodeRuntime) -> dict[str, Any]:
    if node.accounting_overflow:
        return _check_failed(
            "NodeResourcesFit",
            "E_NODE_ACCOUNTING_OVERFLOW",
            "node assignment ledger is inconsistent; all further placements are denied",
            {
                "overflow": node.overflow_details,
                "remainingSnapshot": _remaining(node),
            },
        )
    remaining = _remaining(node)
    request = pod.requests
    cpu_shortfall = max(0, request.cpu_millicores - remaining["cpuMillicores"])
    memory_shortfall = max(0, request.memory_bytes - remaining["memoryBytes"])
    details = {
        "remainingBefore": remaining,
        "request": _resources_dict(request),
        "projectedRemaining": {
            "cpuMillicores": remaining["cpuMillicores"] - request.cpu_millicores,
            "memoryBytes": remaining["memoryBytes"] - request.memory_bytes,
        },
        "shortfall": {
            "cpuMillicores": cpu_shortfall,
            "memoryBytes": memory_shortfall,
        },
        "fallbackReason": "request exceeds remaining allocatable resources after exact integer cumulative deduction",
    }
    if cpu_shortfall or memory_shortfall:
        failing = []
        if cpu_shortfall:
            failing.append("cpu")
        if memory_shortfall:
            failing.append("memory")
        details["resource"] = "+".join(failing)
        return _check_failed(
            "NodeResourcesFit",
            "E_INSUFFICIENT_RESOURCES",
            f"insufficient {details['resource']} on node {node.spec.name}",
            details,
        )
    return _check_passed("NodeResourcesFit", details)


def evaluate_node(world: World, pod: PodSpec, node: NodeRuntime, *, now_seconds: int) -> dict[str, Any]:
    checks = [
        _check_passed("NodeAccounting", {"accountingOverflow": False})
        if not node.accounting_overflow
        else _check_failed(
            "NodeAccounting",
            "E_NODE_ACCOUNTING_OVERFLOW",
            "node assignment ledger replay found an over-capacity assigned Pod",
            {"overflow": node.overflow_details},
        ),
        _node_selector_check(pod, node.spec),
        _node_affinity_check(pod, node.spec),
        _taints_check(pod, node.spec, now_seconds),
        _pod_affinity_check(pod, node.spec, world),
        _pod_anti_affinity_check(pod, node.spec, world),
        _resource_check(pod, node),
    ]
    first_rejection = next((check for check in checks if not check["passed"]), None)
    return {
        "node": node.spec.name,
        "passed": first_rejection is None,
        "checks": checks,
        "firstRejection": first_rejection,
        "remainingBefore": _remaining(node),
        "allocatable": _resources_dict(node.spec.allocatable),
    }


def _constraint_snapshot(world: World, topology_keys: list[str]) -> dict[str, Any]:
    placed = [
        {
            "name": item.name,
            "node": item.node_name,
            "source": item.source,
            "priority": item.priority,
            "labels": dict(sorted(item.labels.items())),
            "topology": {
                key: world.nodes[item.node_name].spec.labels.get(key)
                for key in topology_keys
            },
        }
        for item in world.placed
    ]
    buckets = []
    for key in topology_keys:
        values: dict[str, list[str]] = {}
        for item in world.placed:
            value = world.nodes[item.node_name].spec.labels.get(key)
            if value is not None:
                values.setdefault(value, []).append(item.name)
        buckets.append(
            {
                "topologyKey": key,
                "values": {value: names for value, names in sorted(values.items())},
            }
        )
    return {"placedPods": placed, "topologyBuckets": buckets}


def _constraint_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_names = {item["name"] for item in before["placedPods"]}
    after_names = {item["name"] for item in after["placedPods"]}
    return {
        "addedPods": sorted(after_names - before_names),
        "removedPods": sorted(before_names - after_names),
        "changed": before_names != after_names,
    }


def _resource_attempt(node: NodeRuntime, pod: PodSpec) -> dict[str, Any]:
    return {
        "remainingBefore": _remaining(node),
        "request": _resources_dict(pod.requests),
        "fallbackReason": "resource filter denied this candidate; remaining state was not mutated",
    }


def _apply_placement(world: World, node: NodeRuntime, pod: PodSpec, sequence: int) -> dict[str, Any]:
    before = _remaining(node)
    node.used_cpu += pod.requests.cpu_millicores
    node.used_memory += pod.requests.memory_bytes
    after = _remaining(node)
    entry = {
        "sequence": sequence,
        "pod": pod.name,
        "source": "pending",
        "request": _resources_dict(pod.requests),
        "remainingBefore": before,
        "remainingAfter": after,
    }
    node.placement_ledger.append(entry)
    world.placed.append(PlacedPod(pod.name, dict(pod.labels), node.spec.name, pod.priority, "scheduled", False))
    return entry


def _preemption_plan(world: World, pod: PodSpec, *, now_seconds: int) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    allowed_failures = {"E_INSUFFICIENT_RESOURCES", "E_POD_ANTI_AFFINITY_VIOLATION"}
    for node_name in world.nodes:
        node = world.nodes[node_name]
        trace = evaluate_node(world, pod, node, now_seconds=now_seconds)
        failing_codes = {
            check.get("code")
            for check in trace["checks"]
            if not check["passed"]
        }
        if not failing_codes or not failing_codes.issubset(allowed_failures):
            continue
        victims = sorted(
            (
                placed
                for placed in world.placed
                if placed.source == "assigned"
                and placed.node_name == node_name
                and placed.preemptable
                and placed.priority < pod.priority
            ),
            key=lambda item: (item.priority, item.name),
        )
        if not victims:
            continue
        candidate_world = world.clone()
        candidate_node = candidate_world.nodes[node_name]
        victim_records = []
        for victim in victims:
            before = _remaining(candidate_node)
            candidate_node.used_cpu -= _request_for_placed(victim, candidate_node.spec).cpu_millicores
            candidate_node.used_memory -= _request_for_placed(victim, candidate_node.spec).memory_bytes
            after = _remaining(candidate_node)
            candidate_node.assigned_status[victim.name] = "preempted"
            candidate_node.preemption_ledger.append(
                {
                    "victim": victim.name,
                    "priority": victim.priority,
                    "reclaimed": _resources_dict(_request_for_placed(victim, candidate_node.spec)),
                    "remainingBefore": before,
                    "remainingAfter": after,
                    "reason": f"lower priority than incoming Pod {pod.name}",
                }
            )
            victim_records.append(
                {
                    "name": victim.name,
                    "priority": victim.priority,
                    "node": node_name,
                    "remainingBefore": before,
                    "remainingAfter": after,
                }
            )
            candidate_world.placed = [item for item in candidate_world.placed if item.name != victim.name]
        after_trace = evaluate_node(candidate_world, pod, candidate_node, now_seconds=now_seconds)
        if not after_trace["passed"]:
            continue
        cpu_after = candidate_node.used_cpu + pod.requests.cpu_millicores
        memory_after = candidate_node.used_memory + pod.requests.memory_bytes
        score = score_node(
            pod=pod,
            node=candidate_node.spec,
            cpu_after=cpu_after,
            memory_after=memory_after,
            placed_pods=candidate_world.placed,
            node_labels_by_name={name: runtime.spec.labels for name, runtime in candidate_world.nodes.items()},
        )
        candidates.append(
            {
                "world": candidate_world,
                "node": node_name,
                "victims": victim_records,
                "traceAfterPreemption": after_trace,
                "score": score,
                "remainingAfterPreemption": _remaining(candidate_node),
            }
        )
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: (-item["score"]["scoreUnits"], item["node"]))[0]


def _request_for_placed(placed: PlacedPod, node: NodeSpec) -> ResourceAmount:
    if placed.source == "assigned":
        for assigned in node.assigned_pods:
            if assigned.name == placed.name:
                return assigned.requests
    # Scheduled pods are never valid preemption victims in this workbench.
    return ResourceAmount(0, 0)


def _topology_keys(batch: BatchSpec) -> list[str]:
    keys: set[str] = set()
    for pod in batch.pods:
        for term in pod.affinity.pod.required_terms + tuple(item.term for item in pod.affinity.pod.preferred_terms):
            keys.add(term.topology_key)
        for term in pod.affinity.pod_anti.required_terms + tuple(item.term for item in pod.affinity.pod_anti.preferred_terms):
            keys.add(term.topology_key)
    return sorted(keys)


def _node_invariant(node: NodeRuntime) -> dict[str, Any]:
    initial_cpu = sum(item["request"]["cpuMillicores"] for item in node.initial_ledger if item["accepted"])
    initial_memory = sum(item["request"]["memoryBytes"] for item in node.initial_ledger if item["accepted"])
    scheduled_cpu = sum(item["request"]["cpuMillicores"] for item in node.placement_ledger)
    scheduled_memory = sum(item["request"]["memoryBytes"] for item in node.placement_ledger)
    reclaimed_cpu = sum(item["reclaimed"]["cpuMillicores"] for item in node.preemption_ledger)
    reclaimed_memory = sum(item["reclaimed"]["memoryBytes"] for item in node.preemption_ledger)
    expected_cpu = initial_cpu + scheduled_cpu - reclaimed_cpu
    expected_memory = initial_memory + scheduled_memory - reclaimed_memory
    return {
        "cpu": {
            "allocatable": node.spec.allocatable.cpu_millicores,
            "initialAccepted": initial_cpu,
            "scheduled": scheduled_cpu,
            "preemptionReclaimed": reclaimed_cpu,
            "used": node.used_cpu,
            "remaining": node.spec.allocatable.cpu_millicores - node.used_cpu,
            "equation": "allocatable - initialAccepted - scheduled + preemptionReclaimed = remaining",
            "consistent": expected_cpu == node.used_cpu
            and node.spec.allocatable.cpu_millicores - node.used_cpu >= 0,
        },
        "memory": {
            "allocatable": node.spec.allocatable.memory_bytes,
            "initialAccepted": initial_memory,
            "scheduled": scheduled_memory,
            "preemptionReclaimed": reclaimed_memory,
            "used": node.used_memory,
            "remaining": node.spec.allocatable.memory_bytes - node.used_memory,
            "equation": "allocatable - initialAccepted - scheduled + preemptionReclaimed = remaining",
            "consistent": expected_memory == node.used_memory
            and node.spec.allocatable.memory_bytes - node.used_memory >= 0,
        },
        "signed64Safe": all(
            value <= MAX_INT64 for value in (initial_cpu, initial_memory, scheduled_cpu, scheduled_memory, node.used_cpu, node.used_memory)
        ),
    }


def _node_result(node: NodeRuntime) -> dict[str, Any]:
    return {
        "name": node.spec.name,
        "allocatable": _resources_dict(node.spec.allocatable),
        "labels": dict(sorted(node.spec.labels.items())),
        "taints": [
            {
                "key": taint.key,
                "value": taint.value,
                "effect": taint.effect,
                "timeAddedSeconds": taint.time_added_seconds,
            }
            for taint in node.spec.taints
        ],
        "used": {"cpuMillicores": node.used_cpu, "memoryBytes": node.used_memory},
        "remaining": _remaining(node),
        "accountingOverflow": node.accounting_overflow,
        "assignedPods": [
            {"name": name, "status": status} for name, status in sorted(node.assigned_status.items())
        ],
        "initialLedger": node.initial_ledger,
        "placementLedger": node.placement_ledger,
        "preemptionLedger": node.preemption_ledger,
        "invariant": _node_invariant(node),
    }


def _rejection_for_trace(trace: dict[str, Any]) -> dict[str, Any]:
    failure = trace["firstRejection"] or {}
    return {
        "node": trace["node"],
        "code": failure.get("code", "E_SCHEDULING_FAILED"),
        "message": failure.get("message", "no node passed all filters"),
        "details": failure.get("details", {}),
    }


def _record_score_audit(
    audit: list[dict[str, Any]],
    *,
    pod_name: str,
    node_name: str,
    score: dict[str, Any],
) -> None:
    exact = score["rounding"]["componentResidualTotalExact"]
    numerator, denominator = (int(part) for part in exact.split("/", maxsplit=1))
    residual_units = Fraction(numerator, denominator)
    audit.append(
        {
            "pod": pod_name,
            "node": node_name,
            "score": score["score"],
            "residualUnitsExact": exact,
            "residualPoints": format_fraction(residual_units / SCORE_SCALE, 12),
        }
    )


def schedule_batch(batch: BatchSpec, *, batch_id: str) -> dict[str, Any]:
    world = initialize_world(batch)
    topology_keys = _topology_keys(batch)
    initial_constraints = _constraint_snapshot(world, topology_keys)
    ordered_pods = sorted(enumerate(batch.pods), key=lambda item: (-item[1].priority, item[0]))
    steps: list[dict[str, Any]] = []
    placed_results: list[dict[str, Any]] = []
    rejected_results: list[dict[str, Any]] = []
    preempted_results: list[dict[str, Any]] = []
    score_rounding_audit: list[dict[str, Any]] = []

    for sequence, (_, pod) in enumerate(ordered_pods, start=1):
        before_constraints = _constraint_snapshot(world, topology_keys)
        traces = [evaluate_node(world, pod, world.nodes[node_spec.name], now_seconds=batch.now_seconds) for node_spec in batch.nodes]

        scored_candidates = []
        for trace in traces:
            if not trace["passed"]:
                continue
            node = world.nodes[trace["node"]]
            score = score_node(
                pod=pod,
                node=node.spec,
                cpu_after=node.used_cpu + pod.requests.cpu_millicores,
                memory_after=node.used_memory + pod.requests.memory_bytes,
                placed_pods=world.placed,
                node_labels_by_name={name: runtime.spec.labels for name, runtime in world.nodes.items()},
            )
            _record_score_audit(score_rounding_audit, pod_name=pod.name, node_name=trace["node"], score=score)
            scored_candidates.append({"node": trace["node"], **score})

        preemption_event: dict[str, Any] | None = None
        selected_node: str | None = None
        selected_score: dict[str, Any] | None = None
        selected_trace: dict[str, Any] | None = None

        if scored_candidates:
            winner = sorted(scored_candidates, key=lambda item: (-item["scoreUnits"], item["node"]))[0]
            selected_node = winner["node"]
            selected_score = winner
            selected_trace = next(trace for trace in traces if trace["node"] == selected_node)
        elif batch.preemption_enabled:
            plan = _preemption_plan(world, pod, now_seconds=batch.now_seconds)
            if plan is not None:
                world = plan["world"]
                node = world.nodes[plan["node"]]
                selected_node = plan["node"]
                selected_score = plan["score"]
                selected_trace = plan["traceAfterPreemption"]
                traces = [selected_trace if trace["node"] == selected_node else trace for trace in traces]
                _record_score_audit(
                    score_rounding_audit,
                    pod_name=pod.name,
                    node_name=selected_node,
                    score=selected_score,
                )
                preemption_event = {
                    "selected": True,
                    "incomingPod": pod.name,
                    "incomingPriority": pod.priority,
                    "node": plan["node"],
                    "victims": plan["victims"],
                    "remainingAfterPreemption": plan["remainingAfterPreemption"],
                    "resourceRecalculation": "victim requests were added back before evaluating the incoming Pod",
                }
                for victim in plan["victims"]:
                    preempted_results.append(
                        {
                            "pod": victim["name"],
                            "node": plan["node"],
                            "priority": victim["priority"],
                            "preemptedBy": pod.name,
                            "reason": "lower priority victim reclaims resources for a higher priority Pod",
                        }
                    )

        if selected_node is not None and selected_score is not None and selected_trace is not None:
            selected_trace["selected"] = True
            node = world.nodes[selected_node]
            placement_entry = _apply_placement(world, node, pod, sequence)
            after_constraints = _constraint_snapshot(world, topology_keys)
            step = {
                "sequence": sequence,
                "pod": pod.name,
                "priority": pod.priority,
                "podResources": {
                    "requests": _resources_dict(pod.requests),
                    "limits": _resources_dict(pod.limits) if pod.limits is not None else None,
                },
                "decision": "placed",
                "selectedNode": selected_node,
                "filterTrace": traces,
                "scoreCandidates": sorted(scored_candidates, key=lambda item: (-item["scoreUnits"], item["node"])),
                "selectedScore": selected_score,
                "placementLedgerEntry": placement_entry,
                "preemption": preemption_event,
                "constraintBefore": before_constraints,
                "constraintAfter": after_constraints,
                "constraintDelta": _constraint_delta(before_constraints, after_constraints),
            }
            placed_results.append(
                {
                    "pod": pod.name,
                    "node": selected_node,
                    "priority": pod.priority,
                    "score": selected_score["score"],
                    "scoreUnits": selected_score["scoreUnits"],
                }
            )
        else:
            after_constraints = _constraint_snapshot(world, topology_keys)
            per_node_rejections = [_rejection_for_trace(trace) for trace in traces]
            trace_order = {trace["node"]: index for index, trace in enumerate(traces)}
            passed_counts = {
                trace["node"]: sum(1 for check in trace["checks"] if check["passed"])
                for trace in traces
            }
            failure_stage = {
                trace["node"]: next(
                    (index for index, check in enumerate(trace["checks"]) if not check["passed"]),
                    -1,
                )
                for trace in traces
            }
            primary_node = sorted(
                traces,
                key=lambda trace: (
                    -passed_counts[trace["node"]],
                    -failure_stage[trace["node"]],
                    trace_order[trace["node"]],
                ),
            )[0]["node"]
            primary = next(item for item in per_node_rejections if item["node"] == primary_node)
            resource_attempts = {
                trace["node"]: _resource_attempt(world.nodes[trace["node"]], pod)
                for trace in traces
                if any(
                    check.get("code") == "E_INSUFFICIENT_RESOURCES"
                    for check in trace["checks"]
                    if not check["passed"]
                )
            }
            step = {
                "sequence": sequence,
                "pod": pod.name,
                "priority": pod.priority,
                "podResources": {
                    "requests": _resources_dict(pod.requests),
                    "limits": _resources_dict(pod.limits) if pod.limits is not None else None,
                },
                "decision": "rejected",
                "selectedNode": None,
                "filterTrace": traces,
                "scoreCandidates": [],
                "selectedScore": None,
                "preemption": preemption_event,
                "rejection": {
                    "code": primary["code"],
                    "message": primary["message"],
                    "primaryNode": primary["node"],
                    "details": primary["details"],
                    "perNode": per_node_rejections,
                },
                "resourceAttempts": resource_attempts,
                "constraintBefore": before_constraints,
                "constraintAfter": after_constraints,
                "constraintDelta": _constraint_delta(before_constraints, after_constraints),
            }
            rejected_results.append(
                {
                    "pod": pod.name,
                    "priority": pod.priority,
                    "reason": primary,
                    "perNodeRejections": per_node_rejections,
                }
            )
        steps.append(step)

    final_constraints = _constraint_snapshot(world, topology_keys)
    node_results = [_node_result(world.nodes[node_spec.name]) for node_spec in batch.nodes]
    all_consistent = all(
        not node["accountingOverflow"]
        and node["invariant"]["cpu"]["consistent"]
        and node["invariant"]["memory"]["consistent"]
        for node in node_results
    )
    cumulative_residual = sum(
        (
            Fraction(*(int(part) for part in item["residualUnitsExact"].split("/", maxsplit=1)))
            for item in score_rounding_audit
        ),
        Fraction(0),
    )
    return {
        "batchId": batch_id,
        "specName": batch.name,
        "status": "completed",
        "schedulingOrder": [
            {"pod": pod.name, "priority": pod.priority, "sourceIndex": index}
            for index, (_, pod) in enumerate(ordered_pods)
        ],
        "summary": {
            "nodes": len(batch.nodes),
            "pendingPods": len(batch.pods),
            "placed": len(placed_results),
            "rejected": len(rejected_results),
            "preempted": len(preempted_results),
        },
        "placed": placed_results,
        "rejected": rejected_results,
        "preempted": preempted_results,
        "steps": steps,
        "nodes": node_results,
        "constraints": {
            "topologyKeys": topology_keys,
            "initial": initial_constraints,
            "final": final_constraints,
        },
        "scoreContract": {
            "scoreDecimals": SCORE_DECIMALS,
            "internalScale": 1_000_000,
            "method": "integer micro-point accumulation; half-up only at fractional component boundaries",
            "totalRange": "0.000000..100.000000",
            "roundingAudit": {
                "evaluations": score_rounding_audit,
                "evaluationCount": len(score_rounding_audit),
                "cumulativeResidualExact": f"{cumulative_residual.numerator}/{cumulative_residual.denominator}",
                "cumulativeResidualPoints": format_fraction(cumulative_residual / SCORE_SCALE, 12),
                "note": "Residuals are summed as exact fractions before the display-only decimal conversion.",
            },
        },
        "resourceConsistency": {
            "allNodesConsistent": all_consistent,
            "note": "Every accepted deduction was non-negative and integer-exact; rejected attempts did not mutate state.",
        },
        "preemption": {
            "enabled": batch.preemption_enabled,
            "events": [step["preemption"] for step in steps if step.get("preemption")],
        },
    }


def find_pod_trace(result: dict[str, Any], pod_name: str) -> dict[str, Any] | None:
    for step in result["steps"]:
        if step["pod"] == pod_name:
            return step
    return None


def find_node(result: dict[str, Any], node_name: str) -> dict[str, Any] | None:
    for node in result["nodes"]:
        if node["name"] == node_name:
            return node
    return None
