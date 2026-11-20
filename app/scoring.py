from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from math import floor
from typing import Any, Protocol, Sequence

from app.matcher import evaluate_term, matches_label_selector, topology_value, tolerance_matches
from app.models import NodeAffinity, NodeSpec, PodAffinity, PodSpec, ResourceAmount

SCORE_SCALE = 1_000_000
SCORE_DECIMALS = 6
TOTAL_MAX_POINTS = 100

NODE_PREFERRED_MAX = 35
POD_AFFINITY_MAX = 20
POD_ANTI_AFFINITY_MAX = 15
RESOURCE_BALANCE_MAX = 30
PREFER_NO_SCHEDULE_PENALTY = 5


class PlacedPodView(Protocol):
    name: str
    labels: dict[str, str]
    node_name: str


@dataclass(frozen=True)
class ComponentScore:
    name: str
    max_points: int
    ratio: Fraction
    units: int
    residual_units: Fraction
    detail: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "component": self.name,
            "maxPoints": self.max_points,
            "ratio": f"{self.ratio.numerator}/{self.ratio.denominator}",
            "points": format_units(self.units),
            "pointsUnits": self.units,
            "roundingErrorPoints": format_fraction(self.residual_units / SCORE_SCALE, 9),
            "detail": self.detail,
        }


def _round_half_up(value: Fraction) -> int:
    return floor(value + Fraction(1, 2))


def _ratio_component(name: str, ratio: Fraction, max_points: int, detail: dict[str, Any]) -> ComponentScore:
    bounded = max(Fraction(0), min(Fraction(1), ratio))
    exact_units = bounded * max_points * SCORE_SCALE
    units = _round_half_up(exact_units)
    return ComponentScore(name, max_points, bounded, units, exact_units - units, detail)


def format_units(units: int) -> str:
    sign = "-" if units < 0 else ""
    absolute = abs(units)
    whole, fraction = divmod(absolute, SCORE_SCALE)
    return f"{sign}{whole}.{fraction:0{SCORE_DECIMALS}d}"


def format_fraction(value: Fraction, places: int = 9) -> str:
    scale = 10**places
    scaled = value * scale
    rounded = floor(scaled + Fraction(1, 2)) if scaled >= 0 else -floor(-scaled + Fraction(1, 2))
    sign = "-" if rounded < 0 else ""
    absolute = abs(rounded)
    whole, fraction = divmod(absolute, scale)
    return f"{sign}{whole}.{fraction:0{places}d}"


def _node_preferred_component(
    node: NodeSpec,
    affinity: NodeAffinity,
) -> ComponentScore:
    total_weight = 0
    matched_weight = 0
    evaluations: list[dict[str, Any]] = []
    for weight, requirements in affinity.preferred_terms:
        total_weight += weight
        passed, results, first_failure = evaluate_term(node.labels, requirements)
        if passed:
            matched_weight += weight
        evaluations.append(
            {
                "weight": weight,
                "matched": passed,
                "expressions": results,
                "firstFailure": first_failure,
            }
        )
    ratio = Fraction(matched_weight, total_weight) if total_weight else Fraction(0)
    return _ratio_component(
        "nodePreferredAffinity",
        ratio,
        NODE_PREFERRED_MAX,
        {
            "matchedWeight": matched_weight,
            "totalWeight": total_weight,
            "terms": evaluations,
            "neutralWhenAbsent": True,
        },
    )


def _pod_soft_component(
    *,
    component_name: str,
    node: NodeSpec,
    affinity: PodAffinity,
    placed_pods: Sequence[PlacedPodView],
    node_labels_by_name: dict[str, dict[str, str]],
    require_absence: bool,
) -> ComponentScore:
    total_weight = 0
    satisfied_weight = 0
    evaluations: list[dict[str, Any]] = []
    for weighted in affinity.preferred_terms:
        total_weight += weighted.weight
        term = weighted.term
        candidate_topology = topology_value(node.labels, term.topology_key)
        matching_pods: list[dict[str, Any]] = []
        if candidate_topology is not None:
            for placed in placed_pods:
                if not matches_label_selector(placed.labels, term.selector):
                    continue
                placed_labels = node_labels_by_name.get(placed.node_name, {})
                placed_topology = topology_value(placed_labels, term.topology_key)
                if placed_topology is not None and placed_topology == candidate_topology:
                    matching_pods.append(
                        {
                            "name": placed.name,
                            "node": placed.node_name,
                            "topologyValue": placed_topology,
                        }
                    )
        satisfied = (not matching_pods) if require_absence else bool(matching_pods)
        if satisfied:
            satisfied_weight += weighted.weight
        evaluations.append(
            {
                "weight": weighted.weight,
                "topologyKey": term.topology_key,
                "candidateTopologyValue": candidate_topology,
                "matchingPods": matching_pods,
                "satisfied": satisfied,
            }
        )
    ratio = Fraction(satisfied_weight, total_weight) if total_weight else Fraction(0)
    return _ratio_component(
        component_name,
        ratio,
        POD_AFFINITY_MAX if not require_absence else POD_ANTI_AFFINITY_MAX,
        {
            "satisfiedWeight": satisfied_weight,
            "totalWeight": total_weight,
            "terms": evaluations,
            "requireAbsence": require_absence,
        },
    )


def _resource_balance_component(
    allocatable: ResourceAmount,
    cpu_after: int,
    memory_after: int,
) -> ComponentScore:
    if allocatable.cpu_millicores <= 0 or allocatable.memory_bytes <= 0:
        ratio = Fraction(0)
        cpu_ratio = Fraction(0)
        memory_ratio = Fraction(0)
    else:
        cpu_ratio = Fraction(cpu_after, allocatable.cpu_millicores)
        memory_ratio = Fraction(memory_after, allocatable.memory_bytes)
        ratio = max(Fraction(0), Fraction(1) - abs(cpu_ratio - memory_ratio))
    return _ratio_component(
        "resourceBalance",
        ratio,
        RESOURCE_BALANCE_MAX,
        {
            "cpuAfter": cpu_after,
            "memoryAfter": memory_after,
            "cpuUtilization": format_fraction(cpu_ratio, 9),
            "memoryUtilization": format_fraction(memory_ratio, 9),
            "method": "1 - abs(cpuUtilization - memoryUtilization)",
        },
    )


def _prefer_no_schedule_penalty(node: NodeSpec, pod: PodSpec) -> tuple[int, list[dict[str, Any]]]:
    untolerated: list[dict[str, Any]] = []
    for taint in node.taints:
        if taint.effect != "PreferNoSchedule":
            continue
        tolerated = any(tolerance_matches(taint, toleration, effect=taint.effect) for toleration in pod.tolerations)
        if not tolerated:
            untolerated.append({"key": taint.key, "value": taint.value, "effect": taint.effect})
    return -len(untolerated) * PREFER_NO_SCHEDULE_PENALTY * SCORE_SCALE, untolerated


def score_node(
    *,
    pod: PodSpec,
    node: NodeSpec,
    cpu_after: int,
    memory_after: int,
    placed_pods: Sequence[PlacedPodView],
    node_labels_by_name: dict[str, dict[str, str]],
) -> dict[str, Any]:
    components: list[ComponentScore] = [
        _node_preferred_component(node, pod.affinity.node),
        _pod_soft_component(
            component_name="podAffinityPreferred",
            node=node,
            affinity=pod.affinity.pod,
            placed_pods=placed_pods,
            node_labels_by_name=node_labels_by_name,
            require_absence=False,
        ),
        _pod_soft_component(
            component_name="podAntiAffinityPreferred",
            node=node,
            affinity=pod.affinity.pod_anti,
            placed_pods=placed_pods,
            node_labels_by_name=node_labels_by_name,
            require_absence=True,
        ),
        _resource_balance_component(node.allocatable, cpu_after, memory_after),
    ]

    penalty_units, untolerated_prefer = _prefer_no_schedule_penalty(node, pod)
    penalty_components = []
    if untolerated_prefer:
        penalty_components.append(
            {
                "component": "preferNoSchedulePenalty",
                "maxPoints": 0,
                "points": format_units(penalty_units),
                "pointsUnits": penalty_units,
                "penaltyPerTaint": PREFER_NO_SCHEDULE_PENALTY,
                "taints": untolerated_prefer,
            }
        )

    positive_units = sum(component.units for component in components)
    unclamped_units = positive_units + penalty_units
    effective_units = max(0, min(TOTAL_MAX_POINTS * SCORE_SCALE, unclamped_units))
    clamp_units = effective_units - unclamped_units
    residual_total = sum((component.residual_units for component in components), Fraction(0))
    return {
        "score": format_units(effective_units),
        "scoreUnits": effective_units,
        "scoreDecimals": SCORE_DECIMALS,
        "components": [component.as_dict() for component in components] + penalty_components,
        "rounding": {
            "method": "half-up per fractional component in integer micro-points; total is summed as integers",
            "scale": SCORE_SCALE,
            "componentResidualTotalPoints": format_fraction(residual_total / SCORE_SCALE, 9),
            "componentResidualTotalExact": f"{residual_total.numerator}/{residual_total.denominator}",
            "unclampedUnits": unclamped_units,
            "clampAdjustmentPoints": format_units(clamp_units),
            "note": "All arithmetic before formatting uses integers or exact fractions; repeated identical inputs are bit-for-bit stable.",
        },
        "tieBreak": "higher scoreUnits first, then lexicographically smaller node name",
    }
