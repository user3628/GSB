from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from typing import Any

from .config import LabConfig, SOURCE_ORDER
from .errors import BusinessError, ErrorCode


@dataclass(frozen=True)
class RouteDecision:
    source: str
    configured_enabled: bool
    requested_depth: int
    quota_depth: int
    maximum_depth: int
    minimum_depth: int
    enabled: bool
    effective_depth: int
    planned_evaluations: int
    reason: str
    reason_text: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "configured_enabled": self.configured_enabled,
            "requested_depth": self.requested_depth,
            "quota_depth": self.quota_depth,
            "maximum_depth": self.maximum_depth,
            "minimum_depth": self.minimum_depth,
            "enabled": self.enabled,
            "effective_depth": self.effective_depth,
            "planned_evaluations": self.planned_evaluations,
            "reason": self.reason,
            "reason_text": self.reason_text,
        }


@dataclass(frozen=True)
class RecallDecision:
    scale: str
    item_count: int
    query_count: int
    global_budget: int
    used_evaluations: int
    remaining_evaluations: int
    minimum_depth: int
    priority: tuple[str, ...]
    routes: dict[str, RouteDecision]
    fusion_top_k: int
    fusion_top_k_reason: str

    @property
    def enabled_sources(self) -> tuple[str, ...]:
        return tuple(name for name in SOURCE_ORDER if self.routes[name].enabled)

    @property
    def disabled_sources(self) -> tuple[str, ...]:
        return tuple(name for name in SOURCE_ORDER if not self.routes[name].enabled)

    def as_dict(self) -> dict[str, Any]:
        return {
            "scale": self.scale,
            "item_count": self.item_count,
            "query_count": self.query_count,
            "global_budget": self.global_budget,
            "used_evaluations": self.used_evaluations,
            "remaining_evaluations": self.remaining_evaluations,
            "minimum_depth": self.minimum_depth,
            "priority": list(self.priority),
            "enabled_sources": list(self.enabled_sources),
            "disabled_sources": list(self.disabled_sources),
            "routes": {name: self.routes[name].as_dict() for name in SOURCE_ORDER},
            "fusion_top_k": self.fusion_top_k,
            "fusion_top_k_reason": self.fusion_top_k_reason,
            "eliminated_routes": [
                {
                    "source": name,
                    "reason": self.routes[name].reason,
                    "reason_text": self.routes[name].reason_text,
                }
                for name in self.disabled_sources
            ],
        }


def _distribute_extra_depth(
    capacities: dict[str, int],
    extra_units: int,
    priority: tuple[str, ...],
) -> dict[str, int]:
    """Deterministically apportion integer depth units by largest remainder."""
    total_capacity = sum(capacities.values())
    allocation = {name: 0 for name in capacities}
    if extra_units <= 0 or total_capacity <= 0:
        return allocation
    extra_units = min(extra_units, total_capacity)
    remainders: list[tuple[Fraction, int, str]] = []
    for priority_index, name in enumerate(priority):
        if name not in capacities:
            continue
        numerator = extra_units * capacities[name]
        whole, remainder = divmod(numerator, total_capacity)
        allocation[name] = whole
        remainders.append((Fraction(remainder, total_capacity), priority_index, name))
    remaining = extra_units - sum(allocation.values())
    remainders.sort(key=lambda entry: (-entry[0], entry[1]))
    for _, _, name in remainders:
        if remaining <= 0:
            break
        if allocation[name] < capacities[name]:
            allocation[name] += 1
            remaining -= 1
    # A second deterministic pass is only needed when apportionment rounded down
    # across more than one candidate per route.
    while remaining > 0:
        progressed = False
        for name in priority:
            if remaining <= 0:
                break
            if name in capacities and allocation[name] < capacities[name]:
                allocation[name] += 1
                remaining -= 1
                progressed = True
        if not progressed:
            break
    return allocation


def decide_recall(
    config: LabConfig,
    *,
    scale: str,
    item_count: int,
    query_count: int,
) -> RecallDecision:
    if item_count < 0 or query_count < 0:
        raise BusinessError(ErrorCode.INVALID_INPUT, "item and query counts must be non-negative")
    budget = config.budget.global_candidate_evaluations
    details: dict[str, dict[str, Any]] = {}
    eligible_order: list[str] = []

    for name in SOURCE_ORDER:
        source = config.sources[name]
        requested_depth = min(source.candidate_depth, item_count) if source.enabled else 0
        quota_depth = source.quota // query_count if query_count > 0 else 0
        maximum_depth = min(requested_depth, quota_depth)
        route_minimum = min(config.budget.minimum_depth, maximum_depth) if maximum_depth > 0 else 0
        details[name] = {
            "requested_depth": requested_depth,
            "quota_depth": quota_depth,
            "maximum_depth": maximum_depth,
            "route_minimum": route_minimum,
        }
        if maximum_depth > 0:
            eligible_order.append(name)

    eligible_by_priority = [name for name in config.budget.priority if name in eligible_order]
    minimum_cost = lambda names: sum(details[name]["route_minimum"] for name in names) * query_count
    selected: list[str] = []
    for count in range(len(eligible_by_priority), 0, -1):
        candidate = eligible_by_priority[:count]
        if minimum_cost(candidate) <= budget:
            selected = candidate
            break

    degradation_reason: str | None = None
    if not selected and eligible_by_priority:
        first = eligible_by_priority[0]
        possible_depth = budget // query_count if query_count else 0
        possible_depth = min(details[first]["maximum_depth"], possible_depth)
        if possible_depth > 0:
            selected = [first]
            details[first]["route_minimum"] = possible_depth
            degradation_reason = "GLOBAL_BUDGET_BELOW_MINIMUM"

    allocations: dict[str, int] = {}
    if selected and query_count > 0:
        base = {name: details[name]["route_minimum"] for name in selected}
        base_cost = sum(base.values()) * query_count
        remaining_budget = max(0, budget - base_cost)
        extra_units = remaining_budget // query_count
        capacities = {
            name: max(0, details[name]["maximum_depth"] - base[name])
            for name in selected
        }
        extra = _distribute_extra_depth(capacities, extra_units, config.budget.priority)
        allocations.update({name: base[name] + extra[name] for name in selected})

    routes: dict[str, RouteDecision] = {}
    for name in SOURCE_ORDER:
        source = config.sources[name]
        info = details[name]
        depth = allocations.get(name, 0)
        enabled = name in selected and depth > 0
        if not source.enabled:
            reason = "DISABLED_CONFIG"
            reason_text = "该路在配置中被关闭"
        elif item_count == 0:
            reason = "DISABLED_NO_ITEMS"
            reason_text = "物品库为空，无法召回"
        elif query_count == 0:
            reason = "DISABLED_NO_QUERIES"
            reason_text = "查询集为空，无需召回"
        elif info["quota_depth"] <= 0:
            reason = "DISABLED_SOURCE_QUOTA"
            reason_text = "单路配额不足以覆盖一个查询的一个候选槽位"
        elif not enabled and info["maximum_depth"] > 0:
            reason = "DISABLED_GLOBAL_BUDGET"
            reason_text = "全局预算无法覆盖该路最小深度，按优先级淘汰更低优先级路"
        elif enabled and degradation_reason and info.get("route_minimum") == depth:
            reason = degradation_reason
            reason_text = "全局预算不足最小深度，降档到可执行的最低深度"
        elif enabled and depth < info["requested_depth"]:
            reason = "ENABLED_DEGRADED"
            reason_text = "在单路配额与全局预算约束下按容量比例降档"
        elif enabled and info["requested_depth"] < source.candidate_depth:
            reason = "ENABLED_BOUNDED_BY_ITEMS"
            reason_text = "候选深度受物品总数上界约束"
        elif enabled:
            reason = "ENABLED_FULL"
            reason_text = "按配置深度完整启用"
        else:
            reason = "DISABLED_UNKNOWN"
            reason_text = "没有可用候选槽位"

        planned = depth * query_count
        routes[name] = RouteDecision(
            source=name,
            configured_enabled=source.enabled,
            requested_depth=info["requested_depth"],
            quota_depth=info["quota_depth"],
            maximum_depth=info["maximum_depth"],
            minimum_depth=info["route_minimum"],
            enabled=enabled,
            effective_depth=depth,
            planned_evaluations=planned,
            reason=reason,
            reason_text=reason_text,
        )

    used = sum(route.planned_evaluations for route in routes.values())
    for route in routes.values():
        if route.planned_evaluations > config.sources[route.source].quota:
            raise BusinessError(
                ErrorCode.INTERNAL,
                "budget decision exceeded source quota",
                {"source": route.source, "used": route.planned_evaluations},
            )
        if query_count and route.effective_depth > route.quota_depth:
            raise BusinessError(
                ErrorCode.INTERNAL,
                "budget decision exceeded source quota depth",
                {"source": route.source},
            )
    if used > budget:
        raise BusinessError(
            ErrorCode.INTERNAL,
            "budget decision exceeded global budget",
            {"used": used, "budget": budget},
        )

    possible_union_depth = sum(route.effective_depth for route in routes.values())
    if possible_union_depth <= 0:
        fusion_top_k = 0
        top_k_reason = "NO_ACTIVE_RECALL_ROUTE"
    elif possible_union_depth < config.fusion.top_k:
        fusion_top_k = possible_union_depth
        top_k_reason = "BOUNDED_BY_UNION_CANDIDATES"
    else:
        fusion_top_k = config.fusion.top_k
        top_k_reason = "CONFIGURED_TOP_K"

    return RecallDecision(
        scale=scale,
        item_count=item_count,
        query_count=query_count,
        global_budget=budget,
        used_evaluations=used,
        remaining_evaluations=budget - used,
        minimum_depth=config.budget.minimum_depth,
        priority=config.budget.priority,
        routes=routes,
        fusion_top_k=fusion_top_k,
        fusion_top_k_reason=top_k_reason,
    )
