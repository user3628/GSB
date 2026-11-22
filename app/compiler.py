from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from fractions import Fraction
from typing import Any, Iterable, Mapping

from app.benchmark import kahan_sum, naive_sum
from app.fixedpoint import BudgetDerivation, derive_budget, format_q6
from app.models import Route, RouteKind


ROUTE_ARTIFACT_KINDS = (
    "match_entry",
    "pool_group",
    "health_plan",
    "forward_cache",
)
AGGREGATE_ARTIFACT_KINDS = ("match_index", "upstream_budget")


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_json(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def normalized_routes_payload(routes: Iterable[Route]) -> list[dict[str, Any]]:
    return [
        route.normalized_payload()
        for route in sorted(routes, key=lambda route: route.route_id)
    ]


def compute_snapshot_digest(version: int, routes: Iterable[Route]) -> str:
    return sha256_json(
        {"version": version, "routes": normalized_routes_payload(routes)}
    )


def snapshot_payload_json(version: int, routes: Iterable[Route]) -> str:
    return canonical_json(
        {"version": version, "routes": normalized_routes_payload(routes)}
    )


@dataclass(frozen=True)
class CompiledArtifact:
    key: str
    kind: str
    route_id: str | None
    payload: dict[str, Any]
    digest: str


@dataclass(frozen=True)
class RoutePlan:
    route: Route
    budget: BudgetDerivation
    artifacts: Mapping[str, CompiledArtifact]


@dataclass(frozen=True)
class UpstreamAggregate:
    upstream: str
    route_ids: tuple[str, ...]
    total_wait_q6: int
    health_penalty_q6: int
    naive_total_wait: float
    kahan_total_wait: float
    naive_health_penalty: float
    kahan_health_penalty: float

    def payload(self) -> dict[str, Any]:
        epsilon = math.ulp(1.0)
        count = max(1, len(self.route_ids))
        return {
            "upstream": self.upstream,
            "route_ids": list(self.route_ids),
            "route_count": len(self.route_ids),
            "aggregation": {
                "canonical": "exact_q6_integer_sum",
                "compensated_check": "kahan_binary64",
                "order_policy": "route_ids_sorted_lexicographically",
            },
            "total_wait_q6": self.total_wait_q6,
            "total_wait_display": format_q6(self.total_wait_q6),
            "health_penalty_q6": self.health_penalty_q6,
            "health_penalty_display": format_q6(self.health_penalty_q6),
            "comparison": {
                "naive_total_wait_float64": self.naive_total_wait,
                "kahan_total_wait_float64": self.kahan_total_wait,
                "naive_total_wait_error_q6": self.naive_total_wait
                - float(self.total_wait_q6),
                "kahan_total_wait_error_q6": self.kahan_total_wait
                - float(self.total_wait_q6),
                "naive_health_penalty_error_q6": self.naive_health_penalty
                - float(self.health_penalty_q6),
                "kahan_health_penalty_error_q6": self.kahan_health_penalty
                - float(self.health_penalty_q6),
                "naive_first_order_bound_q6": float(
                    Fraction(count * epsilon)
                    / Fraction(max(1.0 - count * epsilon, 1e-300))
                    * abs(self.total_wait_q6)
                ),
            },
        }


@dataclass(frozen=True)
class CompiledSnapshot:
    version: int
    digest: str
    routes: tuple[RoutePlan, ...]
    routes_by_id: Mapping[str, RoutePlan]
    reverse_matchers: tuple[RoutePlan, ...]
    forward_matchers: tuple[RoutePlan, ...]
    artifacts: tuple[CompiledArtifact, ...]
    artifacts_by_key: Mapping[str, CompiledArtifact]
    match_index: dict[str, Any]
    upstream_aggregates: tuple[UpstreamAggregate, ...]
    events: tuple[dict[str, Any], ...] = ()

    @property
    def route_count(self) -> int:
        return len(self.routes)

    def match_reverse(self, method: str, path: str) -> RoutePlan | None:
        upper = method.upper()
        for plan in self.reverse_matchers:
            route = plan.route
            if "*" not in route.methods and upper not in route.methods:
                continue
            prefix = route.path_prefix or ""
            if prefix == "/":
                return plan
            if path == prefix or path.startswith(prefix.rstrip("/") + "/"):
                return plan
        return None

    def match_forward(self, method: str, host: str, port: int) -> RoutePlan | None:
        upper = method.upper()
        normalized_host = host.strip("[]").lower()
        for plan in self.forward_matchers:
            route = plan.route
            if "*" not in route.methods and upper not in route.methods:
                continue
            if route.target_host == normalized_host and route.target_port == port:
                return plan
        return None


def _artifact(
    kind: str, route_id: str | None, payload: dict[str, Any], key: str | None = None
) -> CompiledArtifact:
    artifact_key = key or f"{kind}:{route_id}"
    return CompiledArtifact(
        key=artifact_key,
        kind=kind,
        route_id=route_id,
        payload=payload,
        digest=sha256_json(payload),
    )


def _route_artifacts(route: Route, budget: BudgetDerivation) -> Mapping[str, CompiledArtifact]:
    matcher = {
        "route_id": route.route_id,
        "kind": route.kind.value,
        "methods": list(route.methods),
        "selectors": (
            {"path_prefix": route.path_prefix}
            if route.kind is RouteKind.reverse
            else {"target_host": route.target_host, "target_port": route.target_port}
        ),
        "priority": route.priority,
    }
    match_entry = _artifact("match_entry", route.route_id, matcher)

    pool_material = {
        "origin": route.upstream,
        "connect_budget_q6": budget.connect_budget_q6,
        "attempts": budget.attempts,
    }
    pool_group = _artifact(
        "pool_group",
        route.route_id,
        {
            "route_id": route.route_id,
            "origin": route.upstream,
            "pool_key": sha256_json(pool_material)[:20],
            "connect_budget_q6": budget.connect_budget_q6,
            "max_connections": max(1, budget.attempts),
            "keepalive": False,
        },
    )

    health_plan = _artifact(
        "health_plan",
        route.route_id,
        {
            "route_id": route.route_id,
            "origin": route.upstream,
            "declared_status": route.health_status.value,
            "probe_interval_ms": max(1000, route.timeout_ms * 2),
            "probe_timeout_q6": budget.connect_budget_q6,
            "failure_threshold": budget.attempts,
            "penalty_weight_q6": budget.health_penalty_q6,
        },
    )

    cache_material = {
        "route": route.normalized_payload(),
        "timeout_q6": budget.timeout_q6,
        "total_wait_q6": budget.total_wait_q6,
        "penalty_q6": budget.health_penalty_q6,
    }
    forward_cache = _artifact(
        "forward_cache",
        route.route_id,
        {
            "route_id": route.route_id,
            "cache_key": sha256_json(cache_material)[:24],
            "upstream": route.upstream,
            "methods": list(route.methods),
            "forwarded_headers": dict(sorted(route.headers.items())),
            "timeout_q6": budget.timeout_q6,
            "retries": route.retries,
        },
    )

    by_kind = {
        "match_entry": match_entry,
        "pool_group": pool_group,
        "health_plan": health_plan,
        "forward_cache": forward_cache,
    }
    return {artifact.key: artifact for artifact in by_kind.values()}


def _aggregate_upstreams(
    plans: Iterable[RoutePlan],
) -> tuple[UpstreamAggregate, ...]:
    grouped: dict[str, list[RoutePlan]] = {}
    for plan in plans:
        grouped.setdefault(plan.route.upstream, []).append(plan)

    aggregates: list[UpstreamAggregate] = []
    for upstream in sorted(grouped):
        group = sorted(grouped[upstream], key=lambda plan: plan.route.route_id)
        wait_values = [plan.budget.total_wait_q6 for plan in group]
        penalty_values = [plan.budget.health_penalty_q6 for plan in group]
        aggregates.append(
            UpstreamAggregate(
                upstream=upstream,
                route_ids=tuple(plan.route.route_id for plan in group),
                total_wait_q6=sum(wait_values),
                health_penalty_q6=sum(penalty_values),
                naive_total_wait=naive_sum(wait_values),
                kahan_total_wait=kahan_sum(wait_values),
                naive_health_penalty=naive_sum(penalty_values),
                kahan_health_penalty=kahan_sum(penalty_values),
            )
        )
    return tuple(aggregates)


def compile_snapshot(
    version: int,
    routes: Iterable[Route],
    digest: str | None = None,
    events: Iterable[dict[str, Any]] = (),
) -> CompiledSnapshot:
    route_list = sorted(routes, key=lambda route: route.route_id)
    digest = digest or compute_snapshot_digest(version, route_list)

    plans: list[RoutePlan] = []
    all_artifacts: list[CompiledArtifact] = []
    for route in route_list:
        budget = derive_budget(
            route.timeout_ms, route.retries, route.health_status
        )
        route_artifacts = _route_artifacts(route, budget)
        all_artifacts.extend(route_artifacts.values())
        plans.append(RoutePlan(route=route, budget=budget, artifacts=route_artifacts))

    reverse_plans = tuple(
        sorted(
            (plan for plan in plans if plan.route.kind is RouteKind.reverse),
            key=lambda plan: (
                -plan.route.priority,
                -len(plan.route.path_prefix or ""),
                plan.route.route_id,
            ),
        )
    )
    forward_plans = tuple(
        sorted(
            (plan for plan in plans if plan.route.kind is RouteKind.forward),
            key=lambda plan: (-plan.route.priority, plan.route.route_id),
        )
    )

    aggregates = _aggregate_upstreams(plans)
    match_index_payload = {
        "version": version,
        "reverse": [
            {
                "route_id": plan.route.route_id,
                "methods": list(plan.route.methods),
                "path_prefix": plan.route.path_prefix,
                "priority": plan.route.priority,
            }
            for plan in reverse_plans
        ],
        "forward": [
            {
                "route_id": plan.route.route_id,
                "methods": list(plan.route.methods),
                "target_host": plan.route.target_host,
                "target_port": plan.route.target_port,
                "priority": plan.route.priority,
            }
            for plan in forward_plans
        ],
    }
    match_artifact = _artifact(
        "match_index", None, match_index_payload, key="match_index"
    )
    all_artifacts.append(match_artifact)

    for aggregate in aggregates:
        upstream_key = hashlib.sha256(aggregate.upstream.encode("utf-8")).hexdigest()[:20]
        all_artifacts.append(
            _artifact(
                "upstream_budget",
                None,
                aggregate.payload(),
                key=f"upstream_budget:{upstream_key}",
            )
        )

    return CompiledSnapshot(
        version=version,
        digest=digest,
        routes=tuple(plans),
        routes_by_id={plan.route.route_id: plan for plan in plans},
        reverse_matchers=reverse_plans,
        forward_matchers=forward_plans,
        artifacts=tuple(all_artifacts),
        artifacts_by_key={artifact.key: artifact for artifact in all_artifacts},
        match_index=match_index_payload,
        upstream_aggregates=aggregates,
        events=tuple(events),
    )


def changed_field_names(old: Route, new: Route) -> list[str]:
    old_payload = old.normalized_payload()
    new_payload = new.normalized_payload()
    fields = sorted(set(old_payload) | set(new_payload))
    return [field for field in fields if old_payload.get(field) != new_payload.get(field)]


def cascade_events(
    old: CompiledSnapshot | None,
    new: CompiledSnapshot,
    *,
    full_snapshot: bool,
) -> list[dict[str, Any]]:
    old_routes = old.routes_by_id if old else {}
    new_routes = new.routes_by_id
    affected: set[str] = set()
    reason_by_route: dict[str, str] = {}

    for route_id in sorted(set(old_routes) | set(new_routes)):
        if full_snapshot:
            affected.add(route_id)
            reason_by_route[route_id] = "full_snapshot_replace"
            continue
        old_plan = old_routes.get(route_id)
        new_plan = new_routes.get(route_id)
        if old_plan is None or new_plan is None:
            affected.add(route_id)
            reason_by_route[route_id] = "route_added" if old_plan is None else "route_removed"
            continue
        changed = changed_field_names(old_plan.route, new_plan.route)
        if changed:
            affected.add(route_id)
            reason_by_route[route_id] = "fields_changed:" + ",".join(changed)

    events: list[dict[str, Any]] = []

    def emit(route_id: str | None, kind: str, action: str, reason: str) -> None:
        events.append(
            {
                "version": new.version,
                "route_id": route_id,
                "artifact_kind": kind,
                "action": action,
                "reason": reason,
            }
        )

    for route_id in sorted(affected):
        reason = reason_by_route[route_id]
        old_plan = old_routes.get(route_id)
        new_plan = new_routes.get(route_id)
        for kind in ROUTE_ARTIFACT_KINDS:
            if old_plan is not None:
                emit(route_id, kind, "INVALIDATED", reason)
            if new_plan is not None:
                emit(route_id, kind, "REBUILT", reason)

    if affected:
        for kind in ("match_index",):
            emit(None, kind, "INVALIDATED", "depends_on_changed_match_entries")
            emit(None, kind, "REBUILT", "rebuilt_from_complete_match_table")

        impacted_upstreams: set[str] = set()
        for route_id in affected:
            if route_id in old_routes:
                impacted_upstreams.add(old_routes[route_id].route.upstream)
            if route_id in new_routes:
                impacted_upstreams.add(new_routes[route_id].route.upstream)
        for upstream in sorted(impacted_upstreams):
            emit(None, "upstream_budget", "INVALIDATED", f"upstream={upstream}")
            emit(None, "upstream_budget", "REBUILT", f"upstream={upstream}")

    return events


def route_plan_payload(plan: RoutePlan) -> dict[str, Any]:
    return {
        "route": plan.route.normalized_payload(),
        "budget": plan.budget.payload(),
        "artifacts": {
            kind: {
                "key": artifact.key,
                "digest": artifact.digest,
                "payload": artifact.payload,
            }
            for kind, artifact in sorted(plan.artifacts.items())
        },
    }


def compiled_state_payload(
    snapshot: CompiledSnapshot,
    *,
    events: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    return {
        "version": snapshot.version,
        "digest": snapshot.digest,
        "route_count": snapshot.route_count,
        "routes": [route_plan_payload(plan) for plan in snapshot.routes],
        "upstream_aggregates": [
            aggregate.payload() for aggregate in snapshot.upstream_aggregates
        ],
        "artifact_summary": {
            kind: sum(1 for artifact in snapshot.artifacts if artifact.kind == kind)
            for kind in ROUTE_ARTIFACT_KINDS + AGGREGATE_ARTIFACT_KINDS
        },
        "match_index": snapshot.match_index,
        "events": [dict(event) for event in events],
    }
