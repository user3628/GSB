"""Artifact catalog, dependency propagation and materialized values."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from .models import (
    BUDGET_FIELDS,
    PRICE_FIELDS,
    RESOURCE_FIELDS,
    SERVICE_KEYS,
    SLO_FIELDS,
    CapacitySnapshot,
)


RESOURCE_SPEC = "RESOURCE_SPEC"
PRICE = "PRICE"
BUDGET_POLICY = "BUDGET_POLICY"
SLO_WEIGHT = "SLO_WEIGHT"
SERVICE_COST = "SERVICE_COST"
SERVICE_SHARE = "SERVICE_SHARE"
SERVICE_BUDGET_REMAINING = "SERVICE_BUDGET_REMAINING"
SERVICE_BUDGET_VERDICT = "SERVICE_BUDGET_VERDICT"
TOTAL_COST = "TOTAL_COST"
TOTAL_BUDGET_REMAINING = "TOTAL_BUDGET_REMAINING"
WEIGHT_DENOMINATOR = "WEIGHT_DENOMINATOR"
WEIGHTED_COST_INDEX = "WEIGHTED_COST_INDEX"
AGGREGATE_BUDGET_VERDICT = "AGGREGATE_BUDGET_VERDICT"


def service_artifact(service: str, suffix: str) -> str:
    return f"svc:{service}:{suffix}"


def build_catalog(service_names: list[str]) -> dict[str, dict[str, Any]]:
    names = sorted(service_names)
    catalog: dict[str, dict[str, Any]] = {}

    def add(
        artifact_id: str,
        kind: str,
        service_name: str | None,
        dependencies: list[str],
    ) -> None:
        catalog[artifact_id] = {
            "artifact_id": artifact_id,
            "artifact_kind": kind,
            "service_name": service_name,
            "dependencies": sorted(dependencies),
        }

    for name in names:
        add(service_artifact(name, "resource_spec"), RESOURCE_SPEC, name, [])
        add(service_artifact(name, "price"), PRICE, name, [])
        add(service_artifact(name, "budget_policy"), BUDGET_POLICY, name, [])
        add(service_artifact(name, "slo_weight"), SLO_WEIGHT, name, [])
        add(
            service_artifact(name, "cost"),
            SERVICE_COST,
            name,
            [
                service_artifact(name, "resource_spec"),
                service_artifact(name, "price"),
            ],
        )

    all_costs = [service_artifact(name, "cost") for name in names]
    all_shares = [service_artifact(name, "share") for name in names]
    all_remaining = [service_artifact(name, "budget_remaining") for name in names]
    all_slo = [service_artifact(name, "slo_weight") for name in names]
    all_policies = [service_artifact(name, "budget_policy") for name in names]

    add("agg:total_cost", TOTAL_COST, None, all_costs)
    add(
        "agg:total_budget_remaining",
        TOTAL_BUDGET_REMAINING,
        None,
        ["agg:total_cost"],
    )
    add("agg:weight_denominator", WEIGHT_DENOMINATOR, None, all_slo)
    add(
        "agg:weighted_cost_index",
        WEIGHTED_COST_INDEX,
        None,
        all_costs + ["agg:weight_denominator"],
    )
    for name in names:
        add(
            service_artifact(name, "share"),
            SERVICE_SHARE,
            name,
            [service_artifact(name, "cost"), "agg:total_cost"],
        )
        add(
            service_artifact(name, "budget_remaining"),
            SERVICE_BUDGET_REMAINING,
            name,
            [
                service_artifact(name, "cost"),
                service_artifact(name, "budget_policy"),
            ],
        )
        add(
            service_artifact(name, "budget_verdict"),
            SERVICE_BUDGET_VERDICT,
            name,
            [
                service_artifact(name, "cost"),
                service_artifact(name, "share"),
                service_artifact(name, "budget_remaining"),
                service_artifact(name, "budget_policy"),
            ],
        )
    add(
        "agg:budget_verdict",
        AGGREGATE_BUDGET_VERDICT,
        None,
        all_shares + all_remaining + all_policies + ["agg:total_cost", "agg:total_budget_remaining"],
    )
    return catalog


def direct_changes(
    changed: dict[str, tuple[str, ...]],
) -> dict[str, dict[str, Any]]:
    direct: dict[str, dict[str, Any]] = {}
    for name, fields in sorted(changed.items()):
        field_set = set(fields)
        mapping = [
            (RESOURCE_FIELDS, "resource_spec"),
            (PRICE_FIELDS, "price"),
            (BUDGET_FIELDS, "budget_policy"),
            (SLO_FIELDS, "slo_weight"),
        ]
        for field_group, suffix in mapping:
            hit = sorted(field_set & field_group)
            if hit:
                artifact_id = service_artifact(name, suffix)
                direct[artifact_id] = {
                    "reason": "DIRECT_FIELD_CHANGE",
                    "cause": f"{name}:{','.join(hit)}",
                }
    unknown = sorted(set().union(*(set(fields) for fields in changed.values())) - SERVICE_KEYS)
    if unknown:
        raise ValueError(f"unclassified changed fields: {unknown}")
    return direct


def propagate_invalidations(
    catalog: dict[str, dict[str, Any]],
    direct: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    reverse: dict[str, list[str]] = defaultdict(list)
    for artifact_id, record in catalog.items():
        for dependency in record["dependencies"]:
            reverse[dependency].append(artifact_id)
    for values in reverse.values():
        values.sort()

    queue: deque[tuple[str, int]] = deque()
    seen: dict[str, int] = {}
    for artifact_id in sorted(direct):
        if artifact_id not in catalog:
            raise ValueError(f"unknown artifact {artifact_id}")
        seen[artifact_id] = 0
        queue.append((artifact_id, 0))

    while queue:
        artifact_id, wave = queue.popleft()
        for child in reverse.get(artifact_id, []):
            child_wave = wave + 1
            if child not in seen or child_wave < seen[child]:
                seen[child] = child_wave
                queue.append((child, child_wave))

    events: list[dict[str, Any]] = []
    for artifact_id, wave in sorted(seen.items(), key=lambda item: (item[1], item[0])):
        record = catalog[artifact_id]
        source = direct.get(artifact_id)
        if source is None:
            dependency_names = sorted(
                dependency for dependency in record["dependencies"] if dependency in seen
            )
            source = {
                "reason": "DOWNSTREAM_DEPENDENCY",
                "cause": ",".join(dependency_names),
            }
        events.append(
            {
                "artifact_id": artifact_id,
                "artifact_kind": record["artifact_kind"],
                "service_name": record["service_name"],
                "wave": wave,
                "reason": source["reason"],
                "cause": source["cause"],
            }
        )
    return events


def all_artifacts_full(
    catalog: dict[str, dict[str, Any]], reason: str
) -> list[dict[str, Any]]:
    return [
        {
            "artifact_id": artifact_id,
            "artifact_kind": record["artifact_kind"],
            "service_name": record["service_name"],
            "wave": 0,
            "reason": reason,
            "cause": reason,
        }
        for artifact_id, record in sorted(catalog.items())
    ]


def build_artifact_values(
    snapshot: CapacitySnapshot, metrics: dict[str, Any]
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for name, service in sorted(snapshot.services.items()):
        service_metric = metrics["services"][name]
        values[service_artifact(name, "resource_spec")] = {
            "replicas": service.replicas,
            "cores_per_replica": service.cores_per_replica,
            "memory_gb_per_replica": service.memory_gb_per_replica,
            "min_cores": service.min_cores,
            "max_cores": service.max_cores,
            "min_memory_gb": service.min_memory_gb,
            "max_memory_gb": service.max_memory_gb,
        }
        values[service_artifact(name, "price")] = {
            "price_core_cents_per_core_hour": service.price_core_cents_per_core_hour,
            "price_memory_cents_per_gb_hour": service.price_memory_cents_per_gb_hour,
        }
        values[service_artifact(name, "budget_policy")] = {
            "budget_cents_per_hour": service.budget_cents_per_hour,
            "max_cost_share_ppm": service.max_cost_share_ppm,
        }
        values[service_artifact(name, "slo_weight")] = {
            "slo_target_ppm": service.slo_target_ppm
        }
        values[service_artifact(name, "cost")] = {
            "cost_cents_per_hour": service_metric["cost_cents_per_hour"],
            "cost_cents_per_hour_fixed": service_metric["cost_cents_per_hour_fixed"],
        }
        values[service_artifact(name, "share")] = {
            "share_ppm": service_metric["share_ppm"],
            "share_fixed": service_metric["share_fixed"],
            "share_exact_fraction": service_metric["share_exact_fraction"],
        }
        values[service_artifact(name, "budget_remaining")] = {
            "budget_remaining_cents_per_hour": service_metric[
                "budget_remaining_cents_per_hour"
            ],
            "budget_remaining_cents_per_hour_fixed": service_metric[
                "budget_remaining_cents_per_hour_fixed"
            ],
        }
        service_violations = [
            violation
            for violation in metrics["verdict"]["violations"]
            if violation.get("service") == name
        ]
        values[service_artifact(name, "budget_verdict")] = {
            "status": "VIOLATION" if service_violations else "PASS",
            "violations": service_violations,
        }

    aggregate = metrics["aggregate"]
    values["agg:total_cost"] = {
        "total_cost_cents_per_hour": aggregate["total_cost_cents_per_hour"],
        "total_cost_cents_per_hour_fixed": aggregate["total_cost_cents_per_hour_fixed"],
    }
    values["agg:total_budget_remaining"] = {
        "total_budget_remaining_cents_per_hour": aggregate[
            "total_budget_remaining_cents_per_hour"
        ],
        "total_budget_remaining_cents_per_hour_fixed": aggregate[
            "total_budget_remaining_cents_per_hour_fixed"
        ],
    }
    values["agg:weight_denominator"] = {
        "weight_denominator_ppm": aggregate["weight_denominator_ppm"]
    }
    values["agg:weighted_cost_index"] = {
        "weighted_cost_index_micro_cents": aggregate[
            "weighted_cost_index_micro_cents"
        ],
        "weighted_cost_index_fixed_cents": aggregate[
            "weighted_cost_index_fixed_cents"
        ],
        "weighted_cost_index_exact_fraction": aggregate[
            "weighted_cost_index_exact_fraction"
        ],
    }
    values["agg:budget_verdict"] = metrics["verdict"]
    return values


def rebuild_scope(
    *,
    mode: str,
    reason: str,
    catalog: dict[str, dict[str, Any]],
    events: list[dict[str, Any]],
    changed: dict[str, tuple[str, ...]] | None,
) -> dict[str, Any]:
    rebuilt = sorted(event["artifact_id"] for event in events)
    rebuilt_set = set(rebuilt)
    reused = sorted(set(catalog) - rebuilt_set)
    waves: dict[int, list[str]] = defaultdict(list)
    for event in events:
        waves[event["wave"]].append(event["artifact_id"])
    direct_services = []
    if changed is not None:
        for name, fields in sorted(changed.items()):
            direct_services.append({"service": name, "changed_fields": list(fields)})
    return {
        "mode": mode,
        "reason": reason,
        "direct_services": direct_services,
        "waves": [
            {"wave": wave, "artifacts": sorted(artifacts)}
            for wave, artifacts in sorted(waves.items())
        ],
        "invalidated_artifacts": rebuilt,
        "rebuilt_artifacts": rebuilt,
        "reused_artifacts": reused,
        "invalidated_count": len(rebuilt),
        "rebuilt_count": len(rebuilt),
        "reused_count": len(reused),
        "artifact_count": len(catalog),
        "full_rebuild": mode == "full",
        "decision_basis": "artifact_dependency_graph_only",
    }


def weighted_denominator_changed(changed: dict[str, tuple[str, ...]]) -> bool:
    return any(set(fields) & SLO_FIELDS for fields in changed.values())

