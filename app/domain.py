from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np

from app.errors import DomainError


SEVERITY_RANK = {"critical": 0, "high": 1, "medium": 2, "low": 3}
SCORE_WEIGHTS = np.array([0.40, 0.20, 0.20, 0.20], dtype=float)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def unit_fingerprint(unit: dict[str, Any]) -> str:
    return content_hash(
        {
            "image_digest": unit["image_digest"],
            "config": unit["config"],
            "dependencies": sorted(unit["dependencies"]),
            "expected_version": unit["expected_version"],
        }
    )


def state_hash(environment: dict[str, Any]) -> str:
    units = environment["units"]
    return content_hash(
        {
            "environment_version": environment["version"],
            "version_lag_tolerance": environment["version_lag_tolerance"],
            "is_baseline": environment["is_baseline"],
            "units": {
                name: {
                    "image_digest": unit["image_digest"],
                    "config": unit["config"],
                    "dependencies": sorted(unit["dependencies"]),
                    "expected_version": unit["expected_version"],
                }
                for name, unit in sorted(units.items())
            },
        }
    )


def validate_dependency_references(environment: dict[str, Any]) -> None:
    units = environment["units"]
    missing: list[dict[str, str]] = []
    for service in sorted(units):
        for dependency in sorted(units[service]["dependencies"]):
            if dependency not in units:
                missing.append(
                    {
                        "environment": environment["name"],
                        "service": service,
                        "missing_dependency": dependency,
                    }
                )
    if missing:
        raise DomainError(
            "E_SPEC_INVALID",
            "dependency references must exist inside the same environment",
            status_code=400,
            details={"missing_dependencies": missing},
        )


@dataclass(frozen=True)
class Topology:
    layers: list[list[str]]
    blocked_units: list[str]
    cycles: list[list[str]]

    @property
    def complete(self) -> bool:
        return not self.blocked_units


def _find_cycles(units: dict[str, dict[str, Any]], candidates: set[str]) -> list[list[str]]:
    """Tarjan SCCs restricted to the blocked part of the graph."""
    index = 0
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    cycles: list[list[str]] = []

    def visit(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)

        for dependency in sorted(units[node]["dependencies"]):
            if dependency not in candidates:
                continue
            if dependency not in indices:
                visit(dependency)
                lowlinks[node] = min(lowlinks[node], lowlinks[dependency])
            elif dependency in on_stack:
                lowlinks[node] = min(lowlinks[node], indices[dependency])

        if lowlinks[node] == indices[node]:
            component: list[str] = []
            while True:
                member = stack.pop()
                on_stack.remove(member)
                component.append(member)
                if member == node:
                    break
            component.sort()
            self_loop = len(component) == 1 and component[0] in units[
                component[0]
            ]["dependencies"]
            if len(component) > 1 or self_loop:
                cycles.append(component)

    for node in sorted(candidates):
        if node not in indices:
            visit(node)
    return sorted(cycles)


def compute_topology(units: dict[str, dict[str, Any]]) -> Topology:
    """Kahn layering that keeps the uncontaminated prefix when cycles exist."""
    dependency_sets = {
        name: set(unit["dependencies"]) for name, unit in units.items()
    }
    dependents: dict[str, set[str]] = {name: set() for name in units}
    for name, dependencies in dependency_sets.items():
        for dependency in dependencies:
            dependents[dependency].add(name)

    remaining = {name: len(deps) for name, deps in dependency_sets.items()}
    ready = sorted(name for name, count in remaining.items() if count == 0)
    layers: list[list[str]] = []
    processed: set[str] = set()

    while ready:
        layer = sorted(ready)
        ready = []
        layers.append(layer)
        for name in layer:
            processed.add(name)
        for name in layer:
            for dependent in sorted(dependents[name]):
                remaining[dependent] -= 1
                if remaining[dependent] == 0:
                    ready.append(dependent)
        ready.sort()

    blocked = sorted(set(units) - processed)
    cycles = _find_cycles(units, set(blocked)) if blocked else []
    return Topology(layers=layers, blocked_units=blocked, cycles=cycles)


def cycle_error(environment: str, topology: Topology) -> DomainError:
    return DomainError(
        "E_DEPENDENCY_CYCLE",
        "dependency cycle prevents complete topology layering",
        details={
            "environment": environment,
            "cycle": topology.cycles[0] if topology.cycles else [],
            "cycles": topology.cycles,
            "partial_layers": topology.layers,
            "blocked_units": topology.blocked_units,
        },
    )


def assert_acyclic(environment: dict[str, Any]) -> Topology:
    topology = compute_topology(environment["units"])
    if not topology.complete:
        raise cycle_error(environment["name"], topology)
    return topology


def reverse_dependency_closure(
    units: dict[str, dict[str, Any]], seeds: Iterable[str]
) -> list[str]:
    dependents: dict[str, set[str]] = {name: set() for name in units}
    for name, unit in units.items():
        for dependency in unit["dependencies"]:
            dependents.setdefault(dependency, set()).add(name)

    affected: set[str] = set()
    queue = sorted(seed for seed in seeds if seed in units)
    while queue:
        name = queue.pop(0)
        if name in affected:
            continue
        affected.add(name)
        queue.extend(sorted(dependents.get(name, set()) - affected))
    return sorted(affected)


def transitive_dependencies(
    units: dict[str, dict[str, Any]], service: str
) -> list[str]:
    found: set[str] = set()
    queue = list(units[service]["dependencies"])
    while queue:
        dependency = queue.pop(0)
        if dependency in found:
            continue
        found.add(dependency)
        queue.extend(units[dependency]["dependencies"])
    return sorted(found)


def _similarity(left: Iterable[str], right: Iterable[str]) -> float:
    a, b = set(left), set(right)
    if not a and not b:
        return 1.0
    return len(a & b) / len(a | b)


def compare_units_for_score(
    left: dict[str, Any] | None, right: dict[str, Any] | None
) -> tuple[float, dict[str, float]]:
    if left is None or right is None:
        return 0.0, {
            "image": 0.0,
            "expected_version": 0.0,
            "config": 0.0,
            "dependencies": 0.0,
        }

    config_keys = set(left["config"]) | set(right["config"])
    if config_keys:
        config_similarity = sum(
            1
            for key in config_keys
            if key in left["config"]
            and key in right["config"]
            and left["config"][key] == right["config"][key]
        ) / len(config_keys)
    else:
        config_similarity = 1.0

    components = {
        "image": 1.0 if left["image_digest"] == right["image_digest"] else 0.0,
        "expected_version": (
            1.0 if left["expected_version"] == right["expected_version"] else 0.0
        ),
        "config": config_similarity,
        "dependencies": _similarity(
            left["dependencies"], right["dependencies"]
        ),
    }
    values = np.array(
        [
            components["image"],
            components["expected_version"],
            components["config"],
            components["dependencies"],
        ],
        dtype=float,
    )
    return round(float(np.dot(SCORE_WEIGHTS, values) * 100.0), 2), components


def consistency_score(
    baseline_units: dict[str, dict[str, Any]],
    units: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    services = sorted(set(baseline_units) | set(units))
    per_service: dict[str, float] = {}
    component_totals = {
        "image": 0.0,
        "expected_version": 0.0,
        "config": 0.0,
        "dependencies": 0.0,
    }
    for service in services:
        score, components = compare_units_for_score(
            baseline_units.get(service), units.get(service)
        )
        per_service[service] = score
        for key, value in components.items():
            component_totals[key] += value
    divisor = max(1, len(services))
    return {
        "score": round(sum(per_service.values()) / divisor, 2),
        "services_compared": len(services),
        "service_scores": per_service,
        "component_similarity": {
            key: round(value / divisor, 4)
            for key, value in component_totals.items()
        },
    }


def build_revision_artifacts(
    environments: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    baseline_name = next(
        name for name, environment in environments.items() if environment["is_baseline"]
    )
    baseline_units = environments[baseline_name]["units"]
    baseline_version = environments[baseline_name]["version"]

    artifacts: list[dict[str, Any]] = []
    references: list[dict[str, Any]] = []
    topologies: dict[str, Topology] = {}
    scores: dict[str, dict[str, Any]] = {}
    plans: dict[str, list[dict[str, Any]]] = {}

    for environment_name in sorted(environments):
        environment = environments[environment_name]
        topology = assert_acyclic(environment)
        topologies[environment_name] = topology

        order = [service for layer in topology.layers for service in layer]
        plan_items: list[dict[str, Any]] = []
        for position, service in enumerate(order, start=1):
            unit = environment["units"][service]
            plan_item = {
                "environment": environment_name,
                "service": service,
                "order": position,
                "layer": next(
                    index for index, layer in enumerate(topology.layers) if service in layer
                ),
                "action": "deploy",
                "image_digest": unit["image_digest"],
                "config": unit["config"],
                "dependencies": sorted(unit["dependencies"]),
                "dependency_fingerprints": {
                    dependency: unit_fingerprint(environment["units"][dependency])
                    for dependency in transitive_dependencies(
                        environment["units"], service
                    )
                },
                "expected_version": unit["expected_version"],
                "unit_hash": unit_fingerprint(unit),
            }
            plan_items.append(plan_item)
            artifacts.append(
                _artifact(environment_name, "deployment_plan", f"service:{service}", plan_item)
            )
        plans[environment_name] = plan_items

        topology_payload = {
            "environment": environment_name,
            "layers": topology.layers,
            "blocked_units": topology.blocked_units,
            "cycles": topology.cycles,
            "complete": topology.complete,
        }
        artifacts.append(
            _artifact(environment_name, "topology", "environment", topology_payload)
        )

        expected_state = {
            "environment": environment_name,
            "environment_version": environment["version"],
            "is_baseline": environment["is_baseline"],
            "version_lag_tolerance": environment["version_lag_tolerance"],
            "state_hash": state_hash(environment),
            "service_count": len(environment["units"]),
            "config_key_count": sum(
                len(unit["config"]) for unit in environment["units"].values()
            ),
            "services": sorted(environment["units"]),
        }
        artifacts.append(
            _artifact(environment_name, "expected_state", "environment", expected_state)
        )

    for environment_name in sorted(environments):
        environment = environments[environment_name]
        score_data = consistency_score(baseline_units, environment["units"])
        lag = baseline_version - environment["version"]
        behind_by = max(0, lag)
        score_payload = {
            "environment": environment_name,
            "baseline_environment": baseline_name,
            "environment_version": environment["version"],
            "baseline_version": baseline_version,
            "version_lag": lag,
            "behind_by": behind_by,
            "version_lag_tolerance": environment["version_lag_tolerance"],
            "lag_exceeded": behind_by > environment["version_lag_tolerance"],
            **score_data,
        }
        if environment_name == baseline_name:
            score_payload["score"] = 100.0
        scores[environment_name] = score_payload
        artifacts.append(
            _artifact(environment_name, "consistency_score", "environment", score_payload)
        )

    for environment_name in sorted(environments):
        environment = environments[environment_name]
        for service in sorted(environment["units"]):
            artifact_keys = [
                ("deployment_plan", f"service:{service}"),
                ("topology", "environment"),
                ("expected_state", "environment"),
                ("consistency_score", "environment"),
            ]
            for artifact_type, artifact_key in artifact_keys:
                references.append(
                    _reference(
                        environment_name,
                        "unit",
                        service,
                        artifact_type,
                        artifact_key,
                    )
                )
            for config_key in sorted(environment["units"][service]["config"]):
                for artifact_type, artifact_key in artifact_keys:
                    references.append(
                        _reference(
                            environment_name,
                            "config_key",
                            f"{service}:{config_key}",
                            artifact_type,
                            artifact_key,
                        )
                    )

    return {
        "baseline": baseline_name,
        "artifacts": artifacts,
        "references": references,
        "topologies": topologies,
        "scores": scores,
        "plans": plans,
    }


def _artifact(
    environment: str, artifact_type: str, artifact_key: str, payload: dict[str, Any]
) -> dict[str, Any]:
    return {
        "environment": environment,
        "artifact_type": artifact_type,
        "artifact_key": artifact_key,
        "payload": payload,
        "content_hash": content_hash(payload),
    }


def _reference(
    environment: str,
    subject_type: str,
    subject_key: str,
    artifact_type: str,
    artifact_key: str,
) -> dict[str, str]:
    return {
        "environment": environment,
        "subject_type": subject_type,
        "subject_key": subject_key,
        "artifact_type": artifact_type,
        "artifact_key": artifact_key,
    }


def build_diagnostics(
    environments: dict[str, dict[str, Any]],
    scores: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    baseline_name = next(
        name for name, environment in environments.items() if environment["is_baseline"]
    )
    baseline = environments[baseline_name]
    baseline_units = baseline["units"]
    items: list[dict[str, Any]] = []
    lag_markers: list[dict[str, Any]] = []

    for environment_name in sorted(environments):
        if environment_name == baseline_name:
            continue
        environment = environments[environment_name]
        units = environment["units"]
        for service in sorted(set(baseline_units) | set(units)):
            left = baseline_units.get(service)
            right = units.get(service)
            if left is None or right is None:
                items.append(
                    _diagnostic(
                        "critical",
                        environment_name,
                        service,
                        "SERVICE_MISSING",
                        baseline_name,
                        left is not None,
                        right is not None,
                        left,
                        right,
                    )
                )
                continue
            if left["image_digest"] != right["image_digest"]:
                items.append(
                    _diagnostic(
                        "critical",
                        environment_name,
                        service,
                        "IMAGE_DIGEST_MISMATCH",
                        baseline_name,
                        True,
                        True,
                        left["image_digest"],
                        right["image_digest"],
                    )
                )
            if left["expected_version"] != right["expected_version"]:
                items.append(
                    _diagnostic(
                        "high",
                        environment_name,
                        service,
                        "EXPECTED_VERSION_MISMATCH",
                        baseline_name,
                        True,
                        True,
                        left["expected_version"],
                        right["expected_version"],
                    )
                )
            if set(left["dependencies"]) != set(right["dependencies"]):
                items.append(
                    _diagnostic(
                        "high",
                        environment_name,
                        service,
                        "DEPENDENCY_MISMATCH",
                        baseline_name,
                        True,
                        True,
                        sorted(left["dependencies"]),
                        sorted(right["dependencies"]),
                    )
                )
            for config_key in sorted(set(left["config"]) | set(right["config"])):
                in_left = config_key in left["config"]
                in_right = config_key in right["config"]
                left_value = left["config"].get(config_key)
                right_value = right["config"].get(config_key)
                if in_left != in_right or left_value != right_value:
                    items.append(
                        _diagnostic(
                            "medium",
                            environment_name,
                            service,
                            "CONFIG_KEY_MISMATCH",
                            baseline_name,
                            in_left,
                            in_right,
                            left_value,
                            right_value,
                            config_key=config_key,
                        )
                    )

        score = scores[environment_name]
        marker = {
            "code": "ENVIRONMENT_LAG",
            "severity": "high",
            "environment": environment_name,
            "baseline_environment": baseline_name,
            "environment_version": environment["version"],
            "baseline_version": baseline["version"],
            "behind_by": score["behind_by"],
            "tolerance": environment["version_lag_tolerance"],
            "values": {
                "baseline": baseline["version"],
                "environment": environment["version"],
            },
        }
        if score["lag_exceeded"]:
            lag_markers.append(marker)
            items.append(
                {
                    "severity": "high",
                    "environment": environment_name,
                    "baseline_environment": baseline_name,
                    "service": None,
                    "diff_type": "ENVIRONMENT_LAG",
                    "config_key": None,
                    "left": {
                        "environment": baseline_name,
                        "value": baseline["version"],
                        "tolerance": None,
                    },
                    "right": {
                        "environment": environment_name,
                        "value": environment["version"],
                        "tolerance": environment["version_lag_tolerance"],
                    },
                    "details": {
                        "behind_by": score["behind_by"],
                        "tolerance": environment["version_lag_tolerance"],
                    },
                }
            )

    items.sort(
        key=lambda item: (
            SEVERITY_RANK[item["severity"]],
            item["environment"],
            item["service"] or "",
            item["diff_type"],
            item["config_key"] or "",
        )
    )
    return {
        "baseline_environment": baseline_name,
        "baseline_version": baseline["version"],
        "items": items,
        "environment_lag_markers": lag_markers,
        "counts": {
            severity: sum(1 for item in items if item["severity"] == severity)
            for severity in SEVERITY_RANK
        },
        "scores": scores,
    }


def _diagnostic(
    severity: str,
    environment: str,
    service: str,
    diff_type: str,
    baseline_environment: str,
    left_present: bool,
    right_present: bool,
    left_value: Any,
    right_value: Any,
    *,
    config_key: str | None = None,
) -> dict[str, Any]:
    return {
        "severity": severity,
        "environment": environment,
        "baseline_environment": baseline_environment,
        "service": service,
        "diff_type": diff_type,
        "config_key": config_key,
        "left": {
            "environment": baseline_environment,
            "value": left_value,
            "present": left_present,
        },
        "right": {
            "environment": environment,
            "value": right_value,
            "present": right_present,
        },
    }
