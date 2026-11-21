"""Component dependency graph analysis and deterministic batching."""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from .errors import DomainError, E_DEPENDENCY_CYCLE
from .models import ComponentDeclaration


def _component_map(components: Iterable[ComponentDeclaration]) -> dict[str, ComponentDeclaration]:
    return {component.component_id: component for component in components}


def find_dependency_cycle(components: Iterable[ComponentDeclaration]) -> list[str] | None:
    """Return a concrete cycle path (first node repeated), or ``None``."""

    component_map = _component_map(components)
    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(node: str) -> list[str] | None:
        if node in visiting:
            start = stack.index(node)
            return stack[start:] + [node]
        if node in visited:
            return None
        visiting.add(node)
        stack.append(node)
        for dependency in sorted(component_map[node].depends_on):
            if dependency in component_map:
                cycle = visit(dependency)
                if cycle:
                    return cycle
        stack.pop()
        visiting.remove(node)
        visited.add(node)
        return None

    for component_id in sorted(component_map):
        cycle = visit(component_id)
        if cycle:
            return cycle
    return None


def raise_for_cycle(components: Iterable[ComponentDeclaration]) -> None:
    cycle = find_dependency_cycle(components)
    if cycle:
        raise DomainError(
            E_DEPENDENCY_CYCLE,
            "component dependency graph contains a cycle",
            details={"cycle": cycle, "cycle_ids": sorted(set(cycle))},
        )


def topological_batches(
    components: Iterable[ComponentDeclaration], subset: Iterable[str] | None = None
) -> list[list[str]]:
    """Return deterministic dependency-first batches for an optional subset.

    Edges to components outside ``subset`` are already satisfied for the
    purpose of this plan.  A cycle in the complete graph is still reported.
    """

    component_map = _component_map(components)
    raise_for_cycle(component_map.values())
    selected = set(component_map) if subset is None else set(subset)
    unknown = selected - set(component_map)
    if unknown:
        raise DomainError(
            "E_UNKNOWN_COMPONENT",
            "topology subset contains unknown components",
            details={"unknown_components": sorted(unknown)},
            http_status=422,
        )

    completed: set[str] = set()
    remaining = set(selected)
    batches: list[list[str]] = []
    while remaining:
        ready = sorted(
            component_id
            for component_id in remaining
            if all(
                dependency in completed or dependency not in selected
                for dependency in component_map[component_id].depends_on
            )
        )
        if not ready:
            raise DomainError(
                E_DEPENDENCY_CYCLE,
                "component dependency graph contains a cycle",
                details={"cycle_ids": sorted(remaining)},
            )
        batches.append(ready)
        completed.update(ready)
        remaining.difference_update(ready)
    return batches


def full_batch_indices(components: Iterable[ComponentDeclaration]) -> dict[str, int]:
    batches = topological_batches(components)
    return {component_id: index for index, batch in enumerate(batches) for component_id in batch}


def affected_components(
    components: Iterable[ComponentDeclaration],
    changed_config_keys: Iterable[str],
    changed_component_ids: Iterable[str],
) -> list[str]:
    """Compute direct and transitive downstream impact.

    A component is directly affected by a changed config key it references or
    by a changed declaration.  Dependants are then added in reverse graph
    order regardless of their own configuration references.
    """

    component_map = _component_map(components)
    changed_keys = set(changed_config_keys)
    affected = {
        component.component_id
        for component in components
        if component.component_id in set(changed_component_ids)
        or changed_keys.intersection(component.config_keys)
    }
    dependants: dict[str, set[str]] = defaultdict(set)
    for component in components:
        for dependency in component.depends_on:
            dependants[dependency].add(component.component_id)

    queue = list(affected)
    while queue:
        current = queue.pop(0)
        for dependant in sorted(dependants.get(current, set())):
            if dependant not in affected:
                affected.add(dependant)
                queue.append(dependant)
    return sorted(affected)

