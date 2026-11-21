"""Topological layering, deterministic batch ordering and impact analysis."""
from __future__ import annotations

import numpy as np

from app.errors import ErrorCode, WorkbenchError
from app.models import LayerBatch, NormalizedResource


class GraphResult:
    def __init__(
        self,
        resources: list[NormalizedResource],
        layers: list[list[str]],
        batches: list[LayerBatch],
        edge_count: int,
        order: list[str],
    ):
        self.resources = resources
        self.layers = layers
        self.batches = batches
        self.edge_count = edge_count
        self.order = order


def build_graph(resources: list[NormalizedResource]) -> GraphResult:
    ids = [resource.resource_id for resource in resources]
    id_set = set(ids)
    by_id = {resource.resource_id: resource for resource in resources}

    for resource in resources:
        missing = [parent for parent in resource.parents if parent not in id_set]
        if missing:
            raise WorkbenchError(
                ErrorCode.E_SNAPSHOT_INVALID,
                f"resource {resource.resource_id} references unknown parents {missing}",
                {"resource_id": resource.resource_id, "missing": missing},
            )

    indegree = {resource_id: 0 for resource_id in ids}
    children: dict[str, list[str]] = {resource_id: [] for resource_id in ids}
    edge_count = 0
    for resource in resources:
        for parent in resource.parents:
            indegree[resource.resource_id] += 1
            children[parent].append(resource.resource_id)
            edge_count += 1

    layer_of: dict[str, int] = {}
    remaining = dict(indegree)
    layers: list[list[str]] = []
    order: list[str] = []

    while remaining:
        ready = [resource_id for resource_id, degree in remaining.items() if degree == 0]
        if not ready:
            cycle = _extract_cycle(remaining, children)
            raise WorkbenchError(
                ErrorCode.E_DEPENDENCY_CYCLE,
                "dependency graph contains a cycle",
                {"cycle": cycle},
            )
        ready.sort(key=lambda rid: (-by_id[rid].weight_permille, rid))
        layer_index = len(layers)
        for resource_id in ready:
            layer_of[resource_id] = layer_index
            order.append(resource_id)
            del remaining[resource_id]
        layers.append(ready)
        for resource_id in ready:
            for child in children[resource_id]:
                if child in remaining:
                    remaining[child] -= 1

    batches: list[LayerBatch] = []
    for layer_index, member_ids in enumerate(layers):
        ordered = sorted(member_ids, key=lambda rid: (-by_id[rid].weight_permille, rid))
        batches.append(LayerBatch(layer=layer_index, batch=layer_index, resources=ordered))
        for resource_id in ordered:
            by_id[resource_id].layer = layer_index
            by_id[resource_id].batch = layer_index

    return GraphResult(list(by_id.values()), layers, batches, edge_count, order)


def _extract_cycle(remaining: dict[str, int], children: dict[str, list[str]]) -> list[str]:
    """Return one concrete cycle from the nodes still in the graph."""
    node = next(iter(remaining))
    stack: list[str] = []
    on_stack: set[str] = set()
    visited: set[str] = set()
    index = {name: i for i, name in enumerate(remaining)}

    def dfs(current: str) -> list[str] | None:
        stack.append(current)
        on_stack.add(current)
        visited.add(current)
        for child in sorted(children[current], key=lambda name: index.get(name, 0)):
            if child not in remaining:
                continue
            if child not in visited:
                found = dfs(child)
                if found is not None:
                    return found
            elif child in on_stack:
                start = stack.index(child)
                return stack[start:] + [child]
        stack.pop()
        on_stack.remove(current)
        return None

    cycle = dfs(node)
    return cycle or [node, node]


def downstream_closure(resources: list[NormalizedResource], roots: list[str]) -> set[str]:
    """Transitive downstream set (descendants, excluding the roots themselves)."""
    ids = [resource.resource_id for resource in resources]
    index = {resource_id: i for i, resource_id in enumerate(ids)}
    size = len(ids)
    adjacency = np.zeros((size, size), dtype=np.uint8)
    for resource in resources:
        child_index = index[resource.resource_id]
        for parent in resource.parents:
            adjacency[index[parent], child_index] = 1

    reachable = np.zeros(size, dtype=np.uint8)
    frontier = np.zeros(size, dtype=np.uint8)
    for root in roots:
        if root in index:
            frontier[index[root]] = 1
    for _ in range(size):
        next_frontier = (frontier @ adjacency).astype(np.uint8)
        next_frontier[reachable.astype(bool)] = 0
        if not next_frontier.any():
            break
        reachable[next_frontier.astype(bool)] = 1
        frontier = next_frontier
    return {ids[i] for i, flag in enumerate(reachable) if flag}
