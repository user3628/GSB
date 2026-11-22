from __future__ import annotations

import heapq
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Iterable

from .errors import BusinessError
from .models import Application


@dataclass(frozen=True)
class CascadeResult:
    trigger_app_ids: tuple[str, ...]
    ordered_app_ids: tuple[str, ...]
    affected_app_ids: tuple[str, ...]
    affected_cluster_ids: tuple[str, ...]


class DependencyGraph:
    """Directed graph where ``app -> dependency``.

    Reverse edges therefore point from an upstream dependency to every direct
    downstream dependent. Cascade rollbacks traverse those reverse edges.
    """

    def __init__(self, applications: Iterable[Application], cluster_ids: Iterable[str]) -> None:
        self.apps = {app.app_id: app for app in applications}
        self.cluster_ids = frozenset(cluster_ids)
        self.dependencies: dict[str, set[str]] = {
            app_id: set(app.dependencies) for app_id, app in self.apps.items()
        }
        self.dependents: dict[str, set[str]] = defaultdict(set)
        for app_id, dependencies in self.dependencies.items():
            for dependency in dependencies:
                self.dependents[dependency].add(app_id)
        self._app_ids = frozenset(self.apps)

    def clusters_for(self, app_ids: Iterable[str]) -> list[str]:
        return sorted({self.apps[app_id].cluster_id for app_id in app_ids})

    def downstream_reachable(self, trigger_app_ids: Iterable[str]) -> list[str]:
        roots = sorted(set(trigger_app_ids))
        missing = [app_id for app_id in roots if app_id not in self.apps]
        if missing:
            raise BusinessError(
                "E_NOT_FOUND", "one or more rollback applications do not exist",
                {"missing_app_ids": missing},
            )
        reachable: set[str] = set(roots)
        queue: deque[str] = deque(roots)
        while queue:
            dependency = queue.popleft()
            for dependent in sorted(self.dependents.get(dependency, ())):
                if dependent not in reachable:
                    reachable.add(dependent)
                    queue.append(dependent)
        return sorted(reachable)

    def cycle_components(self, nodes: Iterable[str]) -> list[list[str]]:
        """Return cyclic strongly connected components using iterative Kosaraju."""

        node_set = set(nodes)
        visited: set[str] = set()
        finish_order: list[str] = []
        for start in sorted(node_set):
            if start in visited:
                continue
            visited.add(start)
            stack: list[tuple[str, object]] = [
                (start, iter(sorted(self.dependents.get(start, set()) & node_set)))
            ]
            while stack:
                node, iterator = stack[-1]
                try:
                    next_node = next(iterator)  # type: ignore[arg-type]
                except StopIteration:
                    finish_order.append(node)
                    stack.pop()
                    continue
                if next_node not in visited:
                    visited.add(next_node)
                    stack.append(
                        (
                            next_node,
                            iter(sorted(self.dependents.get(next_node, set()) & node_set)),
                        )
                    )

        assigned: set[str] = set()
        components: list[list[str]] = []
        for start in reversed(finish_order):
            if start in assigned:
                continue
            component: list[str] = []
            stack = [start]
            assigned.add(start)
            while stack:
                node = stack.pop()
                component.append(node)
                for predecessor in sorted(self.dependencies.get(node, set()) & node_set):
                    if predecessor not in assigned:
                        assigned.add(predecessor)
                        stack.append(predecessor)
            component.sort()
            if len(component) > 1:
                components.append(component)
            elif component[0] in self.dependencies.get(component[0], set()):
                components.append(component)
        components.sort(key=lambda item: item[0])
        return components

    def _cycle_error(self, trigger_ids: list[str], reachable: list[str]) -> BusinessError:
        components = self.cycle_components(reachable)
        cycle_nodes = sorted({node for component in components for node in component})
        outside = sorted(set(reachable) - set(cycle_nodes))
        return BusinessError(
            "E_DEPENDENCY_CYCLE",
            "dependency cycle detected while computing rollback cascade",
            {
                "trigger_app_ids": trigger_ids,
                "cycle_app_ids": cycle_nodes,
                "cycle_components": components,
                "affected_app_ids": reachable,
                "outside_cycle_app_ids": outside,
                "partial_rollback_order": outside,
                "affected_cluster_ids": self.clusters_for(reachable),
            },
        )

    def rollback_cascade(self, trigger_app_ids: Iterable[str]) -> CascadeResult:
        triggers = sorted(set(trigger_app_ids))
        reachable = self.downstream_reachable(triggers)
        if self.cycle_components(reachable):
            raise self._cycle_error(triggers, reachable)

        # Root edges are trigger -> dependent. Kahn levels give the longest
        # distance from a trigger; farther downstream apps roll back first.
        indegree = {node: 0 for node in reachable}
        for node in reachable:
            for dependent in self.dependents.get(node, set()) & set(reachable):
                indegree[dependent] += 1
        depth = {node: 0 for node in reachable}
        ready = [node for node in reachable if indegree[node] == 0]
        heapq.heapify(ready)
        visited: list[str] = []
        while ready:
            node = heapq.heappop(ready)
            visited.append(node)
            for dependent in sorted(self.dependents.get(node, set()) & set(reachable)):
                depth[dependent] = max(depth[dependent], depth[node] + 1)
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    heapq.heappush(ready, dependent)
        if len(visited) != len(reachable):  # Defensive; cycle was checked above.
            raise self._cycle_error(triggers, reachable)
        ordered = sorted(reachable, key=lambda app_id: (-depth[app_id], app_id))
        return CascadeResult(
            trigger_app_ids=tuple(triggers),
            ordered_app_ids=tuple(ordered),
            affected_app_ids=tuple(reachable),
            affected_cluster_ids=tuple(self.clusters_for(reachable)),
        )

    def dependency_levels(self, app_ids: Iterable[str]) -> dict[str, int]:
        """Assign dependency levels; a dependency always has a lower level."""

        actionable = set(app_ids)
        indegree = {app_id: 0 for app_id in actionable}
        for app_id in actionable:
            indegree[app_id] = len(self.dependencies[app_id] & actionable)
        ready = [app_id for app_id, degree in indegree.items() if degree == 0]
        heapq.heapify(ready)
        levels: dict[str, int] = {}
        while ready:
            app_id = heapq.heappop(ready)
            levels[app_id] = max(
                (levels[dependency] + 1 for dependency in self.dependencies[app_id] & actionable),
                default=0,
            )
            for dependent in sorted(self.dependents.get(app_id, set()) & actionable):
                indegree[dependent] -= 1
                if indegree[dependent] == 0:
                    heapq.heappush(ready, dependent)
        if len(levels) != len(actionable):
            remaining = sorted(actionable - set(levels))
            components = self.cycle_components(remaining)
            cycle_nodes = sorted({node for component in components for node in component})
            raise BusinessError(
                "E_DEPENDENCY_CYCLE",
                "dependency cycle prevents convergence batch construction",
                {
                    "cycle_app_ids": cycle_nodes,
                    "cycle_components": components,
                    "affected_app_ids": sorted(actionable),
                    "outside_cycle_app_ids": sorted(actionable - set(cycle_nodes)),
                    "affected_cluster_ids": self.clusters_for(actionable),
                },
            )
        return levels

    def downstream_closure(self, trigger_app_ids: Iterable[str]) -> list[str]:
        return self.downstream_reachable(trigger_app_ids)
