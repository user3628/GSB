"""影响分析与按依赖拓扑排序的分发计划生成。"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import DependencyCycleError
from .models import ComponentDeclaration


@dataclass(frozen=True)
class Plan:
    version: int
    affected_keys: tuple[str, ...]
    affected_components: tuple[str, ...]
    ordered_components: tuple[str, ...]

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "affected_keys": list(self.affected_keys),
            "affected_components": list(self.affected_components),
            "ordered_components": list(self.ordered_components),
        }


def detect_cycle(declarations: dict[str, ComponentDeclaration]) -> list[str]:
    """以确定性 DFS 检测组件依赖环，返回环上涉及的组件（无环时为空）。"""

    WHITE, GRAY, BLACK = 0, 1, 2
    color = {component_id: WHITE for component_id in sorted(declarations)}
    stack: list[str] = []
    cycle: list[str] = []

    def visit(node: str) -> bool:
        color[node] = GRAY
        stack.append(node)
        for neighbor in sorted(declarations[node].depends_on_components):
            if color[neighbor] == GRAY:
                start = stack.index(neighbor)
                cycle.extend(sorted(set(stack[start:] + [neighbor])))
                return True
            if color[neighbor] == WHITE and visit(neighbor):
                return True
        stack.pop()
        color[node] = BLACK
        return False

    for component_id in sorted(declarations):
        if color[component_id] == WHITE and visit(component_id):
            return cycle
    return []


def topological_order(declarations: dict[str, ComponentDeclaration]) -> list[str]:
    """Kahn 拓扑排序：被依赖者先于依赖者，同层按组件标识字典序，保证可重复。"""

    cycle = detect_cycle(declarations)
    if cycle:
        raise DependencyCycleError(
            "组件依赖存在环", details={"components_in_cycle": cycle}
        )
    indegree = {component_id: 0 for component_id in declarations}
    dependents: dict[str, list[str]] = {component_id: [] for component_id in declarations}
    for component_id, declaration in declarations.items():
        for dependency in declaration.depends_on_components:
            indegree[component_id] += 1
            dependents[dependency].append(component_id)
    ready = sorted(component_id for component_id, degree in indegree.items() if degree == 0)
    order: list[str] = []
    while ready:
        node = ready.pop(0)
        order.append(node)
        for dependent in sorted(dependents[node]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)
                ready.sort()
    return order


def compute_affected(
    new_items: dict,
    previous_items: dict | None,
    declarations: dict[str, ComponentDeclaration],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """计算配置键变化集合与受影响组件（含下游连带组件，做依赖闭包）。"""

    previous_items = previous_items or {}
    changed_keys = {
        key
        for key in set(new_items) | set(previous_items)
        if new_items.get(key) != previous_items.get(key)
    }
    affected: set[str] = set()
    for component_id, declaration in declarations.items():
        if changed_keys.intersection(declaration.depends_on_keys):
            affected.add(component_id)

    # 下游闭包：任何（传递）依赖了受影响组件的组件都要连带重新分发。
    changed = True
    while changed:
        changed = False
        for component_id, declaration in declarations.items():
            if component_id in affected:
                continue
            if set(declaration.depends_on_components).intersection(affected):
                affected.add(component_id)
                changed = True
    return tuple(sorted(changed_keys)), tuple(sorted(affected))


def build_plan(
    version: int,
    new_items: dict,
    previous_items: dict | None,
    declarations: dict[str, ComponentDeclaration],
) -> Plan:
    """生成与快照版本绑定的计划；相同输入恒产生相同顺序。"""

    cycle = detect_cycle(declarations)
    if cycle:
        raise DependencyCycleError(
            "组件依赖存在环", details={"components_in_cycle": cycle}
        )
    affected_keys, affected_components = compute_affected(
        new_items, previous_items, declarations
    )
    full_order = topological_order(declarations)
    ordered = tuple(component_id for component_id in full_order if component_id in set(affected_components))
    return Plan(
        version=version,
        affected_keys=affected_keys,
        affected_components=affected_components,
        ordered_components=ordered,
    )
