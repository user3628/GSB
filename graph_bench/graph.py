"""依赖图算法：拓扑分层、环检测、下游影响、编排批次、权重一致度。

图约定：边 ``父 -> 子``，父资源必须先于子资源编排，因此父层级严格小于子层级。

确定性约定（保证同一版本重复查询逐位一致）：
- 所有标识集合输出前按字典序排序；
- 同层节点按标识排序，批次因此也是有序的；
- 环路径从环内最小标识开始，按边顺序展开；
- 浮点一致度统一四舍五入到 6 位小数。
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any, Callable

import numpy as np

from .models import Resource

CONSISTENCY_DECIMALS = 6


def build_adjacency(resources: list[Resource]) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """返回 ``(children, parents)`` 邻接表，邻接列表均按标识排序。"""

    ids = [item.id for item in resources]
    children: dict[str, list[str]] = {rid: [] for rid in ids}
    parents: dict[str, list[str]] = {rid: [] for rid in ids}
    resource_map = {item.id: item for item in resources}
    for item in resources:
        ordered = sorted(item.parents)
        parents[item.id] = ordered
        for parent in ordered:
            children[parent].append(item.id)
    for rid in children:
        children[rid].sort()
    return children, parents


def compute_layers(
    resources: list[Resource],
) -> tuple[dict[str, int], list[str], list[list[str]]]:
    """Kahn 分层。

    返回 ``(层级映射, 受阻资源, 分层结果)``：
    - 无环时所有资源都有层级，受阻列表为空；
    - 有环时环上节点及其唯一可达下游全部留在受阻列表中，
      其余资源仍得到满足“父层级 < 子层级”的层级结果。
    """

    children, parents = build_adjacency(resources)
    indegree = {rid: len(parents[rid]) for rid in parents}
    ready: deque[str] = deque(sorted(rid for rid, degree in indegree.items() if degree == 0))
    layer_of: dict[str, int] = {}
    layers: list[list[str]] = []

    while ready:
        current_level = sorted(ready)
        ready.clear()
        level_index = len(layers)
        layers.append(current_level)
        next_ready: set[str] = set()
        for rid in current_level:
            layer_of[rid] = level_index
            for child in children[rid]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    next_ready.add(child)
        ready.extend(sorted(next_ready))

    blocked = sorted(rid for rid in indegree if rid not in layer_of)
    return layer_of, blocked, layers


def find_cycles(resources: list[Resource]) -> list[list[str]]:
    """用 Tarjan 强连通分量找环，并展开为有序环路径。

    - 自环返回 ``[rid, rid]``；
    - 多节点 SCC 返回从环内最小标识出发、沿边回到起点的节点序列（含起点收尾）；
    - 环之间按其最小标识排序，输出稳定。
    """

    children, _ = build_adjacency(resources)
    index_counter = 0
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    components: list[list[str]] = []

    def strong_connect(node: str) -> None:
        nonlocal index_counter
        indices[node] = index_counter
        lowlink[node] = index_counter
        index_counter += 1
        stack.append(node)
        on_stack.add(node)

        for child in children[node]:
            if child not in indices:
                strong_connect(child)
                lowlink[node] = min(lowlink[node], lowlink[child])
            elif child in on_stack:
                lowlink[node] = min(lowlink[node], indices[child])

        if lowlink[node] == indices[node]:
            component: list[str] = []
            while True:
                member = stack.pop()
                on_stack.remove(member)
                component.append(member)
                if member == node:
                    break
            components.append(sorted(component))

    for rid in sorted(children):
        if rid not in indices:
            strong_connect(rid)

    cycles: list[list[str]] = []
    for component in components:
        if len(component) == 1:
            sole = component[0]
            if sole in children[sole]:
                cycles.append([sole, sole])
            continue
        member_set = set(component)
        start = min(component)
        path = [start]
        current = start
        visited_edges: set[tuple[str, str]] = set()
        while True:
            next_node = next(child for child in children[current] if child in member_set and (current, child) not in visited_edges)
            visited_edges.add((current, next_node))
            path.append(next_node)
            current = next_node
            if current == start:
                break
        cycles.append(path)

    cycles.sort(key=lambda cycle: cycle[0])
    return cycles


def compute_downstream_impacts(
    resources: list[Resource],
    layer_of: dict[str, int],
) -> dict[str, list[str]]:
    """计算每个资源的下游影响集合（经依赖边可达的全部后代，不含自身）。

    环上资源没有可信层级，不参与计算；其影响集合留空并由受阻清单另行表达。
    """

    children, _ = build_adjacency(resources)
    memo: dict[str, frozenset[str]] = {}

    def descendants(node: str) -> frozenset[str]:
        if node in memo:
            return memo[node]
        memo[node] = frozenset()  # 占位，防止异常环结构导致无限递归
        reached: set[str] = set()
        for child in children[node]:
            if child not in layer_of:
                continue
            reached.add(child)
            reached.update(descendants(child))
        memo[node] = frozenset(reached)
        return memo[node]

    return {rid: sorted(descendants(rid)) for rid in sorted(children) if rid in layer_of}


def compute_weight_consistency(resources: list[Resource]) -> dict[str, float]:
    """跨资源权重一致度。

    把每个资源与其“共享同一父资源的兄弟资源”分组：父资源的直接子节点互为兄弟；
    没有父资源的根节点组成根分组。一致度定义为

        ``1 - 组内权重千分比的总体标准差 / 1000``

    单成员分组没有分歧，一致度为 1.0。结果四舍五入到 6 位小数，
    同一资源属于多个兄弟分组时取最低分（最严格口径），保证结果确定且保守。
    """

    _, parents = build_adjacency(resources)
    groups: dict[str, list[str]] = {"__root__": sorted(rid for rid, plist in parents.items() if not plist)}
    resource_map = {item.id: item for item in resources}
    for parent_id in sorted(parents):
        siblings = sorted(rid for rid, plist in parents.items() if parent_id in plist)
        if siblings:
            groups[parent_id] = siblings

    scores: dict[str, float] = {rid: 1.0 for rid in parents}
    for members in groups.values():
        if len(members) <= 1:
            continue
        weights = np.array([resource_map[rid].weight_milli for rid in members], dtype=np.float64)
        std = float(np.std(weights))  # 总体标准差（ddof=0）
        group_score = round(max(0.0, 1.0 - std / 1000.0), CONSISTENCY_DECIMALS)
        for rid in members:
            scores[rid] = min(scores[rid], group_score)

    return {rid: round(scores[rid], CONSISTENCY_DECIMALS) for rid in sorted(scores)}


def compute_graph_view(resources: list[Resource]) -> dict[str, Any]:
    """一次性计算某快照版本的完整派生视图。"""

    layer_of, blocked, layers = compute_layers(resources)
    cycles = find_cycles(resources) if blocked else []
    impacts = compute_downstream_impacts(resources, layer_of)
    consistency = compute_weight_consistency(resources)
    resource_map = {item.id: item for item in resources}

    resource_views: dict[str, dict[str, Any]] = {}
    for rid in sorted(resource_map):
        if rid in layer_of:
            resource_views[rid] = {
                "resource_id": rid,
                "layer": layer_of[rid],
                "batch": f"batch-{layer_of[rid]}",
                "downstream": impacts.get(rid, []),
                "weight_consistency": consistency[rid],
                "status": "ready",
            }
        else:
            resource_views[rid] = {
                "resource_id": rid,
                "layer": None,
                "batch": None,
                "downstream": [],
                "weight_consistency": consistency[rid],
                "status": "blocked_by_cycle",
            }

    batch_views = [
        {"batch": f"batch-{index}", "layer": index, "resources": sorted(members)}
        for index, members in enumerate(layers)
    ]
    return {
        "is_acyclic": not blocked,
        "cycles": cycles,
        "blocked_resources": blocked,
        "layers": [{"layer": index, "resources": members} for index, members in enumerate(layers)],
        "batches": batch_views,
        "resources": resource_views,
    }


def filter_impact_with_budget(
    downstream: list[str],
    *,
    budget_ms: float | None,
    deadline: float | None,
    sleep_hook: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """在时间预算内逐批产出影响面，超预算时返回部分结果与未完成标识。

    ``sleep_hook`` 仅供测试注入确定性延迟；生产路径不睡眠。
    无论是否超时，输出的节点都来自同一已提交版本，绝不跨版本混合。
    """

    if budget_ms is None or deadline is None:
        return {"completed": True, "items": list(downstream), "remaining": []}

    completed_items: list[str] = []
    for rid in downstream:
        if sleep_hook is not None:
            sleep_hook()
        if time.monotonic() >= deadline:
            remaining = downstream[len(completed_items):]
            return {
                "completed": False,
                "items": completed_items,
                "remaining": remaining,
                "budget_ms": budget_ms,
            }
        completed_items.append(rid)
    return {"completed": True, "items": completed_items, "remaining": []}
