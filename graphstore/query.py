"""查询引擎：多跳邻接遍历、最短路径、模式路径匹配。

所有查询都针对一个不可变的 :class:`~graphstore.view.SnapshotView` 执行，
天然绑定快照版本。快速模式（``fast``）通过节点访问上限与索引预取预算提前
截断，返回被截断的分支数；精确模式（``exact``）不设截断。两种模式共享同一
套展开顺序，保证快速结果是精确结果的严格子集。
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .errors import GraphInconsistentError, WeightInvalidError
from .indexes import adj_dependency, label_dependency
from .view import SnapshotView

DEFAULT_NODE_BUDGET = 8
DEFAULT_PREFETCH_BUDGET = 4


@dataclass(frozen=True, slots=True)
class FastBudget:
    node_budget: int = DEFAULT_NODE_BUDGET
    prefetch_budget: int = DEFAULT_PREFETCH_BUDGET

    @classmethod
    def from_request(cls, params: Optional[dict]):
        params = params or {}
        return cls(
            node_budget=int(params.get("node_budget", DEFAULT_NODE_BUDGET)),
            prefetch_budget=int(params.get("prefetch_budget", DEFAULT_PREFETCH_BUDGET)),
        )


def query_signature(kind: str, params: dict) -> str:
    import hashlib
    import json

    canonical = json.dumps(
        {"kind": kind, "params": params}, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def request_hash(body: dict) -> str:
    import hashlib
    import json

    canonical = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def ensure_positive_weights(view: SnapshotView):
    bad = [
        edge.to_dict()
        for edge in sorted(
            view.snapshot.edges.values(),
            key=lambda edge: (edge.source, edge.type, edge.target),
        )
        if edge.weight <= 0
    ]
    if bad:
        raise WeightInvalidError(
            "图中存在零权重或负权重边，最短路径要求严格正权重",
            {"invalid_edges": bad[:10], "invalid_count": len(bad)},
        )


def _require_node(view: SnapshotView, node_id: str, *, hop: int, operation: str):
    if node_id not in view.snapshot.nodes:
        raise GraphInconsistentError(
            f"{operation} 过程中节点缺失: {node_id}",
            {"missing_node": node_id, "hop": hop, "operation": operation},
        )


def _adjacency(view: SnapshotView, edge_type: str, source: str, *, hop: int,
               operation: str) -> List[Tuple[str, float]]:
    neighbors = view.index.lookup_adjacency(edge_type, source)
    for target, _weight in neighbors:
        if target not in view.snapshot.nodes:
            raise GraphInconsistentError(
                f"{operation} 过程中邻接索引引用的目标节点缺失: {target}",
                {"missing_node": target, "source": source, "edge_type": edge_type,
                 "hop": hop, "operation": operation},
            )
    return neighbors


def multi_hop(view: SnapshotView, params: dict, mode: str,
              budget: FastBudget) -> Dict[str, Any]:
    start = params["start"]
    max_hops = int(params["max_hops"])
    edge_types: Optional[Sequence[str]] = params.get("edge_types")
    _require_node(view, start, hop=0, operation="multi_hop")

    visited = {start}
    frontier = [start]
    truncated_branches = 0
    prefetches = 0
    reached: Dict[str, List[str]] = {"0": [start]}
    used_adj: set[tuple[str, str]] = set()

    for hop in range(1, max_hops + 1):
        next_frontier: List[str] = []
        for source in frontier:
            types = edge_types if edge_types is not None else view.index.outgoing_types(source)
            for edge_type in sorted(types):
                used_adj.add((edge_type, source))
                if mode == "fast":
                    prefetches += 1
                    if prefetches > budget.prefetch_budget:
                        truncated_branches += 1
                        continue
                neighbors = _adjacency(view, edge_type, source, hop=hop - 1,
                                       operation="multi_hop")
                if mode == "fast" and len(visited) >= budget.node_budget:
                    truncated_branches += sum(1 for target, _ in neighbors
                                              if target not in visited)
                    continue
                for target, _weight in neighbors:
                    if target not in visited:
                        if mode == "fast" and len(visited) >= budget.node_budget:
                            truncated_branches += 1
                            continue
                        visited.add(target)
                        next_frontier.append(target)
        reached[str(hop)] = sorted(next_frontier)
        frontier = next_frontier
        if not frontier:
            break

    result = {
        "query": "multi_hop",
        "version": view.version,
        "mode": mode,
        "start": start,
        "max_hops": max_hops,
        "edge_types": list(edge_types) if edge_types else None,
        "nodes_by_hop": reached,
        "nodes": sorted(visited),
        "truncated_branches": truncated_branches,
        "budget": None if mode == "exact" else {
            "node_budget": budget.node_budget,
            "prefetch_budget": budget.prefetch_budget,
            "prefetches_used": prefetches,
        },
    }
    deps = {adj_dependency(edge_type, source) for edge_type, source in used_adj}
    deps.add(label_dependency(view.snapshot.nodes[start].label))
    result["dependencies"] = sorted(deps)
    return result


def shortest_path(view: SnapshotView, params: dict, mode: str,
                  budget: FastBudget) -> Dict[str, Any]:
    source = params["source"]
    target = params["target"]
    edge_types: Optional[Sequence[str]] = params.get("edge_types")
    ensure_positive_weights(view)
    _require_node(view, source, hop=0, operation="shortest_path")
    _require_node(view, target, hop=0, operation="shortest_path")

    distances: Dict[str, float] = {source: 0.0}
    predecessors: Dict[str, Tuple[str, str, float]] = {}
    settled: List[str] = []
    heap: List[Tuple[float, str]] = [(0.0, source)]
    truncated_branches = 0
    used_adj: set[tuple[str, str]] = set()

    while heap:
        current_dist, current = heapq.heappop(heap)
        if current in settled:
            continue
        if mode == "fast" and len(settled) >= budget.node_budget:
            truncated_branches += len(heap) + 1
            break
        settled.append(current)
        if current == target:
            break
        types = edge_types if edge_types is not None else view.index.outgoing_types(current)
        for edge_type in sorted(types):
            used_adj.add((edge_type, current))
            neighbors = _adjacency(view, edge_type, current, hop=len(settled),
                                   operation="shortest_path")
            for neighbor, weight in neighbors:
                if weight <= 0:
                    raise WeightInvalidError(
                        "遍历到零权重或负权重边",
                        {"source": current, "target": neighbor, "edge_type": edge_type,
                         "weight": weight},
                    )
                new_dist = current_dist + weight
                if new_dist < distances.get(neighbor, float("inf")):
                    distances[neighbor] = new_dist
                    predecessors[neighbor] = (current, edge_type, weight)
                    heapq.heappush(heap, (new_dist, neighbor))

    path = _reconstruct_path(source, target, predecessors)
    result = {
        "query": "shortest_path",
        "version": view.version,
        "mode": mode,
        "source": source,
        "target": target,
        "edge_types": list(edge_types) if edge_types else None,
        "found": path is not None,
        "path": path,
        "distance": distances.get(target) if path is not None else None,
        "settled_nodes": sorted(settled),
        "truncated_branches": truncated_branches,
        "budget": None if mode == "exact" else {
            "node_budget": budget.node_budget,
            "prefetch_budget": budget.prefetch_budget,
            "settled_used": len(settled),
        },
    }
    deps = {adj_dependency(edge_type, node) for edge_type, node in used_adj}
    for key in ("source", "target"):
        deps.add(label_dependency(view.snapshot.nodes[params[key]].label))
    result["dependencies"] = sorted(deps)
    return result


def _reconstruct_path(source: str, target: str,
                      predecessors: Dict[str, Tuple[str, str, float]]):
    if source == target:
        return {"nodes": [source], "edges": [], "hops": 0}
    if target not in predecessors:
        return None
    nodes = [target]
    edges = []
    current = target
    while current != source:
        previous, edge_type, weight = predecessors[current]
        edges.append({"source": previous, "target": current,
                      "type": edge_type, "weight": weight})
        nodes.append(previous)
        current = previous
    nodes.reverse()
    edges.reverse()
    return {"nodes": nodes, "edges": edges, "hops": len(edges)}


def pattern_match(view: SnapshotView, params: dict, mode: str,
                  budget: FastBudget) -> Dict[str, Any]:
    start = params["start"]
    pattern: Sequence[dict] = params["pattern"]
    _require_node(view, start, hop=0, operation="pattern_match")

    matched_paths: List[dict] = []
    truncated_branches = 0
    visited_nodes = 0
    used_adj: set[tuple[str, str]] = set()
    used_labels: set[str] = set()

    def dfs(node_id: str, step: int, node_path: List[str],
            edge_path: List[dict]) -> None:
        nonlocal truncated_branches, visited_nodes
        if step == len(pattern):
            matched_paths.append({"nodes": list(node_path), "edges": list(edge_path)})
            return
        spec = pattern[step]
        edge_type = spec["edge_type"]
        target_label = spec.get("target_label")
        used_adj.add((edge_type, node_id))
        neighbors = _adjacency(view, edge_type, node_id, hop=step,
                               operation="pattern_match")
        for target, weight in neighbors:
            target_node = view.snapshot.nodes[target]
            if target_label is not None and target_node.label != target_label:
                continue
            used_labels.add(target_node.label)
            if mode == "fast":
                visited_nodes += 1
                if visited_nodes > budget.node_budget:
                    truncated_branches += 1
                    continue
            node_path.append(target)
            edge_path.append({"source": node_id, "target": target,
                              "type": edge_type, "weight": weight})
            dfs(target, step + 1, node_path, edge_path)
            node_path.pop()
            edge_path.pop()

    dfs(start, 0, [start], [])
    matched_paths.sort(key=lambda item: (item["nodes"],
                                         [(edge["type"], edge["target"]) for edge in item["edges"]]))
    result = {
        "query": "pattern_match",
        "version": view.version,
        "mode": mode,
        "start": start,
        "pattern": list(pattern),
        "matched_paths": matched_paths,
        "path_count": len(matched_paths),
        "truncated_branches": truncated_branches,
        "budget": None if mode == "exact" else {
            "node_budget": budget.node_budget,
            "prefetch_budget": budget.prefetch_budget,
            "node_visits": visited_nodes,
        },
    }
    deps = {adj_dependency(edge_type, node) for edge_type, node in used_adj}
    deps.update(label_dependency(label) for label in used_labels)
    deps.add(label_dependency(view.snapshot.nodes[start].label))
    result["dependencies"] = sorted(deps)
    return result


QUERY_HANDLERS = {
    "multi_hop": (multi_hop, ("start", "max_hops")),
    "shortest_path": (shortest_path, ("source", "target")),
    "pattern_match": (pattern_match, ("start", "pattern")),
}


def execute(view: SnapshotView, kind: str, params: dict, mode: str,
            budget: Optional[FastBudget] = None) -> Dict[str, Any]:
    if kind not in QUERY_HANDLERS:
        raise GraphInconsistentError("未知查询类型", {"query_type": kind})
    handler, required = QUERY_HANDLERS[kind]
    missing = [name for name in required if name not in params]
    if missing:
        raise GraphInconsistentError("查询缺少必填参数", {"missing": missing})
    if mode not in ("fast", "exact"):
        raise GraphInconsistentError("模式必须是 fast 或 exact", {"mode": mode})
    return handler(view, params, mode, budget or FastBudget())
