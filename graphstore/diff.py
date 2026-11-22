"""快速/精确双模式结果的逐项差异核对。"""

from __future__ import annotations


def _as_node_set(result: dict) -> set:
    if result["query"] == "multi_hop":
        return set(result["nodes"])
    if result["query"] == "shortest_path":
        path = result.get("path")
        return set(path["nodes"]) if path else set()
    return {node for path in result["matched_paths"] for node in path["nodes"]}


def _diff_results(kind: str, fast: dict, exact: dict) -> dict:
    fast_nodes = _as_node_set(fast)
    exact_nodes = _as_node_set(exact)
    return {
        "fast_subset_of_exact": fast_nodes <= exact_nodes,
        "missing_in_fast": sorted(exact_nodes - fast_nodes),
        "unexpected_in_fast": sorted(fast_nodes - exact_nodes),
        "fast_truncated_branches": fast["truncated_branches"],
        "exact_truncated_branches": exact["truncated_branches"],
        "bounded_bias": {
            "max_missing_nodes": len(exact_nodes - fast_nodes),
            "truncated": fast["truncated_branches"] > 0,
        },
    }
