"""基于 NumPy 的只读图统计（不参与查询正确性，供上层分析与演示使用）。"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from .snapshot import GraphSnapshot


def degree_stats(snapshot: "GraphSnapshot") -> dict:
    node_ids = sorted(snapshot.nodes)
    index = {node_id: i for i, node_id in enumerate(node_ids)}
    out_degree = np.zeros(len(node_ids), dtype=np.int64)
    in_degree = np.zeros(len(node_ids), dtype=np.int64)
    weights = []
    for edge in snapshot.edges.values():
        out_degree[index[edge.source]] += 1
        in_degree[index[edge.target]] += 1
        weights.append(edge.weight)
    weight_array = np.asarray(weights, dtype=np.float64)
    return {
        "node_count": int(len(node_ids)),
        "edge_count": int(len(snapshot.edges)),
        "out_degree": {
            "min": int(out_degree.min()) if len(node_ids) else 0,
            "max": int(out_degree.max()) if len(node_ids) else 0,
            "mean": float(out_degree.mean()) if len(node_ids) else 0.0,
        },
        "in_degree": {
            "min": int(in_degree.min()) if len(node_ids) else 0,
            "max": int(in_degree.max()) if len(node_ids) else 0,
            "mean": float(in_degree.mean()) if len(node_ids) else 0.0,
        },
        "weight": {
            "min": float(weight_array.min()) if weights else None,
            "max": float(weight_array.max()) if weights else None,
            "mean": float(weight_array.mean()) if weights else None,
        },
    }
