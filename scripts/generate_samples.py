"""生成确定性的本地固定样本（快照、异常快照、查询请求）。

运行：``python -m scripts.generate_samples``。所有输出内容固定，不依赖网络。
"""

from __future__ import annotations

import json
from pathlib import Path

from graphstore.snapshot import CHECKSUM_ALGORITHM, compute_checksum
from graphstore.models import Edge, Node

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "samples"


def write_json(path: Path, payload: dict):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8")


def snapshot_payload(version: int, generated_at: str, nodes, edges,
                     *, corrupt: bool = False):
    checksum = compute_checksum(nodes, edges)
    if corrupt:
        checksum = "0" * len(checksum)
    return {
        "version": version,
        "generated_at": generated_at,
        "nodes": [node.to_dict() for node in nodes],
        "edges": [edge.to_dict() for edge in edges],
        "checksum": {"algorithm": CHECKSUM_ALGORITHM, "value": checksum},
    }


def base_nodes():
    return [
        Node("p1", "Person", {"name": "Alice", "age": 30}),
        Node("p2", "Person", {"name": "Bob", "age": 25}),
        Node("p3", "Person", {"name": "Carol", "age": 41}),
        Node("p4", "Person", {"name": "Dan", "age": 35}),
        Node("c1", "City", {"name": "Shanghai"}),
        Node("c2", "City", {"name": "Hangzhou"}),
        Node("o1", "Company", {"name": "Acme"}),
        Node("o2", "Company", {"name": "Globex"}),
        Node("x1", "Product", {"name": "Widget", "price": 99.5}),
        Node("x2", "Product", {"name": "Gadget", "price": 19.0}),
    ]


def base_edges():
    rows = [
        ("p1", "p2", "KNOWS", 1.0),
        ("p2", "p1", "KNOWS", 1.0),
        ("p2", "p3", "KNOWS", 2.0),
        ("p3", "p4", "KNOWS", 1.5),
        ("p1", "p3", "KNOWS", 3.0),
        ("p1", "c1", "LIVES_IN", 1.0),
        ("p2", "c2", "LIVES_IN", 1.0),
        ("p3", "c1", "LIVES_IN", 1.0),
        ("p4", "c2", "LIVES_IN", 1.0),
        ("p1", "o1", "WORKS_FOR", 1.0),
        ("p2", "o1", "WORKS_FOR", 1.0),
        ("p3", "o2", "WORKS_FOR", 1.0),
        ("p4", "o2", "WORKS_FOR", 1.0),
        ("o1", "c1", "LOCATED_IN", 1.0),
        ("o2", "c2", "LOCATED_IN", 1.0),
        ("p1", "x1", "PURCHASED", 2.0),
        ("p2", "x1", "PURCHASED", 2.0),
        ("p3", "x2", "PURCHASED", 2.5),
        ("p4", "x2", "PURCHASED", 2.5),
    ]
    return [Edge(source, target, edge_type, weight)
            for source, target, edge_type, weight in rows]


def main():
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    nodes = base_nodes()
    edges = base_edges()

    write_json(
        SAMPLE_DIR / "snapshot_v1.json",
        snapshot_payload(1, "2026-11-23T09:00:00+08:00", nodes, edges),
    )

    v2_nodes = nodes + [
        Node("p5", "Person", {"name": "Eve", "age": 28}),
        Node("c3", "City", {"name": "Suzhou"}),
    ]
    v2_edges = edges + [
        Edge("p5", "p1", "KNOWS", 1.0),
        Edge("p5", "c3", "LIVES_IN", 1.0),
        Edge("p5", "o1", "WORKS_FOR", 1.0),
        Edge("p5", "x1", "PURCHASED", 2.0),
    ]
    write_json(
        SAMPLE_DIR / "snapshot_v2.json",
        snapshot_payload(2, "2026-11-23T10:00:00+08:00", v2_nodes, v2_edges),
    )

    write_json(
        SAMPLE_DIR / "snapshot_bad_checksum.json",
        snapshot_payload(3, "2026-11-23T11:00:00+08:00", nodes, edges, corrupt=True),
    )

    bad_weight_edges = [
        edge if edge.source != "p1" or edge.target != "p2"
        else Edge("p1", "p2", "KNOWS", 0.0)
        for edge in edges
    ]
    write_json(
        SAMPLE_DIR / "snapshot_bad_weight.json",
        snapshot_payload(1, "2026-11-23T12:00:00+08:00", nodes, bad_weight_edges),
    )

    requests = {
        "requests": [
            {
                "request_id": "q-multihop-1",
                "query": "multi_hop",
                "version": 1,
                "mode": "exact",
                "params": {"start": "p1", "max_hops": 2},
            },
            {
                "request_id": "q-multihop-fast-1",
                "query": "multi_hop",
                "version": 1,
                "mode": "fast",
                "fast_budget": {"node_budget": 6, "prefetch_budget": 4},
                "params": {"start": "p1", "max_hops": 2},
            },
            {
                "request_id": "q-shortest-1",
                "query": "shortest_path",
                "version": 1,
                "mode": "exact",
                "params": {"source": "p1", "target": "p4"},
            },
            {
                "request_id": "q-pattern-1",
                "query": "pattern_match",
                "version": 1,
                "mode": "exact",
                "params": {
                    "start": "p1",
                    "pattern": [
                        {"edge_type": "KNOWS"},
                        {"edge_type": "LIVES_IN", "target_label": "City"},
                    ],
                },
            },
            {
                "request_id": "q-pattern-fast-1",
                "query": "pattern_match",
                "version": 1,
                "mode": "fast",
                "fast_budget": {"node_budget": 3, "prefetch_budget": 3},
                "params": {
                    "start": "p1",
                    "pattern": [
                        {"edge_type": "KNOWS"},
                        {"edge_type": "LIVES_IN", "target_label": "City"},
                    ],
                },
            },
        ]
    }
    write_json(SAMPLE_DIR / "query_requests.json", requests)
    print(f"样本已写入 {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
