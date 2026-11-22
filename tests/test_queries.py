import pytest

from graphstore.errors import GraphInconsistentError, WeightInvalidError
from graphstore.snapshot import GraphSnapshot, compute_checksum
from graphstore.indexes import IndexBundle
from graphstore.view import SnapshotView
from graphstore.models import Edge, Node


def test_multi_hop_sorted_dedup(engine):
    result = engine.query({
        "query": "multi_hop", "version": 1, "mode": "exact",
        "params": {"start": "p1", "max_hops": 2},
    })
    assert result["nodes"] == sorted(result["nodes"])
    assert len(result["nodes"]) == len(set(result["nodes"]))
    assert result["nodes_by_hop"]["0"] == ["p1"]
    assert result["nodes_by_hop"]["1"] == ["c1", "o1", "p2", "p3", "x1"]
    assert "p4" in result["nodes_by_hop"]["2"]
    assert result["cache"]["hit"] is False


def test_multi_hop_cache_hit(engine):
    body = {"query": "multi_hop", "version": 1, "mode": "exact",
            "params": {"start": "p1", "max_hops": 2}}
    first = engine.query(dict(body))
    second = engine.query(dict(body))
    assert first["cache"]["hit"] is False
    assert second["cache"]["hit"] is True
    assert first["nodes"] == second["nodes"]


def test_shortest_path_deterministic(engine):
    result = engine.query({
        "query": "shortest_path", "version": 1, "mode": "exact",
        "params": {"source": "p1", "target": "p4"},
    })
    assert result["found"] is True
    assert result["path"]["nodes"][0] == "p1"
    assert result["path"]["nodes"][-1] == "p4"
    assert result["distance"] == pytest.approx(4.5)


def test_shortest_path_zero_weight(tmp_path):
    from graphstore.engine import GraphEngine

    eng = GraphEngine(str(tmp_path / "w.db"))
    eng.start()
    nodes = [Node("a", "L"), Node("b", "L")]
    edges = [Edge("a", "b", "REL", 0.0)]
    checksum = compute_checksum(nodes, edges)
    GraphSnapshot.build(1, "t", nodes, edges, checksum)
    payload = {
        "version": 1, "generated_at": "t",
        "nodes": [n.to_dict() for n in nodes],
        "edges": [e.to_dict() for e in edges],
        "checksum": {"algorithm": "sha256", "value": checksum},
    }
    import json
    path = tmp_path / "s.json"
    path.write_text(json.dumps(payload))
    eng.load_snapshot(str(path))
    with pytest.raises(WeightInvalidError) as exc:
        eng.query({"query": "shortest_path", "version": 1, "mode": "exact",
                   "params": {"source": "a", "target": "b"}})
    assert exc.value.code == "E_WEIGHT_INVALID"
    eng.close()


def test_negative_weight_rejected(engine):
    engine.apply_delta({
        "add_edges": [{"source": "p1", "target": "p4", "type": "KNOWS", "weight": -1.0}],
        "generated_at": "2026-11-23T13:00:00+08:00",
    })
    with pytest.raises(WeightInvalidError) as exc:
        engine.query({"query": "shortest_path", "version": 2, "mode": "exact",
                      "params": {"source": "p1", "target": "p4"}})
    assert exc.value.code == "E_WEIGHT_INVALID"
    assert exc.value.context["invalid_edges"]


def test_missing_node_detected(engine):
    with pytest.raises(GraphInconsistentError) as exc:
        engine.query({"query": "multi_hop", "version": 1, "mode": "exact",
                      "params": {"start": "ghost", "max_hops": 1}})
    assert exc.value.code == "E_GRAPH_INCONSISTENT"
    assert exc.value.context["missing_node"] == "ghost"


def test_inconsistent_adjacency_detected():
    nodes = [Node("a", "L"), Node("b", "L")]
    edges = [Edge("a", "b", "REL", 1.0)]
    snapshot = GraphSnapshot.build(1, "t", nodes, edges)
    index = IndexBundle.from_snapshot(snapshot)
    index.adj_index[("REL", "a")]["ghost"] = 1.0
    view = SnapshotView(snapshot, index)
    from graphstore.query import FastBudget, multi_hop

    with pytest.raises(GraphInconsistentError) as exc:
        multi_hop(view, {"start": "a", "max_hops": 1}, "exact", FastBudget())
    assert exc.value.context["missing_node"] == "ghost"


def test_pattern_match(engine):
    result = engine.query({
        "query": "pattern_match", "version": 1, "mode": "exact",
        "params": {"start": "p1", "pattern": [
            {"edge_type": "KNOWS"},
            {"edge_type": "LIVES_IN", "target_label": "City"},
        ]},
    })
    paths = {tuple(path["nodes"]) for path in result["matched_paths"]}
    assert ("p1", "p2", "c2") in paths
    assert ("p1", "p3", "c1") in paths
    assert all(all(edge["type"] in ("KNOWS", "LIVES_IN") for edge in path["edges"])
               for path in result["matched_paths"])
