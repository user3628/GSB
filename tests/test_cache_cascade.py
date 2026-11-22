from graphstore.indexes import adj_dependency
from graphstore.query import query_signature


def test_cache_invalidated_for_affected_adjacency(engine):
    params = {"start": "p1", "max_hops": 2}
    signature = query_signature("multi_hop", params)
    first = engine.query({"query": "multi_hop", "version": 1, "mode": "exact",
                          "params": params})
    assert first["cache"]["hit"] is False
    assert adj_dependency("KNOWS", "p1") in first["cache"]["dependencies"]

    engine.apply_delta({
        "add_edges": [{"source": "p1", "target": "p4", "type": "KNOWS", "weight": 1.0}],
        "generated_at": "2026-11-23T13:00:00+08:00",
    })
    cached = engine.store.cache_get(2, signature)
    assert cached is None
    second = engine.query({"query": "multi_hop", "version": 2, "mode": "exact",
                           "params": params})
    assert second["cache"]["hit"] is False
    assert "p4" in second["nodes_by_hop"]["1"]


def test_unaffected_cache_carried_over(engine):
    params = {"start": "p1", "max_hops": 1}
    engine.query({"query": "multi_hop", "version": 1, "mode": "exact", "params": params})
    signature = query_signature("multi_hop", params)
    engine.apply_delta({
        "add_nodes": [{"id": "x9", "label": "Product", "properties": {}}],
        "generated_at": "2026-11-23T13:00:00+08:00",
    })
    cached = engine.store.cache_get(2, signature)
    assert cached is not None
    hit = engine.query({"query": "multi_hop", "version": 2, "mode": "exact",
                        "params": params})
    assert hit["cache"]["hit"] is True
    assert "x9" not in hit["nodes"]


def test_label_change_invalidates_downstream(engine):
    engine.query({"query": "pattern_match", "version": 1, "mode": "exact",
                  "params": {"start": "p1", "pattern": [
                      {"edge_type": "LIVES_IN", "target_label": "City"}]}})
    signature = query_signature(
        "pattern_match",
        {"start": "p1", "pattern": [
            {"edge_type": "LIVES_IN", "target_label": "City"}]},
    )
    engine.apply_delta({
        "remove_nodes": ["c1"],
        "generated_at": "2026-11-23T13:00:00+08:00",
    })
    assert engine.store.cache_get(2, signature) is None
