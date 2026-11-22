from graphstore.indexes import adj_dependency, label_dependency


def test_label_inverted_index(engine):
    view = engine.registry.acquire(1)
    try:
        assert view.index.lookup_label("Person") == ["p1", "p2", "p3", "p4"]
        assert view.index.lookup_label("City") == ["c1", "c2"]
        assert view.index.lookup_label("Unknown") == []
    finally:
        engine.registry.release(view)


def test_adjacency_index(engine):
    view = engine.registry.acquire(1)
    try:
        assert view.index.lookup_adjacency("KNOWS", "p1") == [("p2", 1.0), ("p3", 3.0)]
        assert view.index.lookup_adjacency("LIVES_IN", "p1") == [("c1", 1.0)]
        assert view.index.lookup_adjacency("KNOWS", "c1") == []
    finally:
        engine.registry.release(view)


def test_incremental_index_only_touches_affected_keys(engine):
    before = engine.registry.acquire(1)
    result = engine.apply_delta({
        "add_nodes": [{"id": "p6", "label": "Person", "properties": {}}],
        "add_edges": [{"source": "p6", "target": "p1", "type": "KNOWS", "weight": 1.0}],
        "generated_at": "2026-11-23T13:00:00+08:00",
    })
    engine.registry.release(before)
    assert result["affected_dependencies"] == sorted([
        label_dependency("Person"),
        adj_dependency("KNOWS", "p6"),
    ])
    view = engine.registry.acquire(2)
    try:
        assert view.index.lookup_label("Person") == ["p1", "p2", "p3", "p4", "p6"]
        assert view.index.lookup_adjacency("KNOWS", "p6") == [("p1", 1.0)]
    finally:
        engine.registry.release(view)


def test_delete_cleans_empty_entries(engine):
    engine.apply_delta({
        "remove_edges": [{"source": "p3", "target": "x2", "type": "PURCHASED"}],
        "generated_at": "2026-11-23T13:00:00+08:00",
    })
    view = engine.registry.acquire(2)
    try:
        assert ("PURCHASED", "p3") not in view.index.adj_index
        assert view.index.lookup_adjacency("PURCHASED", "p3") == []
    finally:
        engine.registry.release(view)


def test_delete_node_cascades_edges_and_entries(engine):
    engine.apply_delta({
        "remove_nodes": ["x2"],
        "generated_at": "2026-11-23T13:00:00+08:00",
    })
    view = engine.registry.acquire(2)
    try:
        assert "x2" not in view.snapshot.nodes
        assert all(edge.target != "x2" and edge.source != "x2"
                   for edge in view.snapshot.edges.values())
        assert ("PURCHASED", "p3") not in view.index.adj_index
        assert ("PURCHASED", "p4") not in view.index.adj_index
        assert view.index.lookup_label("Product") == ["x1"]
        assert engine.verify_integrity()["consistent"]
    finally:
        engine.registry.release(view)
