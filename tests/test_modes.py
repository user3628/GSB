def test_fast_mode_is_subset_and_reports_truncation(engine):
    params = {"start": "p1", "max_hops": 2}
    fast = engine.query({
        "query": "multi_hop", "version": 1, "mode": "fast",
        "fast_budget": {"node_budget": 5, "prefetch_budget": 3},
        "params": params,
    })
    exact = engine.query({
        "query": "multi_hop", "version": 1, "mode": "exact", "params": params,
    })
    assert set(fast["nodes"]) < set(exact["nodes"])
    assert fast["truncated_branches"] > 0
    assert fast["budget"]["node_budget"] == 5
    assert exact["budget"] is None


def test_audit_itemizes_diff(engine):
    audit = engine.audit_modes({
        "query": "multi_hop", "version": 1,
        "fast_budget": {"node_budget": 5, "prefetch_budget": 3},
        "params": {"start": "p1", "max_hops": 2},
    })
    diff = audit["diff"]
    assert diff["fast_subset_of_exact"] is True
    assert diff["unexpected_in_fast"] == []
    assert diff["missing_in_fast"]
    assert audit["fast"]["version"] == audit["exact"]["version"] == 1


def test_fast_shortest_path_never_invents(engine):
    params = {"source": "p1", "target": "p4"}
    fast = engine.query({
        "query": "shortest_path", "version": 1, "mode": "fast",
        "fast_budget": {"node_budget": 2, "prefetch_budget": 2},
        "params": params,
    })
    exact = engine.query({
        "query": "shortest_path", "version": 1, "mode": "exact", "params": params,
    })
    if fast["found"]:
        fast_nodes = set(fast["path"]["nodes"])
        exact_nodes = set(exact["path"]["nodes"])
        assert fast_nodes <= exact_nodes
    else:
        assert fast["truncated_branches"] > 0


def test_fast_pattern_is_subset(engine):
    params = {"start": "p1", "pattern": [
        {"edge_type": "KNOWS"},
        {"edge_type": "LIVES_IN", "target_label": "City"},
    ]}
    fast = engine.query({
        "query": "pattern_match", "version": 1, "mode": "fast",
        "fast_budget": {"node_budget": 2, "prefetch_budget": 2},
        "params": params,
    })
    exact = engine.query({
        "query": "pattern_match", "version": 1, "mode": "exact", "params": params,
    })
    fast_set = {tuple(p["nodes"]) for p in fast["matched_paths"]}
    exact_set = {tuple(p["nodes"]) for p in exact["matched_paths"]}
    assert fast_set <= exact_set
