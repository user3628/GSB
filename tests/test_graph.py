from __future__ import annotations

from graph_bench.graph import (
    compute_graph_view,
    compute_layers,
    compute_weight_consistency,
    filter_impact_with_budget,
    find_cycles,
)
from graph_bench.models import Resource


def make_resource(rid: str, parents: list[str], weight: int = 500) -> Resource:
    return Resource(id=rid, replicas=1, parents=tuple(parents), weight_milli=weight, desired_version="v1")


def test_layers_respect_strict_parent_ordering() -> None:
    resources = [
        make_resource("a", []),
        make_resource("b", ["a"]),
        make_resource("c", ["a"]),
        make_resource("d", ["b", "c"]),
    ]
    layer_of, blocked, layers = compute_layers(resources)
    assert blocked == []
    assert layer_of == {"a": 0, "b": 1, "c": 1, "d": 2}
    assert layers == [["a"], ["b", "c"], ["d"]]
    for resource in resources:
        for parent in resource.parents:
            assert layer_of[parent] < layer_of[resource.id]


def test_cycle_returns_path_and_partial_layers() -> None:
    resources = [
        make_resource("root", []),
        make_resource("x", ["root", "z"]),
        make_resource("y", ["x"]),
        make_resource("z", ["y"]),
        make_resource("orphan", []),
    ]
    layer_of, blocked, layers = compute_layers(resources)
    assert set(blocked) == {"x", "y", "z"}
    assert layer_of == {"root": 0, "orphan": 0}
    cycles = find_cycles(resources)
    assert cycles == [["x", "y", "z", "x"]]
    view = compute_graph_view(resources)
    assert view["is_acyclic"] is False
    assert view["layers"] == [{"layer": 0, "resources": ["orphan", "root"]}]
    assert view["resources"]["x"]["status"] == "blocked_by_cycle"
    assert view["resources"]["root"]["status"] == "ready"


def test_self_loop_is_detected() -> None:
    resources = [make_resource("solo", ["solo"])]
    assert find_cycles(resources) == [["solo", "solo"]]
    _, blocked, _ = compute_layers(resources)
    assert blocked == ["solo"]


def test_downstream_impact_is_sorted_and_transitive() -> None:
    resources = [
        make_resource("a", []),
        make_resource("b", ["a"]),
        make_resource("c", ["a"]),
        make_resource("d", ["b"]),
    ]
    layer_of, _, _ = compute_layers(resources)
    view = compute_graph_view(resources)
    assert view["resources"]["a"]["downstream"] == ["b", "c", "d"]
    assert view["resources"]["b"]["downstream"] == ["d"]
    assert view["resources"]["d"]["downstream"] == []


def test_weight_consistency_groups_and_determinism() -> None:
    resources = [
        make_resource("root", [], weight=1000),
        make_resource("x", ["root"], weight=800),
        make_resource("y", ["root"], weight=600),
        make_resource("z", ["root"], weight=600),
    ]
    first = compute_weight_consistency(resources)
    second = compute_weight_consistency(resources)
    assert first == second
    # 组 [600, 800, 600] 的总体标准差约为 94.2809，一致度约 0.905719。
    assert first["root"] == 1.0
    assert first["x"] == first["y"] == first["z"]
    assert 0.90 < first["x"] < 0.91


def test_budget_filter_marks_incomplete_without_dropping_version_info() -> None:
    downstream = [f"r{index}" for index in range(10)]
    import time

    deadline = time.monotonic() + 0.03
    result = filter_impact_with_budget(
        downstream,
        budget_ms=30.0,
        deadline=deadline,
        sleep_hook=lambda: time.sleep(0.01),
    )
    assert result["completed"] is False
    assert result["remaining"]
    assert result["items"] + result["remaining"] == downstream
