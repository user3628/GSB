import pytest

from config_dispatch.errors import DependencyCycleError
from config_dispatch.models import ComponentDeclaration
from config_dispatch.planner import build_plan, detect_cycle, topological_order


def make_declarations(spec):
    result = {}
    for component_id, keys, deps in spec:
        result[component_id] = ComponentDeclaration(
            component_id=component_id,
            depends_on_keys=tuple(keys),
            expected_version=1,
            endpoint=f"http://127.0.0.1:19000",
            depends_on_components=tuple(deps),
        )
    return result


def test_topological_order_respects_dependencies():
    declarations = make_declarations(
        [
            ("gateway", ["f"], ["order"]),
            ("order", ["f"], ["inventory"]),
            ("inventory", ["f"], ["queue"]),
            ("queue", ["q"], []),
        ]
    )
    order = topological_order(declarations)
    assert order.index("queue") < order.index("inventory")
    assert order.index("inventory") < order.index("order")
    assert order.index("order") < order.index("gateway")


def test_affected_closure_and_plan_repeatability():
    declarations = make_declarations(
        [
            ("queue", ["q.name"], []),
            ("inventory", ["f"], ["queue"]),
            ("gateway", ["f"], ["inventory"]),
            ("audit", ["log.level"], []),
        ]
    )
    old = {"q.name": "v1", "log.level": "INFO", "f": 0}
    new = {"q.name": "v2", "log.level": "INFO", "f": 0}
    plan_a = build_plan(2, new, old, declarations)
    plan_b = build_plan(2, new, old, declarations)
    assert plan_a.affected_keys == ("q.name",)
    # queue 直接受影响，inventory 与 gateway 因下游依赖连带分发；audit 不受影响
    assert set(plan_a.affected_components) == {"queue", "inventory", "gateway"}
    assert plan_a.ordered_components == ("queue", "inventory", "gateway")
    assert plan_a.to_dict() == plan_b.to_dict()


def test_dependency_cycle_detected_with_members():
    declarations = make_declarations(
        [("a", ["x"], ["b"]), ("b", ["x"], ["a"]), ("c", ["x"], [])]
    )
    cycle = detect_cycle(declarations)
    assert set(cycle) == {"a", "b"}
    with pytest.raises(DependencyCycleError) as exc:
        build_plan(1, {"x": 1}, None, declarations)
    assert exc.value.code == "E_DEPENDENCY_CYCLE"
    assert set(exc.value.details["components_in_cycle"]) == {"a", "b"}


def test_first_snapshot_affects_all_components():
    declarations = make_declarations([("a", ["x"], []), ("b", ["y"], [])])
    plan = build_plan(1, {"x": 1, "y": 2}, None, declarations)
    assert set(plan.affected_components) == {"a", "b"}
    assert plan.version == 1
