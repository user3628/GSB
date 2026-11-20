from __future__ import annotations

from decimal import Decimal

import pytest

from join_planner.config import PlannerConfig
from join_planner.errors import PlannerError
from join_planner.optimizer import PlanOptimizer
from join_planner.query import QueryCatalog, parse_query


def test_exact_search_enumerates_connected_left_deep_orders(snapshot_store, catalog: QueryCatalog) -> None:
    optimizer = PlanOptimizer(PlannerConfig(fast_candidate_budget=2))
    plan = optimizer.plan(snapshot_store.current, catalog.get("q_product_priority"), "exact")
    assert plan["search"]["candidate_space_size"] == 8
    assert plan["search"]["candidates_evaluated"] == 8
    assert plan["search"]["pruned_branches"] == 0
    assert plan["search"]["cartesian_products"] is False
    assert len(plan["selected_order"]) == 4
    applied = [item for step in plan["steps"] for item in step["predicates_applied"]]
    assert len(applied) == len(set(applied)) == 6
    for step in plan["steps"]:
        assert Decimal(step["finite_upper_bound"]).is_finite()
        assert Decimal(step["estimated_rows"]).is_finite()


def test_fast_mode_prunes_and_is_not_cheaper_than_exact(snapshot_store, catalog: QueryCatalog) -> None:
    config = PlannerConfig(fast_candidate_budget=2, comparison_relative_tolerance=Decimal("1e-9"))
    optimizer = PlanOptimizer(config)
    query = catalog.get("q_product_priority")
    exact = optimizer.plan(snapshot_store.current, query, "exact")
    fast = optimizer.plan(snapshot_store.current, query, "fast")
    assert fast["mode"] == "fast"
    assert fast["search"]["candidates_evaluated"] == 2
    assert fast["search"]["pruned_branches"] == 6
    assert fast["search"]["optimality_guarantee"] is False
    exact_cost = Decimal(exact["cost"]["comparison_total"]["fixed"])
    fast_cost = Decimal(fast["cost"]["comparison_total"]["fixed"])
    assert fast_cost >= exact_cost - exact_cost * config.comparison_relative_tolerance


def test_repeated_inputs_are_bitwise_deterministic(snapshot_store, catalog: QueryCatalog) -> None:
    optimizer = PlanOptimizer(PlannerConfig())
    query = catalog.get("q_customer_revenue")
    first = optimizer.plan(snapshot_store.current, query, "exact")
    second = optimizer.plan(snapshot_store.current, query, "exact")
    assert first == second
    for component in ("scan", "join", "intermediate_write", "total"):
        rendered = first["cost"][component]["fixed"]
        integer, fraction = rendered.split(".")
        assert len(fraction) == 6


def test_relative_tolerance_ties_use_lexicographic_order(snapshot_store, catalog: QueryCatalog) -> None:
    optimizer = PlanOptimizer(PlannerConfig(comparison_relative_tolerance=Decimal("1")))
    plan = optimizer.plan(snapshot_store.current, catalog.get("q_product_priority"), "exact")
    assert plan["selected_order"] == ["customers", "orders", "order_items", "products"]
    assert plan["comparison"]["tie_break"] == "ascending lexicographic selected_order"


def test_disconnected_relations_are_rejected(snapshot_store) -> None:
    query = parse_query(
        {
            "query_id": "disconnected",
            "relations": ["customers", "orders"],
            "predicates": [],
        }
    )
    with pytest.raises(PlannerError) as exc_info:
        PlanOptimizer(PlannerConfig()).validate(snapshot_store.current, query)
    assert exc_info.value.code == "E_INVALID_QUERY"
    assert exc_info.value.details["missing_relations"] == ["orders"]


def test_relation_limit_returns_search_space_error(snapshot_store) -> None:
    relations = [{"alias": f"c{index}", "table": "customers"} for index in range(9)]
    predicates = [
        {
            "id": f"p{index}",
            "type": "equality",
            "left": {"relation": f"c{index}", "column": "customer_id"},
            "right": {"relation": f"c{index + 1}", "column": "customer_id"},
        }
        for index in range(8)
    ]
    query = parse_query(
        {
            "query_id": "too_large",
            "relations": relations,
            "predicates": predicates,
        }
    )
    with pytest.raises(PlannerError) as exc_info:
        PlanOptimizer(PlannerConfig(max_relations=8)).validate(snapshot_store.current, query)
    assert exc_info.value.code == "E_SEARCH_SPACE_TOO_LARGE"
    assert exc_info.value.details["relation_count"] == 9
    assert exc_info.value.details["max_relations"] == 8
    assert exc_info.value.details["candidate_scale"]["upper_bound"] == "362880"

