from __future__ import annotations

from decimal import Decimal
import json

from join_planner.config import PlannerConfig
from join_planner.optimizer import PlanOptimizer
from join_planner.query import QueryCatalog, parse_query
from join_planner.selectivity import SelectivityEstimator
from join_planner.snapshot import Snapshot, checksum_for_snapshot, parse_snapshot_text


def test_equality_range_and_self_join_selectivity(
    snapshot_store,
    catalog: QueryCatalog,
) -> None:
    snapshot: Snapshot = snapshot_store.current
    config = PlannerConfig()
    revenue = catalog.get("q_customer_revenue")
    estimator = SelectivityEstimator(snapshot, revenue, config)
    estimates = {predicate.predicate_id: estimator.estimate(predicate) for predicate in revenue.predicates}
    assert estimates["cr_customer"].selectivity == Decimal(1) / Decimal(12000)
    assert estimates["cr_amount"].selectivity == Decimal(1400) / Decimal(4999)
    assert all(Decimal(0) < item.selectivity <= Decimal(1) for item in estimates.values())
    assert estimates["cr_amount"].assumption_source == "independence_assumption"

    employee = catalog.get("q_employee_manager")
    estimator = SelectivityEstimator(snapshot, employee, config)
    self_join = estimator.estimate(employee.predicates[0])
    assert self_join.selectivity == Decimal(1) / Decimal(5000)
    assert self_join.formula == "1 / max(outer_ndv, inner_ndv)"


def test_correlation_is_declared_and_applied_once(snapshot_store, catalog: QueryCatalog) -> None:
    snapshot = snapshot_store.current
    query = catalog.get("q_product_priority")
    plan = PlanOptimizer(PlannerConfig(fast_candidate_budget=1)).plan(snapshot, query, "exact")
    corrections = plan["correlation_corrections"]
    assert len(corrections) == 1
    assert corrections[0]["relation"] == "orders"
    assert corrections[0]["coefficient"] == "0.820000000000"
    assert corrections[0]["source"] == "declared_correlation"
    orders_step = next(step for step in plan["steps"] if step["relation"] == "orders")
    assert orders_step["combined_step_selectivity"] != "1.000000000000"
    assert orders_step["selectivity_clamped"] is False


def test_correlation_adjustment_is_clamped_to_one(snapshot_store, snapshot_dir) -> None:
    payload = json.loads((snapshot_dir / "snapshot_v1.json").read_text(encoding="utf-8"))
    payload["relations"]["orders"]["correlations"][0]["coefficient"] = 1000000000
    payload["checksum"] = checksum_for_snapshot(payload)
    snapshot = parse_snapshot_text(json.dumps(payload))
    query = parse_query(
        {
            "query_id": "clamped_correlation",
            "relations": ["orders"],
            "predicates": [
                {
                    "id": "status",
                    "type": "equality",
                    "left": {"relation": "orders", "column": "status"},
                    "literal": "paid",
                },
                {
                    "id": "priority",
                    "type": "equality",
                    "left": {"relation": "orders", "column": "priority"},
                    "literal": 1,
                },
            ],
        }
    )
    plan = PlanOptimizer(PlannerConfig()).plan(snapshot, query, "exact")
    assert plan["correlation_corrections"][0]["coefficient"] == "1000000000.000000000000"
    assert plan["steps"][0]["combined_step_selectivity"] == "1.000000000000"
    assert plan["steps"][0]["selectivity_clamped"] is True

