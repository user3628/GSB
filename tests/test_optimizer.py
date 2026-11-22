from __future__ import annotations

import json
import unittest
from decimal import Decimal

from support import make_snapshot

from joincost.errors import BusinessError, E_NO_CONNECTED_PLAN, E_SEARCH_SPACE_TOO_LARGE
from joincost.fixed import MAX_INTERMEDIATE_ROWS
from joincost.model import canonical_json, finalize_snapshot, normalize_relation
from joincost.optimizer import build_plan


EQUALITY_PREDICATES = [
    {
        "id": "ab",
        "kind": "equality",
        "relation_a": "a",
        "column_a": "id",
        "relation_b": "b",
        "column_b": "a_id",
    },
    {
        "id": "bc",
        "kind": "equality",
        "relation_a": "b",
        "column_a": "id",
        "relation_b": "c",
        "column_b": "b_id",
    },
    {
        "id": "range_a",
        "kind": "range",
        "relation_a": "a",
        "column_a": "created_at",
        "range_fraction_permille": 250,
    },
    {
        "id": "self_b",
        "kind": "self_join",
        "relation_a": "b",
        "column_a": "parent_id",
        "column_b": "id",
    },
]


class OptimizerTests(unittest.TestCase):
    def test_plan_cost_breakdown_and_log_bounds(self):
        plan = build_plan(make_snapshot(version=5), EQUALITY_PREDICATES, request_id="r1")
        total = Decimal(plan["cost"]["total"])
        components = sum(
            Decimal(plan["cost"][name])
            for name in ("scan", "join", "intermediate_write")
        )
        self.assertEqual(total, components)
        self.assertGreater(total, 0)
        self.assertFalse(plan["search"]["cartesian_products_allowed"])
        for step in plan["plan"]["steps"]:
            self.assertLessEqual(step["upper_bound_rows"], MAX_INTERMEDIATE_ROWS)
            self.assertTrue(step["finite_upper_bound"])
            self.assertTrue(step["overflow_capped"] in {True, False})
        for estimate in plan["predicate_selectivity"]:
            value = Decimal(estimate["selectivity"])
            self.assertGreater(value, 0)
            self.assertLessEqual(value, 1)
            self.assertTrue(estimate["range_check"]["valid"])

    def test_same_request_is_bit_identical(self):
        snapshot = make_snapshot(version=5)
        first = build_plan(snapshot, EQUALITY_PREDICATES, request_id="same")
        second = build_plan(snapshot, EQUALITY_PREDICATES, request_id="same")
        self.assertEqual(canonical_json(first), canonical_json(second))

    def test_tolerance_selects_stable_lexicographic_plan(self):
        snapshot = make_snapshot(version=7)
        tolerant = build_plan(
            snapshot,
            EQUALITY_PREDICATES,
            {"tolerance_ppm": 1_000_000, "max_relations": 8},
            request_id="tol",
        )
        self.assertEqual(tolerant["ordered_relations"], ["a", "b", "c"])
        self.assertGreaterEqual(tolerant["tolerance_contract"]["equivalent_candidates_within_tolerance"], 1)
        strict = build_plan(
            snapshot,
            EQUALITY_PREDICATES,
            {"tolerance_ppm": 0, "max_relations": 8},
            request_id="strict",
        )
        self.assertFalse(strict["tolerance_contract"]["selected_via_tolerance"])
        self.assertEqual(strict["cost"]["total_fixed"], strict["tolerance_contract"]["absolute_minimum_cost_fixed"])

    def test_zero_correlation_and_full_range_stay_inside_selectivity_bounds(self):
        snapshot = make_snapshot(version=8)
        predicates = [
            {
                "id": "zero",
                "kind": "equality",
                "relation_a": "a",
                "column_a": "id",
                "relation_b": "b",
                "column_b": "id",
                "correlation_coefficient": 0,
            },
            {
                "id": "full",
                "kind": "range",
                "relation_a": "c",
                "column_a": "x",
                "range_fraction_permille": 1000,
                "correlation_coefficient": 1,
            },
            {
                "id": "bc",
                "kind": "equality",
                "relation_a": "b",
                "column_a": "id",
                "relation_b": "c",
                "column_b": "id",
            },
        ]
        plan = build_plan(snapshot, predicates)
        by_id = {item["id"]: item for item in plan["predicate_selectivity"]}
        self.assertGreater(Decimal(by_id["zero"]["selectivity"]), 0)
        self.assertEqual(by_id["full"]["selectivity"], "1.000000000")

    def test_cartesian_only_graph_is_rejected(self):
        snapshot = make_snapshot(version=9)
        predicates = [
            {
                "id": "ab",
                "kind": "equality",
                "relation_a": "a",
                "column_a": "id",
                "relation_b": "b",
                "column_b": "id",
            }
        ]
        with self.assertRaises(BusinessError) as raised:
            build_plan(snapshot, predicates)
        self.assertEqual(raised.exception.code, E_NO_CONNECTED_PLAN)
        self.assertGreater(raised.exception.details["pruned_branches"], 0)

    def test_search_space_limit_reports_pruned_branches(self):
        relations = [(f"r{index}", 100 + index, 0.5, 1.0) for index in range(9)]
        snapshot = make_snapshot(version=10, relations=relations)
        with self.assertRaises(BusinessError) as raised:
            build_plan(snapshot, [], {"max_relations": 8})
        self.assertEqual(raised.exception.code, E_SEARCH_SPACE_TOO_LARGE)
        self.assertGreater(raised.exception.details["pruned_branches"], 0)

    def test_log_domain_caps_large_intermediate(self):
        relations = [
            ("huge_a", (2**63) - 1, 1.0, 1.0),
            ("huge_b", (2**63) - 1, 1.0, 1.0),
        ]
        normalized = [
            normalize_relation(
                name=name,
                row_count=rows,
                ndv_ratio_value=ratio,
                correlation_value=corr,
                source_kind="permille_integer",
                source_precision_places=3,
            )
            for name, rows, ratio, corr in relations
        ]
        snapshot = finalize_snapshot(
            version=11,
            relations=normalized,
            format_name="binary",
            format_version=2,
            decision="ACCEPT",
            degradations=[],
        )
        predicates = [
            {
                "id": "huge",
                "kind": "equality",
                "relation_a": "huge_a",
                "column_a": "id",
                "relation_b": "huge_b",
                "column_b": "id",
            }
        ]
        plan = build_plan(snapshot, predicates)
        last = plan["plan"]["steps"][-1]
        self.assertEqual(last["upper_bound_rows"], MAX_INTERMEDIATE_ROWS)
        self.assertTrue(last["overflow_capped"])


if __name__ == "__main__":
    unittest.main()

