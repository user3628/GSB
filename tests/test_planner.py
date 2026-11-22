import unittest

from tests.helpers import load_config, make_workspace
from ragwb.planner import (STRATEGY_EXACT, STRATEGY_FOCUS,
                           STRATEGY_LAYERED, BudgetLedger, plan_turn)


class PlannerTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = make_workspace()
        self.config = load_config(self.data_dir)

    def test_small_tier_uses_exact_full(self):
        plan = plan_turn(self.config, paragraph_count=24, turn_count=4,
                         remaining_budget=1000, focus_present=True)
        self.assertEqual(plan["tier"], "small")
        self.assertEqual(plan["strategy"], STRATEGY_EXACT)
        rejected = {item["strategy"] for item in plan["rejected_strategies"]}
        self.assertIn(STRATEGY_LAYERED, rejected)
        self.assertIn(STRATEGY_FOCUS, rejected)
        self.assertTrue(any(
            "small_tier_forbids" in item["reason"]
            for item in plan["rejected_strategies"]))

    def test_medium_tier_uses_layered_truncate(self):
        plan = plan_turn(self.config, paragraph_count=120, turn_count=12,
                         remaining_budget=10_000, focus_present=True)
        self.assertEqual(plan["tier"], "medium")
        self.assertEqual(plan["strategy"], STRATEGY_LAYERED)
        self.assertEqual(plan["planned_evaluations"], 40)

    def test_large_tier_uses_focus_first(self):
        plan = plan_turn(self.config, paragraph_count=500, turn_count=40,
                         remaining_budget=10_000, focus_present=True)
        self.assertEqual(plan["tier"], "large")
        self.assertEqual(plan["strategy"], STRATEGY_FOCUS)
        self.assertEqual(plan["planned_evaluations"], 12)

    def test_budget_never_exceeded_and_clamping_recorded(self):
        plan = plan_turn(self.config, paragraph_count=24, turn_count=4,
                         remaining_budget=5, focus_present=True)
        self.assertLessEqual(plan["planned_evaluations"], 5)
        ledger = BudgetLedger(budget=10)
        first = ledger.charge("s", 0, 8)
        second = ledger.charge("s", 1, 8)
        self.assertEqual(first["charged"], 8)
        self.assertEqual(second["charged"], 2)
        self.assertEqual(ledger.consumed, 10)
        self.assertIn("clamped_to_remaining_budget", plan["reason"])
