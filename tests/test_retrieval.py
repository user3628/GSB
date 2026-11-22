import math
import unittest

from tests.helpers import load_config, make_workspace
from ragwb.errors import (E_BUDGET_EXCEEDED, E_NUMERIC,
                          E_RETRIEVAL_EMPTY, WorkbenchError)
from ragwb.planner import plan_turn
from ragwb import retrieval as retrieval_mod


class RetrievalTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = make_workspace()
        self.config = load_config(self.data_dir)
        self.docs = []
        from ragwb.pipeline import load_workspace
        self.docs = load_workspace(self.data_dir)["docs"]

    def test_ordering_and_components(self):
        query_product = {
            "session_id": "s", "turn_index": 0, "lang": "zh",
            "rewritten_query": "极光电池的容量是多少？",
        }
        plan = plan_turn(self.config, paragraph_count=len(self.docs),
                         turn_count=3, remaining_budget=1000,
                         focus_present=True)
        charge = {"session_id": "s", "turn_index": 0,
                  "requested": plan["estimated_cost"],
                  "charged": plan["estimated_cost"],
                  "consumed_total": plan["estimated_cost"]}
        product = retrieval_mod.retrieve(
            query_product, self.docs, self.config, plan, charge)
        scores = [candidate["score"] for candidate in product["candidates"]]
        doc_ids = [candidate["doc_id"] for candidate in
                   product["candidates"]]
        self.assertEqual(scores, sorted(scores, reverse=True))
        # Ties (if any) must resolve by ascending doc_id.
        for left, right in zip(product["candidates"],
                               product["candidates"][1:]):
            if math.isclose(left["score"], right["score"]):
                self.assertLess(left["doc_id"], right["doc_id"])
        self.assertIn("log_overlap", product["candidates"][0]["components"])
        self.assertEqual(doc_ids[0], "kb-0001")

    def test_empty_query_business_code(self):
        plan = plan_turn(self.config, paragraph_count=len(self.docs),
                         turn_count=3, remaining_budget=1000,
                         focus_present=True)
        charge = {"charged": plan["estimated_cost"], "requested": 1}
        with self.assertRaises(WorkbenchError) as caught:
            retrieval_mod.retrieve(
                {"session_id": "s", "turn_index": 0,
                 "rewritten_query": "   "},
                self.docs, self.config, plan, charge)
        self.assertEqual(caught.exception.code, E_RETRIEVAL_EMPTY)
        self.assertEqual(caught.exception.stage, "retrieval")
        self.assertEqual(caught.exception.session_id, "s")

    def test_zero_candidates_after_budget_business_code(self):
        plan = plan_turn(self.config, paragraph_count=len(self.docs),
                         turn_count=3, remaining_budget=0,
                         focus_present=True)
        charge = {"charged": 0, "requested": plan["estimated_cost"]}
        with self.assertRaises(WorkbenchError) as caught:
            retrieval_mod.retrieve(
                {"session_id": "s", "turn_index": 2,
                 "rewritten_query": "云岭手机售价"},
                self.docs, self.config, plan, charge)
        self.assertEqual(caught.exception.code, E_RETRIEVAL_EMPTY)
        self.assertEqual(caught.exception.context["cause"],
                         E_BUDGET_EXCEEDED)

    def test_numeric_guard(self):
        doc = {"doc_id": "x", "text": "极光电池", "version": 1,
               "channel": "official", "lang": "zh"}
        max_version = 1
        result = retrieval_mod.score_paragraph(
            "极光电池", doc, self.config, max_version)
        self.assertTrue(math.isfinite(result["score"]))
        with self.assertRaises(WorkbenchError) as caught:
            retrieval_mod._log_component(
                float("nan"), session_id="s", turn_index=0,
                component="log_overlap")
        self.assertEqual(caught.exception.code, E_NUMERIC)


if __name__ == "__main__":
    unittest.main()
