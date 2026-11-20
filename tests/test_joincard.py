"""Join cardinality: log-domain accumulation, cartesian ceiling policy."""

import math
import unittest

from qopt.joincard import CARTESIAN_CAP, JoinPred, estimate_join
from qopt.stats import parse_snapshot


def make_snapshot():
    return parse_snapshot({
        "format": "qopt-snapshot/1",
        "version": "j1",
        "generated_at": "2026-11-20T00:00:00Z",
        "tables": {
            "a": {"row_count": 1000, "pages": 10,
                  "columns": {"id": {"ndv": 1000, "null_frac": 0.0}}},
            "b": {"row_count": 500, "pages": 5,
                  "columns": {"a_id": {"ndv": 800, "null_frac": 0.0}}},
            "c": {"row_count": 100, "pages": 1,
                  "columns": {"b_id": {"ndv": 0, "null_frac": 1.0}}},
        },
        "indexes": [],
        "correlations": [],
    })


SNAP = make_snapshot()


class JoinCardinalityTest(unittest.TestCase):
    def test_two_table_equijoin(self):
        jp = [JoinPred("a", "id", "b", "a_id")]
        res = estimate_join({"a": 1000.0, "b": 500.0}, jp, SNAP)
        # 1000 * 500 / max(1000, 800) = 500
        self.assertAlmostEqual(res["final_cardinality"], 500.0, places=6)
        self.assertFalse(res["cap_applied"])
        self.assertEqual(len(res["steps"]), 1)

    def test_log_domain_avoids_overflow(self):
        res = estimate_join({"a": 1e200, "b": 1e200}, [], SNAP)
        step = res["steps"][0]
        self.assertTrue(step["cartesian"])
        self.assertTrue(step["cap_applied"])
        self.assertEqual(res["final_cardinality"], CARTESIAN_CAP)
        # uncapped log value still reported for explainability
        self.assertAlmostEqual(step["log_cardinality"],
                               math.log(1e200) * 2, places=6)

    def test_cartesian_flagged_not_silent(self):
        res = estimate_join({"a": 10.0, "b": 20.0}, [], SNAP)
        self.assertTrue(res["steps"][0]["cartesian"])
        self.assertTrue(any("cartesian" in n for n in res["notes"]))
        self.assertAlmostEqual(res["final_cardinality"], 200.0)

    def test_zero_base_cardinality(self):
        jp = [JoinPred("a", "id", "b", "a_id")]
        res = estimate_join({"a": 0.0, "b": 500.0}, jp, SNAP)
        self.assertEqual(res["final_cardinality"], 0.0)

    def test_zero_ndv_join_column_safe(self):
        # ndv=0 must not raise log(0); clamped to 1.
        jp = [JoinPred("a", "id", "c", "b_id")]
        res = estimate_join({"a": 100.0, "c": 50.0}, jp, SNAP)
        self.assertAlmostEqual(res["final_cardinality"], 100.0 * 50.0 / 1000.0)

    def test_greedy_order_deterministic(self):
        jp = [JoinPred("a", "id", "b", "a_id")]
        res1 = estimate_join({"a": 1000.0, "b": 500.0}, jp, SNAP)
        res2 = estimate_join({"b": 500.0, "a": 1000.0}, jp, SNAP)
        self.assertEqual(res1["join_order"], res2["join_order"])
        self.assertEqual(res1["join_order"][0], "b")  # smaller base first


if __name__ == "__main__":
    unittest.main()
