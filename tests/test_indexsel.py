"""Index selection: prefix rules, exclusion reasons, reproducible ties."""

import unittest

from qopt.cost import CostBreakdown, choose_path
from qopt.indexsel import AccessPath, analyse_prefix, select_access_path
from qopt.stats import ColumnStats, IndexSpec, TableStats


def table(rows=10000, pages=200):
    return TableStats("t", rows, pages, {
        "a": ColumnStats("a", 1000, 0.0),
        "b": ColumnStats("b", 500, 0.0),
        "c": ColumnStats("c", 100, 0.0),
    })


class P:
    def __init__(self, column, type="eq"):
        self.column = column
        self.type = type


class PrefixTest(unittest.TestCase):
    def test_eq_prefix_then_range_stops(self):
        idx = IndexSpec("i", "t", ("a", "b", "c"), 50)
        preds = {"a": [P("a")], "b": [P("b", "range")], "c": [P("c")]}
        eq, rng, used = analyse_prefix(idx, preds)
        self.assertEqual(eq, ["a"])
        self.assertEqual(rng, "b")
        self.assertEqual(used, ["a", "b"])  # c unusable after the range

    def test_gap_breaks_prefix(self):
        idx = IndexSpec("i", "t", ("a", "b", "c"), 50)
        preds = {"a": [P("a")], "c": [P("c")]}
        eq, rng, used = analyse_prefix(idx, preds)
        self.assertEqual(used, ["a"])  # b missing -> c not reachable

    def test_in_counts_as_equality(self):
        idx = IndexSpec("i", "t", ("a", "b"), 50)
        preds = {"a": [P("a", "in")], "b": [P("b")]}
        eq, rng, used = analyse_prefix(idx, preds)
        self.assertEqual(used, ["a", "b"])

    def test_no_predicate_on_leading_column(self):
        idx = IndexSpec("i", "t", ("a", "b"), 50)
        eq, rng, used = analyse_prefix(idx, {"b": [P("b")]})
        self.assertEqual(used, [])


class SelectionTest(unittest.TestCase):
    def test_exclusion_reasons_present(self):
        t = table()
        preds = [P("a"), P("b", "range")]
        sels = [0.001, 0.05]
        indexes = [
            IndexSpec("idx_ab", "t", ("a", "b"), 100),
            IndexSpec("idx_c", "t", ("c",), 100),
            IndexSpec("idx_b", "t", ("b",), 100),
        ]
        chosen, candidates = select_access_path(t, preds, sels, indexes)
        self.assertEqual(chosen.key, "idx:idx_ab")
        by_access = {c["access"]: c for c in candidates}
        self.assertEqual(by_access["idx:idx_c"]["status"], "excluded")
        self.assertIn("prefix_mismatch", by_access["idx:idx_c"]["reason"])
        self.assertIn("cost", by_access["idx:idx_b"]["reason"])
        self.assertIn("cost", by_access["seq_scan"]["reason"])
        # every non-chosen candidate carries a concrete reason
        for c in candidates:
            if c["status"] == "excluded":
                self.assertTrue(c["reason"])

    def test_partial_prefix_coverage(self):
        t = table()
        preds = [P("a"), P("c")]  # only a covered by (a,b)
        sels = [0.0005, 0.02]
        indexes = [IndexSpec("idx_ab", "t", ("a", "b"), 100)]
        chosen, candidates = select_access_path(t, preds, sels, indexes)
        self.assertEqual(chosen.key, "idx:idx_ab")
        self.assertEqual(list(chosen.matched_columns), ["a"])
        self.assertEqual(list(chosen.residual_columns), ["c"])


class TieBreakTest(unittest.TestCase):
    def _path(self, key, total):
        return AccessPath(key=key, method="index_scan", index_name=key,
                          matched_columns=(), residual_columns=(),
                          sel_matched=1.0,
                          cost=CostBreakdown(total / 2, total / 2, 0.0))

    def test_tie_breaks_by_stable_key(self):
        p1 = self._path("idx:b", 10.0)
        p2 = self._path("idx:a", 10.0)
        best1, _ = choose_path([p1, p2])
        best2, _ = choose_path([p2, p1])  # reversed input order
        self.assertIs(best1, best2)
        self.assertEqual(best1.key, "idx:a")

    def test_within_tolerance_treated_as_tie(self):
        p1 = self._path("idx:a", 100.0)
        p2 = self._path("idx:b", 100.0 + 1e-8)  # within rel_tol=1e-9
        best, excluded = choose_path([p2, p1])
        self.assertEqual(best.key, "idx:a")
        self.assertIn("tie", excluded[0][1])

    def test_beyond_tolerance_picks_cheaper(self):
        p1 = self._path("idx:a", 100.0)
        p2 = self._path("idx:b", 90.0)
        best, _ = choose_path([p1, p2])
        self.assertEqual(best.key, "idx:b")


if __name__ == "__main__":
    unittest.main()
