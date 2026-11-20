"""Selectivity estimation: eq / range / in / null, edges, correlations."""

import unittest

from qopt.selectivity import (Pred, combine_conjuncts, estimate_predicate,
                              parse_predicates)
from qopt.stats import parse_snapshot


def make_snapshot(extra_tables=None, correlations=None):
    doc = {
        "format": "qopt-snapshot/1",
        "version": "t1",
        "generated_at": "2026-11-20T00:00:00Z",
        "tables": {
            "t": {
                "row_count": 1000,
                "pages": 50,
                "columns": {
                    "a": {"ndv": 100, "null_frac": 0.1},
                    "h": {
                        "ndv": 30, "null_frac": 0.0,
                        "histogram": {
                            "boundaries": [0, 10, 20, 30],
                            "counts": [5, 15, 10],
                            "ndv_per_bucket": [5, 5, 5],
                        },
                    },
                    "hh": {
                        "ndv": 3, "null_frac": 0.0,
                        "histogram": {
                            "boundaries": [0, 5, 5, 10],
                            "counts": [5, 100, 5],
                            "ndv_per_bucket": [1, 1, 1],
                        },
                    },
                    "z": {"ndv": 0, "null_frac": 1.0},
                },
            },
            "empty": {"row_count": 0, "pages": 0,
                      "columns": {"a": {"ndv": 0, "null_frac": 0.0}}},
        },
        "indexes": [],
        "correlations": correlations or [],
    }
    if extra_tables:
        doc["tables"].update(extra_tables)
    return parse_snapshot(doc)


SNAP = make_snapshot()


def sel(ptype, column, **kw):
    pred = Pred(ptype, "t", column, **kw)
    return estimate_predicate(pred, SNAP)


class EqualityTest(unittest.TestCase):
    def test_eq_without_histogram(self):
        # (1 - null_frac) / ndv = 0.9 / 100
        self.assertAlmostEqual(sel("eq", "a", value=7), 0.009)

    def test_eq_with_histogram(self):
        # value 7 -> bucket 0: (5/30)/5
        self.assertAlmostEqual(sel("eq", "h", value=7), (5 / 30) / 5)

    def test_eq_boundary_right_closed(self):
        # value 10 belongs to bucket 0 (right-closed), value 20 to bucket 1.
        self.assertAlmostEqual(sel("eq", "h", value=10), (5 / 30) / 5)
        self.assertAlmostEqual(sel("eq", "h", value=20), (15 / 30) / 5)

    def test_eq_outside_histogram(self):
        self.assertEqual(sel("eq", "h", value=999), 0.0)

    def test_eq_null_value(self):
        self.assertEqual(sel("eq", "a", value=None), 0.0)

    def test_eq_point_mass_bucket(self):
        # degenerate bucket [5,5] holds a heavy hitter: 100/110
        self.assertAlmostEqual(sel("eq", "hh", value=5), 100 / 110)

    def test_eq_zero_ndv(self):
        self.assertEqual(sel("eq", "z", value=1), 0.0)  # no division by zero

    def test_eq_empty_table(self):
        pred = Pred("eq", "empty", "a", value=1)
        self.assertEqual(estimate_predicate(pred, SNAP), 0.0)


class RangeTest(unittest.TestCase):
    def test_range_interpolation(self):
        # [5,15]: bucket0 (10-5)/10*5=2.5, bucket1 (15-10)/10*15=7.5 -> 10/30
        self.assertAlmostEqual(sel("range", "h", low=5, high=15), 10 / 30)

    def test_range_full(self):
        self.assertAlmostEqual(sel("range", "h", low=0, high=30), 1.0)

    def test_range_open_sides(self):
        self.assertAlmostEqual(sel("range", "h", low=None, high=None), 1.0)
        self.assertAlmostEqual(sel("range", "h", low=25, high=None),
                               (5 / 10 * 10) / 30)

    def test_range_point_mass_inclusion(self):
        # degenerate bucket at 5 included only when the endpoint includes it
        self.assertAlmostEqual(sel("range", "hh", low=5, high=5), 100 / 110)
        self.assertEqual(sel("range", "hh", low=5, high=5,
                             low_inclusive=False), 0.0)

    def test_range_zero_ndv(self):
        self.assertEqual(sel("range", "z", low=1, high=2), 0.0)


class InAndNullTest(unittest.TestCase):
    def test_in_list(self):
        got = sel("in", "a", values=[1, 2, 3])
        self.assertAlmostEqual(got, 3 * 0.009)

    def test_in_clamped_to_non_null(self):
        got = sel("in", "a", values=list(range(500)))
        self.assertLessEqual(got, 0.9)  # 1 - null_frac
        self.assertGreaterEqual(got, 0.0)

    def test_in_empty(self):
        self.assertEqual(sel("in", "a", values=[]), 0.0)

    def test_null_predicates(self):
        self.assertAlmostEqual(sel("is_null", "a"), 0.1)
        self.assertAlmostEqual(sel("is_not_null", "a"), 0.9)

    def test_results_in_unit_interval(self):
        for p in [Pred("eq", "t", "h", value=3),
                  Pred("range", "t", "h", low=1, high=29),
                  Pred("in", "t", "h", values=[1, 2]),
                  Pred("is_null", "t", "h")]:
            s = estimate_predicate(p, SNAP)
            self.assertGreaterEqual(s, 0.0)
            self.assertLessEqual(s, 1.0)


class CorrelationTest(unittest.TestCase):
    def _snap(self, correlations):
        return make_snapshot(correlations=correlations)

    def test_functional_dependency(self):
        snap = self._snap([{"table": "t", "columns": ["a", "h"],
                            "kind": "functional_dependency", "value": 1.0}])
        preds = [Pred("eq", "t", "a", value=1), Pred("eq", "t", "h", value=5)]
        sels = [estimate_predicate(p, snap) for p in preds]
        joint, notes = combine_conjuncts("t", list(zip(preds, sels)), snap)
        self.assertAlmostEqual(joint, max(sels))
        self.assertTrue(any("functional dependency" in n for n in notes))

    def test_coefficient_bounded(self):
        snap = self._snap([{"table": "t", "columns": ["a", "h"],
                            "kind": "coefficient", "value": 0.5}])
        preds = [Pred("eq", "t", "a", value=1), Pred("eq", "t", "h", value=5)]
        sels = [estimate_predicate(p, snap) for p in preds]
        joint, notes = combine_conjuncts("t", list(zip(preds, sels)), snap)
        indep = sels[0] * sels[1]
        expect = 0.5 * indep + 0.5 * min(sels)
        self.assertAlmostEqual(joint, expect)
        self.assertTrue(0.0 <= joint <= 1.0)
        self.assertTrue(any("correlation coefficient" in n for n in notes))

    def test_undeclared_keeps_independence(self):
        preds = [Pred("eq", "t", "a", value=1), Pred("eq", "t", "h", value=5)]
        sels = [estimate_predicate(p, SNAP) for p in preds]
        joint, notes = combine_conjuncts("t", list(zip(preds, sels)), SNAP)
        self.assertAlmostEqual(joint, sels[0] * sels[1])
        self.assertTrue(any("independence assumption" in n for n in notes))


class ParseTest(unittest.TestCase):
    def test_and_flattening_and_sort(self):
        preds = parse_predicates([
            {"type": "and", "predicates": [
                {"type": "eq", "table": "t", "column": "b", "value": 1},
                {"type": "eq", "table": "t", "column": "a", "value": 1},
            ]},
        ], {"t"})
        self.assertEqual([p.column for p in preds], ["a", "b"])

    def test_invalid_range(self):
        from qopt.errors import QueryInvalidError
        with self.assertRaises(QueryInvalidError):
            parse_predicates(
                [{"type": "range", "table": "t", "column": "a",
                  "low": 10, "high": 1}], {"t"})


if __name__ == "__main__":
    unittest.main()
