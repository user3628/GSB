from __future__ import annotations

import unittest

import numpy as np

from colscan.predicate import FALSE, TRUE, UNKNOWN, evaluate_predicate


class PredicateTests(unittest.TestCase):
    def test_sql_three_valued_null_semantics(self) -> None:
        arrays = {
            "score": np.asarray([10.0, 20.0, 0.0, 10.0], dtype=np.float64),
            "status": np.asarray(["active", "archived", "", "active"], dtype=object),
        }
        nulls = {
            "score": np.asarray([False, False, True, False]),
            "status": np.asarray([False, False, False, False]),
        }
        predicate = {
            "op": "and",
            "predicates": [
                {"column": "score", "op": "ge", "value": 10.0},
                {"column": "status", "op": "ne", "value": "archived"},
            ],
        }
        mask = evaluate_predicate(predicate, arrays, nulls, {"score": "float64", "status": "string"})
        self.assertEqual(mask.tolist(), [TRUE, FALSE, UNKNOWN, TRUE])

    def test_or_and_not_with_unknown(self) -> None:
        arrays = {"x": np.asarray([1, 2], dtype=np.int64)}
        nulls = {"x": np.asarray([False, True])}
        predicate = {
            "op": "or",
            "predicates": [
                {"column": "x", "op": "gt", "value": 5},
                {"column": "x", "op": "is_null"},
            ],
        }
        mask = evaluate_predicate(predicate, arrays, nulls, {"x": "int64"})
        self.assertEqual(mask.tolist(), [FALSE, TRUE])

    def test_is_null_and_is_not_null(self) -> None:
        arrays = {"x": np.asarray([0, 0], dtype=np.int64)}
        nulls = {"x": np.asarray([True, False])}
        is_null = evaluate_predicate({"column": "x", "op": "is_null"}, arrays, nulls, {"x": "int64"})
        not_null = evaluate_predicate({"column": "x", "op": "is_not_null"}, arrays, nulls, {"x": "int64"})
        self.assertEqual(is_null.tolist(), [TRUE, FALSE])
        self.assertEqual(not_null.tolist(), [FALSE, TRUE])


if __name__ == "__main__":
    unittest.main()

