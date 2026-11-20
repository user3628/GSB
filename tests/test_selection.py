"""Selection stage: variance filter, mutual information, RFE."""

import copy
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np

from workbench.config import DEFAULT_CONFIG
from workbench.select import select_stage
from workbench.util import Deadline


def _matrix(cols):
    X = np.column_stack(cols).astype("<f8")
    return X.tobytes(order="C"), X.shape[0], X.shape[1]


def _cfg(method):
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    cfg["selection"]["method"] = method
    return cfg


class TestVariance(unittest.TestCase):
    def test_threshold_and_scores(self):
        a = np.array([0.0, 1.0] * 50)
        b = np.full(100, 3.0)
        c = np.linspace(0.0, 1.0, 100)
        mat, n, p = _matrix([a, b, c])
        cfg = _cfg("variance")
        cfg["selection"]["variance"]["threshold"] = 0.1
        art = select_stage(mat, n, p, ["a", "b", "c"], None, cfg, Deadline(None))
        self.assertEqual([k["feature"] for k in art["kept"]], ["a"])
        dropped = {d["feature"]: d for d in art["dropped"]}
        self.assertEqual(set(dropped), {"b", "c"})
        self.assertEqual(dropped["b"]["reason"], "variance_below_threshold")
        self.assertAlmostEqual(dropped["b"]["score"], 0.0)
        self.assertAlmostEqual(art["kept"][0]["score"], 0.25)


class TestMutualInfo(unittest.TestCase):
    def test_ranking_and_top_k(self):
        rng = np.random.RandomState(1)
        y = rng.randint(0, 2, 400).astype(np.float64)
        perfect = y.copy()
        noise = rng.randint(0, 2, 400).astype(np.float64)
        weak = np.where(rng.uniform(size=400) < 0.1, 1.0 - y, y)
        mat, n, p = _matrix([perfect, noise, weak])
        cfg = _cfg("mutual_info")
        cfg["selection"]["mutual_info"]["threshold"] = 0.0
        cfg["selection"]["mutual_info"]["top_k"] = 2
        art = select_stage(mat, n, p, ["perfect", "noise", "weak"], y, cfg, Deadline(None))
        kept = [k["feature"] for k in art["kept"]]
        self.assertEqual(kept[0], "perfect")
        p1 = float(y.mean())
        expected_mi = -(p1 * np.log(p1) + (1.0 - p1) * np.log(1.0 - p1))
        self.assertAlmostEqual(art["kept"][0]["score"], expected_mi, places=12)
        self.assertEqual(len(kept), 2)
        reasons = {d["feature"]: d["reason"] for d in art["dropped"]}
        self.assertEqual(reasons["noise"], "mi_top_k_cut")

    def test_threshold_drop(self):
        rng = np.random.RandomState(2)
        y = rng.randint(0, 2, 400).astype(np.float64)
        noise = rng.randint(0, 2, 400).astype(np.float64)
        mat, n, p = _matrix([noise])
        cfg = _cfg("mutual_info")
        cfg["selection"]["mutual_info"]["threshold"] = 0.5
        art = select_stage(mat, n, p, ["noise"], y, cfg, Deadline(None))
        self.assertEqual(art["kept"], [])
        self.assertEqual(art["dropped"][0]["reason"], "mi_below_threshold")


class TestRFE(unittest.TestCase):
    def test_eliminates_noise_features(self):
        rng = np.random.RandomState(3)
        n = 300
        x1 = rng.normal(size=n)
        x2 = rng.normal(size=n)
        y = (x1 + 0.5 * x2 + rng.normal(scale=0.01, size=n) > 0).astype(np.float64)
        noise_cols = [rng.normal(size=n) for _ in range(6)]
        mat, nn, p = _matrix([x1, x2] + noise_cols)
        names = ["x1", "x2"] + ["noise%d" % i for i in range(6)]
        cfg = _cfg("rfe")
        cfg["selection"]["rfe"]["n_features_to_select"] = 2
        cfg["selection"]["rfe"]["step"] = 2
        art = select_stage(mat, nn, p, names, y, cfg, Deadline(None))
        self.assertEqual({k["feature"] for k in art["kept"]}, {"x1", "x2"})
        self.assertEqual(len(art["dropped"]), 6)
        self.assertTrue(all(d["reason"] == "rfe_eliminated" for d in art["dropped"]))

    def test_deterministic_repeated(self):
        rng = np.random.RandomState(4)
        X = [rng.normal(size=200) for _ in range(5)]
        y = (X[0] > 0).astype(np.float64)
        mat, n, p = _matrix(X)
        names = ["f%d" % i for i in range(5)]
        cfg = _cfg("rfe")
        cfg["selection"]["rfe"]["n_features_to_select"] = 2
        a1 = select_stage(mat, n, p, names, y, cfg, Deadline(None))
        a2 = select_stage(mat, n, p, names, y, cfg, Deadline(None))
        self.assertEqual(a1, a2)


if __name__ == "__main__":
    unittest.main()
