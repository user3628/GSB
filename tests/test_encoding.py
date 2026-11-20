"""Encoding stage: type inference, imputation, onehot/hash/target encoding."""

import copy
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np

from workbench.config import DEFAULT_CONFIG, validate_config
from workbench.encode import encode_stage, hash_bucket, target_encode
from workbench.infer import infer_stage
from workbench.sample import generate_csv
from workbench.util import Deadline, read_csv_columns


def _cfg():
    return copy.deepcopy(DEFAULT_CONFIG)


class TestInference(unittest.TestCase):
    def test_numeric_vs_categorical(self):
        columns = {
            "num": ["1.5", "2.5", "", "3.0"],
            "cat": ["a", "b", "a", ""],
            "mostly_num": ["1", "2", "x", "4"],
            "label": ["0", "1", "0", "1"],
        }
        cfg = _cfg()
        art = infer_stage(columns, cfg)
        kinds = {c["name"]: c["kind"] for c in art["columns"]}
        self.assertEqual(kinds["num"], "numeric")
        self.assertEqual(kinds["cat"], "categorical")
        self.assertEqual(kinds["label"], "label")
        # 3/4 = 0.75 parseable < 0.9 threshold -> categorical
        self.assertEqual(kinds["mostly_num"], "categorical")
        stats = {c["name"]: c for c in art["columns"]}
        self.assertEqual(stats["num"]["missing"], 1)
        self.assertEqual(stats["cat"]["distinct"], 2)

    def test_threshold_configurable(self):
        columns = {"mixed": ["1", "2", "x", "4"], "label": ["0", "1", "0", "1"]}
        cfg = _cfg()
        cfg["inference"]["numeric_ratio_threshold"] = 0.5
        art = infer_stage(columns, cfg)
        self.assertEqual(art["columns"][0]["kind"], "numeric")


class TestImputationAndEncoders(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb-enc-")
        self.sample = os.path.join(self.tmp, "s.csv")
        generate_csv(self.sample, seed=7, rows=400)
        _, self.columns = read_csv_columns(self.sample)
        self.cfg = _cfg()
        self.infer = infer_stage(self.columns, self.cfg)

    def test_deterministic_bytes(self):
        meta1, mat1, _ = encode_stage(self.columns, self.infer, self.cfg, Deadline(None))
        meta2, mat2, _ = encode_stage(self.columns, self.infer, self.cfg, Deadline(None))
        self.assertEqual(mat1, mat2)
        self.assertEqual(meta1, meta2)

    def test_no_missing_after_imputation(self):
        _, mat, _ = encode_stage(self.columns, self.infer, self.cfg, Deadline(None))
        X = np.frombuffer(mat, dtype="<f8")
        self.assertFalse(np.isnan(X).any())

    def test_numeric_mean_imputation(self):
        meta, mat, _ = encode_stage(self.columns, self.infer, self.cfg, Deadline(None))
        cols = {c["name"]: i for i, c in enumerate(meta["columns"])}
        X = np.frombuffer(mat, dtype="<f8").reshape(meta["rows"], meta["cols"])
        income = self.columns["income"]
        observed = np.array([float(v) for v in income if v != ""], dtype=np.float64)
        expected_mean = float(observed.mean())
        self.assertAlmostEqual(meta["columns"][cols["income"]]["domain"]["impute_value"],
                               expected_mean)
        for i, v in enumerate(income):
            if v == "":
                self.assertEqual(X[i, cols["income"]], expected_mean)

    def test_onehot_sorted_categories(self):
        meta, mat, _ = encode_stage(self.columns, self.infer, self.cfg, Deadline(None))
        seg_cols = [c["name"] for c in meta["columns"] if c["source"] == "segment"]
        self.assertEqual(seg_cols, sorted(seg_cols))
        self.assertTrue(all(c.startswith("segment__oh__") for c in seg_cols))
        cols = {c["name"]: i for i, c in enumerate(meta["columns"])}
        X = np.frombuffer(mat, dtype="<f8").reshape(meta["rows"], meta["cols"])
        seg = self.columns["segment"]
        for c in seg_cols:
            cat = c.split("__oh__", 1)[1]
            for i, v in enumerate(seg):
                self.assertEqual(X[i, cols[c]], 1.0 if v == cat else 0.0)

    def test_hash_encoding_buckets(self):
        meta, mat, _ = encode_stage(self.columns, self.infer, self.cfg, Deadline(None))
        cols = {c["name"]: i for i, c in enumerate(meta["columns"])}
        X = np.frombuffer(mat, dtype="<f8").reshape(meta["rows"], meta["cols"])
        n_buckets = self.cfg["encoding"]["hash"]["n_buckets"]
        token = self.cfg["missing"]["constant"]
        for i, v in enumerate(self.columns["city"]):
            v = v if v != "" else token
            b = hash_bucket(v, n_buckets)
            for bucket in range(n_buckets):
                self.assertEqual(X[i, cols["city__hash__%d" % bucket]],
                                 1.0 if bucket == b else 0.0)

    def test_missing_token_is_category(self):
        meta, _, _ = encode_stage(self.columns, self.infer, self.cfg, Deadline(None))
        chan_cols = [c["name"] for c in meta["columns"] if c["source"] == "channel"]
        self.assertIn("channel__oh____MISSING__", chan_cols)

    def test_column_mapping_and_domains(self):
        meta, _, _ = encode_stage(self.columns, self.infer, self.cfg, Deadline(None))
        for c in meta["columns"]:
            self.assertIn("name", c)
            self.assertIn("source", c)
            self.assertIn("encoder", c)
            self.assertIn("domain", c)
            self.assertIn("kind", c["domain"])


class TestTargetEncoding(unittest.TestCase):
    def test_no_self_leak(self):
        rng = np.random.RandomState(0)
        values = [str(rng.randint(0, 5)) for _ in range(300)]
        y = rng.randint(0, 2, 300).astype(np.float64)
        enc1, _ = target_encode(values, y, 5, 10.0, 42)
        y2 = y.copy()
        y2[17] = 1.0 - y2[17]
        enc2, _ = target_encode(values, y2, 5, 10.0, 42)
        self.assertEqual(enc1[17], enc2[17])  # own label must not affect own encoding
        self.assertFalse(np.array_equal(enc1, enc2))  # but others may change

    def test_unseen_category_falls_back_to_global_mean(self):
        values = ["a"] * 40 + ["b"] * 40 + ["rare"]
        y = np.array([1.0] * 40 + [0.0] * 40 + [1.0])
        enc, diag = target_encode(values, y, 5, 10.0, 3)
        rare_diag = diag["rare"]
        self.assertGreaterEqual(rare_diag["unseen_folds"], 1)
        # in folds where 'rare' is unseen the value must equal that fold's
        # training global mean; verify one such fold value manually
        folds = np.random.RandomState(3).permutation(len(values)) % 5
        rare_fold = folds[-1]
        train_mask = folds != rare_fold
        gmean = float(y[train_mask].mean())
        self.assertEqual(rare_diag["fold_values"][rare_fold], gmean)

    def test_smoothing_formula(self):
        values = ["a", "a", "b", "b"]
        y = np.array([1.0, 0.0, 1.0, 1.0])
        # 2 folds: rows are permuted deterministically; recompute by hand
        seed = 11
        smoothing = 2.0
        folds = np.random.RandomState(seed).permutation(4) % 2
        enc, _ = target_encode(values, y, 2, smoothing, seed)
        for k in (0, 1):
            tr = folds != k
            gmean = float(y[tr].mean())
            for cat in ("a", "b"):
                idx = [i for i in range(4) if values[i] == cat and tr[i]]
                cnt = len(idx)
                expect = gmean if cnt == 0 else (y[idx].sum() + smoothing * gmean) / (cnt + smoothing)
                for i in range(4):
                    if values[i] == cat and folds[i] == k:
                        self.assertAlmostEqual(enc[i], expect)

    def test_fold_seed_determinism(self):
        values = ["x", "y", "x", "z", "y", "x"] * 10
        y = np.array([1.0 if i % 3 else 0.0 for i in range(60)])
        enc1, _ = target_encode(values, y, 5, 10.0, 99)
        enc2, _ = target_encode(values, y, 5, 10.0, 99)
        enc3, _ = target_encode(values, y, 5, 10.0, 100)
        np.testing.assert_array_equal(enc1, enc2)
        self.assertFalse(np.array_equal(enc1, enc3))

    def test_diagnostics_expose_fold_spread(self):
        rng = np.random.RandomState(5)
        values = [str(rng.randint(0, 8)) for _ in range(500)]
        y = rng.randint(0, 2, 500).astype(np.float64)
        _, diag = target_encode(values, y, 5, 1.0, 42)
        for cat, d in diag.items():
            self.assertEqual(len(d["fold_values"]), 5)
            self.assertAlmostEqual(d["spread"],
                                   max(d["fold_values"]) - min(d["fold_values"]))
        self.assertTrue(any(d["spread"] > 0 for d in diag.values()))


if __name__ == "__main__":
    unittest.main()
