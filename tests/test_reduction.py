from __future__ import annotations

import unittest

from dimcluster.reduction import _tier_profile, compute_reduction
from dimcluster.samples import generate_samples

from .helpers import preprocessed_small, workspace


class ReductionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = workspace()
        generate_samples(cls.root / "samples")
        cls.manifest = cls.root / "samples" / "manifest.json"
        cls.pre = preprocessed_small(cls.manifest)

    def test_same_input_and_config_are_byte_identical(self) -> None:
        from .helpers import small_config

        config = small_config(self.manifest, ["small"])
        first = compute_reduction(self.pre, "preprocess-digest", "small", config)
        second = compute_reduction(self.pre, "preprocess-digest", "small", config)
        self.assertEqual(first.artifacts, second.artifacts)
        self.assertEqual(first.metadata["candidate"], "pca")
        self.assertLessEqual(first.metadata["target_dimension"], 5)

    def test_all_three_candidates_are_deterministic(self) -> None:
        from .helpers import small_config

        for candidate in ("pca", "truncated_svd", "random_projection"):
            config = small_config(self.manifest, ["small"])
            config["reduction"]["mode"] = "manual"
            config["reduction"]["candidate"] = candidate
            config["reduction"]["target_dimension"] = 4
            first = compute_reduction(self.pre, "preprocess-digest", "small", config)
            second = compute_reduction(self.pre, "preprocess-digest", "small", config)
            self.assertEqual(first.artifacts["reduced.npy"], second.artifacts["reduced.npy"])
            self.assertEqual(first.artifacts["loadings.npy"], second.artifacts["loadings.npy"])
            self.assertEqual(first.metadata["candidate"], candidate)

    def test_size_aware_dimension_caps(self) -> None:
        self.assertEqual(_tier_profile(400, 13, 0.02, "balanced")["dimension_cap"], 5)
        self.assertEqual(_tier_profile(4000, 46, 0.03, "balanced")["dimension_cap"], 10)
        self.assertEqual(_tier_profile(20000, 101, 0.03, "balanced")["dimension_cap"], 6)
        self.assertEqual(_tier_profile(20000, 101, 0.03, "low")["dimension_cap"], 4)


if __name__ == "__main__":
    unittest.main()
