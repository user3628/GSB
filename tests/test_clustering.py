from __future__ import annotations

import unittest

import numpy as np

from dimcluster.clustering import _kmeans_lloyd


class ClusteringTests(unittest.TestCase):
    def test_empty_clusters_are_explicitly_relocated(self) -> None:
        X = np.zeros((12, 2), dtype=np.float64)
        result = _kmeans_lloyd(
            X,
            k=3,
            seed=7,
            max_iter=10,
            tol=1e-8,
            empty_policy="relocate_farthest",
        )
        self.assertGreaterEqual(len(result["relocations"]), 1)
        self.assertEqual(len(np.unique(result["labels"])), 3)
        self.assertTrue(all(size > 0 for size in result["sizes"]))
        self.assertEqual(result["empty_cluster_policy"], "relocate_farthest")

    def test_canonical_label_order_is_stable(self) -> None:
        X = np.asarray(
            [[0.0, 0.0], [0.1, 0.0], [10.0, 0.0], [10.1, 0.0], [5.0, 5.0], [5.1, 5.1]],
            dtype=np.float64,
        )
        first = _kmeans_lloyd(X, 3, 11, 50, 1e-9, "relocate_farthest")
        second = _kmeans_lloyd(X, 3, 11, 50, 1e-9, "relocate_farthest")
        np.testing.assert_array_equal(first["labels"], second["labels"])
        np.testing.assert_allclose(first["centers"], second["centers"])
        self.assertTrue(np.all(first["centers"][:-1, 0] <= first["centers"][1:, 0]))

    def test_centroid_movement_history_recorded(self) -> None:
        X = np.asarray(
            [[0.0], [0.1], [0.2], [10.0], [10.1], [10.2]],
            dtype=np.float64,
        )
        result = _kmeans_lloyd(X, 2, 3, 20, 1e-9, "relocate_farthest")
        self.assertGreaterEqual(len(result["iterations"]), 1)
        for iteration in result["iterations"]:
            self.assertIn("centroid_shift", iteration)
            self.assertIn("cluster_sizes", iteration)
            self.assertIn("relocations", iteration)


if __name__ == "__main__":
    unittest.main()
