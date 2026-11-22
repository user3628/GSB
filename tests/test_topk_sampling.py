from __future__ import annotations

import math
import unittest

import numpy as np

from topk_pipeline.errors import (
    E_INVALID_K,
    E_INVALID_NUCLEUS,
    E_INVALID_TEMP,
    E_NUMERIC,
    PipelineError,
)
from topk_pipeline.normalize_sampling import normalize_topk, nucleus_and_sample
from topk_pipeline.topk import StopRequested, chunked_topk
from topk_pipeline.util import canonical_json_bytes


class TopKTests(unittest.TestCase):
    def test_invalid_k_has_shard_and_k(self) -> None:
        logits = np.zeros((2, 4), dtype=np.float64)
        for k, expected_k in [(-2, -2), (0, 0), (5, 5)]:
            with self.subTest(k=k):
                with self.assertRaises(PipelineError) as caught:
                    chunked_topk(logits, k=k, shard_id="bad_k", row_block=1, col_block=2)
                error = caught.exception
                self.assertEqual(error.code, E_INVALID_K)
                self.assertEqual(error.shard_id, "bad_k")
                self.assertEqual(error.k, expected_k)
                self.assertEqual(error.to_dict()["k"], expected_k)

    def test_ties_are_ranked_by_token_and_output_is_byte_stable(self) -> None:
        logits = np.asarray([[5.0, 1.0, 5.0, 5.0, 2.0]], dtype=np.float64)
        first = chunked_topk(logits, k=2, shard_id="ties", row_block=1, col_block=2)
        second = chunked_topk(logits, k=2, shard_id="ties", row_block=1, col_block=2)
        self.assertEqual(canonical_json_bytes(first), canonical_json_bytes(second))
        candidates = first["rows"][0]["candidates"]
        self.assertEqual([candidate["token"] for candidate in candidates], [0, 2])
        self.assertEqual([candidate["rank"] for candidate in candidates], [1, 2])
        self.assertEqual([candidate["comparison_order"] for candidate in candidates], [1, 2])
        self.assertEqual(
            [candidate["source_column_block"] for candidate in candidates], [0, 1]
        )
        self.assertTrue(all(candidate["admission_turn"] >= 1 for candidate in candidates))

    def test_nan_and_positive_infinity_are_numeric_errors(self) -> None:
        for bad in (np.nan, np.inf):
            with self.subTest(bad=bad):
                logits = np.zeros((2, 6), dtype=np.float64)
                logits[1, 4] = bad
                with self.assertRaises(PipelineError) as caught:
                    chunked_topk(logits, k=3, shard_id="nonfinite", row_block=1, col_block=2)
                error = caught.exception
                self.assertEqual(error.code, E_NUMERIC)
                self.assertEqual(error.stage, "filter")
                self.assertEqual(error.row, 1)
                self.assertEqual(error.details["column"], 4)

    def test_all_negative_infinity_reaches_normalization_error(self) -> None:
        logits = np.full((1, 4), -np.inf, dtype=np.float64)
        filtered = chunked_topk(logits, k=2, shard_id="all_neg_inf", row_block=1, col_block=2)
        self.assertEqual(
            [candidate["token"] for candidate in filtered["rows"][0]["candidates"]], [0, 1]
        )
        self.assertEqual(
            [candidate["score"] for candidate in filtered["rows"][0]["candidates"]],
            ["-Infinity", "-Infinity"],
        )
        with self.assertRaises(PipelineError) as caught:
            normalize_topk(filtered, temperature=1.0, shard_id="all_neg_inf")
        error = caught.exception
        self.assertEqual(error.code, E_NUMERIC)
        self.assertEqual(error.stage, "normalize")
        self.assertEqual(error.row, 0)

    def test_stop_is_observed_only_after_a_complete_row_block(self) -> None:
        logits = np.arange(20 * 8, dtype=np.float64).reshape(20, 8)
        calls = {"count": 0}

        def stop_check() -> bool:
            calls["count"] += 1
            return calls["count"] > 1

        with self.assertRaises(StopRequested) as caught:
            chunked_topk(
                logits,
                k=3,
                shard_id="stop_after_rows",
                row_block=4,
                col_block=3,
                stop_check=stop_check,
            )
        self.assertEqual(caught.exception.completed_rows, 4)
        self.assertEqual(caught.exception.stage, "filter")


class SamplingTests(unittest.TestCase):
    def _normalized(self, scores: list[float], *, k: int = 4) -> dict:
        logits = np.asarray([scores], dtype=np.float64)
        filtered = chunked_topk(logits, k=k, shard_id="sample", row_block=1, col_block=3)
        return normalize_topk(filtered, temperature=1.0, shard_id="sample")

    def test_equal_probabilities_crossing_boundary_use_rank_prefix(self) -> None:
        normalized = self._normalized([2.0, 2.0, 2.0, 2.0, 0.0, 0.0, 0.0, 0.0])
        sampled = nucleus_and_sample(
            normalized,
            shard_id="sample",
            shard_ordinal=0,
            seed=77,
            nucleus_enabled=True,
            top_p=0.4,
        )
        row = sampled["rows"][0]
        self.assertEqual(row["cutoff_rank"], 2)
        self.assertTrue(row["equal_probability_boundary"])
        self.assertEqual([candidate["token"] for candidate in row["kept_candidates"]], [0, 1])
        self.assertEqual(len(row["truncated_candidates"]), 2)
        self.assertEqual(row["truncated_candidates"][0]["token"], 2)
        self.assertAlmostEqual(
            sum(candidate["probability"] for candidate in row["kept_candidates"]), 1.0
        )

    def test_same_seed_and_row_are_reproducible(self) -> None:
        normalized = self._normalized([2.0, 1.5, 0.5, 0.1, -1.0, -2.0])
        first = nucleus_and_sample(
            normalized,
            shard_id="sample",
            shard_ordinal=3,
            seed=987654321,
            nucleus_enabled=True,
            top_p=0.9,
        )
        second = nucleus_and_sample(
            normalized,
            shard_id="sample",
            shard_ordinal=3,
            seed=987654321,
            nucleus_enabled=True,
            top_p=0.9,
        )
        self.assertEqual(canonical_json_bytes(first), canonical_json_bytes(second))
        different = nucleus_and_sample(
            normalized,
            shard_id="sample",
            shard_ordinal=3,
            seed=987654322,
            nucleus_enabled=True,
            top_p=0.9,
        )
        self.assertNotEqual(first["rows"][0]["random_uniform"], different["rows"][0]["random_uniform"])

    def test_invalid_temperature(self) -> None:
        normalized = self._normalized([1.0, 0.5, 0.0, -1.0])
        for temperature in (0, -1.0, float("nan")):
            with self.subTest(temperature=temperature):
                with self.assertRaises(PipelineError) as caught:
                    normalize_topk(normalized, temperature=temperature, shard_id="bad_temp")
                self.assertEqual(caught.exception.code, E_INVALID_TEMP)
                self.assertEqual(caught.exception.stage, "normalize")

    def test_invalid_nucleus_threshold(self) -> None:
        normalized = self._normalized([1.0, 0.5, 0.0, -1.0])
        with self.assertRaises(PipelineError) as caught:
            nucleus_and_sample(
                normalized,
                shard_id="bad_nucleus",
                shard_ordinal=0,
                seed=1,
                nucleus_enabled=True,
                top_p=0.0,
            )
        self.assertEqual(caught.exception.code, E_INVALID_NUCLEUS)

    def test_zero_probability_mass_is_numeric_error(self) -> None:
        normalized = {
            "rows": [
                {
                    "row": 0,
                    "candidates": [
                        {"token": 0, "score": 0.0, "probability": 0.0, "rank": 1},
                        {"token": 1, "score": 0.0, "probability": 0.0, "rank": 2},
                    ],
                }
            ]
        }
        with self.assertRaises(PipelineError) as caught:
            nucleus_and_sample(
                normalized,
                shard_id="zero_mass",
                shard_ordinal=0,
                seed=5,
                nucleus_enabled=True,
                top_p=0.9,
            )
        self.assertEqual(caught.exception.code, E_NUMERIC)
        self.assertEqual(caught.exception.row, 0)
        self.assertIn("cutoff_position", caught.exception.details)


if __name__ == "__main__":
    unittest.main()
