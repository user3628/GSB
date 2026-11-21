from __future__ import annotations

import unittest

import numpy as np

from flashblock.config import AttentionConfig
from flashblock.core import arrays_byte_equal, full_attention
from flashblock.errors import FlashBlockError
from flashblock.reference import reference_attention


class CoreAttentionTests(unittest.TestCase):
    def setUp(self) -> None:
        rng = np.random.default_rng(42)
        self.q = rng.normal(0.0, 0.5, size=(2, 2, 35, 40))
        self.k = rng.normal(0.0, 0.5, size=(2, 2, 35, 40))
        self.v = rng.normal(0.0, 0.5, size=(2, 2, 35, 40))

    def test_causal_matches_full_reference_with_non_divisible_blocks(self) -> None:
        config = AttentionConfig(row_block_size=13, col_block_size=11, causal=True)
        result = full_attention(self.q, self.k, self.v, config, shard_id="core-causal")
        reference = reference_attention(self.q, self.k, self.v, config, shard_id="core-causal")
        np.testing.assert_allclose(result.output, reference.output, atol=config.atol, rtol=config.rtol)
        np.testing.assert_allclose(result.row_max, reference.row_max, atol=config.atol, rtol=config.rtol)
        np.testing.assert_allclose(result.row_norm, reference.row_norm, atol=config.atol, rtol=config.rtol)

    def test_non_causal_matches_reference(self) -> None:
        config = AttentionConfig(row_block_size=16, col_block_size=16, causal=False)
        result = full_attention(self.q, self.k, self.v, config, shard_id="core-full")
        reference = reference_attention(self.q, self.k, self.v, config, shard_id="core-full")
        np.testing.assert_allclose(result.output, reference.output, atol=config.atol, rtol=config.rtol)

    def test_repeated_runs_are_byte_identical(self) -> None:
        config = AttentionConfig(row_block_size=9, col_block_size=7, causal=True)
        first = full_attention(self.q, self.k, self.v, config, shard_id="deterministic")
        second = full_attention(self.q, self.k, self.v, config, shard_id="deterministic")
        self.assertTrue(arrays_byte_equal(first.output, second.output))
        self.assertTrue(arrays_byte_equal(first.row_max, second.row_max))
        self.assertTrue(arrays_byte_equal(first.row_norm, second.row_norm))

    def test_extreme_logits_are_clamped_and_counted(self) -> None:
        q = np.full((1, 1, 8, 4), 50.0)
        k = np.full((1, 1, 8, 4), 50.0)
        v = np.arange(32, dtype=np.float64).reshape(1, 1, 8, 4)
        config = AttentionConfig(
            row_block_size=4,
            col_block_size=4,
            causal=True,
            score_clamp=10.0,
            extreme_threshold=5.0,
        )
        result = full_attention(q, k, v, config, shard_id="extreme")
        self.assertTrue(np.isfinite(result.output).all())
        self.assertGreater(result.numeric_stats.clamped_values, 0)
        self.assertGreater(result.numeric_stats.extreme_values, 0)

    def test_non_finite_input_returns_business_code_with_context(self) -> None:
        q = self.q.copy()
        q[0, 1, 17, 3] = np.nan
        config = AttentionConfig(row_block_size=8, col_block_size=8)
        with self.assertRaises(FlashBlockError) as caught:
            full_attention(q, self.k, self.v, config, shard_id="bad-input")
        self.assertEqual(caught.exception.code, "E_NUMERIC")
        self.assertEqual(caught.exception.context["shard_id"], "bad-input")
        self.assertIn("row_block", caught.exception.context)
        self.assertIn("col_block", caught.exception.context)

    def test_batch_split_execution_matches_reference(self) -> None:
        rng = np.random.default_rng(202)
        q = rng.normal(size=(8, 1, 16, 32))
        k = rng.normal(size=(8, 1, 16, 32))
        v = rng.normal(size=(8, 1, 16, 32))
        config = AttentionConfig(
            row_block_size=16,
            col_block_size=16,
            causal=True,
            memory_budget_bytes=100_000,
        )
        result = full_attention(q, k, v, config, shard_id="split")
        self.assertEqual(result.plans[0].decision, "batch_split_required")
        reference = reference_attention(q, k, v, config, shard_id="split")
        np.testing.assert_allclose(result.output, reference.output, atol=config.atol, rtol=config.rtol)


if __name__ == "__main__":
    unittest.main()

