from __future__ import annotations

import tempfile
import unittest

import numpy as np

from flashblock.config import AttentionConfig
from flashblock.core import arrays_byte_equal, full_attention
from flashblock.engine import AttentionEngine, PrefixUpdateRequest
from flashblock.errors import FlashBlockError


class IncrementalUpdateTests(unittest.TestCase):
    def setUp(self) -> None:
        rng = np.random.default_rng(77)
        self.q = rng.normal(0.0, 0.5, size=(2, 1, 40, 12))
        self.k = rng.normal(0.0, 0.5, size=(2, 1, 40, 12))
        self.v = rng.normal(0.0, 0.5, size=(2, 1, 40, 12))
        self.config = AttentionConfig(
            row_block_size=8,
            col_block_size=8,
            causal=True,
            memory_budget_bytes=8 * 1024 * 1024,
        )

    def test_prefix_correction_reuses_unaffected_rows_and_columns(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            engine = AttentionEngine(temporary, "incremental", self.q, self.k, self.v, self.config)
            baseline = engine.run()
            self.assertTrue(baseline.complete)
            rng = np.random.default_rng(991)
            shape = (2, 1, 8, 12)
            k_slice = rng.normal(0.0, 0.5, size=shape)
            v_slice = rng.normal(0.0, 0.5, size=shape)
            updated_k = self.k.copy()
            updated_v = self.v.copy()
            updated_k[:, :, 20:28, :] = k_slice
            updated_v[:, :, 20:28, :] = v_slice

            updated = engine.apply_prefix_update_request(
                PrefixUpdateRequest("incremental", 20, 28, k_slice, v_slice),
                verify=True,
            )
            fresh = full_attention(self.q, updated_k, updated_v, self.config, shard_id="incremental")
            self.assertTrue(updated.complete)
            self.assertTrue(updated.verified)
            self.assertEqual(updated.reused_row_blocks, [0, 1])
            self.assertEqual(updated.recomputed_row_blocks, [2, 3, 4])
            self.assertTrue(arrays_byte_equal(updated.output, fresh.output))
            self.assertTrue(arrays_byte_equal(updated.row_max, fresh.row_max))
            self.assertTrue(arrays_byte_equal(updated.row_norm, fresh.row_norm))

            report = engine.query_invalidations()
            self.assertEqual(report["reasons"], ["prefix_content_changed"])
            self.assertEqual(report["earliest_affected_row_block"], 2)
            self.assertEqual(report["reused_row_blocks"], [0, 1])
            self.assertIn("2", report["reused_col_blocks_by_row"])
            self.assertIn("2", report["recomputed_col_blocks_by_row"])

            np.savez(temporary + "/updated.npz", q=self.q, k=updated_k, v=updated_v)
            reloaded_engine = AttentionEngine.from_npz(
                temporary, "incremental", temporary + "/updated.npz", self.config
            )
            reloaded = reloaded_engine.run()
            self.assertTrue(reloaded.served_from_checkpoint)
            self.assertTrue(arrays_byte_equal(reloaded.output, updated.output))

    def test_config_change_invalidates_all_row_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            engine = AttentionEngine(temporary, "config-change", self.q, self.k, self.v, self.config)
            engine.run()
            new_config = self.config.with_overrides(causal=False)
            result = engine.apply_config_change(new_config)
            fresh = full_attention(self.q, self.k, self.v, new_config, shard_id="config-change")
            self.assertTrue(arrays_byte_equal(result.output, fresh.output))
            report = engine.query_invalidations()
            self.assertIn("causal_mask_changed", report["reasons"])
            self.assertEqual(report["reused_row_blocks"], [])
            self.assertEqual(report["earliest_affected_row_block"], 0)

    def test_block_parameter_and_strategy_change_invalidate_all_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            engine = AttentionEngine(temporary, "structure-change", self.q, self.k, self.v, self.config)
            engine.run()
            new_config = self.config.with_overrides(
                row_block_size=16,
                col_block_size=8,
                online_strategy="logsumexp_block_merge",
            )
            result = engine.apply_config_change(new_config)
            fresh = full_attention(self.q, self.k, self.v, new_config, shard_id="structure-change")
            self.assertTrue(arrays_byte_equal(result.output, fresh.output))
            report = engine.query_invalidations()
            self.assertIn("block_parameters_changed", report["reasons"])
            self.assertIn("online_strategy_changed", report["reasons"])
            self.assertEqual(report["reused_row_blocks"], [])

    def test_prefix_extension_beyond_query_horizon_reuses_all_rows(self) -> None:
        q = self.q[:, :, :24, :]
        k = self.k[:, :, :24, :]
        v = self.v[:, :, :24, :]
        with tempfile.TemporaryDirectory() as temporary:
            engine = AttentionEngine(temporary, "extend", q, k, v, self.config)
            engine.run()
            rng = np.random.default_rng(333)
            shape = (q.shape[0], q.shape[1], 8, q.shape[3])
            extra_k = rng.normal(0.0, 0.5, size=shape)
            extra_v = rng.normal(0.0, 0.5, size=shape)
            extended_k = np.concatenate([k, extra_k], axis=2)
            extended_v = np.concatenate([v, extra_v], axis=2)
            result = engine.apply_prefix_update(
                key_start=24,
                key_end=32,
                k_values=extra_k,
                v_values=extra_v,
                verify=True,
            )
            fresh = full_attention(q, extended_k, extended_v, self.config, shard_id="extend")
            self.assertEqual(result.reused_row_blocks, [0, 1, 2])
            self.assertEqual(result.recomputed_row_blocks, [])
            self.assertTrue(arrays_byte_equal(result.output, fresh.output))
            report = engine.query_invalidations()
            self.assertEqual(report["changed_key_interval"], [24, 32])
            self.assertEqual(report["recomputed_row_blocks"], [])

    def test_mismatch_reports_business_code_and_row_interval(self) -> None:
        config = self.config
        fresh = full_attention(self.q, self.k, self.v, config, shard_id="mismatch")
        altered_output = fresh.output.copy()
        altered_output[0, 0, 0, 0] += 1.0
        with self.assertRaises(FlashBlockError) as caught:
            AttentionEngine._check_byte_equality(
                shard_id="mismatch",
                incremental=(altered_output, fresh.row_max, fresh.row_norm),
                fresh=fresh,
                row_block_start=2,
                row_block_end=4,
            )
        error = caught.exception
        self.assertEqual(error.code, "E_INCREMENTAL_MISMATCH")
        self.assertEqual(error.context["shard_id"], "mismatch")
        self.assertEqual(error.context["row_block_start"], 2)
        self.assertEqual(error.context["row_block_end"], 4)
        self.assertGreater(error.context["max_abs_deviation"], 0.0)


if __name__ == "__main__":
    unittest.main()

