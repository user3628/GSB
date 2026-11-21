"""Stage 3 fusion (hand-recomputable weights) and stage 4 sparsification."""

from __future__ import annotations

import unittest

from voxel_engine.artifacts import encode_fused_field
from voxel_engine.errors import E_SPARSE_EMPTY, EngineError
from voxel_engine.stages.fusion import run_fusion
from voxel_engine.stages.sparsify import run_sparsification


def _block(view_id, voxels):
    return {"view_id": view_id, "voxels": voxels}


class FusionTest(unittest.TestCase):
    def test_hand_recomputable_weighted_average(self):
        block_a = _block("a", {(0, 0, 0): ((10.0, 20.0, 30.0), 2, 1.0)})
        block_b = _block("b", {(1, 0, 0): ((40.0, 50.0, 60.0), 1, 1.0)})
        fused = run_fusion([block_a, block_b], 2.0)["field"]
        # kernel: k(0)=1, k(1)=1-1/2=0.5
        # g=(0,0,0): acc = 1*2*(10,20,30) + 0.5*1*(40,50,60) = (40,65,90), norm 2.5
        color, weight = fused[(0, 0, 0)]
        self.assertEqual(color, (16.0, 26.0, 36.0))
        self.assertEqual(weight, 2.5)
        # g=(1,0,0): acc = 0.5*2*(10,20,30) + 1*1*(40,50,60) = (50,70,90), norm 2
        color, weight = fused[(1, 0, 0)]
        self.assertEqual(color, (25.0, 35.0, 45.0))
        self.assertEqual(weight, 2.0)

    def test_fusion_order_is_shard_order_deterministic(self):
        block_a = _block("a", {(0, 0, 0): ((1.0, 2.0, 3.0), 1, 1.0)})
        block_b = _block("b", {(0, 0, 0): ((4.0, 5.0, 6.0), 1, 1.0)})
        first = encode_fused_field(run_fusion([block_a, block_b], 1.5))
        second = encode_fused_field(run_fusion([block_a, block_b], 1.5))
        self.assertEqual(first, second)

    def test_zero_radius_fuses_only_identical_voxels(self):
        block_a = _block("a", {(0, 0, 0): ((10.0, 10.0, 10.0), 1, 1.0)})
        block_b = _block("b", {(1, 0, 0): ((20.0, 20.0, 20.0), 1, 1.0)})
        fused = run_fusion([block_a, block_b], 0.0)["field"]
        self.assertEqual(len(fused), 2)
        self.assertEqual(fused[(0, 0, 0)][0], (10.0, 10.0, 10.0))
        self.assertEqual(fused[(1, 0, 0)][0], (20.0, 20.0, 20.0))


class SparsifyTest(unittest.TestCase):
    def _field(self, weights):
        return {
            "field": {
                (idx, 0, 0): ((10.0, 20.0, 30.0), weight)
                for idx, weight in enumerate(weights)
            },
            "voxel_count": len(weights),
        }

    def test_pruning_and_stats(self):
        result = run_sparsification(self._field([10.0, 1.0, 9.0]), 0.5)
        stats = result["stats"]
        self.assertEqual(stats["pre_prune_count"], 3)
        self.assertEqual(stats["post_prune_count"], 2)
        self.assertEqual(stats["pruned_count"], 1)
        self.assertEqual(stats["occupancy"], 2)
        self.assertAlmostEqual(stats["sparsity_ratio"], 1.0 / 3.0)
        self.assertEqual(len(result["field"]), 2)
        self.assertEqual(len(stats["color_summary"]["mean"]), 3)

    def test_empty_field_raises_e_sparse_empty(self):
        with self.assertRaises(EngineError) as ctx:
            run_sparsification({"field": {}}, 0.5)
        self.assertEqual(ctx.exception.code, E_SPARSE_EMPTY)
        self.assertEqual(ctx.exception.details["pre_prune_count"], 0)
        self.assertEqual(ctx.exception.details["post_prune_count"], 0)

    def test_all_pruned_raises_with_counts(self):
        with self.assertRaises(EngineError) as ctx:
            run_sparsification(self._field([10.0, 8.0]), 2.0)
        self.assertEqual(ctx.exception.code, E_SPARSE_EMPTY)
        self.assertEqual(ctx.exception.details["pre_prune_count"], 2)
        self.assertEqual(ctx.exception.details["post_prune_count"], 0)

    def test_zero_weight_voxel_raises(self):
        with self.assertRaises(EngineError) as ctx:
            run_sparsification(self._field([10.0, 0.0]), 0.0)
        self.assertEqual(ctx.exception.code, E_SPARSE_EMPTY)
        self.assertEqual(ctx.exception.details["reason"], "zero_weight_voxel")


if __name__ == "__main__":
    unittest.main()
