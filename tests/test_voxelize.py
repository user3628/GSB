"""Stage 2 voxelization: deterministic merge, occlusion, E_VOXEL_INVALID."""

from __future__ import annotations

import unittest

import numpy as np

from voxel_engine.artifacts import encode_voxel_block
from voxel_engine.errors import E_VOXEL_INVALID, EngineError
from voxel_engine.stages.fusion import normalize_block
from voxel_engine.stages.voxelize import run_voxelization

CONFIG = {
    "voxel_resolution": 0.5,
    "truncation_distance": 0.75,
    "scene_origin": [-4.0, -4.0, -4.0],
    "scene_size": [8.0, 8.0, 8.0],
    "voxel_feature_dim": 3,
}


def _records(points, colors, depths, view_id="view_t"):
    count = len(points)
    return {
        "view_id": view_id,
        "count": count,
        "clipped_count": 0,
        "landmark_count": 1,
        "t": np.asarray(depths, dtype="<f8"),
        "xyz": np.asarray(points, dtype="<f8").reshape(count, 3),
        "rgb": np.asarray(colors, dtype="<f8").reshape(count, 3),
        "landmark_index": np.zeros(count, dtype="<u4"),
        "sample_index": np.arange(count, dtype="<u4"),
    }


class VoxelizeTest(unittest.TestCase):
    def test_deterministic_merge_same_voxel(self):
        records = _records(
            [(0.01, 0.0, 0.0), (0.02, 0.0, 0.0), (0.03, 0.0, 0.0)],
            [(10, 20, 30), (40, 50, 60), (70, 80, 90)],
            [5.0, 5.1, 5.2],
        )
        first = run_voxelization(records, CONFIG)
        second = run_voxelization(records, CONFIG)
        base = (8, 8, 8)
        self.assertIn(base, first["voxels"])
        color_sum, count, weight, _ = first["voxels"][base]
        self.assertEqual(count, 3)
        self.assertEqual(color_sum, [120.0, 150.0, 180.0])
        enc_a = encode_voxel_block(normalize_block(first), first)
        enc_b = encode_voxel_block(normalize_block(second), second)
        self.assertEqual(enc_a, enc_b)

    def test_inf_color_raises_e_voxel_invalid(self):
        records = _records(
            [(0.0, 0.0, 0.0)],
            [(float("inf"), 0.0, 0.0)],
            [5.0],
            view_id="view_inf",
        )
        with self.assertRaises(EngineError) as ctx:
            run_voxelization(records, CONFIG)
        self.assertEqual(ctx.exception.code, E_VOXEL_INVALID)
        self.assertEqual(ctx.exception.details["view_id"], "view_inf")
        self.assertEqual(ctx.exception.details["reason"], "non_finite_color")
        self.assertIn("voxel", ctx.exception.details)

    def test_negative_out_of_bounds_voxel_raises(self):
        records = _records(
            [(-9.0, 0.0, 0.0)],
            [(1.0, 2.0, 3.0)],
            [5.0],
            view_id="view_neg",
        )
        with self.assertRaises(EngineError) as ctx:
            run_voxelization(records, CONFIG)
        self.assertEqual(ctx.exception.code, E_VOXEL_INVALID)
        self.assertEqual(ctx.exception.details["view_id"], "view_neg")
        self.assertEqual(ctx.exception.details["reason"], "voxel_out_of_bounds")
        self.assertLess(ctx.exception.details["voxel"][0], 0)

    def test_occluded_sample_is_skipped_and_counted(self):
        records = _records(
            [(0.0, 0.0, 0.0), (0.0, 0.0, 0.0)],
            [(10.0, 10.0, 10.0), (200.0, 200.0, 200.0)],
            [3.0, 6.0],
        )
        block = run_voxelization(records, CONFIG)
        self.assertEqual(block["occluded_skips"], 1)
        color_sum, count, _weight, min_depth = block["voxels"][(8, 8, 8)]
        self.assertEqual(count, 1)
        self.assertEqual(color_sum, [10.0, 10.0, 10.0])
        self.assertEqual(min_depth, 3.0)

    def test_occupancy_stats_recorded(self):
        records = _records(
            [(0.0, 0.0, 0.0), (1.0, 1.0, 1.0)],
            [(10.0, 10.0, 10.0), (20.0, 20.0, 20.0)],
            [5.0, 5.0],
        )
        block = run_voxelization(records, CONFIG)
        self.assertEqual(block["occupied_voxels"], len(block["voxels"]))
        self.assertEqual(block["total_hits"], 2)
        self.assertGreater(block["occupied_voxels"], 2)


if __name__ == "__main__":
    unittest.main()
