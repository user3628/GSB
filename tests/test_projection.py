"""Stage 1 projection sampling: determinism, clipping, color correctness."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np
from PIL import Image

from voxel_engine.artifacts import decode_projection, encode_projection
from voxel_engine.manifest import load_manifest
from voxel_engine.stages.projection import run_projection

from tests.helpers import make_dataset


class ProjectionTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.data = make_dataset(Path(self._tmp.name) / "data")
        self.manifest = load_manifest(self.data / "manifest.json")

    def tearDown(self):
        self._tmp.cleanup()

    def _image(self, view):
        with Image.open(self.data / view.image) as img:
            return np.asarray(img.convert("RGB"), dtype=np.uint8).copy()

    def test_deterministic_byte_identical_records(self):
        view = self.manifest.views[0]
        image = self._image(view)
        first = run_projection(view, image, self.manifest.config)
        second = run_projection(view, image, self.manifest.config)
        self.assertEqual(encode_projection(first), encode_projection(second))

    def test_clipped_samples_are_counted(self):
        clipped_total = 0
        for view in self.manifest.views:
            result = run_projection(view, self._image(view), self.manifest.config)
            clipped_total += result["clipped_count"]
        self.assertGreater(clipped_total, 0)

    def test_records_carry_shard_id_points_and_colors(self):
        view = self.manifest.views[0]
        image = self._image(view)
        result = run_projection(view, image, self.manifest.config)
        self.assertEqual(result["view_id"], view.view_id)
        self.assertGreater(result["count"], 0)
        rot = np.asarray(view.extrinsic.rotation).reshape(3, 3)
        trans = np.asarray(view.extrinsic.translation)
        fx, fy = view.intrinsic.fx, view.intrinsic.fy
        cx, cy = view.intrinsic.cx, view.intrinsic.cy
        for row in range(result["count"]):
            point = result["xyz"][row]
            cam = rot @ point + trans
            self.assertGreater(cam[2], 0.0)
            iu = int(np.rint(fx * cam[0] / cam[2] + cx))
            iv = int(np.rint(fy * cam[1] / cam[2] + cy))
            self.assertTrue(0 <= iu < image.shape[1])
            self.assertTrue(0 <= iv < image.shape[0])
            expected = image[iv, iu].astype(np.float64)
            np.testing.assert_array_equal(result["rgb"][row], expected)

    def test_landmark_depth_sample_projects_back_to_landmark_pixel(self):
        view = self.manifest.views[0]
        image = self._image(view)
        result = run_projection(view, image, self.manifest.config)
        rot = np.asarray(view.extrinsic.rotation).reshape(3, 3)
        trans = np.asarray(view.extrinsic.translation)
        fx, fy = view.intrinsic.fx, view.intrinsic.fy
        cx, cy = view.intrinsic.cx, view.intrinsic.cy
        ray_step = float(self.manifest.config["ray_step"])
        checked = 0
        for lm_index, landmark in enumerate(view.landmarks):
            u, v = landmark.pixel
            if not (0 <= u <= view.width - 1 and 0 <= v <= view.height - 1):
                continue
            rows = [
                row
                for row in range(result["count"])
                if int(result["landmark_index"][row]) == lm_index
                and abs(float(result["t"][row]) - landmark.depth) < ray_step * 1e-6
            ]
            self.assertTrue(rows, "sample at declared depth must exist")
            point = result["xyz"][rows[0]]
            cam = rot @ point + trans
            pu = fx * cam[0] / cam[2] + cx
            pv = fy * cam[1] / cam[2] + cy
            self.assertAlmostEqual(pu, u, places=6)
            self.assertAlmostEqual(pv, v, places=6)
            checked += 1
        self.assertGreater(checked, 0)

    def test_encode_decode_roundtrip(self):
        view = self.manifest.views[1]
        result = run_projection(view, self._image(view), self.manifest.config)
        decoded = decode_projection(encode_projection(result))
        self.assertEqual(decoded["view_id"], view.view_id)
        self.assertEqual(decoded["count"], result["count"])
        self.assertEqual(decoded["clipped_count"], result["clipped_count"])
        np.testing.assert_array_equal(decoded["xyz"], result["xyz"])
        np.testing.assert_array_equal(decoded["rgb"], result["rgb"])


if __name__ == "__main__":
    unittest.main()
