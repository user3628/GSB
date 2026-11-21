"""Generator determinism and manifest content."""

from __future__ import annotations

import filecmp
import tempfile
import unittest
from pathlib import Path

from voxel_engine.manifest import load_manifest

from tests.helpers import make_dataset


class GeneratorTest(unittest.TestCase):
    def test_byte_identical_regeneration(self):
        with tempfile.TemporaryDirectory() as tmp:
            dir_a = make_dataset(Path(tmp) / "a")
            dir_b = make_dataset(Path(tmp) / "b")
            files_a = sorted(p.relative_to(dir_a) for p in dir_a.rglob("*") if p.is_file())
            files_b = sorted(p.relative_to(dir_b) for p in dir_b.rglob("*") if p.is_file())
            self.assertEqual(files_a, files_b)
            for rel in files_a:
                self.assertTrue(
                    filecmp.cmp(dir_a / rel, dir_b / rel, shallow=False),
                    f"file differs: {rel}",
                )

    def test_manifest_declares_required_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = make_dataset(Path(tmp) / "data")
            manifest = load_manifest(data / "manifest.json")
            self.assertGreaterEqual(len(manifest.views), 4)
            for key in (
                "voxel_resolution",
                "truncation_distance",
                "fusion_radius",
                "sparsity_threshold",
                "voxel_feature_dim",
            ):
                self.assertIn(key, manifest.config)
            for view in manifest.views:
                self.assertEqual(len(view.extrinsic.rotation), 9)
                self.assertEqual(len(view.extrinsic.translation), 3)
                self.assertGreater(view.intrinsic.fx, 0)
                self.assertTrue((data / view.image).is_file())
                nodes = {lm.node for lm in view.landmarks}
                self.assertTrue(nodes, "landmarks must declare scene nodes")

    def test_out_of_image_landmarks_exist_for_clipping(self):
        with tempfile.TemporaryDirectory() as tmp:
            data = make_dataset(Path(tmp) / "data")
            manifest = load_manifest(data / "manifest.json")
            out_of_image = 0
            for view in manifest.views:
                for lm in view.landmarks:
                    u, v = lm.pixel
                    if not (0 <= u <= view.width - 1 and 0 <= v <= view.height - 1):
                        out_of_image += 1
            self.assertGreater(out_of_image, 0, "dataset must exercise clipping")


if __name__ == "__main__":
    unittest.main()
