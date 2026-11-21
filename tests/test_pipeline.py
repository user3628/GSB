"""Pipeline invalidation propagation and fingerprint observability."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.helpers import make_dataset, make_engine


class PipelineInvalidationTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.data = make_dataset(root / "data")
        self.engine = make_engine(root / "ws", self.data)
        self.engine.build(mode="full")

    def tearDown(self):
        self._tmp.cleanup()

    def _rebuilt(self, report):
        return set(report["rebuilt"])

    def test_no_change_build_reuses_everything(self):
        report = self.engine.build(mode="incremental")
        self.assertEqual(report["rebuilt"], [])
        self.assertEqual(len(report["reused"]), 10)

    def test_sparsity_threshold_invalidates_only_sparsify(self):
        self.engine.update_config({"sparsity_threshold": 0.1})
        report = self.engine.build(mode="incremental")
        self.assertEqual(self._rebuilt(report), {"sparsify"})
        reasons = self._reasons_for(report, "sparsify")
        self.assertIn("sparsity_threshold", self._changed_keys(reasons))

    def test_fusion_radius_invalidates_fusion_and_sparsify(self):
        self.engine.update_config({"fusion_radius": 2.0})
        report = self.engine.build(mode="incremental")
        self.assertEqual(self._rebuilt(report), {"fusion", "sparsify"})

    def test_voxel_resolution_invalidates_from_voxelize_down(self):
        self.engine.update_config({"voxel_resolution": 0.4})
        report = self.engine.build(mode="incremental")
        expected = {f"voxelize:view_0{i}" for i in range(4)} | {"fusion", "sparsify"}
        self.assertEqual(self._rebuilt(report), expected)
        reasons = self._reasons_for(report, "voxelize:view_00")
        self.assertIn("voxel_resolution", self._changed_keys(reasons))

    def test_truncation_distance_invalidates_projection_down(self):
        self.engine.update_config({"truncation_distance": 0.5})
        report = self.engine.build(mode="incremental")
        self.assertEqual(len(self._rebuilt(report)), 10)

    def test_replace_view_invalidates_single_view_chain(self):
        alt = self.data / "shards_alt" / "view_02.png"
        self.engine.replace_view("view_02", alt)
        report = self.engine.build(mode="incremental")
        self.assertEqual(
            self._rebuilt(report),
            {"projection:view_02", "voxelize:view_02", "fusion", "sparsify"},
        )
        reasons = self._reasons_for(report, "projection:view_02")
        kinds = {reason["kind"] for reason in reasons}
        self.assertIn("input_changed", kinds)
        self.assertIn("image_sha256", self._changed_keys(reasons))

    def test_fingerprints_observable_in_status(self):
        self.engine.update_config({"sparsity_threshold": 0.2})
        status = self.engine.status()
        by_instance = {s["instance"]: s for s in status["stages"]}
        self.assertEqual(by_instance["sparsify"]["status"], "stale")
        self.assertEqual(by_instance["fusion"]["status"], "published")
        self.assertNotEqual(
            by_instance["sparsify"]["desired_fingerprint"],
            by_instance["sparsify"]["published_fingerprint"],
        )
        self.engine.build(mode="incremental")
        status = self.engine.status()
        for stage in status["stages"]:
            self.assertEqual(stage["status"], "published")
            self.assertEqual(
                stage["desired_fingerprint"], stage["published_fingerprint"]
            )

    def test_invalidation_report_persisted(self):
        self.engine.update_config({"voxel_resolution": 0.4})
        self.engine.build(mode="incremental")
        status = self.engine.status()
        report = status["invalidation"]
        self.assertIsNotNone(report)
        rebuilt_entry = next(
            item for item in report["instances"] if item["instance"] == "voxelize:view_00"
        )
        keys = {r["key"] for r in rebuilt_entry["reasons"] if r["kind"] == "config_changed"}
        self.assertIn("voxel_resolution", keys)

    @staticmethod
    def _reasons_for(report, instance):
        entry = next(item for item in report["instances"] if item["instance"] == instance)
        return entry.get("reasons", [])

    @staticmethod
    def _changed_keys(reasons):
        return {r["key"] for r in reasons if r["kind"] == "config_changed"}


if __name__ == "__main__":
    unittest.main()
