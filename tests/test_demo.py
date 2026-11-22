from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from telemetry_align.demo import run_demo


FIXED_SUMMARY_DIGEST = "716aaa760d79c41de6ebffb7f87467eed99b7e29bac84a21c7381e44cf728b16"


class DemoTests(unittest.TestCase):
    def test_repeatable_demo_contains_incremental_and_recovery_proofs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            report = run_demo(Path(directory) / "demo")
            self.assertEqual(report["fixed_summary_digest"], FIXED_SUMMARY_DIGEST)
            self.assertEqual(report["initial"]["counts"]["grid_points"], 31)
            self.assertEqual(report["initial"]["counts"]["point_gap_count"], 3)
            self.assertEqual(report["initial"]["counts"]["conflict_count"], 52)
            self.assertEqual(report["initial"]["checkpoints"]["sequence"], [1, 2, 3, 4])
            self.assertEqual(
                report["incremental"]["earliest_invalid_stage"], "normalize"
            )
            self.assertIn(
                "resample:sensor_b", report["incremental"]["reused_artifacts"]
            )
            self.assertIn(
                "resample:sensor_c", report["incremental"]["reused_artifacts"]
            )
            self.assertEqual(
                report["incremental"]["checkpoint_sequence"], list(range(1, 9))
            )
            self.assertNotEqual(
                report["incremental"]["summary_digest_before"],
                report["incremental"]["summary_digest_after"],
            )
            stop_resume = report["stop_resume"]
            self.assertEqual(stop_resume["stopped_status"], "stopped")
            self.assertEqual(stop_resume["stopped_head_stage"], "resample")
            self.assertEqual(stop_resume["resumed_status"], "completed")
            self.assertTrue(stop_resume["checkpoint_sequence_contiguous"])
            self.assertTrue(stop_resume["grid_bytes_equal"])
            self.assertTrue(stop_resume["fusion_bytes_equal"])
            self.assertTrue(stop_resume["summary_bytes_equal"])
            self.assertTrue(Path(report["report_path"]).exists())
            self.assertTrue(report["v1_compatibility"]["upgrade_equivalent"])
            self.assertTrue(report["v1_compatibility"]["grid_matches_native_v2"])
            self.assertTrue(report["v1_compatibility"]["fusion_matches_native_v2"])


if __name__ == "__main__":
    unittest.main()

