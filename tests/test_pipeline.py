from __future__ import annotations

import copy
import unittest

from dimcluster.config import load_config
from dimcluster.errors import WorkbenchError
from dimcluster.pipeline import Workbench
from dimcluster.samples import generate_samples

from .helpers import small_config, workspace


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = workspace()
        generate_samples(cls.root / "samples")
        cls.manifest = cls.root / "samples" / "manifest.json"
        cls.config = small_config(cls.manifest, ["small"])

    def test_stop_resume_report_matches_clean_full(self) -> None:
        baseline_state = self.root / "baseline-state"
        baseline_out = self.root / "baseline-out"
        baseline = Workbench(
            self.root,
            copy.deepcopy(self.config),
            state_dir=baseline_state,
            output_dir=baseline_out,
            manifest_path=self.manifest,
        )
        baseline.run(mode="full")
        baseline_bytes = baseline.report_bytes()

        resumed_state = self.root / "resumed-state"
        resumed_out = self.root / "resumed-out"
        resumed = Workbench(
            self.root,
            copy.deepcopy(self.config),
            state_dir=resumed_state,
            output_dir=resumed_out,
            manifest_path=self.manifest,
        )
        stopped = resumed.run(mode="full", stop_after_tier="small")
        self.assertTrue(stopped["stopped"])
        self.assertFalse(stopped["report_written"])
        self.assertFalse((resumed_out / "diagnosis_report.json").exists())
        completed = resumed.run(mode="auto", verify_full=True)
        self.assertTrue(completed["report_written"])
        self.assertTrue(completed["incremental_verification"]["ok"])
        self.assertEqual(resumed.report_bytes(), baseline_bytes)

    def test_incremental_reuse_and_full_verification(self) -> None:
        state_dir = self.root / "incremental-state"
        out_dir = self.root / "incremental-out"
        initial = Workbench(
            self.root,
            copy.deepcopy(self.config),
            state_dir=state_dir,
            output_dir=out_dir,
            manifest_path=self.manifest,
        )
        first = initial.run(mode="full")
        self.assertEqual(first["recomputed_stage_count"], 5)

        changed_config = copy.deepcopy(self.config)
        changed_config["clustering"]["n_clusters"] = 4
        changed = Workbench(
            self.root,
            changed_config,
            state_dir=state_dir,
            output_dir=out_dir,
            manifest_path=self.manifest,
        )
        result = changed.run(mode="auto", verify_full=True)
        self.assertEqual(result["earliest_recomputed_stage"]["stage"], "cluster")
        self.assertEqual(result["reused_stage_count"], 2)
        self.assertEqual(result["recomputed_stage_count"], 3)
        self.assertTrue(result["incremental_verification"]["ok"])
        self.assertTrue(result["incremental_verification"]["report_byte_identical"])
        self.assertEqual(changed.state.verify_integrity()["ok"], True)
        active = changed.state.load_active_config()
        self.assertEqual(active["clustering"]["n_clusters"], 4)

    def test_incremental_mismatch_returns_error_with_details(self) -> None:
        state_dir = self.root / "mismatch-state"
        out_dir = self.root / "mismatch-out"
        workbench = Workbench(
            self.root,
            copy.deepcopy(self.config),
            state_dir=state_dir,
            output_dir=out_dir,
            manifest_path=self.manifest,
        )
        workbench.run(mode="full")
        cluster_record = workbench.state.load_latest("cluster", "small")
        self.assertIsNotNone(cluster_record)
        tier_results = {"small": {"cluster": cluster_record}}
        with self.assertRaises(WorkbenchError) as raised:
            workbench._verify_against_full(b"{}", tier_results)
        self.assertEqual(raised.exception.code, "E_INCREMENTAL_MISMATCH")
        self.assertIn("differences", raised.exception.details)
        self.assertGreater(raised.exception.details["difference_count"], 0)

    def test_report_query_rejects_config_from_another_run(self) -> None:
        state_dir = self.root / "stale-report-state"
        out_dir = self.root / "stale-report-out"
        workbench = Workbench(
            self.root,
            copy.deepcopy(self.config),
            state_dir=state_dir,
            output_dir=out_dir,
            manifest_path=self.manifest,
        )
        workbench.run(mode="full")
        changed = copy.deepcopy(self.config)
        changed["clustering"]["n_clusters"] = 4
        other = Workbench(
            self.root,
            changed,
            state_dir=state_dir,
            output_dir=out_dir,
            manifest_path=self.manifest,
        )
        with self.assertRaises(WorkbenchError) as raised:
            other.report()
        self.assertEqual(raised.exception.code, "E_INVALID_STAGE")

    def test_reset_removes_stale_report_before_stopped_unit(self) -> None:
        state_dir = self.root / "reset-stale-state"
        out_dir = self.root / "reset-stale-out"
        workbench = Workbench(
            self.root,
            copy.deepcopy(self.config),
            state_dir=state_dir,
            output_dir=out_dir,
            manifest_path=self.manifest,
        )
        workbench.run(mode="full")
        self.assertTrue((out_dir / "diagnosis_report.json").exists())
        stopped = workbench.run(mode="full", stop_after_tier="small")
        self.assertTrue(stopped["stopped"])
        self.assertFalse(stopped["report_written"])
        self.assertFalse((out_dir / "diagnosis_report.json").exists())

    def test_invalid_config_code(self) -> None:
        with self.assertRaises(WorkbenchError) as raised:
            load_config(overrides=["reduction.target_dimension=1"])
        self.assertEqual(raised.exception.code, "E_INVALID_CONFIG")

    def test_report_marks_dominant_small_tier(self) -> None:
        state_dir = self.root / "report-state"
        out_dir = self.root / "report-out"
        workbench = Workbench(
            self.root,
            copy.deepcopy(self.config),
            state_dir=state_dir,
            output_dir=out_dir,
            manifest_path=self.manifest,
        )
        workbench.run(mode="full")
        report = workbench.report()
        diagnosis = report["tiers"]["small"]
        self.assertTrue(diagnosis["triggers"]["dominant_cluster_ratio_exceeded"])
        self.assertTrue(diagnosis["fallback"]["required"])
        self.assertEqual(report["overall"]["flagged_tiers"], ["small"])

    def test_fingerprints_expose_stage_versions(self) -> None:
        state_dir = self.root / "fingerprint-state"
        out_dir = self.root / "fingerprint-out"
        workbench = Workbench(
            self.root,
            copy.deepcopy(self.config),
            state_dir=state_dir,
            output_dir=out_dir,
            manifest_path=self.manifest,
        )
        workbench.run(mode="full")
        fingerprints = workbench.fingerprints()
        self.assertIn("preprocess", fingerprints["tiers"]["small"])
        self.assertIsNotNone(
            fingerprints["tiers"]["small"]["cluster"]["checkpoint"]
        )
        self.assertEqual(
            fingerprints["config_sha256"], workbench.config_sha256
        )


if __name__ == "__main__":
    unittest.main()
