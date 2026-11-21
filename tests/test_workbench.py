from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kde_drift.canonical import canonical_json_bytes
from kde_drift.config import normalize_config
from kde_drift.data import load_dataset
from kde_drift.errors import GracefulStop
from kde_drift.stop import StopController
from kde_drift.workbench import Workbench

from tests.common import fast_config, generate_small_data


class WorkbenchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.temp = tempfile.TemporaryDirectory()
        cls.data_dir = generate_small_data(Path(cls.temp.name) / "data")
        cls.dataset = load_dataset(cls.data_dir, "small")
        cls.config = fast_config()
        cls.baseline_workdir = Path(cls.temp.name) / "baseline"
        cls.baseline = Workbench(cls.dataset, cls.config, cls.baseline_workdir).run()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp.cleanup()

    def test_checkpoint_chain_has_four_monotonic_sequences(self) -> None:
        sequences = [
            self.baseline["records"][stage]["sequence"]
            for stage in ("bandwidth", "density", "metrics", "drift")
        ]
        self.assertEqual(sequences, [1, 2, 3, 4])
        self.assertTrue(Workbench(self.dataset, self.config, Path(self.temp.name) / "baseline").status()["valid"])

    def test_threshold_change_invalidates_only_drift_and_reuses_upstream(self) -> None:
        workdir = Path(self.temp.name) / "threshold-incremental"
        baseline = Workbench(self.dataset, self.config, workdir).run()
        changed_config = fast_config(drift={"threshold": 0.27})
        workbench = Workbench(self.dataset, changed_config, workdir)
        analysis = workbench.analyze()
        self.assertEqual(analysis["earliest_invalid_stage"], "drift")
        self.assertEqual(analysis["reusable_stages"], ["bandwidth", "density", "metrics"])
        self.assertIn(
            "drift.threshold",
            analysis["stages"]["drift"]["reasons"][0]["changed_paths"],
        )
        result = workbench.run()
        self.assertEqual(result["outer_sequence"], 5)
        self.assertEqual(result["records"]["drift"]["stage_revision"], 2)
        for stage in ("bandwidth", "density", "metrics"):
            self.assertEqual(
                result["records"][stage]["sequence"], baseline["records"][stage]["sequence"]
            )
        self.assertEqual(result["records"]["drift"]["sequence"], 5)
        self.assertNotEqual(result["config_version"], baseline["config_version"])

    def test_grid_change_invalidates_density_and_downstream(self) -> None:
        workdir = Path(self.temp.name) / "grid-incremental"
        Workbench(self.dataset, self.config, workdir).run()
        changed = fast_config(grid={"points": 64})
        analysis = Workbench(self.dataset, changed, workdir).analyze()
        self.assertEqual(analysis["earliest_invalid_stage"], "density")
        self.assertEqual(analysis["reusable_stages"], ["bandwidth"])
        self.assertEqual(analysis["affected_stages"], ["density", "metrics", "drift"])

    def test_alpha_change_changes_decision_and_fingerprint(self) -> None:
        workdir = Path(self.temp.name) / "alpha-incremental"
        baseline = Workbench(self.dataset, self.config, workdir).run()
        strict = normalize_config(
            {
                **self.config,
                "drift": {**self.config["drift"], "alpha": 0.001},
            }
        )
        result = Workbench(self.dataset, strict, workdir).run()
        self.assertNotEqual(baseline["config_version"], result["config_version"])
        self.assertNotEqual(
            baseline["summary"]["drift_columns"], result["summary"]["drift_columns"]
        )
        self.assertEqual(result["payloads"]["drift"]["config_version"], result["config_version"])

    def test_graceful_stop_then_resume_is_byte_identical(self) -> None:
        workdir = Path(self.temp.name) / "interrupted"
        workbench = Workbench(self.dataset, self.config, workdir)
        with self.assertRaises(GracefulStop) as context:
            workbench.run(stop=StopController(stop_after_units=self.dataset.columns + 1))
        self.assertEqual(context.exception.stage, "density")
        intermediate = workbench.status()
        self.assertEqual(intermediate["earliest_invalid_stage"], "density")
        self.assertEqual(intermediate["latest_sequence"], 1)
        resumed = workbench.run(stop=StopController())
        for stage in ("density", "metrics", "drift"):
            self.assertEqual(
                canonical_json_bytes(self.baseline["payloads"][stage], strict=True),
                canonical_json_bytes(resumed["payloads"][stage], strict=True),
            )
        self.assertEqual(
            canonical_json_bytes(self.baseline["summary"], strict=True),
            canonical_json_bytes(resumed["summary"], strict=True),
        )
        self.assertEqual(
            [resumed["records"][stage]["sequence"] for stage in ("bandwidth", "density", "metrics", "drift")],
            [1, 2, 3, 4],
        )

    def test_export_is_deterministic_and_contains_numeric_diagnostics(self) -> None:
        first = Workbench(self.dataset, self.config, self.baseline_workdir).export_bytes("density")
        second = Workbench(self.dataset, self.config, self.baseline_workdir).export_bytes("density")
        self.assertEqual(first, second)
        report = Workbench(self.dataset, self.config, self.baseline_workdir).export_bytes("report")
        self.assertIn(b"verified_upper_bound", first)
        self.assertIn(b"drift_report", report)


if __name__ == "__main__":
    unittest.main()
