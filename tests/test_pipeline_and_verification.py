import tempfile
import unittest
from pathlib import Path

from shotcheck.generator import generate_all
from shotcheck.pipeline import Pipeline, StopController, GracefulStop
from shotcheck.util import read_json
from shotcheck.verification import verify_all


class PipelineIntegrationTests(unittest.TestCase):
    def test_two_tiers_and_reference_truth(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            generate_all(root, seed=20261123)
            for tier, expected_count in (("small", 14), ("large", 51)):
                workspace = root / tier
                run = Pipeline(workspace).run()
                result = run["result"]
                reference = read_json(workspace / "inputs/reference.json")
                self.assertEqual(len(reference["shots"]), expected_count)
                for shot_id, reference_shot in reference["shots"].items():
                    outcome = result["outcomes"][shot_id]
                    self.assertEqual(
                        outcome["verdict"],
                        reference_shot["expected_verdict"],
                        shot_id,
                    )
                    actual_codes = outcome["error_codes"]
                    self.assertEqual(
                        actual_codes, reference_shot["expected_error_codes"], shot_id
                    )
                    if reference_shot["expected_coverage"] is not None:
                        self.assertEqual(
                            result["shots"][shot_id]["coverage"],
                            reference_shot["expected_coverage"]["coverage"],
                            shot_id,
                        )
                        self.assertEqual(
                            result["shots"][shot_id]["score"],
                            reference_shot["expected_coverage"]["score"],
                            shot_id,
                        )
                self.assertTrue(result["strategy_parity"]["all_equal"])
                first_bytes = (workspace / "exports/result.json").read_bytes()
                reused = Pipeline(workspace).run()
                self.assertEqual(
                    reused["run_meta"]["invalidation"]["reused_stages"],
                    ["normalize", "duration", "coverage", "strategy"],
                )
                self.assertEqual(
                    (workspace / "exports/result.json").read_bytes(), first_bytes
                )

    def test_small_scale_avoids_truncation_and_large_selects_chunk(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            generate_all(root, seed=20261123)
            small = Pipeline(root / "small").run()["result"]
            large = Pipeline(root / "large").run()["result"]
            self.assertEqual(small["strategy_selection"]["strategy"], "exhaustive")
            truncated = next(
                item
                for item in small["strategy_selection"]["candidates"]
                if item["strategy"] == "keyword_priority_truncated"
            )
            self.assertEqual(truncated["status"], "rejected")
            self.assertIn("small_scale", truncated["rejection_reason"])
            self.assertEqual(
                large["strategy_selection"]["strategy"], "chunk_incremental"
            )
            self.assertTrue(large["strategy_parity"]["all_equal"])

    def test_graceful_stop_and_resume_byte_identical(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            generate_all(root, seed=20261123)
            workspace = root / "large"
            baseline = Pipeline(workspace).run()
            baseline_bytes = (workspace / "exports/result.json").read_bytes()
            result_path = workspace / "exports/result.json"
            result_path.unlink()
            for directory in ("checkpoints", "objects"):
                for item in (workspace / directory).glob("*"):
                    if item.is_file():
                        item.unlink()
            controller = StopController()
            controller.configure_target("coverage", 2)
            with self.assertRaises(GracefulStop) as stopped:
                Pipeline(workspace, stop_controller=controller).run()
            self.assertEqual(stopped.exception.stage, "coverage")
            self.assertEqual(stopped.exception.completed_shots, 2)
            self.assertFalse(result_path.exists())
            resumed = Pipeline(workspace).run()
            self.assertEqual(
                resumed["run_meta"]["invalidation"]["earliest_stage"], "coverage"
            )
            self.assertEqual(result_path.read_bytes(), baseline_bytes)

    def test_full_verification_report_is_green(self):
        with tempfile.TemporaryDirectory() as temp:
            report = verify_all(temp, seed=20261123)
            self.assertTrue(report["all_passed"], report)
            self.assertTrue((Path(temp) / "verification/report.json").exists())
            self.assertTrue((Path(temp) / "verification/report.md").exists())


if __name__ == "__main__":
    unittest.main()
