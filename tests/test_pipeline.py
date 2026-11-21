from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from contract_elements.canonical import read_json
from contract_elements.pipeline import BatchEngine

from .common import config, paths


OUTPUT_FILES = (
    "results.jsonl",
    "candidates.jsonl",
    "conflicts.jsonl",
    "errors.jsonl",
    "summary.json",
    "experiment_summary.json",
)


class PipelineTests(unittest.TestCase):
    def test_batch_metrics_fingerprint_and_byte_determinism(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = BatchEngine(config(), paths(root / "out1", root / "run1")).run()
            second = BatchEngine(config(), paths(root / "out2", root / "run2")).run()
            self.assertEqual(first["status"], "completed")
            summary = read_json(root / "out1/summary.json")
            self.assertEqual(summary["metrics"]["element_extraction"]["precision"], "1.000000")
            self.assertEqual(summary["metrics"]["element_extraction"]["recall"], "1.000000")
            self.assertEqual(summary["metrics"]["conflict_resolution"]["accuracy"], "1.000000")
            self.assertEqual(summary["metrics"]["version_compatibility"]["success_rate"], "1.000000")
            self.assertEqual(summary["counts"]["input_records"], 36)
            self.assertEqual(summary["counts"]["contracts"], 9)
            self.assertTrue(summary["version_fingerprint"])
            for name in OUTPUT_FILES:
                self.assertEqual(
                    (root / "out1" / name).read_bytes(),
                    (root / "out2" / name).read_bytes(),
                    name,
                )

    def test_stop_and_resume_matches_full_run_byte_for_byte(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            full = BatchEngine(config(), paths(root / "full", root / "full-runtime")).run()
            stopped = BatchEngine(config(), paths(root / "resume", root / "resume-runtime")).run(stop_after_units=7)
            resumed = BatchEngine(config(), paths(root / "resume", root / "resume-runtime")).run(resume=True, clear_stop=True)
            self.assertEqual(full["status"], "completed")
            self.assertEqual(stopped["status"], "stopped")
            self.assertEqual(stopped["seq"], 7)
            self.assertEqual(resumed["status"], "completed")
            for name in OUTPUT_FILES:
                self.assertEqual(
                    (root / "full" / name).read_bytes(),
                    (root / "resume" / name).read_bytes(),
                    name,
                )


if __name__ == "__main__":
    unittest.main()

