from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from detdiff.demo import initialize_demo, run_demo
from detdiff.pipeline import build_workspace, published_manifest
from detdiff.config import load_config


class DemoAndErrorProbeTests(unittest.TestCase):
    def test_demo_performs_stop_resume_incremental_update_and_error_probes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "demo"
            result = run_demo(root, reset=True)
            self.assertEqual(result["resume_from"], "schedule")
            self.assertEqual(result["interrupted_build"]["interrupted_after"], {"shard": "aurora-en", "stage": "encode"})
            self.assertTrue(result["resumed_build"]["consistency_verified"])
            self.assertTrue(result["baseline_full_consistency"]["ok"])
            self.assertEqual(result["baseline_full_consistency"]["files_compared"], 20)
            self.assertEqual(
                result["incremental_update"]["build"]["rebuilt"]["mountain-zh"],
                ["encode", "schedule", "sample", "quantize"],
            )
            self.assertTrue(result["post_update_full_consistency"]["ok"])
            self.assertEqual(result["error_code_probes"]["invalid_blend"]["code"], "E_INVALID_CONFIG")
            self.assertEqual(result["error_code_probes"]["invalid_guidance"]["code"], "E_INVALID_CONFIG")
            self.assertEqual(result["error_code_probes"]["missing_reference"]["code"], "E_MISSING_REFERENCE")
            diagnostics = Path(result["diagnostics_html"])
            self.assertTrue(diagnostics.is_file())
            html = diagnostics.read_text("utf-8")
            self.assertIn("detdiff diagnostics", html)
            self.assertNotIn("http://", html)
            self.assertNotIn("https://", html)
            report = Path(result["report_path"])
            self.assertTrue(report.is_file())

    def test_fixed_seed_demo_fixture_builds_to_identical_manifests(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            first = initialize_demo(Path(temp) / "first", reset=True)
            second = initialize_demo(Path(temp) / "second", reset=True)
            build_workspace(first)
            build_workspace(second)
            first_manifest = published_manifest(load_config(first / "config.yaml"))
            second_manifest = published_manifest(load_config(second / "config.yaml"))
            self.assertEqual(first_manifest, second_manifest)


if __name__ == "__main__":
    unittest.main()

