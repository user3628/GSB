from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tests.support import ROOT


class DemoIntegrationTests(unittest.TestCase):
    def test_repeatable_interop_demo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "demo"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "demo_interop.py"),
                    "--out",
                    str(out),
                ],
                cwd=ROOT,
                env={**os.environ, "PYTHONPATH": str(ROOT)},
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            report = json.loads((out / "demo-report.json").read_text(encoding="utf-8"))
            self.assertTrue(report["ok"])
            verification = report["verification"]
            self.assertTrue(verification["binary_jsonl_bidirectional"])
            self.assertTrue(verification["legacy_degraded"])
            self.assertTrue(verification["future_rejected_without_state_change"])
            self.assertTrue(verification["conflict_override_and_skip"])
            self.assertTrue(verification["atomic_batch_rollback"])
            self.assertTrue(verification["crash_recovery"])
            self.assertEqual(verification["baseline_alert"], "CRITICAL")
            preflight = report["steps"]["preflight"]
            self.assertEqual(preflight["legacy-v1.bin"]["verdict"], "DEGRADE")
            self.assertEqual(
                preflight["future-v3-required.jsonl"]["business_code"],
                "E_FORMAT_UNSUPPORTED",
            )


if __name__ == "__main__":
    unittest.main()
