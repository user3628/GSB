from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from helpers import make_workspace


class CliTests(unittest.TestCase):
    def test_pause_file_injection_returns_130_then_resume_succeeds(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "workspace")
            pause = Path(temp) / "pause.flag"
            pause.write_text("stop", encoding="utf-8")
            stopped = _cli(["build", "--workspace", str(root), "--pause-file", str(pause), "--json"])
            self.assertEqual(stopped.returncode, 130, stopped.stderr)
            payload = json.loads(stopped.stdout)
            self.assertTrue(payload["interrupted"])
            self.assertIn("resume_command", payload)
            self.assertFalse((root / "outputs" / "alpha.png").exists())

            pause.unlink()
            resumed = _cli(["build", "--workspace", str(root), "--json"])
            self.assertEqual(resumed.returncode, 0, resumed.stderr)
            status = _cli(["status", "--workspace", str(root), "--json"])
            self.assertEqual(status.returncode, 0, status.stderr)
            status_payload = json.loads(status.stdout)
            self.assertTrue(all(not stage["invalid"] for shard in status_payload["shards"] for stage in shard["stages"]))
            self.assertEqual(status_payload["shards"][0]["reuse_plan"], ["encode", "schedule", "sample", "quantize"])

    def test_cli_emits_missing_reference_code_and_exit_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = make_workspace(Path(temp) / "workspace", missing_reference=True)
            result = _cli(["build", "--workspace", str(root), "--json"])
            self.assertEqual(result.returncode, 3)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["code"], "E_MISSING_REFERENCE")
            self.assertEqual(payload["shard"], "beta")
            self.assertFalse((root / "outputs" / "alpha.png").exists())


def _cli(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    return subprocess.run(
        [sys.executable, "-m", "detdiff.cli", *arguments],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
        check=False,
    )


if __name__ == "__main__":
    unittest.main()

