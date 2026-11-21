from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from .common import ROOT


class CliTests(unittest.TestCase):
    def test_parse_and_v1_import_commands(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            parse = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "contract_elements.cli",
                    "parse",
                    "--contract-id",
                    "C-ALPHA-01",
                    "--output",
                    str(root / "output"),
                    "--runtime",
                    str(root / "runtime"),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )
            parsed = json.loads(parse.stdout)
            self.assertEqual(parsed["contract_id"], "C-ALPHA-01")
            self.assertTrue(parsed["conflicts"])

            imported = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "contract_elements.cli",
                    "import-v1",
                    "--output",
                    str(root / "v1-export.jsonl"),
                    "--internal-output",
                    str(root / "internal-v2.json"),
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=True,
            )
            result = json.loads(imported.stdout)
            self.assertEqual(result["status"], "ok")
            self.assertTrue(result["compatibility"]["success"])
            self.assertTrue((root / "v1-export.jsonl").exists())


if __name__ == "__main__":
    unittest.main()

