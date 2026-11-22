from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from slo_budget import codec
from slo_budget.errors import SloError

from tests.support import (
    CURRENT_BIN,
    FUTURE_BIN,
    FUTURE_JSONL,
    LEGACY_BIN,
    LEGACY_JSONL,
    ROOT,
    SAMPLES,
    load_samples_summary,
    sha256,
)


class SampleIntegrityTests(unittest.TestCase):
    def test_pinned_sha256_values_match_files(self) -> None:
        summary = load_samples_summary()
        for relative_path, details in summary["files"].items():
            path = SAMPLES / relative_path
            self.assertTrue(path.exists(), relative_path)
            self.assertEqual(path.stat().st_size, details["bytes"], relative_path)
            self.assertEqual(sha256(path), details["sha256"], relative_path)

    def test_pinned_business_codes_are_reproducible(self) -> None:
        expectations = load_samples_summary()["expectations"]
        self.assertEqual(expectations["windows/legacy-v1.bin"]["verdict"], "DEGRADE")
        self.assertEqual(expectations["windows/legacy-v1.jsonl"]["verdict"], "DEGRADE")
        self.assertEqual(
            expectations["windows/future-v3-required.bin"]["business_code"],
            "E_FORMAT_UNSUPPORTED",
        )
        self.assertEqual(
            expectations["windows/future-v3-required.jsonl"]["business_code"],
            "E_FORMAT_UNSUPPORTED",
        )
        for relative_path in (
            "corrupt/current-v2-truncated.bin",
            "corrupt/current-v2-bad-crc.bin",
            "corrupt/current-v2-truncated.jsonl",
        ):
            self.assertEqual(expectations[relative_path]["business_code"], "E_RECORD_CORRUPT")
            with self.assertRaises(SloError) as caught:
                codec.decode(SAMPLES / relative_path)
            self.assertEqual(caught.exception.code, "E_RECORD_CORRUPT")
        self.assertEqual(
            expectations["invalid/count-invalid.jsonl"]["business_code"],
            "E_COUNT_INVALID",
        )
        self.assertEqual(
            expectations["conflicts/merge-reject.jsonl"]["business_code"],
            "E_IMPORT_CONFLICT",
        )

    def test_generator_is_byte_for_byte_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "samples"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "generate_samples.py"),
                    "--out",
                    str(out),
                    "--clean",
                ],
                cwd=ROOT,
                env={**os.environ, "PYTHONPATH": str(ROOT)},
                capture_output=True,
                text=True,
                timeout=60,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            summary = load_samples_summary()
            for relative_path in summary["files"]:
                original = SAMPLES / relative_path
                regenerated = out / relative_path
                self.assertTrue(regenerated.exists(), relative_path)
                self.assertEqual(original.read_bytes(), regenerated.read_bytes(), relative_path)
            self.assertEqual(
                (SAMPLES / "samples.json").read_bytes(),
                (out / "samples.json").read_bytes(),
            )
            self.assertEqual(
                (SAMPLES / "SHA256SUMS").read_bytes(),
                (out / "SHA256SUMS").read_bytes(),
            )


if __name__ == "__main__":
    unittest.main()
