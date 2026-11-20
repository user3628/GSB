from __future__ import annotations

import csv
import json
import unittest

from dimcluster.samples import generate_samples, verify_samples

from .helpers import workspace


class SampleGenerationTests(unittest.TestCase):
    def test_three_tiers_and_manifest(self) -> None:
        root = workspace()
        summary = generate_samples(root / "samples")
        self.assertEqual([item["rows"] for item in summary["tiers"]], [400, 4000, 20000])
        self.assertEqual([item["columns"] for item in summary["tiers"]], [8, 24, 48])
        verification = verify_samples(root / "samples" / "manifest.json")
        self.assertTrue(verification["ok"])
        manifest = json.loads((root / "samples" / "manifest.json").read_text())
        for item in manifest["files"]:
            semantics = item["column_semantics"]
            self.assertTrue(any(column["kind"] == "continuous" for column in semantics))
            self.assertTrue(any(column["kind"] == "categorical" for column in semantics))
            self.assertTrue(any(column["redundant_of"] for column in semantics))
            self.assertTrue(any(column["detected"]["missing_count"] > 0 for column in semantics))
            with (root / "samples" / item["file"]).open(newline="") as handle:
                reader = csv.reader(handle)
                header = next(reader)
                rows = sum(1 for _ in reader)
            self.assertEqual(rows, item["rows"])
            self.assertEqual(len(header), item["columns"])

    def test_regeneration_is_byte_identical(self) -> None:
        root = workspace()
        first = generate_samples(root / "samples")
        manifest_one = (root / "samples" / "manifest.json").read_bytes()
        second = generate_samples(root / "samples")
        manifest_two = (root / "samples" / "manifest.json").read_bytes()
        self.assertEqual(first["tiers"], second["tiers"])
        self.assertEqual(manifest_one, manifest_two)


if __name__ == "__main__":
    unittest.main()
