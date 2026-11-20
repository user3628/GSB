from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from rateguard.samples import TIERS, generate_all, load_batches


class SampleGeneratorTests(unittest.TestCase):
    def test_three_scales_and_repeatable_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
            first = generate_all(first_dir)
            second = generate_all(second_dir)
            self.assertEqual(first, second)
            self.assertEqual(set(first["tiers"]), {"small", "medium", "large"})
            for name, spec in TIERS.items():
                metadata = first["tiers"][name]
                batches = load_batches(Path(first_dir) / f"{name}.jsonl")
                self.assertEqual(metadata["seed"], spec.seed)
                self.assertEqual(metadata["event_count"], spec.event_count)
                self.assertEqual(sum(len(batch["events"]) for batch in batches), spec.event_count)
                self.assertEqual(
                    (Path(first_dir) / f"{name}.jsonl").read_bytes(),
                    (Path(second_dir) / f"{name}.jsonl").read_bytes(),
                )
                routes = {event["route"] for batch in batches for event in batch["events"]}
                self.assertEqual(routes, set(metadata["routes"]))

    def test_manifest_is_stable_json(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = generate_all(directory)
            loaded = json.loads((Path(directory) / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest, loaded)
            self.assertEqual(loaded["rng"], "numpy.random.Generator(PCG64)")


if __name__ == "__main__":
    unittest.main()

