from __future__ import annotations

import unittest
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

from contract_elements.canonical import atomic_write_jsonl, read_json, read_jsonl
from contract_elements.pipeline import BatchEngine, DatasetPaths

from .common import ROOT, config, paths


class VersioningTests(unittest.TestCase):
    def test_priority_change_reuses_normalize_and_extract(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            p = paths(root / "out1", root / "runtime")
            BatchEngine(config(), p).run()
            before = read_json(root / "runtime/version_state.json")

            changed = config()
            changed["arbitration"]["clause_priority"]["1"] = 1
            BatchEngine(changed, paths(root / "out2", root / "runtime")).run()
            after = read_json(root / "runtime/version_state.json")

            self.assertEqual(after["invalidations"][0]["stage"], "arbitrate")
            self.assertEqual(after["invalidations"][0]["reason"], "arbitration_priority_or_policy_changed")
            self.assertEqual(before["stage_digests"]["normalize"], after["stage_digests"]["normalize"])
            self.assertEqual(before["stage_digests"]["extract"], after["stage_digests"]["extract"])
            self.assertNotEqual(before["stage_digests"]["arbitrate"], after["stage_digests"]["arbitrate"])
            self.assertEqual(after["cache"]["normalize"]["hit"], 36)
            self.assertEqual(after["cache"]["extract"]["hit"], 36)
            self.assertEqual(after["cache"]["arbitrate"]["hit"], 0)

    def test_seed_change_invalidates_extract_and_downstream(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            seed_path = root / "seeds.jsonl"
            original_seeds = read_jsonl(ROOT / "data/seeds.jsonl")
            atomic_write_jsonl(seed_path, original_seeds)
            p = DatasetPaths(
                input_path=ROOT / "data/contracts.jsonl",
                seed_path=seed_path,
                dictionary_path=ROOT / "data/elements.json",
                v1_path=ROOT / "data/annotations_v1.jsonl",
                gold_path=ROOT / "data/gold_conflicts.json",
                output_dir=root / "out1",
                runtime_dir=root / "runtime",
            )
            BatchEngine(config(), p).run()
            before = read_json(root / "runtime/version_state.json")

            modified = deepcopy(original_seeds)
            modified[-1]["annotations"][0]["normalized_value"] = "0.056000"
            atomic_write_jsonl(seed_path, modified)
            second_paths = DatasetPaths(
                input_path=ROOT / "data/contracts.jsonl",
                seed_path=seed_path,
                dictionary_path=ROOT / "data/elements.json",
                v1_path=ROOT / "data/annotations_v1.jsonl",
                gold_path=ROOT / "data/gold_conflicts.json",
                output_dir=root / "out2",
                runtime_dir=root / "runtime",
            )
            BatchEngine(config(), second_paths).run()
            after = read_json(root / "runtime/version_state.json")
            self.assertEqual(after["invalidations"][0]["stage"], "extract")
            self.assertEqual(after["invalidations"][0]["reason"], "seed_annotation_changed")
            self.assertEqual(before["stage_digests"]["normalize"], after["stage_digests"]["normalize"])
            self.assertNotEqual(before["stage_digests"]["extract"], after["stage_digests"]["extract"])
            self.assertEqual(after["cache"]["normalize"]["hit"], 36)

    def test_text_change_invalidates_from_normalize(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "contracts.jsonl"
            original = read_jsonl(ROOT / "data/contracts.jsonl")
            atomic_write_jsonl(input_path, original)
            p = DatasetPaths(
                input_path=input_path,
                seed_path=ROOT / "data/seeds.jsonl",
                dictionary_path=ROOT / "data/elements.json",
                v1_path=ROOT / "data/annotations_v1.jsonl",
                gold_path=ROOT / "data/gold_conflicts.json",
                output_dir=root / "out1",
                runtime_dir=root / "runtime",
            )
            BatchEngine(config(), p).run()
            before = read_json(root / "runtime/version_state.json")
            expanded = deepcopy(original)
            extra = deepcopy(original[0])
            extra["contract_id"] = "C-EXTRA-10"
            extra["record_id"] = "EXTRA01"
            extra["clause_no"] = 1
            expanded.append(extra)
            atomic_write_jsonl(input_path, expanded)
            second_paths = DatasetPaths(
                input_path=input_path,
                seed_path=ROOT / "data/seeds.jsonl",
                dictionary_path=ROOT / "data/elements.json",
                v1_path=ROOT / "data/annotations_v1.jsonl",
                gold_path=ROOT / "data/gold_conflicts.json",
                output_dir=root / "out2",
                runtime_dir=root / "runtime",
            )
            BatchEngine(config(), second_paths).run()
            after = read_json(root / "runtime/version_state.json")
            self.assertEqual(after["invalidations"][0]["stage"], "normalize")
            self.assertEqual(after["invalidations"][0]["reason"], "clause_text_or_identity_changed")
            self.assertNotEqual(before["stage_digests"]["normalize"], after["stage_digests"]["normalize"])
            self.assertEqual(after["cache"]["normalize"]["hit"], 36)
            self.assertEqual(after["cache"]["normalize"]["miss"], 1)

    def test_weight_and_precision_change_invalidates_extract(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            BatchEngine(config(), paths(root / "out1", root / "runtime")).run()
            before = read_json(root / "runtime/version_state.json")
            changed = config()
            changed["extraction"]["regex_weight_overrides"]["RATIO_ENGLISH"] = "0.100000"
            changed["extraction"]["ratio_scale"] = 8
            BatchEngine(changed, paths(root / "out2", root / "runtime")).run()
            after = read_json(root / "runtime/version_state.json")
            self.assertEqual(after["invalidations"][0]["stage"], "extract")
            self.assertEqual(before["stage_digests"]["normalize"], after["stage_digests"]["normalize"])
            self.assertNotEqual(before["stage_digests"]["extract"], after["stage_digests"]["extract"])

    def test_effective_date_change_reuses_normalize_and_extract(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            input_path = root / "contracts.jsonl"
            original = read_jsonl(ROOT / "data/contracts.jsonl")
            atomic_write_jsonl(input_path, original)
            p = DatasetPaths(
                input_path=input_path,
                seed_path=ROOT / "data/seeds.jsonl",
                dictionary_path=ROOT / "data/elements.json",
                v1_path=ROOT / "data/annotations_v1.jsonl",
                gold_path=ROOT / "data/gold_conflicts.json",
                output_dir=root / "out1",
                runtime_dir=root / "runtime",
            )
            BatchEngine(config(), p).run()
            before = read_json(root / "runtime/version_state.json")
            changed_rows = deepcopy(original)
            changed_rows[0]["effective_date"] = "2025-03-16"
            atomic_write_jsonl(input_path, changed_rows)
            BatchEngine(config(), p).run()
            after = read_json(root / "runtime/version_state.json")
            self.assertEqual(after["invalidations"][0]["stage"], "arbitrate")
            self.assertEqual(after["invalidations"][0]["reason"], "effective_date_changed")
            self.assertEqual(before["stage_digests"]["normalize"], after["stage_digests"]["normalize"])
            self.assertEqual(before["stage_digests"]["extract"], after["stage_digests"]["extract"])
            self.assertNotEqual(before["stage_digests"]["arbitrate"], after["stage_digests"]["arbitrate"])
            self.assertEqual(after["cache"]["normalize"]["hit"], 36)
            self.assertEqual(after["cache"]["extract"]["hit"], 36)


if __name__ == "__main__":
    unittest.main()

