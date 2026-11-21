from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from contract_elements.canonical import read_jsonl, sha256_file
from contract_elements.pipeline import BatchEngine

from .common import ROOT, config, paths


class InjectionTests(unittest.TestCase):
    def test_all_injected_business_errors_are_stable_and_sources_unchanged(self) -> None:
        config_before = sha256_file(ROOT / "config/default.json")
        input_before = sha256_file(ROOT / "data/contracts.jsonl")
        injection_before = sha256_file(ROOT / "data/injections.json")
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = BatchEngine(
                config(),
                paths(root / "output", root / "runtime"),
                injection_path=ROOT / "data/injections.json",
            )
            result = engine.run()
            self.assertEqual(result["status"], "completed")
            errors = read_jsonl(root / "output/errors.jsonl")
            pairs = {(error["code"], error["stage"]) for error in errors}
            self.assertEqual(
                pairs,
                {
                    ("E_INVALID_CLAUSE_NO", "normalize"),
                    ("E_TEXT_TOO_LONG", "normalize"),
                    ("E_UNKNOWN_ELEMENT", "extract"),
                    ("E_INVALID_DATE", "arbitrate"),
                },
            )
            for error in errors:
                self.assertTrue(error["contract_id"])
                self.assertTrue(error["stage"])
        self.assertEqual(config_before, sha256_file(ROOT / "config/default.json"))
        self.assertEqual(input_before, sha256_file(ROOT / "data/contracts.jsonl"))
        self.assertEqual(injection_before, sha256_file(ROOT / "data/injections.json"))


if __name__ == "__main__":
    unittest.main()

