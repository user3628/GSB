from __future__ import annotations

import unittest
from copy import deepcopy

from contract_elements.interop import V1Interop, load_v1
from contract_elements.normalization import ClauseNormalizer, utf8_byte_to_codepoint_offset

from .common import ROOT, config


class InteropTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = config()
        self.rows = load_v1(ROOT / "data/annotations_v1.jsonl")
        self.interop = V1Interop(self.config, ClauseNormalizer(self.config))

    def test_v1_import_derives_clause_numbers_and_parses_fixed_amounts(self) -> None:
        structure = self.interop.import_rows(self.rows)
        self.assertEqual(structure["structure_version"], 2)
        clauses = [clause for contract in structure["contracts"] for clause in contract["clauses"]]
        self.assertEqual([clause["clause_no"] for clause in clauses], [1, 2, 3])
        amounts = [
            annotation["normalized_value"]
            for clause in clauses
            for annotation in clause["annotations"]
            if annotation["element_type"] == "amount"
        ]
        self.assertEqual(amounts, ["CNY:1250000.00", "CNY:80000.00", "CNY:15000.00"])

    def test_export_reimport_fixed_point_and_thousand_separators(self) -> None:
        first_export = self.interop.export_rows(self.interop.import_rows(self.rows))
        first_bytes = self.interop.canonical_rows_bytes(first_export)
        second_export = self.interop.export_rows(self.interop.import_rows(first_export))
        second_bytes = self.interop.canonical_rows_bytes(second_export)
        self.assertEqual(first_bytes, second_bytes)
        self.assertTrue(any('"value":"1,250,000.00"' in line for line in first_bytes.decode("utf-8").splitlines()))
        compatibility = self.interop.compatibility(self.rows)
        self.assertTrue(compatibility["success"])
        self.assertEqual(compatibility["successful_records"], 2)
        self.assertTrue(compatibility["internal_structure_bytes_identical"])

    def test_utf8_byte_offset_conversion_is_configurable(self) -> None:
        config = deepcopy(self.config)
        config["interop"]["offset_basis"] = "utf8_byte"
        text = "第1条 甲方应支付人民币100,000.00元。"
        surface = "100,000.00"
        char_start = text.index(surface)
        byte_start = len(text[:char_start].encode("utf-8"))
        byte_end = byte_start + len(surface.encode("utf-8"))
        rows = [{
            "structure_version": 1,
            "contract_id": "C-V1-UTF8",
            "template_id": "TPL-V1",
            "clauses": [{
                "clause_text": text,
                "elements": [{"type": "amount", "start": byte_start, "end": byte_end, "value": "100,000.00"}],
            }],
        }]
        interop = V1Interop(config, ClauseNormalizer(config))
        structure = interop.import_rows(rows)
        annotation = structure["contracts"][0]["clauses"][0]["annotations"][0]
        self.assertEqual(annotation["normalized_value"], "CNY:100000.00")
        self.assertEqual(utf8_byte_to_codepoint_offset(text, byte_start), char_start)


if __name__ == "__main__":
    unittest.main()

