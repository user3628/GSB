from __future__ import annotations

import unittest

from contract_elements.canonical import canonical_json
from contract_elements.errors import BusinessError
from contract_elements.normalization import ClauseNormalizer

from .common import config


class NormalizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.normalizer = ClauseNormalizer(config())

    def test_nfkc_whitespace_sequence_split_and_stable_id(self) -> None:
        record = {
            "contract_id": "C-NORM-01",
            "clause_no": 1,
            "template_id": "TPL-N",
            "clause_text": "第１条　甲方  应支付人民币１，２００，０００．００元。\n乙方应于２０２５年１月１５日交付。",
        }
        first = self.normalizer.normalize_record(record, 0)
        second = self.normalizer.normalize_record(record, 0)
        self.assertEqual(canonical_json(first), canonical_json(second))
        self.assertEqual(first["normalized_text"], "甲方 应支付人民币1,200,000.00元。\n乙方应于2025年1月15日交付。")
        self.assertEqual(len(first["units"]), 2)
        self.assertEqual(first["units"][0]["text"], "甲方 应支付人民币1,200,000.00元。")
        self.assertEqual(first["units"][1]["text"], "乙方应于2025年1月15日交付。")
        self.assertTrue(first["units"][0]["clause_id"].startswith("C-NORM-01#0001.00."))
        self.assertLess(first["units"][0]["source_start"], first["units"][1]["source_start"])
        self.assertEqual(first["units"][0]["contract_id"], "C-NORM-01")

    def test_empty_and_sequence_only_clause_returns_business_error(self) -> None:
        for text in ("第1条   ", "第12条"):
            with self.subTest(text=text), self.assertRaises(BusinessError) as context:
                self.normalizer.normalize_record({"contract_id": "C-EMPTY", "clause_text": text}, 0)
            error = context.exception.to_dict()
            self.assertEqual(error["code"], "E_CLAUSE_EMPTY")
            self.assertEqual(error["contract_id"], "C-EMPTY")
            self.assertEqual(error["stage"], "normalize")
            self.assertEqual(len(error["details"]["offset"]), 2)

    def test_annotation_offsets_map_to_normalized_unit(self) -> None:
        text = "第1条 甲方应支付人民币1,200,000.00元。"
        surface = "人民币1,200,000.00元"
        start = text.index(surface)
        result = self.normalizer.normalize_record(
            {
                "contract_id": "C-MAP",
                "clause_text": text,
                "annotations": [
                    {
                        "element_type": "amount",
                        "start": start,
                        "end": start + len(surface),
                        "normalized_value": "CNY:1200000.00",
                    }
                ],
            },
            0,
        )
        annotation = result["units"][0]["annotations"][0]
        self.assertEqual(annotation["text"], surface)
        self.assertEqual(annotation["normalized_value"], "CNY:1200000.00")


if __name__ == "__main__":
    unittest.main()

