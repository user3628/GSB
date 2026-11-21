from __future__ import annotations

import unittest

from contract_elements.extraction import DictionaryStore, ElementExtractor, SeedEstimator
from contract_elements.normalization import ClauseNormalizer

from .common import ROOT, config


class ExtractionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = config()
        self.normalizer = ClauseNormalizer(self.config)
        import json

        entries = json.loads((ROOT / "data/elements.json").read_text(encoding="utf-8"))["entries"]
        seeds = [
            json.loads(line)
            for line in (ROOT / "data/seeds.jsonl").read_text(encoding="utf-8").splitlines()
            if line
        ]
        self.dictionary = DictionaryStore(entries)
        model = SeedEstimator(self.normalizer, self.dictionary, self.config["extraction"]).estimate(seeds)
        self.extractor = ElementExtractor(model, self.dictionary, self.config["extraction"])

    def _extract(self, text: str) -> dict:
        normalized = self.normalizer.normalize_record(
            {"contract_id": "C-EXTRACT", "clause_no": 1, "clause_text": text}, 0
        )
        return self.extractor.extract_unit(normalized["units"][0], "2025-01-01")

    def test_all_four_element_types_and_fixed_point_values(self) -> None:
        result = self._extract(
            "第1条 甲方应于2025年1月15日支付人民币1,200,000.00元，期限30个工作日，违约金0.5%。"
        )
        values = {
            candidate["element_type"]: candidate["normalized_value"]
            for candidate in result["candidates"]
            if candidate["valid"]
        }
        self.assertEqual(values["amount"], "CNY:1200000.00")
        self.assertEqual(values["date"], "2025-01-15")
        self.assertEqual(values["term"], "business_day:30")
        self.assertEqual(values["penalty_ratio"], "0.005000")
        for candidate in result["candidates"]:
            self.assertGreaterEqual(float(candidate["confidence"]), 0.0)
            self.assertLessEqual(float(candidate["confidence"]), 1.0)
            self.assertEqual(candidate["offset_basis"], "normalized_clause_codepoint")
            self.assertTrue(candidate["evidence"])

    def test_nan_and_quantization_overflow_are_retained_as_numeric_errors(self) -> None:
        nan_result = self._extract("第1条 金额为人民币NaN元。")
        self.assertTrue(any(candidate["valid"] is False for candidate in nan_result["candidates"]))
        self.assertIn("E_NUMERIC", {error["code"] for error in nan_result["errors"]})
        nan_error = [error for error in nan_result["errors"] if error["code"] == "E_NUMERIC"][0]
        self.assertEqual(nan_error["contract_id"], "C-EXTRACT")
        self.assertEqual(nan_error["details"]["element"], "amount")

        overflow_result = self._extract("第1条 金额为人民币999999999999999999999999999999.00元。")
        overflow_error = [error for error in overflow_result["errors"] if error["code"] == "E_NUMERIC"][0]
        self.assertEqual(overflow_error["details"]["element"], "amount")
        self.assertTrue(any(candidate["normalized_value"] is None for candidate in overflow_result["candidates"]))


if __name__ == "__main__":
    unittest.main()

