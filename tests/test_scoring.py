from __future__ import annotations

import unittest

from eval_engine.scoring import score_record
from eval_engine.util import canonical_bytes, normalize_text, tokenize


def _score(output: str, reference: str | None) -> dict:
    record = {
        "record_key": "key-1",
        "run_id": "run-1",
        "model_id": "model-1",
        "benchmark_id": "bench-1",
        "sample_id": "sample-1",
        "source_batch": "batch-1",
        "language": "zh",
        "question_type": "reasoning",
        "output": output,
        "reference_answer": reference,
    }
    return score_record(record, 600_000, 400_000)


class ScoringTests(unittest.TestCase):
    def test_nfkc_whitespace_width_and_case_are_normalized(self) -> None:
        output = "  ＡＮＳＷＥＲ\u3000 Blue   CIRCLE ７ "
        reference = "answer blue circle 7"
        self.assertEqual(normalize_text(output), reference)
        result = _score(output, reference)
        self.assertEqual(result["exact_match"], 1)
        self.assertEqual(result["score_micros"], 1_000_000)
        self.assertEqual(result["token_overlap_micros"], 1_000_000)

    def test_partial_overlap_is_fixed_point_and_stable(self) -> None:
        first = _score("answer blue", "answer blue circle 7")
        second = _score("answer blue", "answer blue circle 7")
        self.assertGreater(first["token_overlap_micros"], 0)
        self.assertLess(first["token_overlap_micros"], 1_000_000)
        self.assertGreater(first["score_micros"], 0)
        self.assertEqual(canonical_bytes(first), canonical_bytes(second))

    def test_empty_missing_reference_and_zero_overlap_have_reasons(self) -> None:
        empty = _score("", "answer blue")
        self.assertEqual(empty["score_micros"], 0)
        self.assertIn("empty_output", empty["score_reasons"])
        self.assertIn("zero_token_overlap", empty["score_reasons"])

        missing = _score("answer blue", None)
        self.assertFalse(missing["rule_applicable"])
        self.assertEqual(missing["score_micros"], 0)
        self.assertIn("missing_reference_answer", missing["score_reasons"])

        no_overlap = _score("zzz 999", "answer blue")
        self.assertEqual(no_overlap["token_overlap_micros"], 0)
        self.assertIn("zero_token_overlap", no_overlap["score_reasons"])

    def test_cjk_tokenization_is_character_level(self) -> None:
        tokens = tokenize("答案 蓝色 blue")
        self.assertEqual(tokens, ["答", "案", "蓝", "色", "blue"])


if __name__ == "__main__":
    unittest.main()
