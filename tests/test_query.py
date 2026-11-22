import unittest

from ragwb import query as query_mod
from ragwb.io_utils import canonical_json


class QueryRewriteTest(unittest.TestCase):
    def test_pronoun_replacement_offsets_and_byte_determinism(self):
        turn = {
            "session_id": "s", "turn_index": 1,
            "question": "它是什么时候发布的？", "lang": "zh",
            "coref_spans": [
                {"start": 0, "end": 1, "text": "它", "kind": "pronoun"}],
        }
        focus = query_mod.Focus(entity="极光电池")
        first = query_mod.rewrite_turn(turn, focus)
        second = query_mod.rewrite_turn(turn, focus)
        self.assertEqual(
            first["rewritten_query"], "极光电池是什么时候发布的？")
        self.assertFalse(first["fallback"])
        self.assertEqual(first["replacements"][0]["start"], 0)
        self.assertEqual(first["replacements"][0]["end"], 4)
        self.assertEqual(canonical_json(first), canonical_json(second))

    def test_fallback_when_focus_empty(self):
        turn = {
            "session_id": "s", "turn_index": 0,
            "question": "它的价格是多少？", "lang": "zh",
            "coref_spans": [
                {"start": 0, "end": 1, "text": "它", "kind": "pronoun"}],
        }
        product = query_mod.rewrite_turn(turn, query_mod.Focus())
        self.assertTrue(product["fallback"])
        self.assertEqual(product["fallback_reason"],
                         "empty_dialogue_focus")
        self.assertEqual(product["rewritten_query"], turn["question"])
        self.assertEqual(product["replacements"], [])

    def test_fallback_when_no_anaphor(self):
        turn = {
            "session_id": "s", "turn_index": 0,
            "question": "星河芯片的主频是多少？", "lang": "zh",
            "coref_spans": [],
        }
        product = query_mod.rewrite_turn(turn, query_mod.Focus())
        self.assertTrue(product["fallback"])
        self.assertEqual(product["fallback_reason"],
                         "no_anaphor_detected")

    def test_english_coreference(self):
        turn = {
            "session_id": "s", "turn_index": 1,
            "question": "When was it released?", "lang": "en",
            "coref_spans": [],
        }
        product = query_mod.rewrite_turn(
            turn, query_mod.Focus(entity="Nimbus drone"))
        self.assertEqual(
            product["rewritten_query"],
            "When was Nimbus drone released?")
        self.assertEqual(product["replacements"][0]["replacement"],
                         "Nimbus drone")


if __name__ == "__main__":
    unittest.main()
