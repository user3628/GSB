import unittest

from shotcheck.duration import parse_duration_phrase
from shotcheck.text import normalize_and_split, normalize_text


class TextNormalizationTests(unittest.TestCase):
    def test_nfkc_and_whitespace(self):
        self.assertEqual(normalize_text("  ＡＢＣ\u3000１２．５ｓ  "), "ABC 12.5s")

    def test_units_have_offsets_and_types(self):
        result = normalize_and_split(
            "黄色雨衣女孩站在月台边，雨夜中霓虹灯闪烁 and slow dolly-in，"
            "不要出现红色气球，镜头持续约三秒半。",
            subject_terms=["雨衣女孩"],
            scene_terms=["雨夜", "霓虹灯"],
            negative_terms=["不要"],
        )
        self.assertEqual(
            [unit["type"] for unit in result["units"]],
            ["subject_description", "scene_description", "scene_description", "constraint", "duration"],
        )
        for unit in result["units"]:
            self.assertEqual(
                result["normalized_prompt"][unit["start"] : unit["end"]],
                unit["text"],
            )

    def test_constraint_wins_over_duration_hint(self):
        result = normalize_and_split(
            "不要出现3秒黑屏。",
            subject_terms=["机器人"],
            scene_terms=[],
            negative_terms=["不要"],
        )
        self.assertEqual(result["units"][0]["type"], "constraint")

    def test_no_subject_unit_is_detectable(self):
        result = normalize_and_split(
            "雨夜中霓虹灯闪烁，slow dolly-in。",
            subject_terms=["黑猫"],
            scene_terms=["雨夜", "霓虹灯"],
            negative_terms=[],
        )
        self.assertFalse(
            any(unit["type"] == "subject_description" for unit in result["units"])
        )


class DurationParsingTests(unittest.TestCase):
    def assert_parsed(self, phrase, value, source, precision):
        parsed = parse_duration_phrase("s", phrase)
        self.assertTrue(parsed["ok"], parsed)
        result = parsed["result"]
        self.assertEqual(result["duration_ms"], value)
        self.assertEqual(result["source"], source)
        self.assertEqual(result["precision_digits"], precision)

    def test_chinese_quantifier(self):
        self.assert_parsed("约三秒半", 3500, "cn_quantifier", 1)
        self.assert_parsed("十二秒", 12000, "cn_quantifier", 0)
        self.assert_parsed("一点五秒", 1500, "cn_quantifier", 1)

    def test_arabic_and_fullwidth(self):
        self.assert_parsed("2.5s", 2500, "arabic_unit", 1)
        self.assert_parsed("２．５ｓ", 2500, "arabic_unit", 1)
        self.assert_parsed("1500ms", 1500, "arabic_unit", 0)
        self.assert_parsed("3秒半", 3500, "arabic_unit", 1)

    def test_frames(self):
        self.assert_parsed("八帧", 320, "frames", 0)
        self.assert_parsed("10 frames", 400, "frames", 0)

    def test_equal_duplicate_phrases_are_not_a_conflict(self):
        self.assert_parsed("2s 2秒", 2000, "arabic_unit", 0)

    def test_unknown_unit(self):
        parsed = parse_duration_phrase("s1", "约三光年")
        self.assertFalse(parsed["ok"])
        error = parsed["error"]
        self.assertEqual(error["code"], "E_DURATION_PARSE")
        self.assertEqual(error["shot_id"], "s1")
        self.assertEqual(error["reason"], "unknown_unit")
        self.assertIsInstance(error["phrase_offset"], int)

    def test_non_positive_values(self):
        for phrase in ("0s", "-2s", "零秒"):
            parsed = parse_duration_phrase("s1", phrase)
            self.assertFalse(parsed["ok"], phrase)
            self.assertEqual(parsed["error"]["reason"], "non_positive_duration")
            self.assertIn("phrase_offset", parsed["error"])

    def test_conflicting_values_do_not_silently_choose_first(self):
        parsed = parse_duration_phrase("s1", "约2秒，3s")
        self.assertFalse(parsed["ok"])
        error = parsed["error"]
        self.assertEqual(error["reason"], "conflicting_durations")
        self.assertEqual(error["parsed_values"], [2000, 3000])
        self.assertEqual(len(error["phrase_offsets"]), 2)

    def test_missing_expression(self):
        parsed = parse_duration_phrase("s1", "稍后处理")
        self.assertFalse(parsed["ok"])
        self.assertEqual(parsed["error"]["reason"], "no_duration_expression")


if __name__ == "__main__":
    unittest.main()
