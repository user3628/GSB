import unittest

from shotcheck.config import default_config
from shotcheck.generator import _targets
from shotcheck.matching import analyze_shot, longest_term_matches
from shotcheck.text import normalize_and_split


def _normalize(prompt, subjects, scenes=None, negatives=None):
    return normalize_and_split(
        prompt,
        subject_terms=subjects,
        scene_terms=scenes or [],
        negative_terms=negatives or [],
    )


class MatchingTests(unittest.TestCase):
    def test_leftmost_longest_non_overlapping(self):
        units = [
            {
                "unit_id": "u1",
                "text": "红气球",
                "start": 0,
                "end": 3,
                "type": "subject_description",
            }
        ]
        matches = longest_term_matches(units, ["气球", "红气球"])
        self.assertEqual([match["term"] for match in matches], ["红气球"])
        self.assertEqual((matches[0]["start"], matches[0]["end"]), (0, 3))

    def test_negative_window_conflict_deducts_coverage(self):
        prompt = "红色气球漂浮在屋顶，不要出现红色气球，镜头持续2s。"
        normalized = _normalize(
            prompt,
            ["红色气球"],
            ["屋顶"],
            ["不要"],
        )
        reference_shot = {
            "units": normalized["units"],
            "duration": {"target_ms": 2000},
            "targets": {
                "must_subject_coverage": {"min": 1.0, "max": 1.0},
                "scene_coverage": {"min": 1.0, "max": 1.0},
                "duration_consistency": {"min": 1.0, "max": 1.0},
                "score": {"min": 0.85, "max": 1.0},
            },
        }
        decision = analyze_shot(
            shot={
                "shot_id": "s1",
                "must_scenes": ["屋顶"],
                "negative_terms": ["不要"],
            },
            subject_entry={
                "required_subjects": ["红色气球"],
                "forbidden_subjects": [],
            },
            reference_shot=reference_shot,
            normalize_result=normalized,
            duration_result={"duration_ms": 2000},
            config=default_config(),
        )
        self.assertEqual(decision["verdict"], "rejected")
        self.assertEqual(decision["conflicted_subjects"], ["红色气球"])
        self.assertEqual(
            decision["coverage"]["must_subject_coverage"]["numerator"], 0
        )
        self.assertEqual(decision["conflicts"][0]["type"], "required_subject_in_negative_window")
        self.assertIsInstance(decision["conflicts"][0]["required_span"]["start"], int)
        self.assertIsInstance(decision["conflicts"][0]["negative_span"]["start"], int)
        self.assertIn("E_SEMANTIC_CONFLICT", [error["code"] for error in decision["errors"]])

    def test_safe_negative_does_not_conflict(self):
        prompt = "黑猫蹲在门口，不要出现红色气球，镜头持续2s。"
        normalized = _normalize(prompt, ["黑猫"], [], ["不要"])
        reference_shot = {
            "units": normalized["units"],
            "duration": {"target_ms": 2000},
            "targets": {
                "must_subject_coverage": {"min": 1.0, "max": 1.0},
                "duration_consistency": {"min": 1.0, "max": 1.0},
                "score": {"min": 0.85, "max": 1.0},
            },
        }
        decision = analyze_shot(
            shot={"shot_id": "s1", "must_scenes": [], "negative_terms": ["不要"]},
            subject_entry={"required_subjects": ["黑猫"], "forbidden_subjects": []},
            reference_shot=reference_shot,
            normalize_result=normalized,
            duration_result={"duration_ms": 2000},
            config=default_config(),
        )
        self.assertEqual(decision["conflicts"], [])
        self.assertEqual(decision["verdict"], "pass")
        self.assertEqual(decision["na_components"], ["scene_coverage"])

    def test_forbidden_subject_is_rejected(self):
        prompt = "黄色雨衣女孩站在月台边，黑猫蹲在旧书店门口，镜头持续2s。"
        normalized = _normalize(
            prompt,
            ["雨衣女孩", "黑猫"],
            ["月台", "旧书店"],
            [],
        )
        reference_shot = {
            "units": normalized["units"],
            "duration": {"target_ms": 2000},
            "targets": {
                "must_subject_coverage": {"min": 1.0, "max": 1.0},
                "scene_coverage": {"min": 1.0, "max": 1.0},
                "duration_consistency": {"min": 1.0, "max": 1.0},
                "score": {"min": 0.85, "max": 1.0},
            },
        }
        decision = analyze_shot(
            shot={
                "shot_id": "s1",
                "must_scenes": ["月台", "旧书店"],
                "negative_terms": [],
            },
            subject_entry={
                "required_subjects": ["雨衣女孩"],
                "forbidden_subjects": ["黑猫"],
            },
            reference_shot=reference_shot,
            normalize_result=normalized,
            duration_result={"duration_ms": 2000},
            config=default_config(),
        )
        self.assertEqual(decision["verdict"], "rejected")
        self.assertIn(
            "E_FORBIDDEN_SUBJECT", [error["code"] for error in decision["errors"]]
        )

    def test_target_interval_controls_review_and_reject(self):
        prompt = "白色机器人抬起手臂，slow dolly-in，镜头持续2s。"
        normalized = _normalize(
            prompt,
            ["白色机器人", "黑猫"],
            ["不存在场景一", "不存在场景二"],
            [],
        )
        reference_shot = {
            "units": normalized["units"],
            "duration": {"target_ms": 2000},
            "targets": {
                "must_subject_coverage": {"min": 1.0, "max": 1.0},
                "scene_coverage": {"min": 1.0, "max": 1.0},
                "duration_consistency": {"min": 1.0, "max": 1.0},
                "score": {"min": 0.85, "max": 1.0},
            },
        }
        reject = analyze_shot(
            shot={
                "shot_id": "s1",
                "must_scenes": ["不存在场景一", "不存在场景二"],
                "negative_terms": [],
            },
            subject_entry={
                "required_subjects": ["白色机器人", "黑猫"],
                "forbidden_subjects": [],
            },
            reference_shot=reference_shot,
            normalize_result=normalized,
            duration_result={"duration_ms": 2000},
            config=default_config(),
        )
        self.assertEqual(reject["score"], 0.525)
        self.assertEqual(reject["verdict"], "rejected")


if __name__ == "__main__":
    unittest.main()
