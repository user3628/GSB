from __future__ import annotations

import unittest

from pipeline.aggregate import summarize_results
from pipeline.calibration import choose_threshold
from pipeline.config import from_dict
from pipeline.errors import (
    E_EMPTY_DOC,
    E_EMPTY_CLASS,
    E_INVALID_FPR_LIMIT,
    E_INVALID_SMOOTHING,
    E_ZERO_SMOOTHING,
    PipelineError,
)
from pipeline.features import extract_features
from pipeline.model import score_features, train_and_score
from pipeline.normalize import normalize_text, tokenize_normalized
from pipeline.scheduler import accepted_batches, plan_schedule
from pipeline.util import canonical_json
from pipeline import yaml_lite


def _config_raw() -> dict:
    return {
        "version": "unit-v1",
        "hash_dim": 16,
        "smoothing": 0.5,
        "unknown_token": "<UNK>",
        "labels": ["a", "b", "c"],
        "thresholds": {"a": 0.1, "b": 0.1, "c": 0.1},
        "target_fpr": 0.2,
        "threshold_max": 0.95,
        "evaluation_budget": 100,
        "workers": 2,
        "class_weights": {"a": 1.0, "b": 1.5, "c": 2.0},
        "calibration": {
            "fold_modulus": 5,
            "fold_remainder": 0,
            "min_samples_per_class": 2,
        },
    }


class ConfigTests(unittest.TestCase):
    def test_yaml_flow_collections_are_supported(self) -> None:
        parsed = yaml_lite.loads(
            "labels: [account, billing]\n"
            "thresholds: {account: 0.1, billing: 0.2}\n"
            "nested: {items: [a, b], enabled: true}\n"
        )
        self.assertEqual(parsed["labels"], ["account", "billing"])
        self.assertEqual(parsed["thresholds"], {"account": 0.1, "billing": 0.2})
        self.assertEqual(parsed["nested"], {"items": ["a", "b"], "enabled": True})

    def test_invalid_smoothing_has_stable_code(self) -> None:
        raw = _config_raw()
        raw["smoothing"] = -0.1
        with self.assertRaises(PipelineError) as context:
            from_dict(raw)
        self.assertEqual(context.exception.code, E_INVALID_SMOOTHING)

    def test_invalid_fpr_limit_has_stable_code(self) -> None:
        raw = _config_raw()
        raw["target_fpr"] = 1.1
        with self.assertRaises(PipelineError) as context:
            from_dict(raw)
        self.assertEqual(context.exception.code, E_INVALID_FPR_LIMIT)


class FeatureTests(unittest.TestCase):
    def test_normalization_tokens_offsets_and_empty_diagnostic(self) -> None:
        normalized = normalize_text("  ＡＢＣ\n\t中文   测试  ")
        self.assertEqual(normalized, "abc 中文 测试")
        tokens = tokenize_normalized(normalized)
        terms = [token.term for token in tokens]
        self.assertIn("abc", terms)
        self.assertIn("中", terms)
        self.assertIn("中文", terms)
        self.assertIn("测试", terms)
        feature = extract_features(
            {"batch_id": "b1", "doc_id": "d1", "text": "ＡＢＣ 中文测试"},
            1,
        )
        self.assertEqual(feature["doc_id"], "d1")
        self.assertGreater(feature["hash_collision_count"], 0)
        self.assertTrue(all("start" in token and "end" in token for token in feature["tokens"]))
        empty = extract_features({"batch_id": "b1", "doc_id": "d2", "text": " \n\t "}, 8)
        self.assertEqual(empty["error_code"], E_EMPTY_DOC)
        self.assertEqual(empty["tokens"], [])


class ModelTests(unittest.TestCase):
    def _training_set(self) -> tuple[list[dict], dict[str, dict]]:
        documents = {
            "d1": {"doc_id": "d1", "batch_id": "b1", "label": "a", "text": "苹果 苹果", "lang": "zh", "channel": "web"},
            "d2": {"doc_id": "d2", "batch_id": "b1", "label": "a", "text": "苹果 香蕉", "lang": "zh", "channel": "web"},
            "d3": {"doc_id": "d3", "batch_id": "b1", "label": "b", "text": "香蕉 香蕉", "lang": "zh", "channel": "api"},
            "d4": {"doc_id": "d4", "batch_id": "b1", "label": "b", "text": "香蕉 梨", "lang": "zh", "channel": "api"},
            "d5": {"doc_id": "d5", "batch_id": "b1", "label": "c", "text": "未知 文本", "lang": "en", "channel": "web"},
        }
        features = [extract_features(document, 64) for document in documents.values()]
        return features, documents

    def test_training_scoring_is_deterministic_and_missing_class_is_explicit(self) -> None:
        features, documents = self._training_set()
        model1, scores1 = train_and_score(
            "b1", features, documents, ("a", "b", "zz"), 0.5, "<UNK>", 64, "full"
        )
        model2, scores2 = train_and_score(
            "b1", features, documents, ("a", "b", "zz"), 0.5, "<UNK>", 64, "full"
        )
        self.assertEqual(canonical_json(model1), canonical_json(model2))
        self.assertEqual(canonical_json(scores1), canonical_json(scores2))
        self.assertIsNone(model1["priors"]["zz"]["log_prior"])
        self.assertEqual(model1["priors"]["zz"]["status"], E_EMPTY_CLASS)
        unknown_doc = extract_features(
            {"batch_id": "b1", "doc_id": "d9", "text": "never-seen-term"}, 64
        )
        unknown_score = score_features(model1, unknown_doc)
        self.assertEqual(unknown_score["tokens"][0]["effective_term"], "<UNK>")
        self.assertIn("probabilities", unknown_score)

    def test_zero_smoothing_is_explicit_not_silent(self) -> None:
        features, documents = self._training_set()
        model, scores = train_and_score(
            "b1", features, documents, ("a", "b"), 0.0, "<UNK>", 64, "full"
        )
        self.assertTrue(model["diagnostics"])
        scored_unknown = [row for row in scores if row["doc_id"] == "d5"][0]
        self.assertEqual(scored_unknown["score_error_codes"], [E_ZERO_SMOOTHING])
        self.assertTrue(all(value is None for value in scored_unknown["log_scores"].values()))


class CalibrationTests(unittest.TestCase):
    def test_threshold_caps_false_positive_rate(self) -> None:
        result = choose_threshold(
            positive_scores=[0.1, 0.8, 0.9],
            negative_scores=[0.05, 0.3, 0.4, 0.7],
            threshold_floor=0.1,
            threshold_max=0.95,
            target_fpr=0.25,
        )
        self.assertTrue(result["target_met"])
        self.assertLessEqual(result["false_positive_rate"], 0.25)
        self.assertEqual(result["false_positive_count"], 1)
        self.assertEqual(result["hit_count"], 2)

    def test_unreachable_target_is_marked_with_best_available_threshold(self) -> None:
        result = choose_threshold(
            positive_scores=[0.1, 0.8, 0.9],
            negative_scores=[0.05, 0.3, 0.4, 0.7],
            threshold_floor=0.1,
            threshold_max=0.5,
            target_fpr=0.0,
        )
        self.assertFalse(result["target_met"])
        self.assertEqual(result["threshold"], 0.5)
        self.assertGreater(result["false_positive_count"], 0)


class AggregateTests(unittest.TestCase):
    def test_weighted_summary_matches_sorted_rows_and_keeps_empty_layers(self) -> None:
        rows = [
            {
                "doc_id": "d2",
                "true_label": "b",
                "predicted_label": "a",
                "language": "en",
                "channel": "web",
                "error_code": None,
                "metric_eligible": True,
                "review_required": False,
            },
            {
                "doc_id": "d1",
                "true_label": "a",
                "predicted_label": "a",
                "language": "en",
                "channel": "web",
                "error_code": None,
                "metric_eligible": True,
                "review_required": False,
            },
            {
                "doc_id": "d3",
                "true_label": "a",
                "predicted_label": None,
                "language": "zh",
                "channel": "api",
                "error_code": None,
                "metric_eligible": True,
                "review_required": False,
            },
            {
                "doc_id": "d4",
                "true_label": "b",
                "predicted_label": None,
                "language": "en",
                "channel": "web",
                "error_code": "E_EMPTY_DOC",
                "metric_eligible": False,
                "review_required": False,
            },
        ]
        summary = summarize_results(
            rows,
            labels=("a", "b", "c"),
            languages=("en", "zh", "missing"),
            channels=("api", "web", "cold"),
            class_weights={"a": 1.0, "b": 1.5, "c": 2.0},
        )
        self.assertEqual(summary["first_doc_id"], "d1")
        self.assertEqual(summary["by_language"]["en"]["classes"]["a"]["tp"], 1)
        self.assertEqual(summary["by_language"]["en"]["classes"]["a"]["fp"], 1)
        self.assertEqual(summary["by_language"]["en"]["classes"]["b"]["fn"], 1)
        self.assertEqual(summary["by_language"]["zh"]["classes"]["a"]["fn"], 1)
        empty = summary["by_language_channel"]["missing|cold"]
        self.assertEqual(empty["sample_count"], 0)
        self.assertIsNone(empty["classes"]["a"]["precision"])
        weighted = summary["overall"]["weighted"]
        expected_precision = (1_000_000_000 * 1) / (1_000_000_000 * 2)
        self.assertAlmostEqual(weighted["precision"], expected_precision, delta=1e-9)
        self.assertEqual(summary["overall"]["error_counts"], {E_EMPTY_DOC: 1})


class SchedulerTests(unittest.TestCase):
    def test_budget_rejects_estimated_overrun_but_keeps_later_fitting_batch(self) -> None:
        from pipeline.data import BatchSpec, Dataset

        def batch(number: int, rows: int) -> BatchSpec:
            return BatchSpec(
                batch_id=f"b{number}",
                batch_no=number,
                tier="small",
                relative_path=f"b{number}.jsonl",
                path=__import__("pathlib").Path(f"/tmp/b{number}.jsonl"),
                row_count=rows,
                declared_labels=("a",),
                languages=("zh",),
                channels=("web",),
                content_digest="x",
            )

        dataset = Dataset(
            root=__import__("pathlib").Path("/tmp"),
            index_path=__import__("pathlib").Path("/tmp/index.json"),
            seed=1,
            batches=(batch(1, 6), batch(2, 5), batch(3, 2)),
        )
        schedule = plan_schedule(dataset, budget=8, workers=2)
        self.assertEqual([item["batch_id"] for item in schedule["accepted"]], ["b1", "b3"])
        self.assertEqual(schedule["rejected"][0]["reason"], "E_EVALUATION_BUDGET_EXCEEDED")
        self.assertEqual([item.batch_id for item in accepted_batches(dataset, 8)], ["b1", "b3"])


if __name__ == "__main__":
    unittest.main()
