from __future__ import annotations

import unittest

from segval.aggregate import aggregate_match_artifacts
from segval.config import DEFAULT_CONFIG, validate_config
from segval.decode import decode_shard
from segval.matching import match_shard


def _square(x0=0.2, y0=0.2, x1=0.4, y1=0.6):
    return [[x0, y0], [x1, y0], [x1, y1], [x0, y1]]


def _shard(predictions, ground_truth=None):
    return {
        "schema_version": 1,
        "shard_id": "shard-0001",
        "shard_index": 1,
        "samples": [
            {
                "image_id": "img-a",
                "width": 100,
                "height": 100,
                "ground_truth": {"person": [_square()]} if ground_truth is None else ground_truth,
                "predictions": predictions,
            }
        ],
    }


def _prediction(prediction_id, confidence, source="baseline", polygon=None, class_name="person"):
    return {
        "prediction_id": prediction_id,
        "class_name": class_name,
        "confidence": confidence,
        "source_operator": source,
        "polygon": polygon if polygon is not None else _square(),
    }


class MatchingTests(unittest.TestCase):
    def setUp(self):
        self.config = validate_config(DEFAULT_CONFIG)

    def test_unique_tie_break_and_unmatched_classification(self):
        shard = _shard(
            [
                _prediction("p-b", 0.91),
                _prediction("p-a", 0.90),
                _prediction("p-high", 0.80, polygon=_square(0.7, 0.7, 0.8, 0.8)),
                _prediction("p-low", 0.20, polygon=_square(0.7, 0.1, 0.8, 0.2)),
            ]
        )
        decoded = decode_shard(shard, self.config)
        matched = match_shard(decoded, self.config)
        detail = matched["details"][0]
        self.assertEqual(detail["counts"]["hits"], 1)
        self.assertEqual(detail["hits"][0]["prediction_id"], "p-a")
        self.assertEqual(detail["counts"]["misses"], 0)
        self.assertEqual(detail["duplicate_predictions"][0]["prediction_id"], "p-b")
        self.assertEqual(
            sorted(item["prediction_id"] for item in detail["duplicate_predictions"]),
            ["p-b", "p-high"],
        )
        self.assertEqual(detail["background_false_positives"][0]["prediction_id"], "p-low")

    def test_missed_ground_truth_and_invalid_predictions(self):
        shard = _shard(
            [
                _prediction("p-invalid", 0.99, polygon=[[0.1, 0.1], [0.2, 0.2]]),
            ]
        )
        decoded = decode_shard(shard, self.config)
        matched = match_shard(decoded, self.config)
        detail = matched["details"][0]
        self.assertEqual(detail["counts"]["hits"], 0)
        self.assertEqual(detail["counts"]["misses"], 1)
        self.assertEqual(detail["counts"]["invalid_predictions"], 1)
        self.assertEqual(detail["eligible_predictions"], 0)

    def test_alternate_matching_strategy_uses_confidence_for_exact_ties(self):
        config = validate_config(
            {
                **DEFAULT_CONFIG,
                "matching": {
                    **DEFAULT_CONFIG["matching"],
                    "strategy": "greedy_global_iou_confidence_tiebreak",
                },
            }
        )
        shard = _shard([_prediction("p-b", 0.91), _prediction("p-a", 0.90)])
        matched = match_shard(decode_shard(shard, config), config)
        self.assertEqual(matched["details"][0]["hits"][0]["prediction_id"], "p-b")

    def test_zero_denominators_are_explicit_na_and_counted(self):
        shard = _shard([], ground_truth={})
        decoded = decode_shard(shard, self.config)
        matched = match_shard(decoded, self.config)
        aggregate = aggregate_match_artifacts([matched])
        totals = aggregate["totals"]
        self.assertIsNone(totals["hit_rate"])
        self.assertEqual(totals["hit_rate_status"], "N/A")
        self.assertEqual(totals["na_metric_count"], 5)
        self.assertIn("average_iou", totals["na_metrics"])

    def test_aggregate_sums_match_detail_counts(self):
        shard = _shard(
            [
                _prediction("p-a", 0.90, source="augment"),
                _prediction("p-b", 0.80, source="ensemble"),
                _prediction("p-low", 0.20, source="tile", polygon=_square(0.7, 0.1, 0.8, 0.2)),
            ],
            ground_truth={"person": [_square(), _square(0.7, 0.7, 0.8, 0.8)]},
        )
        decoded = decode_shard(shard, self.config)
        matched = match_shard(decoded, self.config)
        aggregate = aggregate_match_artifacts([matched])
        totals = aggregate["totals"]
        detail = matched["details"][0]
        self.assertEqual(totals["hits"], detail["counts"]["hits"])
        self.assertEqual(totals["misses"], detail["counts"]["misses"])
        self.assertEqual(totals["duplicates"], detail["counts"]["duplicates"])
        sentinel_rows = [
            row
            for row in aggregate["by_shard_class_source"]
            if row["source_operator"] == "__unmatched__"
        ]
        self.assertTrue(sentinel_rows)
        self.assertEqual(sum(row["misses"] for row in sentinel_rows), totals["misses"])


if __name__ == "__main__":
    unittest.main()

