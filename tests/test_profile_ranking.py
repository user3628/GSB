from __future__ import annotations

import copy
import unittest

from eval_engine.profiling import run_stage3
from eval_engine.ranking import run_stage4
from eval_engine.util import canonical_bytes
from tests.helpers import make_test_config


def _valid_record(model_id: str, language: str, score: int, index: int) -> dict:
    return {
        "record_key": f"{model_id}-{language}-{index}",
        "sample_id": f"s-{language}-{index}",
        "run_id": f"run-{model_id}",
        "model_id": model_id,
        "benchmark_id": "bench-a",
        "source_batch": "batch-a",
        "language": language,
        "question_type": "reasoning",
        "score_micros": score,
        "normalized_output_chars": 8,
    }


class ProfileAndRankingTests(unittest.TestCase):
    def _profile_config(self) -> dict:
        config = make_test_config()
        config["profiling"] = {
            "min_samples_per_layer": 2,
            "min_samples_per_dimension": 4,
            "dimensions": {
                "reasoning": {
                    "weight": 1.0,
                    "layers": {"zh:reasoning": 0.55, "en:reasoning": 0.45},
                }
            },
        }
        config["ranking"].update(
            {
                "score_tolerance": 0.01,
                "min_valid_samples": 1,
                "min_benchmark_coverage": 1,
                "top_k": 2,
            }
        )
        return config

    def test_insufficient_layer_is_null_not_zero(self) -> None:
        config = self._profile_config()
        stage2 = {
            "groups": [
                {
                    "model_id": "model-good",
                    "benchmark_id": "bench-a",
                    "valid_records": [
                        _valid_record("model-good", "zh", 500_000, 1),
                        _valid_record("model-good", "zh", 600_000, 2),
                        _valid_record("model-good", "en", 700_000, 3),
                        _valid_record("model-good", "en", 800_000, 4),
                    ],
                },
                {
                    "model_id": "model-sparse",
                    "benchmark_id": "bench-a",
                    "valid_records": [
                        _valid_record("model-sparse", "zh", 500_000, 1),
                        _valid_record("model-sparse", "en", 700_000, 3),
                        _valid_record("model-sparse", "en", 800_000, 4),
                    ],
                },
            ]
        }
        stage3 = run_stage3(stage2, config, "dep3")
        by_model = {item["model_id"]: item for item in stage3["models"]}
        good = by_model["model-good"]
        self.assertEqual(good["dimensions"]["reasoning"]["status"], "ok")
        self.assertEqual(good["dimensions"]["reasoning"]["score_micros"], 640_000)
        self.assertEqual(
            good["dimensions"]["reasoning"]["effective_weight_micros"], 1_000_000
        )
        sparse = by_model["model-sparse"]
        dimension = sparse["dimensions"]["reasoning"]
        self.assertEqual(dimension["status"], "insufficient_evidence")
        self.assertIsNone(dimension["score_micros"])
        self.assertIsNone(sparse["total_score_micros"])
        self.assertEqual(dimension["effective_weight_micros"], 0)
        self.assertIn(
            "layer_sample_count_below_minimum:zh:reasoning", dimension["reasons"]
        )

    def test_tolerance_tie_arbitration_and_rejection_reasons(self) -> None:
        config = make_test_config()
        config["ranking"].update(
            {
                "score_tolerance": 0.02,
                "min_valid_samples": 5,
                "min_benchmark_coverage": 1,
                "top_k": 1,
            }
        )

        def model(
            model_id: str,
            score: int | None,
            valid: int,
            coverage: int,
            evidence: str = "sufficient",
        ) -> dict:
            return {
                "model_id": model_id,
                "total_score_micros": score,
                "total_score": None,
                "evidence_status": evidence,
                "valid_samples": valid,
                "benchmark_count": coverage,
                "benchmarks": [f"b{index}" for index in range(coverage)],
                "dimensions": {},
            }

        stage3 = {
            "models": [
                model("model-a", 805_000, 8, 3),
                model("model-b", 800_000, 12, 3),
                model("model-c", 800_000, 12, 2),
                model("model-d", 700_000, 10, 3),
                model("model-low", 900_000, 1, 3),
                model("model-null", None, 10, 3, "insufficient"),
            ]
        }
        first = run_stage4(stage3, config, "dep4")
        second = run_stage4(copy.deepcopy(stage3), config, "dep4")
        self.assertEqual(canonical_bytes(first), canonical_bytes(second))
        self.assertEqual(first["selection"]["selected_model_ids"], ["model-b"])
        ranking = {item["model_id"]: item for item in first["ranking"]}
        self.assertEqual(ranking["model-b"]["rank"], 1)
        self.assertTrue(ranking["model-b"]["selected"])
        self.assertEqual(ranking["model-a"]["tie_group"], ranking["model-b"]["tie_group"])
        reasons_a = [item["code"] for item in ranking["model-a"]["decision_reasons"]]
        reasons_c = [item["code"] for item in ranking["model-c"]["decision_reasons"]]
        self.assertIn("tie_broken_by_min_valid_samples", reasons_a)
        self.assertIn("tie_broken_by_benchmark_coverage", reasons_c)
        self.assertIn("outside_top_k", reasons_a)
        rejected = {item["model_id"]: item for item in first["rejected"]}
        self.assertIn("below_min_valid_samples", rejected["model-low"]["reason_codes"])
        self.assertIn(
            "insufficient_profile_evidence", rejected["model-null"]["reason_codes"]
        )


if __name__ == "__main__":
    unittest.main()
