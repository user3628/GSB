from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from eval_engine.util import rounded_ratio
from tests.helpers import make_test_config, record, write_workspace


class AggregationTests(unittest.TestCase):
    def test_exclusions_and_recomputable_means(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            config = make_test_config()
            config["exclusions"]["max_normalized_chars"] = 30
            records = [
                record(model_id="model-a", sample_id="s1", output="答案 蓝色 7"),
                record(
                    model_id="model-a",
                    sample_id="s2",
                    output="答案 蓝色 7",
                    language="fr",
                ),
                record(
                    model_id="model-a",
                    sample_id="s3",
                    output="a" * 50,
                    reference_answer="answer",
                ),
                record(
                    model_id="model-a",
                    sample_id="s4",
                    output="答案 蓝色 7",
                    reference_answer=None,
                ),
                record(
                    model_id="model-a",
                    sample_id="s5",
                    output="答案 蓝色 7",
                    question_type="legacy",
                ),
                record(model_id="model-a", sample_id="s6", output="答案 蓝色 7"),
                record(model_id="model-b", sample_id="s1", output="答案 蓝色 7"),
                record(model_id="model-b", sample_id="s2", output="答案 蓝色 7"),
                record(
                    model_id="model-b",
                    sample_id="s3",
                    output="答案 蓝色 7",
                    reference_answer="answer",
                ),
            ]
            engine = write_workspace(root, records, config)
            engine.build()
            stage2 = engine.store.load_artifact(2)
            group_a = next(
                group
                for group in stage2["groups"]
                if group["model_id"] == "model-a"
            )
            self.assertEqual(group_a["total_samples"], 6)
            self.assertEqual(group_a["valid_samples"], 2)
            self.assertEqual(group_a["excluded_samples"], 4)
            reasons = {
                item["sample_id"]: item["reasons"] for item in group_a["exclusions"]
            }
            self.assertIn("language_mismatch", reasons["s2"])
            self.assertIn("output_length_above_limit", reasons["s3"])
            self.assertIn("scoring_not_applicable", reasons["s4"])
            self.assertIn("unsupported_question_type", reasons["s5"])

            scores = [item["score_micros"] for item in group_a["valid_records"]]
            self.assertEqual(group_a["mean_score_micros"], rounded_ratio(sum(scores), len(scores)))
            expected_quantiles = np.percentile(
                np.asarray(scores, dtype=np.int64), [10, 50, 90], method="nearest"
            )
            self.assertEqual(
                group_a["quantiles_micros"],
                {
                    "p10": int(expected_quantiles[0]),
                    "p50": int(expected_quantiles[1]),
                    "p90": int(expected_quantiles[2]),
                },
            )


if __name__ == "__main__":
    unittest.main()
