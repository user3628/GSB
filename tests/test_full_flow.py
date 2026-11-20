from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from eval_engine.engine import Engine
from eval_engine.generator import generate_workspace


class FullFlowTests(unittest.TestCase):
    def test_fixed_seed_and_full_pipeline_are_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            base = Path(temp_name)
            first_root = base / "first"
            second_root = base / "second"
            first_generation = generate_workspace(first_root, seed=20261121, force=True)
            second_generation = generate_workspace(second_root, seed=20261121, force=True)
            self.assertEqual(first_generation["record_count"], 180)
            self.assertEqual(first_generation["record_count"], second_generation["record_count"])
            for name in first_generation["shards"]:
                self.assertEqual(
                    (first_root / "samples" / name).read_bytes(),
                    (second_root / "samples" / name).read_bytes(),
                )
            self.assertEqual(
                (first_root / "configs" / "engine.json").read_bytes(),
                (second_root / "configs" / "engine.json").read_bytes(),
            )

            first_engine = Engine(first_root)
            second_engine = Engine(second_root)
            first_result = first_engine.build(verify=True)
            second_result = second_engine.build(verify=True)
            self.assertEqual(first_result.status, "completed")
            self.assertEqual(first_result.verification["status"], "consistent")
            self.assertEqual(second_result.verification["status"], "consistent")
            for stage in range(1, 5):
                self.assertEqual(
                    first_engine.store.artifact_path(stage).read_bytes(),
                    second_engine.store.artifact_path(stage).read_bytes(),
                )
            self.assertEqual(
                first_result.ranking, second_result.ranking
            )
            self.assertEqual(
                first_result.ranking["selection"]["selected_model_ids"],
                ["model-c", "model-a"],
            )
            checkpoints = first_engine.store.checkpoints()
            self.assertEqual([item["checkpoint_seq"] for item in checkpoints], [1, 2, 3, 4])
            for checkpoint in checkpoints:
                self.assertEqual(checkpoint["stage_name"], {
                    1: "normalize_and_score",
                    2: "aggregate_and_exclude",
                    3: "build_capability_profiles",
                    4: "rank_and_select",
                }[checkpoint["stage"]])
                self.assertTrue(checkpoint["input_digest"])
                self.assertTrue(checkpoint["config_version"])
                self.assertTrue(checkpoint["artifact_digest"])
                self.assertTrue(checkpoint["shard_content_digest"])


if __name__ == "__main__":
    unittest.main()
