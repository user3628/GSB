from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from eval_engine.engine import Engine
from eval_engine.errors import EngineError
from eval_engine.generator import generate_workspace
from eval_engine.util import canonical_bytes
from tests.helpers import read_json, write_config


def _all_records(root: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted((root / "samples").glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                records.append(json.loads(line))
    return records


class IncrementalTests(unittest.TestCase):
    def test_invalidation_earliest_stage_and_full_consistency(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            generate_workspace(root, force=True)
            engine = Engine(root)
            first = engine.build(verify=True)
            self.assertEqual(first.status, "completed")
            self.assertEqual(
                [item["checkpoint_seq"] for item in engine.store.checkpoints()],
                [1, 2, 3, 4],
            )

            records = _all_records(root)
            target = next(
                item
                for item in records
                if item["benchmark_id"] == "bench-reasoning"
                and item["language"] == "zh"
                and item["sample_id"].endswith("-000")
            )
            update = engine.update_answers(
                "bench-reasoning",
                {target["sample_id"]: "答案 蓝色圆形 99"},
            )
            self.assertEqual(update["data"]["earliest_invalid_stage"], 1)
            self.assertIn("answers_changed", update["data"]["invalidation_reasons"])
            self.assertEqual(
                update["data"]["affected_models"],
                ["model-a", "model-b", "model-c", "model-d", "model-e"],
            )
            answer_build = engine.build(verify=True)
            self.assertEqual(answer_build.status, "completed")
            self.assertEqual(
                [(item["checkpoint_seq"], item["stage"]) for item in answer_build.checkpoints_published],
                [(5, 1), (6, 2), (7, 3), (8, 4)],
            )

            config = read_json(root / "configs" / "engine.json")
            config["exclusions"]["max_normalized_chars"] = 220
            write_config(root, config)
            exclusion_update = engine.store.inspect()
            self.assertEqual(exclusion_update["earliest_invalid_stage"], 2)
            self.assertEqual(exclusion_update["valid_stages"], [1])
            self.assertIn("exclusion_rules_changed", exclusion_update["invalidation_reasons"])
            exclusion_build = engine.build(verify=True)
            self.assertEqual(
                [(item["checkpoint_seq"], item["stage"]) for item in exclusion_build.checkpoints_published],
                [(9, 2), (10, 3), (11, 4)],
            )

            config["profiling"]["dimensions"]["reasoning"]["layers"] = {
                "zh:reasoning": 0.60,
                "en:reasoning": 0.40,
            }
            write_config(root, config)
            profile_update = engine.store.inspect()
            self.assertEqual(profile_update["earliest_invalid_stage"], 3)
            self.assertEqual(profile_update["valid_stages"], [1, 2])
            self.assertIn("profile_weights_changed", profile_update["invalidation_reasons"])
            profile_build = engine.build(verify=True)
            self.assertEqual(
                [(item["checkpoint_seq"], item["stage"]) for item in profile_build.checkpoints_published],
                [(12, 3), (13, 4)],
            )

            config["ranking"]["top_k"] = 1
            write_config(root, config)
            ranking_update = engine.store.inspect()
            self.assertEqual(ranking_update["earliest_invalid_stage"], 4)
            self.assertEqual(ranking_update["valid_stages"], [1, 2, 3])
            self.assertIn("ranking_rules_changed", ranking_update["invalidation_reasons"])
            ranking_build = engine.build(verify=True)
            self.assertEqual(
                [(item["checkpoint_seq"], item["stage"]) for item in ranking_build.checkpoints_published],
                [(14, 4)],
            )
            self.assertEqual(ranking_build.verification["status"], "consistent")

    def test_single_batch_replacement_reports_only_changed_model(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            generate_workspace(root, force=True)
            engine = Engine(root)
            engine.build(verify=True)
            records = [
                item
                for item in _all_records(root)
                if item["source_batch"] == "batch-reasoning-zh"
            ]
            for item in records:
                if item["model_id"] == "model-c":
                    item["output"] = ""
            replacement = engine.replace_batch("batch-reasoning-zh", records)
            self.assertEqual(replacement["data"]["earliest_invalid_stage"], 1)
            self.assertEqual(replacement["data"]["affected_models"], ["model-c"])
            self.assertIn("samples_changed", replacement["data"]["invalidation_reasons"])
            result = engine.build(verify=True)
            self.assertEqual(result.verification["status"], "consistent")
            self.assertEqual(engine.store.inspect()["last_checkpoint_seq"], 8)

    def test_incremental_mismatch_has_first_stage_and_model(self) -> None:
        with tempfile.TemporaryDirectory() as temp_name:
            root = Path(temp_name)
            generate_workspace(root, force=True)
            engine = Engine(root)
            engine.build()
            config = read_json(root / "configs" / "engine.json")
            config["exclusions"]["max_normalized_chars"] = 220
            write_config(root, config)

            original_run_stage = engine._run_stage

            def sabotaged_run_stage(stage, upstream, context, controller):
                artifact = original_run_stage(stage, upstream, context, controller)
                if stage == 2:
                    artifact = json.loads(json.dumps(artifact))
                    artifact["groups"][0]["mean_score_micros"] += 1
                return artifact

            engine._run_stage = sabotaged_run_stage  # type: ignore[method-assign]
            engine.build()
            with self.assertRaises(EngineError) as context_manager:
                engine.verify_consistency()
            error = context_manager.exception
            self.assertEqual(error.code, "E_INCREMENTAL_MISMATCH")
            self.assertEqual(error.details["first_difference_stage"], 2)
            self.assertTrue(error.details["model_id"])


if __name__ == "__main__":
    unittest.main()
