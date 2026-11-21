from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from ctr_cvr_workbench.pipeline import Pipeline

from .helpers import load_raw_config, make_workspace, run_result, write_raw_config


class PipelineDeterminismTests(unittest.TestCase):
    def test_parallel_repeat_and_stop_resume_are_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            roots = [base / "a", base / "b", base / "c"]
            for root in roots:
                make_workspace(root, shards=2, rows_per_shard=55, epochs=3, budget=10000, quota=55)
            result_a = run_result(roots[0], "configs/test.yaml", execution_mode="sequential")
            result_b = run_result(roots[1], "configs/test.yaml", execution_mode="sequential")
            result_parallel = run_result(roots[2], "configs/test.yaml", execution_mode="parallel")
            self.assertEqual(result_a["summary_digest"], result_b["summary_digest"])
            self.assertEqual(result_a["summary_digest"], result_parallel["summary_digest"])
            for relative in (
                "outputs/experiment_summary.json",
                "outputs/train/model.json",
                "outputs/train/metrics.json",
                "outputs/infer/scores.jsonl",
            ):
                self.assertEqual((roots[0] / relative).read_bytes(), (roots[1] / relative).read_bytes())
                self.assertEqual((roots[0] / relative).read_bytes(), (roots[2] / relative).read_bytes())

            with tempfile.TemporaryDirectory() as resume_temporary:
                root = Path(resume_temporary)
                make_workspace(root, shards=2, rows_per_shard=55, epochs=3, budget=10000, quota=55)
                stopped = run_result(root, "configs/test.yaml", execution_mode="sequential", stop_after_epochs=2)
                self.assertEqual(stopped["status"], "stopped")
                resumed = run_result(root, "configs/test.yaml", execution_mode="sequential")
                self.assertEqual(resumed["status"], "completed")
                self.assertEqual(
                    (root / "outputs" / "train" / "model.json").read_bytes(),
                    (roots[0] / "outputs" / "train" / "model.json").read_bytes(),
                )
                self.assertEqual(
                    (root / "outputs" / "train" / "metrics.json").read_bytes(),
                    (roots[0] / "outputs" / "train" / "metrics.json").read_bytes(),
                )

    def test_checkpoint_invalidation_propagates_from_train(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_workspace(root, shards=2, rows_per_shard=50, epochs=2)
            run_result(root, "configs/test.yaml", execution_mode="sequential")
            baseline = Pipeline.status("configs/test.yaml", base_dir=root)
            feature_seq = baseline["stages"]["features"]["seq"]
            raw = load_raw_config(root)
            changed = copy.deepcopy(raw)
            changed["training"]["learning_rate"] = 0.21
            changed_path = write_raw_config(root, changed)
            stale = Pipeline.status(changed_path, base_dir=root)
            self.assertEqual(stale["stages"]["features"]["status"], "valid")
            self.assertEqual(stale["stages"]["train"]["status"], "stale")
            self.assertIn(stale["stages"]["funnel"]["status"], {"blocked", "stale"})
            run_result(root, changed_path, execution_mode="sequential")
            rebuilt = Pipeline.status(changed_path, base_dir=root)
            self.assertEqual(rebuilt["stages"]["train"]["status"], "valid")
            self.assertEqual(rebuilt["stages"]["features"]["seq"], feature_seq)
            self.assertTrue(any(item["earliest_stage"] == "train" for item in rebuilt["invalidations"]))

    def test_checkpoint_sequences_are_monotonic_and_no_partial_temps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_workspace(root, shards=2, rows_per_shard=40, epochs=1)
            run_result(root, "configs/test.yaml", execution_mode="sequential")
            records = sorted((root / "checkpoints").glob("*.json"))
            sequences = [json.loads(path.read_text(encoding="utf-8"))["seq"] for path in records]
            self.assertEqual(sequences, sorted(sequences))
            self.assertEqual(len(sequences), len(set(sequences)))
            stages = {
                json.loads(path.read_text(encoding="utf-8"))["stage"]
                for path in records
                if json.loads(path.read_text(encoding="utf-8"))["kind"] == "stage_complete"
            }
            self.assertEqual(stages, {"features", "train", "funnel", "infer"})
            self.assertFalse(list(root.rglob("*.tmp")))

    def test_feature_dimension_and_threshold_have_distinct_earliest_stages(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_workspace(root, shards=2, rows_per_shard=45, epochs=2)
            run_result(root, "configs/test.yaml", execution_mode="sequential")
            baseline = Pipeline.status("configs/test.yaml", base_dir=root)
            train_seq = baseline["stages"]["train"]["seq"]
            raw = load_raw_config(root)

            dimension_changed = copy.deepcopy(raw)
            dimension_changed["features"]["dimension"] = 96
            dimension_path = write_raw_config(root, dimension_changed, "configs/dimension.yaml")
            dimension_status = Pipeline.status(dimension_path, base_dir=root)
            self.assertEqual(dimension_status["stages"]["features"]["status"], "stale")
            self.assertIn(dimension_status["stages"]["train"]["status"], {"blocked", "stale"})

            threshold_changed = copy.deepcopy(raw)
            threshold_changed["funnel"]["confidence_threshold"] = 0.61
            threshold_path = write_raw_config(root, threshold_changed, "configs/threshold.yaml")
            threshold_status = Pipeline.status(threshold_path, base_dir=root)
            self.assertEqual(threshold_status["stages"]["features"]["status"], "valid")
            self.assertEqual(threshold_status["stages"]["train"]["status"], "valid")
            self.assertEqual(threshold_status["stages"]["funnel"]["status"], "stale")
            self.assertIn(threshold_status["stages"]["infer"]["status"], {"blocked", "stale"})
            run_result(root, threshold_path, execution_mode="sequential")
            rebuilt = Pipeline.status(threshold_path, base_dir=root)
            self.assertEqual(rebuilt["stages"]["train"]["seq"], train_seq)
            self.assertEqual(rebuilt["stages"]["funnel"]["status"], "valid")
            self.assertEqual(rebuilt["stages"]["infer"]["status"], "valid")
            self.assertTrue(any(item["earliest_stage"] == "funnel" for item in rebuilt["invalidations"]))

    def test_combo_epochs_and_sample_content_identify_earliest_stage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            make_workspace(root, shards=2, rows_per_shard=40, epochs=2)
            run_result(root, "configs/test.yaml", execution_mode="sequential")
            raw = load_raw_config(root)
            for field, value in (("combo_weight", 0.5), ("epochs", 3)):
                changed = copy.deepcopy(raw)
                changed["training"][field] = value
                path = write_raw_config(root, changed, f"configs/{field}.yaml")
                status = Pipeline.status(path, base_dir=root)
                self.assertEqual(status["stages"]["features"]["status"], "valid", field)
                self.assertEqual(status["stages"]["train"]["status"], "stale", field)

            shard_path = root / "data" / "shards" / "shard-00000.jsonl"
            lines = shard_path.read_bytes().splitlines()
            first = json.loads(lines[0])
            first["user_features"]["city"] = "sample-content-change"
            lines[0] = json.dumps(
                first, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            new_shard_bytes = b"\n".join(lines) + b"\n"
            shard_path.write_bytes(new_shard_bytes)
            manifest_path = root / "data" / "shards" / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["shards"][0]["sha256"] = __import__("hashlib").sha256(new_shard_bytes).hexdigest()
            manifest["shards"][0]["bytes"] = len(new_shard_bytes)
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            sample_status = Pipeline.status("configs/test.yaml", base_dir=root)
            self.assertEqual(sample_status["stages"]["features"]["status"], "stale")


if __name__ == "__main__":
    unittest.main()
