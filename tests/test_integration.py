from __future__ import annotations

import copy
import shutil
import tempfile
import unittest
from pathlib import Path

from pipeline import yaml_lite
from pipeline.batch import build_cache_plan
from pipeline.checkpoints import CheckpointManager
from pipeline.config import from_dict
from pipeline.data import load_dataset
from pipeline.errors import E_INCREMENTAL_MISMATCH, E_INVALID_SMOOTHING, PipelineError
from pipeline.generate import generate_demo, verify_demo
from pipeline.incremental import compare_workspaces, ensure_workspace_match, replace_batch
from pipeline.runner import run_pipeline
from pipeline.util import atomic_write_jsonl, atomic_write_text, read_json, read_jsonl


class IntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.class_temp = tempfile.TemporaryDirectory()
        cls.root = Path(cls.class_temp.name)
        cls.data = cls.root / "data"
        generate_demo(cls.data, 20261123)
        cls.base_raw = yaml_lite.load(str(cls.data / "config.yaml"))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.class_temp.cleanup()

    def _config(self, name: str, **updates) -> Path:
        raw = copy.deepcopy(self.base_raw)
        raw.update(updates)
        path = self.root / f"{name}.yaml"
        atomic_write_text(path, yaml_lite.dumps(raw))
        return path

    def _small_config(self, name: str, **updates) -> Path:
        values = {"evaluation_budget": 900, "workers": 4}
        values.update(updates)
        return self._config(name, **values)

    def test_demo_data_contract(self) -> None:
        verification = verify_demo(self.data / "index.json")
        self.assertTrue(verification["passed"])
        self.assertEqual(verification["batch_count"], 4)
        self.assertEqual(verification["tier_counts"], {"large": 8000, "small": 800})
        self.assertGreater(verification["empty_doc_count"], 0)
        self.assertTrue(verification["checks"]["large_batches_have_seven_labels"])
        self.assertTrue(verification["checks"]["small_batches_have_four_labels"])

    def test_end_to_end_stop_resume_parallel_and_traceability(self) -> None:
        config = self._small_config("integration-small")
        sequential = self.root / "sequential"
        parallel = self.root / "parallel"
        stopped = self.root / "stopped"
        sequential_result = run_pipeline(sequential, config, self.data / "index.json", execution_workers=1)
        parallel_result = run_pipeline(parallel, config, self.data / "index.json", execution_workers=4)
        self.assertEqual(sequential_result["doc_count"], 800)
        self.assertEqual(parallel_result["results_digest"], sequential_result["results_digest"])
        comparison = compare_workspaces(sequential, parallel)
        self.assertEqual(comparison["status"], "ok")
        schedule = read_json(sequential / "schedule.json")
        self.assertEqual(
            [item["reason"] for item in schedule["rejected"]],
            ["E_EVALUATION_BUDGET_EXCEEDED", "E_EVALUATION_BUDGET_EXCEEDED"],
        )
        stop_result = run_pipeline(
            stopped,
            config,
            self.data / "index.json",
            stop_after_batches=1,
            execution_workers=2,
        )
        self.assertEqual(stop_result["status"], "stopped")
        self.assertFalse((stopped / "results.jsonl").exists())
        self.assertEqual(CheckpointManager(stopped).state()["sequence"], 4)
        resumed = run_pipeline(stopped, config, self.data / "index.json", execution_workers=4)
        self.assertEqual(resumed["status"], "ok")
        self.assertEqual(compare_workspaces(stopped, sequential)["status"], "ok")
        integrity = CheckpointManager(stopped).verify_integrity()
        self.assertTrue(integrity["contiguous"])
        self.assertEqual(integrity["first_sequence"], 1)
        rows = read_jsonl(sequential / "results.jsonl")
        self.assertEqual([row["doc_id"] for row in rows], sorted(row["doc_id"] for row in rows))
        summary = read_json(sequential / "summary.json")
        self.assertEqual(summary["results_digest"], sequential_result["results_digest"])
        self.assertTrue(any(row["error_code"] == "E_EMPTY_DOC" for row in rows))
        empty_row = next(row for row in rows if row["error_code"] == "E_EMPTY_DOC")
        self.assertIsNone(empty_row["predicted_label"])
        feature_diagnostics = read_jsonl(sequential / "diagnostics/features/batch_01_small.jsonl")
        self.assertTrue(any(row["error_code"] == "E_EMPTY_DOC" for row in feature_diagnostics))
        for row in rows[:20]:
            if not row.get("error_code"):
                evidence = row["score_evidence"]
                self.assertTrue((sequential / evidence["path"]).is_file())
                self.assertGreaterEqual(evidence["line"], 1)
                self.assertTrue((sequential / row["calibration_ref"]["path"]).is_file())
        calibration = read_json(sequential / "calibration/batch_01_small.json")
        self.assertIn("fraud", calibration["classes"])
        self.assertTrue(calibration["classes"]["fraud"]["samples"]["holdout_doc_ids"])
        self.assertTrue(calibration["classes"]["fraud"]["review_required"])
        self.assertIn("E_CALIBRATION_LOW_SAMPLES", calibration["classes"]["fraud"]["review_reasons"])
        self.assertTrue((sequential / "summaries/batch_01_small.json").is_file())

    def test_injected_bad_smoothing_and_unreachable_fpr(self) -> None:
        bad_config = self._small_config("bad-smoothing", smoothing=-1.0)
        with self.assertRaises(PipelineError) as smoothing:
            run_pipeline(self.root / "bad-smoothing-ws", bad_config, self.data / "index.json")
        self.assertEqual(smoothing.exception.code, E_INVALID_SMOOTHING)
        raw = copy.deepcopy(self.base_raw)
        raw["evaluation_budget"] = 900
        raw["threshold_max"] = 0.0
        raw["target_fpr"] = 0.0
        raw["thresholds"] = {label: 0.0 for label in raw["thresholds"]}
        unreachable_config = self.root / "unreachable.yaml"
        atomic_write_text(unreachable_config, yaml_lite.dumps(raw))
        workspace = self.root / "unreachable-ws"
        run_pipeline(workspace, unreachable_config, self.data / "index.json")
        calibration = read_json(workspace / "calibration/batch_01_small.json")
        for info in calibration["classes"].values():
            self.assertFalse(info["target_met"])
            self.assertIn("E_FPR_UNREACHABLE", info["review_reasons"])
            self.assertEqual(info["threshold"], 0.0)
        self.assertGreater(read_json(workspace / "report.json")["review_required_count"], 0)

    def test_zero_smoothing_end_to_end_is_explicit_and_traceable(self) -> None:
        config = self._small_config("zero-smoothing", smoothing=0.0)
        workspace = self.root / "zero-smoothing-ws"
        result = run_pipeline(workspace, config, self.data / "index.json", execution_workers=2)
        self.assertEqual(result["status"], "ok")
        rows = read_jsonl(workspace / "results.jsonl")
        zero_rows = [row for row in rows if row.get("error_code") == "E_ZERO_SMOOTHING"]
        self.assertTrue(zero_rows)
        self.assertTrue(zero_rows[0]["score_evidence"])
        self.assertTrue((workspace / zero_rows[0]["score_evidence"]["path"]).is_file())
        model = read_json(workspace / "models/batch_01_small.json")
        self.assertTrue(model["diagnostics"])
        report = read_json(workspace / "report.json")
        self.assertGreater(report["error_counts"]["E_ZERO_SMOOTHING"], 0)

    def test_earliest_invalidation_reuses_upstream_stages(self) -> None:
        config = self._small_config("reuse-base")
        workspace = self.root / "reuse-ws"
        run_pipeline(workspace, config, self.data / "index.json", execution_workers=2)
        manager = CheckpointManager(workspace)
        before = manager.state()
        batch_id = "batch_01_small"
        feature_sequence = before["batches"][batch_id]["stage_records"][0]["sequence"]
        train_sequence = before["batches"][batch_id]["stage_records"][1]["sequence"]
        changed_config = self._small_config(
            "reuse-target-fpr", target_fpr=self.base_raw["target_fpr"] - 0.01
        )
        run_pipeline(workspace, changed_config, self.data / "index.json", execution_workers=2)
        after = manager.state()
        self.assertEqual(after["batches"][batch_id]["stage_records"][0]["sequence"], feature_sequence)
        self.assertEqual(after["batches"][batch_id]["stage_records"][1]["sequence"], train_sequence)
        self.assertGreater(after["batches"][batch_id]["stage_records"][2]["sequence"], train_sequence)
        latest_invalidations = after["batches"][batch_id]["invalidations"]
        self.assertEqual(latest_invalidations[-1]["stage"], "calibration")
        fresh = self.root / "reuse-fresh"
        run_pipeline(fresh, changed_config, self.data / "index.json", execution_workers=2)
        self.assertEqual(compare_workspaces(workspace, fresh)["status"], "ok")

    def test_version_change_detection_maps_to_earliest_stage(self) -> None:
        config_path = self._small_config("detect-source")
        workspace = self.root / "detect-ws"
        run_pipeline(workspace, config_path, self.data / "index.json", execution_workers=1)
        dataset = load_dataset(self.data / "index.json")
        batch = dataset.batches[0]
        manager = CheckpointManager(workspace)
        state = manager.state()
        variants = {
            "hash": ({"hash_dim": self.base_raw["hash_dim"] + 1}, 0),
            "smoothing": ({"smoothing": self.base_raw["smoothing"] + 0.1}, 1),
            "threshold": (
                {"thresholds": {key: 0.09 for key in self.base_raw["thresholds"]}},
                2,
            ),
            "fpr": ({"target_fpr": self.base_raw["target_fpr"] - 0.01}, 2),
            "weights": (
                {"class_weights": {**self.base_raw["class_weights"], "account": 1.1}},
                3,
            ),
        }
        for name, (changes, expected) in variants.items():
            raw = copy.deepcopy(self.base_raw)
            raw.update(changes)
            config = from_dict(raw)
            plan = build_cache_plan(batch, config, workspace, state)
            self.assertEqual(plan.earliest_index, expected, msg=name)
            self.assertTrue(plan.requires_invalidation, msg=name)

    def test_incremental_replace_matches_fresh_and_reuses_other_batches(self) -> None:
        config = self._small_config("incremental-small", evaluation_budget=1200)
        incremental_data = self.root / "incremental-data"
        if incremental_data.exists():
            shutil.rmtree(incremental_data)
        shutil.copytree(self.data, incremental_data)
        workspace = self.root / "incremental-ws"
        run_pipeline(workspace, config, incremental_data / "index.json", execution_workers=2)
        manager = CheckpointManager(workspace)
        before = manager.state()
        before_batch2 = copy.deepcopy(before["batches"]["batch_02_small"])
        source_rows = read_jsonl(incremental_data / "batches/batch_01_small.jsonl")
        changed = 0
        for row in source_rows:
            if row["text"].strip() and changed < 4:
                row["text"] = row["text"] + " 新增 改版 incremental-token-" + str(changed)
                changed += 1
        source = self.root / "replacement-batch-01.jsonl"
        atomic_write_jsonl(source, source_rows)
        result = replace_batch(
            workspace,
            config,
            incremental_data / "index.json",
            "batch_01_small",
            source,
        )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["comparison"]["status"], "ok")
        self.assertTrue(result["new_invalidations"])
        self.assertTrue(
            all(item["earliest_stage"] == "features" for item in result["new_invalidations"])
        )
        after = manager.state()
        self.assertEqual(
            canonical_batch_fingerprints(before_batch2),
            canonical_batch_fingerprints(after["batches"]["batch_02_small"]),
        )

    def test_mismatch_comparison_reports_error_batch_and_field(self) -> None:
        left = self.root / "compare-left"
        right = self.root / "compare-right"
        left.mkdir(exist_ok=True)
        right.mkdir(exist_ok=True)
        row = {"batch_id": "batch_01_small", "doc_id": "d1", "value": 1}
        atomic_write_jsonl(left / "results.jsonl", [row])
        changed = dict(row)
        changed["value"] = 2
        atomic_write_jsonl(right / "results.jsonl", [changed])
        comparison = compare_workspaces(left, right)
        self.assertEqual(comparison["status"], "mismatch")
        self.assertEqual(comparison["error_code"], E_INCREMENTAL_MISMATCH)
        self.assertEqual(comparison["batch_id"], "batch_01_small")
        self.assertIn("results.value", comparison["field"])
        with self.assertRaises(PipelineError) as context:
            ensure_workspace_match(comparison)
        self.assertEqual(context.exception.code, E_INCREMENTAL_MISMATCH)


def canonical_batch_fingerprints(info: dict) -> dict:
    return {
        "next_stage_index": info["next_stage_index"],
        "stage_records": {
            index: {
                "input_fingerprint": record["input_fingerprint"],
                "artifact_digest": record["artifact_digest"],
            }
            for index, record in sorted(info["stage_records"].items())
        },
        "invalidations": info["invalidations"],
    }


if __name__ == "__main__":
    unittest.main()
