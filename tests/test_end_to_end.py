from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from topk_pipeline.checkpoints import CheckpointStore
from topk_pipeline.demo_data import generate_demo_data
from topk_pipeline.errors import E_NUMERIC
from topk_pipeline.manifest import load_manifest, stage_fingerprints
from topk_pipeline.runner import export_summary, run_manifest


class EndToEndTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = tempfile.TemporaryDirectory(prefix="topk-e2e-")
        cls.root = Path(cls._tmp.name)
        cls.data_dir = cls.root / "data"
        generate_demo_data(cls.data_dir)
        cls.manifest_path = cls.data_dir / "manifest.json"
        cls.raw_manifest = json.loads(cls.manifest_path.read_text(encoding="utf-8"))

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmp.cleanup()

    def _absolute_manifest(self, raw: dict, path: Path) -> Path:
        clone = copy.deepcopy(raw)
        for shard in clone["shards"]:
            shard["path"] = str((self.data_dir / shard["path"]).resolve())
        path.write_text(json.dumps(clone, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        return path

    @staticmethod
    def _summary_entry(summary: dict, shard_id: str) -> dict:
        return next(entry for entry in summary["shards"] if entry["shard_id"] == shard_id)

    def test_parallel_and_sequential_summaries_are_byte_identical(self) -> None:
        sequential_ws = self.root / "sequential"
        parallel_ws = self.root / "parallel"
        sequential = run_manifest(
            self.manifest_path, sequential_ws, workers=1, reset_stop_file=True
        )
        parallel = run_manifest(
            self.manifest_path, parallel_ws, workers=2, reset_stop_file=True
        )
        self.assertFalse(sequential["stopped"])
        self.assertFalse(parallel["stopped"])
        sequential_path = sequential_ws / "summary.json"
        parallel_path = parallel_ws / "summary.json"
        export_summary(sequential_path, sequential["summary"])
        export_summary(parallel_path, parallel["summary"])
        self.assertEqual(sequential_path.read_bytes(), parallel_path.read_bytes())

        for entry in parallel["summary"]["shards"]:
            self.assertEqual(entry["status"], "success")
            tokens = [row["sampled_token"] for row in entry["result"]["rows"]]
            self.assertTrue(all(isinstance(token, int) for token in tokens))
            self.assertTrue(
                all(
                    row["sample_log_probability"] == row["sample_log_probability"]
                    for row in entry["result"]["rows"]
                )
            )

    def test_stop_then_resume_matches_clean_run(self) -> None:
        baseline_ws = self.root / "resume-baseline"
        baseline = run_manifest(self.manifest_path, baseline_ws, workers=2, reset_stop_file=True)
        baseline_path = baseline_ws / "summary.json"
        export_summary(baseline_path, baseline["summary"])

        stopped_ws = self.root / "resume-stopped"
        stopped = run_manifest(
            self.manifest_path,
            stopped_ws,
            workers=1,
            stop_after_completed=2,
            reset_stop_file=True,
        )
        self.assertTrue(stopped["stopped"])
        self.assertGreaterEqual(stopped["summary"]["stats"]["successful_shards"], 2)
        completed_ids = {
            entry["shard_id"]
            for entry in stopped["summary"]["shards"]
            if entry["status"] == "success"
        }
        store = CheckpointStore(stopped_ws)
        for shard_id in completed_ids:
            state = store.load_state(shard_id)
            self.assertEqual(
                [record["stage"] for record in state["records"]],
                ["filter", "normalize", "sampling", "validation"],
            )

        resumed = run_manifest(
            self.manifest_path, stopped_ws, workers=2, reset_stop_file=True
        )
        self.assertFalse(resumed["stopped"])
        resumed_path = stopped_ws / "resumed-summary.json"
        export_summary(resumed_path, resumed["summary"])
        self.assertEqual(baseline_path.read_bytes(), resumed_path.read_bytes())

    def test_all_declared_injection_codes(self) -> None:
        cases = json.loads((self.data_dir / "cases.json").read_text(encoding="utf-8"))["cases"]
        for case in cases:
            with self.subTest(case=case["name"]):
                workspace = self.root / "injections" / case["name"]
                result = run_manifest(self.data_dir / case["manifest"], workspace)
                entry = self._summary_entry(result["summary"], case["shard_id"])
                self.assertEqual(entry["status"], case["status"])
                if case.get("code") is not None:
                    self.assertEqual(entry["error"]["code"], case["code"])
                    self.assertEqual(entry["error"]["stage"], case["stage"])
                if case.get("row") is not None:
                    self.assertEqual(entry["error"]["row"], case["row"])
                if case["name"] == "all_tied_max":
                    row = entry["result"]["rows"][0]
                    self.assertEqual(
                        [candidate["token"] for candidate in row["topk_candidates"]],
                        case["tie_tokens_per_row"],
                    )
                    self.assertTrue(
                        all(
                            candidate["score"] == case["score"]
                            for candidate in row["topk_candidates"]
                        )
                    )

    def test_worker_error_keeps_other_completed_shard(self) -> None:
        raw = copy.deepcopy(self.raw_manifest)
        raw["shards"] = [raw["shards"][0], raw["shards"][1]]
        raw["shards"][0]["path"] = str((self.data_dir / "shard_00_small.npy").resolve())
        raw["shards"][1] = {
            "shard_id": "bad_nan",
            "path": str((self.data_dir / "injection_nan.npy").resolve()),
            "k": 2,
            "temperature": 1.0,
            "nucleus_enabled": False,
            "top_p": 1.0,
            "seed": 8,
            "expected_shape": [4, 2],
            "expected_score_range": [-5.0, 5.0],
            "row_block": 2,
            "col_block": 4,
        }
        manifest_path = self.root / "mixed_manifest.json"
        manifest_path.write_text(json.dumps(raw, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        workspace = self.root / "mixed-worker-error"
        result = run_manifest(manifest_path, workspace, workers=2, reset_stop_file=True)
        good = self._summary_entry(result["summary"], "shard_00_small")
        bad = self._summary_entry(result["summary"], "bad_nan")
        self.assertEqual(good["status"], "success")
        self.assertEqual(bad["status"], "error")
        self.assertEqual(bad["error"]["code"], E_NUMERIC)
        self.assertEqual(bad["error"]["stage"], "filter")
        self.assertEqual(result["summary"]["stats"]["successful_shards"], 1)
        self.assertEqual(result["summary"]["stats"]["error_shards"], 1)

    def test_temperature_change_reuses_filter_and_budget_change_reuses_sampling(self) -> None:
        workspace = self.root / "incremental"
        first = run_manifest(self.manifest_path, workspace, workers=2, reset_stop_file=True)
        store = CheckpointStore(workspace)
        before = store.load_state("shard_00_small")
        before_sequences = {record["stage"]: record["sequence"] for record in before["records"]}

        changed_raw = copy.deepcopy(self.raw_manifest)
        changed_raw["shards"][0]["temperature"] = 0.95
        changed_manifest = self._absolute_manifest(changed_raw, self.root / "changed_temperature.json")
        second = run_manifest(changed_manifest, workspace, workers=2, reset_stop_file=True)
        shard_result = next(
            item for item in second["worker_results"] if item["shard_id"] == "shard_00_small"
        )
        self.assertEqual(shard_result["earliest_invalid_stage"], "normalize")
        self.assertEqual(shard_result["reused_stages"], ["filter"])
        after_temperature = store.load_state("shard_00_small")
        after_sequences = {
            record["stage"]: record["sequence"] for record in after_temperature["records"]
        }
        self.assertEqual(after_sequences["filter"], before_sequences["filter"])
        self.assertGreater(after_sequences["normalize"], before_sequences["normalize"])
        self.assertGreater(after_sequences["sampling"], before_sequences["sampling"])
        self.assertGreater(after_sequences["validation"], before_sequences["validation"])

        budget_raw = copy.deepcopy(changed_raw)
        budget_raw["budget"]["global_elements"] += 1
        budget_manifest = self._absolute_manifest(budget_raw, self.root / "changed_budget.json")
        third = run_manifest(budget_manifest, workspace, workers=2, reset_stop_file=True)
        budget_result = next(
            item for item in third["worker_results"] if item["shard_id"] == "shard_00_small"
        )
        self.assertEqual(budget_result["earliest_invalid_stage"], "validation")
        self.assertEqual(
            budget_result["reused_stages"], ["filter", "normalize", "sampling"]
        )
        after_budget = store.load_state("shard_00_small")
        after_budget_sequences = {
            record["stage"]: record["sequence"] for record in after_budget["records"]
        }
        self.assertEqual(after_budget_sequences["sampling"], after_sequences["sampling"])
        self.assertGreater(after_budget_sequences["validation"], after_sequences["validation"])

    def test_fingerprint_chain_maps_changes_to_earliest_stage(self) -> None:
        manifest = load_manifest(self.manifest_path)
        spec = manifest["shards"][0]
        budget = manifest["budget"]
        base = stage_fingerprints(spec, logits_fingerprint="logits-a", budget=budget)

        def first_changed(changed_spec: dict, *, logits: str = "logits-a", changed_budget: dict | None = None) -> str:
            changed = stage_fingerprints(
                changed_spec,
                logits_fingerprint=logits,
                budget=changed_budget or budget,
            )
            for left, right in zip(base, changed):
                if left["input_fingerprint"] != right["input_fingerprint"]:
                    return left["stage"]
            return "none"

        cases = []
        changed = copy.deepcopy(spec)
        changed["k"] = int(changed["k"]) + 1
        cases.append(("k", first_changed(changed), "filter"))
        changed = copy.deepcopy(spec)
        changed["temperature"] = float(changed["temperature"]) + 0.1
        cases.append(("temperature", first_changed(changed), "normalize"))
        changed = copy.deepcopy(spec)
        changed["top_p"] = float(changed["top_p"]) - 0.01
        cases.append(("top_p", first_changed(changed), "sampling"))
        changed = copy.deepcopy(spec)
        changed["seed"] = int(changed["seed"]) + 1
        cases.append(("seed", first_changed(changed), "sampling"))
        cases.append(("logits", first_changed(spec, logits="logits-b"), "filter"))
        changed_budget = dict(budget)
        changed_budget["global_elements"] += 1
        cases.append(("budget", first_changed(spec, changed_budget=changed_budget), "validation"))
        for name, actual, expected in cases:
            with self.subTest(change=name):
                self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
