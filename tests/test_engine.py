from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from segval.canonical import canonical_bytes
from segval.demo_generator import generate_shard
from segval.engine import (
    _execute_jobs,
    fingerprints_payload,
    run_workbench,
    sample_view,
    status_payload,
    update_shard,
    verify_incremental,
)
from segval.errors import WorkbenchError
from segval.shards import import_shard, load_shard_file
from segval.workspace import create_workspace


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEMO_DIR = PROJECT_ROOT / "demo_data"


def _setup_workspace(root: Path, *, generated: bool = False) -> object:
    workspace = create_workspace(root)
    if generated:
        for index in (1, 2, 3, 4):
            import_shard(workspace.root, generate_shard(index))
    else:
        for path in sorted(DEMO_DIR.glob("shard-*.json")):
            import_shard(workspace.root, load_shard_file(path))
    return workspace


class EngineTests(unittest.TestCase):
    def test_generated_demo_is_reproducible(self):
        first = canonical_bytes(generate_shard(3))
        second = canonical_bytes(generate_shard(3))
        self.assertEqual(first, second)
        shard = generate_shard(1)
        self.assertEqual(shard["density_label"], "sparse")
        self.assertGreater(sum(len(s["predictions"]) for s in shard["samples"]), 0)

    def test_serial_and_parallel_exports_are_byte_identical(self):
        with tempfile.TemporaryDirectory() as temp:
            serial_root = Path(temp) / "serial"
            parallel_root = Path(temp) / "parallel"
            serial = _setup_workspace(serial_root, generated=True)
            parallel = _setup_workspace(parallel_root, generated=True)
            serial_run = run_workbench(serial.root, workers=1, clear_stop=True)
            parallel_run = run_workbench(parallel.root, workers=2, clear_stop=True)
            self.assertTrue(serial_run["ok"])
            self.assertTrue(parallel_run["ok"])
            self.assertEqual(serial_run["actual_candidate_evaluations"], parallel_run["actual_candidate_evaluations"])
            for name in ("matches.json", "aggregates.json", "schedule.json"):
                self.assertEqual(
                    (serial.root / "exports" / name).read_bytes(),
                    (parallel.root / "exports" / name).read_bytes(),
                    name,
                )

    def test_stop_resume_matches_full_build(self):
        with tempfile.TemporaryDirectory() as temp:
            reference = _setup_workspace(Path(temp) / "reference", generated=True)
            interrupted = _setup_workspace(Path(temp) / "interrupted", generated=True)
            run_workbench(reference.root, workers=2, clear_stop=True)
            stopped = run_workbench(
                interrupted.root,
                workers=2,
                stop_after_samples=1,
                clear_stop=True,
            )
            self.assertTrue(stopped["stopped"])
            resumed = run_workbench(interrupted.root, workers=2, clear_stop=True)
            self.assertTrue(resumed["ok"])
            self.assertFalse(resumed["stopped"])
            for name in ("matches.json", "aggregates.json"):
                self.assertEqual(
                    (reference.root / "exports" / name).read_bytes(),
                    (interrupted.root / "exports" / name).read_bytes(),
                    name,
                )

    def test_budget_admission_and_config_error_codes(self):
        with tempfile.TemporaryDirectory() as temp:
            workspace = _setup_workspace(Path(temp) / "budget", generated=True)
            workspace.update_config(
                {"budget": {"max_evaluations": 180, "per_worker_quota": 100, "max_workers": 2}}
            )
            result = run_workbench(workspace.root, workers=2, clear_stop=True)
            self.assertLessEqual(result["budget"]["reserved_evaluations"], 180)
            self.assertLessEqual(result["actual_candidate_evaluations"], 180)
            self.assertTrue(result["unprocessed"])
            self.assertIn(
                "E_GLOBAL_BUDGET_EXHAUSTED",
                {item["code"] for item in result["unprocessed"]},
            )
            with self.assertRaises(WorkbenchError) as iou_error:
                workspace.update_config({"matching": {"iou_threshold": 1.5}})
            self.assertEqual(iou_error.exception.code, "E_INVALID_IOU_THRESHOLD")
            with self.assertRaises(WorkbenchError) as budget_error:
                workspace.update_config({"budget": {"max_evaluations": -1}})
            self.assertEqual(budget_error.exception.code, "E_INVALID_BUDGET")

    def test_per_worker_quota_rejection_has_distinct_reason(self):
        with tempfile.TemporaryDirectory() as temp:
            workspace = _setup_workspace(Path(temp) / "quota", generated=True)
            workspace.update_config(
                {"budget": {"max_evaluations": 10000, "per_worker_quota": 100, "max_workers": 2}}
            )
            result = run_workbench(workspace.root, workers=2, clear_stop=True)
            reasons = {item["shard_id"]: item["code"] for item in result["unprocessed"]}
            self.assertEqual(reasons["shard-0003"], "E_PER_WORKER_QUOTA_EXCEEDED")
            self.assertEqual(reasons["shard-0004"], "E_PER_WORKER_QUOTA_EXCEEDED")

    def test_incremental_update_invalidates_only_affected_chain_and_verifies(self):
        with tempfile.TemporaryDirectory() as temp:
            workspace = _setup_workspace(Path(temp) / "incremental", generated=True)
            run_workbench(workspace.root, workers=2, clear_stop=True)
            shard = generate_shard(3)
            shard = copy.deepcopy(shard)
            shard["samples"][0]["predictions"][0]["confidence"] = 0.123456
            import_shard(workspace.root, shard)
            fingerprints = fingerprints_payload(workspace.root)
            row = next(item for item in fingerprints["shards"] if item["shard_id"] == "shard-0003")
            self.assertEqual(row["earliest_invalid_stage"], "decode")
            self.assertIn("SHARD_CONTENT_CHANGED", row["invalidation"]["reasons"]["decode"])
            stale_view = sample_view(workspace.root, shard_id="shard-0003")
            self.assertTrue(stale_view["selected"]["stale"])
            self.assertIn("STALE", stale_view["selected"]["warning"])
            updated = update_shard(workspace.root, shard, workers=2)
            self.assertTrue(updated["ok"])
            verified = verify_incremental(workspace.root, auto_rebuild=False)
            self.assertTrue(verified["ok"])
            self.assertTrue(verified["comparisons"]["matches"]["byte_equal"])
            self.assertTrue(verified["comparisons"]["aggregates"]["byte_equal"])

    def test_incremental_mismatch_is_reported_with_first_difference(self):
        with tempfile.TemporaryDirectory() as temp:
            workspace = _setup_workspace(Path(temp) / "mismatch", generated=True)
            run_workbench(workspace.root, workers=1, clear_stop=True)
            with self.assertRaises(WorkbenchError) as mismatch:
                verify_incremental(workspace.root, inject_mismatch=True)
            error = mismatch.exception
            self.assertEqual(error.code, "E_INCREMENTAL_MISMATCH")
            self.assertIn("matches", error.details["first_differences"])
            self.assertGreaterEqual(error.details["first_differences"]["matches"]["byte_offset"], 0)

    def test_budget_change_invalidates_schedule_only(self):
        with tempfile.TemporaryDirectory() as temp:
            workspace = _setup_workspace(Path(temp) / "schedule", generated=True)
            run_workbench(workspace.root, workers=1, clear_stop=True)
            workspace.update_config({"budget": {"max_evaluations": 180000}})
            status = status_payload(workspace.root)
            self.assertEqual(status["schedule_validation"]["status"], "STALE")
            self.assertIn("BUDGET_CONFIG_CHANGED", status["schedule_validation"]["invalid_reasons"])
            self.assertTrue(all(shard["all_valid"] for shard in status["shards"]))

    def test_matching_strategy_change_invalidates_match(self):
        with tempfile.TemporaryDirectory() as temp:
            workspace = _setup_workspace(Path(temp) / "strategy", generated=True)
            run_workbench(workspace.root, workers=1, clear_stop=True)
            workspace.update_config(
                {"matching": {"strategy": "greedy_global_iou_confidence_tiebreak"}}
            )
            status = status_payload(workspace.root)
            row = status["shards"][0]
            self.assertEqual(row["earliest_invalid_stage"], "match")
            self.assertIn("MATCH_CONFIG_CHANGED", row["invalidation"]["reasons"]["match"])

    def test_worker_failure_preserves_other_worker_result(self):
        with tempfile.TemporaryDirectory() as temp:
            workspace = _setup_workspace(Path(temp) / "workers", generated=True)
            valid_config = workspace.config()
            jobs = [
                {
                    "workspace": str(workspace.root),
                    "shard_id": "shard-0001",
                    "shard_index": 1,
                    "config": valid_config,
                    "target_stage": "decode",
                    "run_token": "test-ok",
                    "stop_after_samples": None,
                },
                {
                    "workspace": str(workspace.root),
                    "shard_id": "shard-0002",
                    "shard_index": 2,
                    "config": {"config_version": "broken"},
                    "target_stage": "decode",
                    "run_token": "test-bad",
                    "stop_after_samples": None,
                },
            ]
            results = _execute_jobs(jobs, 2)
            by_index = {item["shard_index"]: item for item in results}
            self.assertTrue(by_index[1]["ok"])
            self.assertFalse(by_index[2]["ok"])
            self.assertEqual(by_index[2]["error"]["code"], "E_WORKER_CRASH")
            self.assertTrue((workspace.root / "checkpoints" / "shard-0001" / "decode.json").exists())


if __name__ == "__main__":
    unittest.main()

