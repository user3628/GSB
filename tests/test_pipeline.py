from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

import numpy as np

from gspipe.errors import E_INVALID_STAGE, PipelineError
from gspipe.pipeline import Pipeline
from gspipe.pngio import decode_rgb_png, encode_rgb_png
from gspipe.workspace import Workspace


DEMO_SUMMARY_DIGEST = "886d754242dc2decbbd13dc64640b44c35ba4d9e9a0ebd8a213f009c3bb81a58"


def fresh_workspace() -> tuple[Path, Workspace]:
    root = Path(tempfile.mkdtemp(prefix="gspipe-pipeline-"))
    workspace = Workspace(root)
    workspace.init_demo()
    return root, workspace


def load_tasks_json(root: Path) -> dict:
    return json.loads((root / "inputs" / "tasks.json").read_text(encoding="utf-8"))


def save_tasks_json(root: Path, manifest: dict) -> None:
    (root / "inputs" / "tasks.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def find_task(manifest: dict, task_id: str) -> dict:
    return next(task for task in manifest["tasks"] if task["task_id"] == task_id)


class PipelineTests(unittest.TestCase):
    def test_full_demo_latency_and_export(self) -> None:
        root, workspace = fresh_workspace()
        pipeline = Pipeline(root)
        report = pipeline.run()
        self.assertEqual(report["status"], "COMPLETED")
        self.assertEqual(report["checkpoints_written"], 24)
        self.assertEqual(report["summary_digest"], DEMO_SUMMARY_DIGEST)
        self.assertEqual(report["status_counts"], {"PROCESSED": 5, "UNPROCESSED": 1})
        self.assertEqual(len(list((root / "inputs" / "shards").glob("*.png"))), 4)
        self.assertEqual(len(workspace.load_tasks()), 6)
        self.assertGreater(len(report["scheduler"]["accepted"]), 0)

        latest = pipeline.latest_export()
        self.assertIsNotNone(latest)
        self.assertEqual(latest["summary_digest"], DEMO_SUMMARY_DIGEST)
        task6 = next(item for item in latest["summary"]["tasks"] if item["task_id"] == "task_006")
        self.assertEqual(task6["status"], "UNPROCESSED")
        self.assertEqual(task6["reason"], "LATENCY_BUDGET_EXCEEDED")
        self.assertFalse((root / latest["bundle_dir"] / "results" / "task_006.png").exists())

        status = pipeline.status()
        task6_status = next(item for item in status["tasks"] if item["task_id"] == "task_006")
        self.assertEqual(task6_status["status"], "UNPROCESSED")
        self.assertEqual(task6_status["runtime_metrics"]["latency_budget_ms"], 0)
        self.assertFalse(task6_status["runtime_metrics"]["within_latency_budget"])
        self.assertGreater(task6_status["runtime_metrics"]["elapsed_ns"], 0)
        task1_status = next(item for item in status["tasks"] if item["task_id"] == "task_001")
        self.assertEqual(len(task1_status["execution"]["chunks"]), 2)
        self.assertGreater(task1_status["runtime_metrics"]["elapsed_ms"], 0)

    def test_identical_rerun_reuses_checkpoints(self) -> None:
        root, _ = fresh_workspace()
        pipeline = Pipeline(root)
        first = pipeline.run()
        second = pipeline.run()
        self.assertEqual(second["checkpoints_written"], 0)
        self.assertEqual(second["summary_digest"], first["summary_digest"])
        self.assertEqual([item["checkpoints_written"] for item in second["stages"]], [0, 0, 0, 0])

    def test_parallel_and_sequential_reduction_are_byte_identical(self) -> None:
        root_parallel, _ = fresh_workspace()
        root_sequential, _ = fresh_workspace()
        parallel_pipeline = Pipeline(root_parallel)
        sequential_pipeline = Pipeline(root_sequential)
        parallel = parallel_pipeline.run(parallel=True)
        sequential = sequential_pipeline.run(parallel=False)
        self.assertEqual(parallel["summary_digest"], sequential["summary_digest"])
        for task_id in ("task_001", "task_002", "task_003", "task_004", "task_005"):
            self.assertEqual(
                parallel_pipeline.get_result_png(task_id),
                sequential_pipeline.get_result_png(task_id),
                msg=task_id,
            )

    def test_graceful_stop_and_resume_match_full_run(self) -> None:
        reference_root, _ = fresh_workspace()
        reference = Pipeline(reference_root).run()

        root, workspace = fresh_workspace()
        stopped_once = {"value": False}

        def on_progress(event: dict) -> None:
            if (
                not stopped_once["value"]
                and event.get("event") == "checkpoint_published"
                and event.get("stage") == "parse"
            ):
                stopped_once["value"] = True
                workspace.request_stop("test_after_first_parse")

        pipeline = Pipeline(root, on_progress=on_progress)
        first = pipeline.run()
        self.assertEqual(first["status"], "STOPPED")
        self.assertEqual(first["checkpoints_written"], 1)
        self.assertEqual(first["stages"][0]["checkpoints_written"], 1)

        resumed = Pipeline(root).run(resume=True)
        self.assertEqual(resumed["status"], "COMPLETED")
        self.assertEqual(resumed["summary_digest"], reference["summary_digest"])
        resumed_pipeline = Pipeline(root)
        self.assertEqual(
            resumed_pipeline.get_result_png("task_001"),
            Pipeline(reference_root).get_result_png("task_001"),
        )
        for task_id in ("task_001", "task_002", "task_003", "task_004", "task_005"):
            state = resumed_pipeline.checkpoints.state(task_id)
            sequences = [int(record["seq"]) for record in state.records]
            self.assertEqual(sequences, list(range(1, len(sequences) + 1)))
            self.assertEqual([record["stage"] for record in state.records], ["parse", "execute", "verify", "export"])

    def test_query_cache_hit_and_recompute_are_byte_identical(self) -> None:
        root, _ = fresh_workspace()
        pipeline = Pipeline(root)
        pipeline.run()
        first, hit_first = pipeline.query_task("task_001")
        second, hit_second = pipeline.query_task("task_001")
        recomputed, hit_recomputed = pipeline.query_task("task_001", force_recompute=True)
        self.assertFalse(hit_first)
        self.assertTrue(hit_second)
        self.assertFalse(hit_recomputed)
        self.assertEqual(first, second)
        self.assertEqual(first, recomputed)

    def test_execution_cache_hit_preserves_output_bytes(self) -> None:
        root, _ = fresh_workspace()
        pipeline = Pipeline(root)
        first = pipeline.run()
        original = pipeline.get_result_png("task_001")
        shutil.rmtree(root / ".gspipe" / "checkpoints")
        second = Pipeline(root).run()
        self.assertEqual(first["summary_digest"], second["summary_digest"])
        self.assertEqual(original, Pipeline(root).get_result_png("task_001"))
        status = Pipeline(root).status()
        task1 = next(item for item in status["tasks"] if item["task_id"] == "task_001")
        self.assertTrue(task1["runtime_metrics"]["cache_hit"])

    def test_incremental_invalidation_earliest_stages(self) -> None:
        root, _ = fresh_workspace()
        pipeline = Pipeline(root)
        first = pipeline.run()
        first_digest = first["summary_digest"]
        first_state = {task_id: pipeline.checkpoints.state(task_id) for task_id in ("task_001", "task_002", "task_003")}

        manifest = load_tasks_json(root)
        task1 = find_task(manifest, "task_001")
        task1["instruction"] = task1["instruction"].replace("dr=12", "dr=13")
        task1["expected_summary"] = task1["expected_summary"].replace("dr=12", "dr=13")
        save_tasks_json(root, manifest)
        changed = Pipeline(root).run()
        self.assertNotEqual(changed["summary_digest"], first_digest)
        self.assertEqual(
            {(item["task_id"], item["earliest_stage"]) for item in changed["invalidations"]},
            {("task_001", "parse")},
        )
        self.assertIn("instruction_changed", changed["invalidations"][0]["reasons"])
        self.assertGreater(
            Pipeline(root).checkpoints.state("task_001").latest("parse")["seq"],
            first_state["task_001"].latest("parse")["seq"],
        )
        parse_after_instruction = Pipeline(root).checkpoints.state("task_001").latest("parse")["seq"]
        self.assertEqual(
            Pipeline(root).checkpoints.state("task_002").latest("parse")["seq"],
            first_state["task_002"].latest("parse")["seq"],
        )

        config_path = root / "config.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        config["execution"]["scheduler"]["max_workers"] = 2
        config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
        scheduling_changed = Pipeline(root).run()
        self.assertEqual(scheduling_changed["summary_digest"], changed["summary_digest"])
        self.assertEqual(len(scheduling_changed["invalidations"]), 6)
        self.assertTrue(all(item["earliest_stage"] == "execute" for item in scheduling_changed["invalidations"]))
        self.assertTrue(
            all("scheduling_budget_changed" in item["reasons"] for item in scheduling_changed["invalidations"])
        )
        self.assertEqual(Pipeline(root).checkpoints.state("task_001").latest("parse")["seq"], parse_after_instruction)

        manifest = load_tasks_json(root)
        task6 = find_task(manifest, "task_006")
        task6["latency_budget_ms"] = 2000
        save_tasks_json(root, manifest)
        latency_changed = Pipeline(root).run()
        self.assertEqual(
            {(item["task_id"], item["earliest_stage"]) for item in latency_changed["invalidations"]},
            {("task_006", "execute")},
        )
        self.assertIn("latency_budget_changed", latency_changed["invalidations"][0]["reasons"])
        self.assertNotEqual(latency_changed["summary_digest"], scheduling_changed["summary_digest"])
        task6_status = next(item for item in Pipeline(root).status()["tasks"] if item["task_id"] == "task_006")
        self.assertEqual(task6_status["status"], "PROCESSED")

        shard2 = root / "inputs" / "shards" / "shard_02.png"
        image = decode_rgb_png(shard2.read_bytes())
        image[1, 1, 0] = (int(image[1, 1, 0]) + 1) % 256
        shard2.write_bytes(encode_rgb_png(image))
        pixel_changed = Pipeline(root).run()
        self.assertEqual(
            {(item["task_id"], item["earliest_stage"]) for item in pixel_changed["invalidations"]},
            {("task_002", "parse"), ("task_006", "parse")},
        )
        self.assertTrue(all("input_pixels_changed" in item["reasons"] for item in pixel_changed["invalidations"]))

        manifest = load_tasks_json(root)
        task3 = find_task(manifest, "task_003")
        old_execute_seq = Pipeline(root).checkpoints.state("task_003").latest("execute")["seq"]
        task3["expected_summary"] = "rotate(angle=1)>blur(radius=1)"
        save_tasks_json(root, manifest)
        verify_changed = Pipeline(root).run()
        self.assertEqual(
            {(item["task_id"], item["earliest_stage"]) for item in verify_changed["invalidations"]},
            {("task_003", "verify")},
        )
        self.assertEqual(Pipeline(root).checkpoints.state("task_003").latest("execute")["seq"], old_execute_seq)
        task3_status = next(item for item in Pipeline(root).status()["tasks"] if item["task_id"] == "task_003")
        self.assertEqual(task3_status["status"], "MISMATCH")
        invalidation_events = task3_status["invalidations"]
        self.assertTrue(
            any(
                event["earliest_stage"] == "verify" and "expected_summary_changed" in event["reasons"]
                for event in invalidation_events
            )
        )
        self.assertTrue(all(event["old_fingerprint"] and event["new_fingerprint"] for event in invalidation_events))

    def test_budget_rejection_and_invalid_stage_jump(self) -> None:
        root, _ = fresh_workspace()
        config_path = root / "config.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        config["execution"]["scheduler"]["total_step_budget"] = 1000
        config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
        report = Pipeline(root).run()
        self.assertEqual(report["status"], "COMPLETED")
        execute_stage = next(stage for stage in report["stages"] if stage["stage"] == "execute")
        reasons = {item["task_id"]: item["reason"] for item in execute_stage["unprocessed"]}
        self.assertEqual(reasons["task_002"], "TOTAL_STEP_BUDGET")
        self.assertEqual(reasons["task_006"], "TOTAL_STEP_BUDGET")

        fresh_root, _ = fresh_workspace()
        with self.assertRaises(PipelineError) as ctx:
            Pipeline(fresh_root).run(["execute"])
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)
        self.assertEqual(ctx.exception.details["reason"], "missing_or_stale_predecessor")

        with self.assertRaises(PipelineError) as ctx:
            Pipeline(fresh_root).run(["parse", "verify"])
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)
        self.assertEqual(ctx.exception.details["reason"], "non_contiguous_stage_request")


if __name__ == "__main__":
    unittest.main()
