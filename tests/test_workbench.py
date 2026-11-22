from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from image_workbench import yaml_lite
from image_workbench.checkpoint import CheckpointStore
from image_workbench.demo import generate_demo
from image_workbench.imaging import encode_png_rgb, load_rgb
from image_workbench.util import atomic_write_bytes
from image_workbench.workbench import Workbench


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _latest_seqs(workspace: Path, task_id: str) -> dict[str, int]:
    store = CheckpointStore(workspace, task_id)
    latest = store.latest_by_stage(store.read_consistent())
    return {stage: record["seq"] for stage, record in latest.items()}


def _rewrite_tasks(workspace: Path, mutate) -> None:
    path = workspace / "tasks.jsonl"
    tasks = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    for task in tasks:
        mutate(task)
    path.write_text(
        "".join(json.dumps(task, ensure_ascii=False, sort_keys=True) + "\n" for task in tasks),
        encoding="utf-8",
    )


class EndToEndTests(unittest.TestCase):
    def test_parallel_run_budget_verification_and_checkpoint_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "demo"
            generate_demo(workspace)
            result = Workbench(workspace).run()
            self.assertEqual(result["status"], "completed")
            self.assertTrue(result["complete"])
            self.assertEqual(result["accepted_task_ids"], ["t010", "t020", "t030"])
            self.assertEqual(result["rejected_task_ids"], ["t040", "t050"])
            self.assertEqual(result["task_counts"]["verified"], 3)
            self.assertEqual(result["task_counts"]["rejected_chain"], 1)
            self.assertEqual(result["task_counts"]["rejected_budget"], 1)
            self.assertEqual(result["summary_sha256"], "45dc629c63d2e970af5d2d62f3a85be2ee20e7fc15c98e91eff3d5135b09f1b6")
            for task_id in ("t010", "t020", "t030"):
                self.assertEqual(
                    list(_latest_seqs(workspace, task_id).keys()),
                    ["parsed", "executed", "size_checked", "exported"],
                )
                self.assertEqual(CheckpointStore(workspace, task_id).snapshot()["next_seq"], 5)
            self.assertEqual(CheckpointStore(workspace, "t040").snapshot()["next_seq"], 2)
            self.assertEqual(CheckpointStore(workspace, "t050").snapshot()["next_seq"], 2)
            verification = json.loads((workspace / "reports" / "verification.json").read_text())
            by_id = {task["task_id"]: task for task in verification["tasks"]}
            self.assertTrue(by_id["t010"]["size_match"])
            self.assertTrue(by_id["t010"]["digest_match"])
            self.assertIsNone(by_id["t040"]["actual"]["size"])
            self.assertEqual(by_id["t050"]["error"]["position"], 0)

    def test_parallel_and_sequential_reports_are_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            parallel = root / "parallel"
            sequential = root / "sequential"
            generate_demo(parallel)
            generate_demo(sequential)
            Workbench(parallel).run()
            Workbench(sequential).run(sequential=True)
            for name in ("verification.json", "schedule.json", "run_summary.json", "versions.json"):
                self.assertEqual(
                    (parallel / "reports" / name).read_bytes(),
                    (sequential / "reports" / name).read_bytes(),
                    name,
                )
            for task_id in ("t010", "t020", "t030"):
                self.assertEqual(
                    (parallel / "artifacts" / "exported" / f"{task_id}.png").read_bytes(),
                    (sequential / "artifacts" / "exported" / f"{task_id}.png").read_bytes(),
                )

    def test_stop_then_resume_matches_clean_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            resumed = root / "resumed"
            fresh = root / "fresh"
            generate_demo(resumed)
            generate_demo(fresh)
            workbench = Workbench(resumed)
            output: dict[str, object] = {}

            def run_in_thread() -> None:
                output["result"] = workbench.run(sequential=True, task_delay_ms=120)

            thread = threading.Thread(target=run_in_thread)
            thread.start()
            deadline = time.time() + 10
            while time.time() < deadline:
                if (resumed / "artifacts" / "exported" / "t010.png").exists():
                    workbench.request_stop()
                    break
                time.sleep(0.01)
            thread.join(30)
            self.assertFalse(thread.is_alive())
            self.assertEqual(output["result"]["status"], "stopped")
            self.assertFalse((resumed / "reports" / "verification.json").exists())
            resumed_result = workbench.run(sequential=True, clear_stop=True)
            fresh_result = Workbench(fresh).run(sequential=True)
            self.assertEqual(resumed_result["status"], "completed")
            self.assertEqual(resumed_result["summary_sha256"], fresh_result["summary_sha256"])
            for name in ("verification.json", "schedule.json", "run_summary.json", "versions.json"):
                self.assertEqual(
                    (resumed / "reports" / name).read_bytes(),
                    (fresh / "reports" / name).read_bytes(),
                    name,
                )
            for task_id in ("t010", "t020", "t030"):
                store = CheckpointStore(resumed, task_id)
                records = store.read_consistent()
                self.assertEqual([record["seq"] for record in records], [1, 2, 3, 4])

    def test_incremental_invalidation_finds_earliest_stage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "demo"
            generate_demo(workspace)
            workbench = Workbench(workspace)
            workbench.run(sequential=True)
            before = _latest_seqs(workspace, "t010")

            _rewrite_tasks(
                workspace,
                lambda task: task.__setitem__(
                    "instruction", task["instruction"] + " [variant]"
                )
                if task["task_id"] == "t010"
                else None,
            )
            result = workbench.run(sequential=True)
            events = [event for event in result["invalidation_events"] if event["task_id"] == "t010"]
            self.assertEqual(events[0]["earliest_stage"], "parsed")
            self.assertIn("instruction", events[0]["changed_components"])
            after = _latest_seqs(workspace, "t010")
            for stage in ("parsed", "executed", "size_checked", "exported"):
                self.assertGreater(after[stage], before[stage])
            verification = json.loads((workspace / "reports" / "verification.json").read_text())
            t010 = next(task for task in verification["tasks"] if task["task_id"] == "t010")
            self.assertEqual(t010["status"], "verified")

            before_size = _latest_seqs(workspace, "t020")
            _rewrite_tasks(
                workspace,
                lambda task: task["expected_output"].__setitem__("width", 41)
                if task["task_id"] == "t020"
                else None,
            )
            result = workbench.run(sequential=True)
            size_events = [
                event for event in result["invalidation_events"] if event["task_id"] == "t020"
            ]
            self.assertEqual(size_events[0]["earliest_stage"], "size_checked")
            self.assertIn("expected_output_size", size_events[0]["changed_components"])
            after_size = _latest_seqs(workspace, "t020")
            self.assertEqual(after_size["parsed"], before_size["parsed"])
            self.assertEqual(after_size["executed"], before_size["executed"])
            self.assertGreater(after_size["size_checked"], before_size["size_checked"])
            verification = json.loads((workspace / "reports" / "verification.json").read_text())
            t020 = next(task for task in verification["tasks"] if task["task_id"] == "t020")
            self.assertEqual(t020["status"], "failed")
            self.assertEqual(t020["error"]["code"], "E_SIZE_MISMATCH")

    def test_budget_change_invalidates_from_parse_and_replans(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "demo"
            generate_demo(workspace)
            workbench = Workbench(workspace)
            workbench.run(sequential=True)
            config_path = workspace / "config.yaml"
            data = yaml_lite.loads(config_path.read_text(encoding="utf-8"))
            data["global_step_budget"] = 29
            config_path.write_text(
                "\n".join(
                    [
                        "version: demo-v1",
                        "max_workers: 3",
                        "global_step_budget: 29",
                        "worker_step_quota: 12",
                        "task_timeout_seconds: 30.0",
                        "max_chain_length: 8",
                        "reachability:",
                        "  rotate_angle_min: -180.0",
                        "  rotate_angle_max: 180.0",
                        "  max_dimension: 512",
                        "cost_model:",
                        "  steps_per_operator: 2",
                        "  pixels_per_step: 1024",
                        "  min_task_steps: 1",
                        "export:",
                        "  format: png-rgb-v1",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            result = workbench.run(sequential=True)
            self.assertEqual(result["accepted_task_ids"], ["t010", "t020"])
            self.assertIn("t030", result["rejected_task_ids"])
            events = [event for event in result["invalidation_events"] if event["task_id"] == "t010"]
            self.assertEqual(events[0]["earliest_stage"], "parsed")
            self.assertIn("config_version", events[0]["changed_components"])

    def test_export_digest_and_input_pixel_invalidation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory) / "demo"
            generate_demo(workspace)
            workbench = Workbench(workspace)
            workbench.run(sequential=True)
            before_export = _latest_seqs(workspace, "t030")
            _rewrite_tasks(
                workspace,
                lambda task: task["expected_output"].__setitem__("sha256", "1" * 64)
                if task["task_id"] == "t030"
                else None,
            )
            result = workbench.run(sequential=True)
            export_events = [
                event for event in result["invalidation_events"] if event["task_id"] == "t030"
            ]
            self.assertEqual(export_events[0]["earliest_stage"], "exported")
            after_export = _latest_seqs(workspace, "t030")
            self.assertEqual(after_export["parsed"], before_export["parsed"])
            self.assertEqual(after_export["executed"], before_export["executed"])
            self.assertEqual(after_export["size_checked"], before_export["size_checked"])
            self.assertGreater(after_export["exported"], before_export["exported"])
            verification = json.loads((workspace / "reports" / "verification.json").read_text())
            t030 = next(task for task in verification["tasks"] if task["task_id"] == "t030")
            self.assertEqual(t030["status"], "failed")
            self.assertEqual(t030["error"]["code"], "E_DIGEST_MISMATCH")

            shard = workspace / "inputs" / "shard_003.png"
            loaded = load_rgb(shard)
            pixels = loaded.pixels.copy()
            pixels[0, 0, 0] = int(pixels[0, 0, 0]) ^ 0xFF
            atomic_write_bytes(shard, encode_png_rgb(pixels))
            before_pixel = _latest_seqs(workspace, "t050")
            pixel_result = workbench.run(sequential=True)
            pixel_events = [
                event
                for event in pixel_result["invalidation_events"]
                if event["task_id"] == "t050"
            ]
            self.assertEqual(pixel_events[0]["earliest_stage"], "parsed")
            self.assertIn("input_content_digest", pixel_events[0]["changed_components"])
            after_pixel = _latest_seqs(workspace, "t050")
            self.assertGreater(after_pixel["parsed"], before_pixel["parsed"])


if __name__ == "__main__":
    unittest.main()
