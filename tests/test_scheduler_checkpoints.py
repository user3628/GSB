from __future__ import annotations

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from topk_pipeline.checkpoints import CheckpointStore
from topk_pipeline.demo_data import generate_demo_data
from topk_pipeline.errors import (
    E_BUDGET_EXCEEDED,
    E_INVALID_K,
    E_INVALID_STAGE,
    E_MEMORY_BUDGET,
    E_STALE_CHECKPOINT,
    PipelineError,
)
from topk_pipeline.manifest import load_manifest
from topk_pipeline.scheduler import schedule_shards


ROOT = Path(__file__).resolve().parents[1]


class SchedulerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = tempfile.TemporaryDirectory(prefix="topk-scheduler-")
        cls.data_dir = Path(cls._tmp.name) / "data"
        generate_demo_data(cls.data_dir)
        cls.manifest = load_manifest(cls.data_dir / "manifest.json")

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmp.cleanup()

    def test_normal_schedule_respects_all_limits(self) -> None:
        decision = schedule_shards(self.manifest)
        self.assertEqual(len(decision["accepted"]), 4)
        self.assertEqual(decision["unprocessed"], [])
        self.assertLessEqual(decision["consumed_elements"], decision["global_elements"])
        self.assertLessEqual(
            decision["scheduled_peak_memory_bytes"], decision["peak_memory_bytes"]
        )
        for item in decision["accepted"]:
            self.assertLessEqual(item["estimate"]["elements"], decision["worker_elements"])
            self.assertLessEqual(
                item["estimate"]["memory_bytes"], decision["peak_memory_bytes"]
            )
        self.assertGreaterEqual(decision["concurrency"], 1)

    def test_global_budget_rejection(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["budget"]["global_elements"] = 350
        decision = schedule_shards(manifest)
        self.assertEqual(len(decision["accepted"]), 1)
        rejected = decision["unprocessed"][0]
        self.assertEqual(rejected["error"]["code"], E_BUDGET_EXCEEDED)
        self.assertEqual(rejected["error"]["details"]["reason"], "global_element_budget")
        self.assertLessEqual(decision["consumed_elements"], 350)

    def test_worker_quota_rejection(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["budget"]["worker_elements"] = 1000
        decision = schedule_shards(manifest)
        codes = {
            item["shard_id"]: item["error"]["code"] for item in decision["unprocessed"]
        }
        self.assertEqual(codes["shard_01_large"], E_BUDGET_EXCEEDED)
        self.assertFalse(
            any(
                item["spec"]["shard_id"] == "shard_01_large"
                for item in decision["accepted"]
            )
        )

    def test_memory_budget_rejection_and_concurrency_cap(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["budget"]["peak_memory_bytes"] = 100_000
        decision = schedule_shards(manifest)
        rejected = {item["shard_id"]: item["error"]["code"] for item in decision["unprocessed"]}
        self.assertEqual(rejected["shard_01_large"], E_MEMORY_BUDGET)
        self.assertLessEqual(
            decision["scheduled_peak_memory_bytes"], manifest["budget"]["peak_memory_bytes"]
        )
        self.assertEqual(decision["concurrency"], 1)

    def test_invalid_k_is_rejected_for_budget_planning(self) -> None:
        manifest = copy.deepcopy(self.manifest)
        manifest["shards"][0]["k"] = 0
        decision = schedule_shards(manifest)
        rejected = {item["shard_id"]: item["error"] for item in decision["unprocessed"]}
        self.assertEqual(rejected["shard_00_small"]["code"], E_INVALID_K)
        self.assertEqual(rejected["shard_00_small"]["k"], 0)


class CheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="topk-checkpoint-")
        self.workspace = Path(self._tmp.name)
        self.store = CheckpointStore(self.workspace)
        self.shard_id = "shard_a"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    @staticmethod
    def expected(prefix: str = "v1", *, changed: str | None = None) -> list[dict[str, str]]:
        stages = ("filter", "normalize", "sampling", "validation")
        result = []
        for stage in stages:
            token = f"{prefix}-{stage}"
            if stage == changed:
                token += "-changed"
            result.append(
                {
                    "stage": stage,
                    "input_fingerprint": token,
                    "config_version": f"config-{token}",
                }
            )
        return result

    def publish(self, stage: str, sequence: int, expected: list[dict[str, str]]) -> None:
        item = next(entry for entry in expected if entry["stage"] == stage)
        self.store.publish(
            self.shard_id,
            sequence=sequence,
            stage=stage,
            input_fingerprint=item["input_fingerprint"],
            config_version=item["config_version"],
            content_fingerprint="content-v1",
            artifact={"stage": stage, "sequence": sequence},
        )

    def test_atomic_stage_records_and_reuse(self) -> None:
        expected = self.expected()
        prepared = self.store.prepare(
            self.shard_id, expected, content_fingerprint="content-v1"
        )
        self.assertEqual(prepared["reused_stages"], [])
        self.assertEqual(prepared["next_sequence"], 1)
        for sequence, stage in enumerate(("filter", "normalize", "sampling", "validation"), 1):
            self.publish(stage, sequence, expected)
        state = self.store.load_state(self.shard_id)
        self.assertEqual(state["last_sequence"], 4)
        self.assertEqual([record["stage"] for record in state["records"]], list(("filter", "normalize", "sampling", "validation")))
        for record in state["records"]:
            for key in (
                "shard_id",
                "stage",
                "input_fingerprint",
                "config_version",
                "artifact_digest",
                "sequence",
            ):
                self.assertIn(key, record)
        reused = self.store.prepare(self.shard_id, expected, content_fingerprint="content-v1")
        self.assertEqual(reused["reused_stages"], ["filter", "normalize", "sampling", "validation"])
        self.assertIsNone(reused["earliest_invalid_stage"])
        self.assertEqual(reused["next_sequence"], 5)

    def test_stale_duplicate_and_cross_stage_errors(self) -> None:
        expected = self.expected()
        self.publish("filter", 1, expected)
        with self.assertRaises(PipelineError) as caught:
            self.publish("filter", 1, expected)
        self.assertEqual(caught.exception.code, E_STALE_CHECKPOINT)
        with self.assertRaises(PipelineError) as caught:
            self.publish("sampling", 2, expected)
        self.assertEqual(caught.exception.code, E_INVALID_STAGE)
        with self.assertRaises(PipelineError) as caught:
            self.publish("normalize", 5, expected)
        self.assertEqual(caught.exception.code, E_INVALID_STAGE)

    def test_earliest_invalid_stage_truncates_downstream(self) -> None:
        original = self.expected()
        for sequence, stage in enumerate(("filter", "normalize", "sampling", "validation"), 1):
            self.publish(stage, sequence, original)
        changed = self.expected(changed="normalize")
        prepared = self.store.prepare(
            self.shard_id, changed, content_fingerprint="content-v1"
        )
        self.assertEqual(prepared["reused_stages"], ["filter"])
        self.assertEqual(prepared["earliest_invalid_stage"], "normalize")
        self.assertEqual(prepared["next_sequence"], 5)
        state = self.store.load_state(self.shard_id)
        self.assertEqual([record["stage"] for record in state["records"]], ["filter"])
        self.assertEqual(state["last_sequence"], 4)
        self.publish("normalize", prepared["next_sequence"], changed)
        state = self.store.load_state(self.shard_id)
        self.assertEqual([record["stage"] for record in state["records"]], ["filter", "normalize"])
        self.assertEqual(state["records"][1]["sequence"], 5)

    def test_interrupted_temporary_file_is_ignored(self) -> None:
        expected = self.expected()
        self.publish("filter", 1, expected)
        shard_dir = self.workspace / "checkpoints" / self.shard_id
        (shard_dir / ".state.json.tmp.interrupted").write_bytes(b'{"records": [')
        state = self.store.load_state(self.shard_id)
        self.assertEqual(state["last_sequence"], 1)
        self.assertEqual(len(state["records"]), 1)
        prepared = self.store.prepare(
            self.shard_id, expected, content_fingerprint="content-v1"
        )
        self.assertEqual(prepared["reused_stages"], ["filter"])

    def test_cross_process_publication_has_one_winner(self) -> None:
        probe = ROOT / "scripts" / "checkpoint_publish_probe.py"
        processes = [
            subprocess.Popen(
                [sys.executable, str(probe), str(self.workspace), f"publisher-{index}", "--shard-id", self.shard_id],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for index in range(2)
        ]
        outputs = [process.communicate(timeout=20)[0].strip() for process in processes]
        parsed = [json.loads(output) for output in outputs]
        self.assertEqual(sum(1 for item in parsed if item["ok"]), 1)
        stale = [item for item in parsed if not item["ok"]]
        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["error"]["code"], E_STALE_CHECKPOINT)
        state = self.store.load_state(self.shard_id)
        self.assertEqual(len(state["records"]), 1)


if __name__ == "__main__":
    unittest.main()
