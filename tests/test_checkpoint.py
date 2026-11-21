from __future__ import annotations

import json
import multiprocessing
import tempfile
import unittest
from pathlib import Path

from shardseek.checkpoint import CheckpointStore
from shardseek.errors import BusinessError


def _record(stage: str, index: int) -> dict:
    return {
        "shard_id": "shard-a",
        "stage": stage,
        "stage_index": index,
        "input_fingerprint": f"input-{index}",
        "config_version": "config-v1",
        "quantization_fingerprint": "quant-v1",
        "artifact_digest": f"artifact-{index}",
    }


def _session(session_id: str = "session-a") -> dict:
    return {
        "session_id": session_id,
        "start_stage": 1,
        "ordered_shards": ["shard-a"],
        "expected_pairs": [["shard-a", 1], ["shard-a", 2], ["shard-a", 3], ["shard-a", 4]],
        "snapshot_sequence": 1,
    }


def _racing_publish(state_dir: str, barrier, queue) -> None:
    store = CheckpointStore(state_dir)
    barrier.wait()
    try:
        record = store.publish(_record("feature_extract", 1))
        queue.put(("ok", record["seq"]))
    except BusinessError as exc:
        queue.put((exc.code, exc.stage))


class CheckpointTests(unittest.TestCase):
    def test_ordering_continuity_and_atomic_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointStore(Path(directory))
            store.initialize_session(_session())
            with self.assertRaises(BusinessError) as caught:
                store.publish(_record("project_quantize", 2))
            self.assertEqual(caught.exception.code, "E_INVALID_STAGE")
            self.assertEqual(caught.exception.item_id, "shard-a")

            first = store.publish(_record("feature_extract", 1))
            self.assertEqual(first["seq"], 1)
            with self.assertRaises(BusinessError) as caught:
                store.publish(_record("feature_extract", 1))
            self.assertEqual(caught.exception.code, "E_STALE_CHECKPOINT")
            records = [
                store.publish(_record(stage, index))
                for index, stage in enumerate(
                    ("project_quantize", "index_build", "recall_verify"),
                    start=2,
                )
            ]
            self.assertEqual([record["seq"] for record in records], [2, 3, 4])
            payload = store.checkpoint_path.read_text(encoding="utf-8").splitlines()
            self.assertEqual([json.loads(line)["seq"] for line in payload], [1, 2, 3, 4])
            self.assertEqual(store.status()["next_expected"], None)

    def test_concurrent_processes_only_allow_one_next_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointStore(Path(directory))
            store.initialize_session(_session())
            context = multiprocessing.get_context("fork")
            barrier = context.Barrier(2)
            queue = context.Queue()
            processes = [
                context.Process(target=_racing_publish, args=(directory, barrier, queue))
                for _ in range(2)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=10)
                self.assertEqual(process.exitcode, 0)
            outcomes = [queue.get(timeout=2) for _ in range(2)]
            self.assertEqual(sum(1 for outcome in outcomes if outcome[0] == "ok"), 1)
            self.assertIn("E_STALE_CHECKPOINT", {outcome[0] for outcome in outcomes})
            self.assertEqual(len(store.read_records()), 1)

    def test_new_session_archives_previous_checkpoint_log(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointStore(Path(directory))
            store.initialize_session(_session("session-old"))
            store.publish(_record("feature_extract", 1))
            store.initialize_session(_session("session-new"))
            self.assertEqual(store.read_records(), [])
            archived = list(store.archive_dir.glob("*.jsonl"))
            self.assertEqual(len(archived), 1)
            self.assertEqual(len(archived[0].read_text(encoding="utf-8").splitlines()), 1)

    def test_cross_shard_out_of_order_publish_is_stale(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = CheckpointStore(Path(directory))
            session = _session("session-cross")
            session["ordered_shards"] = ["shard-a", "shard-b"]
            session["expected_pairs"] = [
                ["shard-a", 1],
                ["shard-a", 2],
                ["shard-a", 3],
                ["shard-a", 4],
                ["shard-b", 1],
                ["shard-b", 2],
                ["shard-b", 3],
                ["shard-b", 4],
            ]
            store.initialize_session(session)
            record = _record("feature_extract", 1)
            record["shard_id"] = "shard-b"
            with self.assertRaises(BusinessError) as caught:
                store.publish(record)
            self.assertEqual(caught.exception.code, "E_STALE_CHECKPOINT")


if __name__ == "__main__":
    unittest.main()

