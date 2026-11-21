from __future__ import annotations

import json
import multiprocessing
import tempfile
import unittest
from pathlib import Path

from cmir_alignment.canonical import atomic_write_json, digest_obj
from cmir_alignment.checkpoints import CheckpointStore, _record_digest
from cmir_alignment.configs import default_config
from cmir_alignment.datagen import generate_dataset
from cmir_alignment.engine import RetrievalEngine
from cmir_alignment.errors import CMIRError, E_INVALID_STAGE, E_STALE_CHECKPOINT


def _publish_worker(
    root: str,
    barrier: multiprocessing.synchronize.Barrier,
    queue: multiprocessing.queues.Queue,
    unit_index: int = 0,
) -> None:
    barrier.wait()
    try:
        record = CheckpointStore(root).publish(unit_index)
        queue.put(("ok", record["seq"]))
    except CMIRError as exc:
        queue.put(("error", exc.code, exc.details))
    except Exception as exc:  # pragma: no cover - unexpected worker crash
        queue.put(("crash", type(exc).__name__, str(exc)))


def _unit(store: CheckpointStore, *, shard_id: str = "shard-00", stage: str = "feature", shard_seq: int = 1):
    artifact = {
        "schema": "test.artifact.v1",
        "shard_id": shard_id,
        "stage": stage,
        "value": f"{shard_id}:{stage}",
    }
    digest = store.put_object(artifact)
    return {
        "shard_id": shard_id,
        "stage": stage,
        "shard_seq": shard_seq,
        "cycle": 1,
        "input_digest": digest_obj(["input", shard_id, stage]),
        "config_version": "test.v1",
        "config_digest": digest_obj(["config"]),
        "quant_param_digest": digest_obj(["quant"]),
        "artifact_digest": digest,
        "artifact_object": digest,
        "artifact_path": f"artifacts/{stage}/{shard_id}.json",
        "reused": False,
    }


class CheckpointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="cmir-cp-")
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_stale_duplicate_and_cross_shard_publish(self) -> None:
        store = CheckpointStore(self.root / "cp")
        store.create_plan(
            [_unit(store), _unit(store, stage="projection_quant", shard_seq=2)],
            plan_id="plan-a",
            context={"test": True},
        )
        with self.assertRaises(CMIRError) as caught:
            store.publish(1)
        self.assertEqual(caught.exception.code, E_STALE_CHECKPOINT)
        first = store.publish(0)
        self.assertEqual(first["seq"], 1)
        with self.assertRaises(CMIRError) as duplicate:
            store.publish(0)
        self.assertEqual(duplicate.exception.code, E_STALE_CHECKPOINT)
        second = store.publish(1)
        self.assertEqual(second["seq"], 2)

    def test_missing_prerequisite_stage_is_invalid(self) -> None:
        store = CheckpointStore(self.root / "cp")
        store.create_plan(
            [_unit(store, stage="index", shard_seq=3)],
            plan_id="plan-bad-order",
            context={"test": True},
        )
        with self.assertRaises(CMIRError) as caught:
            store.publish(0)
        self.assertEqual(caught.exception.code, E_INVALID_STAGE)

    def test_concurrent_processes_only_one_advances(self) -> None:
        store = CheckpointStore(self.root / "cp")
        store.create_plan(
            [_unit(store)],
            plan_id="plan-race",
            context={"test": True},
        )
        context = multiprocessing.get_context("fork")
        count = 4
        barrier = context.Barrier(count)
        queue = context.Queue()
        processes = [
            context.Process(
                target=_publish_worker,
                args=(str(self.root / "cp"), barrier, queue, 0),
            )
            for _ in range(count)
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join(timeout=20)
        self.assertTrue(all(process.exitcode == 0 for process in processes))
        results = [queue.get(timeout=5) for _ in range(count)]
        successes = [row for row in results if row[0] == "ok"]
        failures = [row for row in results if row[0] == "error"]
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(failures), count - 1)
        self.assertTrue(all(row[1] == E_STALE_CHECKPOINT for row in failures))

    def test_audit_distinguishes_advance_rollback_and_tamper(self) -> None:
        data = self.root / "data"
        generate_dataset(data, shards=3, items_per_shard=3)
        workspace = self.root / "ws"
        engine = RetrievalEngine(workspace, dataset_dir=data, config=default_config(8))
        engine.import_dataset(data)
        engine.build()
        self.assertEqual(engine.checkpoints.audit()["status"], "advance")

        head_path = workspace / "checkpoints" / "head.json"
        head = json.loads(head_path.read_text(encoding="utf-8"))
        original_head = dict(head)
        head["last_seq"] = max(0, int(head["last_seq"]) - 1)
        atomic_write_json(head_path, head)
        self.assertEqual(engine.checkpoints.audit()["status"], "rollback")
        atomic_write_json(head_path, original_head)

        state = json.loads((workspace / "state.json").read_text(encoding="utf-8"))
        first_shard = sorted(state["shards"])[0]
        object_digest = state["shards"][first_shard]["feature"]["object"]
        object_path = workspace / "objects" / object_digest[:2] / f"{object_digest}.json"
        atomic_write_json(object_path, {"tampered": True})
        self.assertEqual(engine.checkpoints.audit()["status"], "tamper")

    def test_recovery_ignores_corrupt_record_and_resumes(self) -> None:
        data = self.root / "data"
        generate_dataset(data, shards=3, items_per_shard=3)
        workspace = self.root / "ws"
        engine = RetrievalEngine(workspace, dataset_dir=data, config=default_config(8))
        engine.import_dataset(data)
        stopped = engine.build(stop_after_units=4)
        self.assertEqual(stopped["status"], "stopped")
        next_record = workspace / "checkpoints" / "records" / "000000000005.json"
        next_record.write_text("{not-json", encoding="utf-8")
        resumed = engine.resume()
        self.assertEqual(resumed["status"], "complete")
        records = engine.checkpoints.recover().records
        self.assertEqual([row["seq"] for row in records], list(range(1, len(records) + 1)))
        self.assertEqual(engine.checkpoints.audit()["status"], "advance")
        self.assertTrue(engine.verify_full_consistency()["consistent"])


if __name__ == "__main__":
    unittest.main()
