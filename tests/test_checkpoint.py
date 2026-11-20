import json
import tempfile
import unittest
from pathlib import Path

from detpipe.checkpoint import CheckpointStore, atomic_write_json
from detpipe.config import PipelineConfig
from detpipe.errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, E_VERSION_MISMATCH, PipeError
from detpipe.pipeline import Pipeline
from detpipe.generate import generate_dataset


class CheckpointTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.dataset = generate_dataset()
        self.config = PipelineConfig(workers=2, shard_size=2)
        self.pipe = Pipeline(self.dataset, self.config, self.root / "ws")
        self.manifest = self.pipe.build_manifest()
        self.pipe.store.write_manifest(self.manifest)

    def tearDown(self):
        self.tmp.cleanup()

    def record(self, index):
        plan = self.pipe.plans[index]
        return self.pipe.build_record(
            plan, self.pipe.process_shard(plan), "processed", worker_id=0
        )


class TestCheckpoint(CheckpointTestBase):
    def test_contiguous_publish_and_recovery(self):
        store = self.pipe.store
        store.publish(self.record(0))
        store.publish(self.record(1))
        self.assertEqual(store.existing_indices(), [0, 1])
        self.assertEqual(store.recover(self.manifest), 2)

    def test_stale_and_gap_codes(self):
        store = self.pipe.store
        r0 = self.record(0)
        store.publish(r0)
        with self.assertRaises(PipeError) as ctx:
            store.publish(self.record(0))
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)
        self.assertEqual(ctx.exception.shard_index, 0)
        with self.assertRaises(PipeError) as ctx:
            store.publish(self.record(2))
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)
        self.assertEqual(ctx.exception.shard_index, 2)

    def test_atomic_write_leaves_no_tmp(self):
        target = self.root / "x" / "doc.json"
        atomic_write_json(target, {"a": 1})
        self.assertEqual(json.loads(target.read_text()), {"a": 1})
        self.assertEqual(list((self.root / "x").parent.glob("_tmp/*")), [])

    def test_stop_flag_lifecycle(self):
        store = self.pipe.store
        self.assertFalse(store.stop_requested())
        store.request_stop()
        self.assertTrue(store.stop_requested())
        store.clear_stop()
        self.assertFalse(store.stop_requested())

    def test_version_mismatch_detected(self):
        store = self.pipe.store
        store.publish(self.record(0))
        broken_manifest = dict(self.manifest)
        broken_manifest["threshold_version"] = "deadbeef"
        with self.assertRaises(PipeError) as ctx:
            store.validate_record_versions(self.record(1), broken_manifest)
        self.assertEqual(ctx.exception.code, E_VERSION_MISMATCH)
        self.assertEqual(ctx.exception.field, "threshold_version")


if __name__ == "__main__":
    unittest.main()
