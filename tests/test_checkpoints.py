"""Checkpoint atomicity and publish protection (E_STALE_CHECKPOINT)."""

from __future__ import annotations

import os

from _base import WorkspaceTestCase

from rerank_bench.checkpoints import CheckpointStore
from rerank_bench.errors import E_STALE_CHECKPOINT, PipelineError


class TestCheckpoints(WorkspaceTestCase):
    def test_publish_and_read_back(self):
        store = CheckpointStore(self.ws_root)
        rec = store.publish("shard-000", "assemble", "fp1", "1.0.0",
                            {"hello": "world"})
        self.assertEqual(rec["seq"], 1)
        self.assertTrue(store.is_valid("shard-000", "assemble", "fp1"))
        self.assertFalse(store.is_valid("shard-000", "assemble", "other"))

    def test_sequence_advances_continuously(self):
        store = CheckpointStore(self.ws_root)
        store.publish("shard-000", "assemble", "fp1", "1", {"a": 1})
        rec = store.publish("shard-000", "score", "fp2", "1", {"b": 2})
        self.assertEqual(rec["seq"], 2)

    def test_stale_publish_rejected(self):
        store = CheckpointStore(self.ws_root)
        store.publish("shard-000", "assemble", "fp1", "1", {"a": 1})
        with self.assertRaises(PipelineError) as ctx:
            store.publish_with_seq(
                {"shard_id": "shard-000", "stage": "score", "seq": 1},
                {"b": 2})
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)

    def test_skipped_sequence_rejected(self):
        store = CheckpointStore(self.ws_root)
        with self.assertRaises(PipelineError) as ctx:
            store.publish_with_seq(
                {"shard_id": "shard-000", "stage": "assemble", "seq": 5},
                {"a": 1})
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)
        self.assertEqual(ctx.exception.details["expected_seq"], 1)

    def test_no_half_written_records(self):
        store = CheckpointStore(self.ws_root)
        store.publish("shard-000", "assemble", "fp1", "1", {"a": 1})
        for dirpath, _, files in os.walk(store.ckpt_dir):
            for name in files:
                self.assertFalse(name.startswith("tmp"),
                                 f"temporary file left behind: {name}")
                self.assertNotIn(".tmp.", name)

    def test_invalidate_removes_record_and_artifact(self):
        store = CheckpointStore(self.ws_root)
        store.publish("shard-000", "assemble", "fp1", "1", {"a": 1})
        store.invalidate("shard-000", "assemble")
        self.assertIsNone(store.get_record("shard-000", "assemble"))
        self.assertFalse(os.path.exists(
            store.artifact_path("shard-000", "assemble")))
