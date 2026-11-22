import json
import os
import shutil
import tempfile
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
import sys

sys.path.insert(0, ROOT)

from asr_pipeline.checkpoint import CheckpointStore, FileLock
from asr_pipeline.errors import (
    CorruptCheckpointError,
    InvalidStageError,
    StaleCheckpointError,
)


class CheckpointTestCase(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.mkdtemp(prefix="asr_ckpt_")
        self.store = CheckpointStore(self.workspace)

    def tearDown(self):
        shutil.rmtree(self.workspace, ignore_errors=True)

    def test_atomic_publish_assigns_monotonic_seq(self):
        first = self.store.publish(
            "s1", "frontend", {"a": 1}, "in", "cfg"
        )
        second = self.store.publish(
            "s1", "decode", {"b": 2}, "in", "cfg"
        )
        self.assertEqual(first["seq"] + 1, second["seq"])
        self.assertFalse(os.path.exists(self.store.shard_path("s1", "frontend") + ".tmp"))

    def test_duplicate_publish_is_stale(self):
        self.store.publish("s1", "frontend", {"a": 1}, "in", "cfg")
        with self.assertRaises(StaleCheckpointError):
            self.store.publish("s1", "frontend", {"a": 2}, "in", "cfg")

    def test_missing_prerequisite_is_invalid_stage(self):
        with self.assertRaises(InvalidStageError):
            self.store.publish("s1", "decode", {"b": 1}, "in", "cfg")

    def test_corrupt_record_is_ignored_on_recovery(self):
        self.store.publish("s1", "frontend", {"a": 1}, "in", "cfg")
        path = self.store.shard_path("s1", "frontend")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{not-json")
        state = self.store.recover()
        self.assertEqual(state["valid"].get("s1", {}), {})
        self.assertTrue(state["ignored"])
        self.assertEqual(state["ignored"][0]["code"], "E_CORRUPT_RECORD")

    def test_force_rebuild_invalidates_downstream(self):
        self.store.publish("s1", "frontend", {"a": 1}, "in", "cfg")
        self.store.publish("s1", "decode", {"b": 1}, "in", "cfg")
        self.store.force_rebuild("s1", "frontend", {"a": 2}, "in2", "cfg")
        self.assertFalse(os.path.exists(self.store.shard_path("s1", "decode")))
        record = self.store.load_record("s1", "frontend")
        self.assertEqual(record["artifact"], {"a": 2})

    def test_file_lock_is_exclusive_across_processes(self):
        import subprocess
        import sys

        helper = os.path.join(ROOT, "scripts", "scripts_concurrent_helper.py")
        lock_path = os.path.join(self.workspace, "x.lock")
        with FileLock(lock_path):
            held = subprocess.run(
                [sys.executable, helper, "try-lock", lock_path],
                capture_output=True, text=True, timeout=10,
            )
            self.assertFalse(json.loads(held.stdout)["acquired"])
        released = subprocess.run(
            [sys.executable, helper, "try-lock", lock_path],
            capture_output=True, text=True, timeout=10,
        )
        self.assertTrue(json.loads(released.stdout)["acquired"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
