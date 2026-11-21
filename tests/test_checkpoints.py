import os
import tempfile
import unittest

from src.checkpoints import (FileLock, publish_checkpoint, read_checkpoint,
                             reset_workspace)
from src.errors import (E_CONFIG_INVALID, E_INVALID_STAGE,
                        E_STALE_CHECKPOINT, ConfigError, StageError,
                        StaleCheckpointError)
from src.pipeline import assert_stage, stage_index


def record(shard_id, sequence, fingerprint="fp", prev=""):
    return {"shard_id": shard_id, "sequence": sequence,
            "prev_shard_id": prev, "input_fingerprint": fingerprint,
            "config_fingerprint": "cfg", "completed_folds": 1,
            "product_fingerprint": "prod", "results": [], "failures": [],
            "plan": {}}


class CheckpointTest(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.mkdtemp(prefix="wb-cp-")

    def test_atomic_publish_and_reread(self):
        publish_checkpoint(self.workspace, record("shard-01", 0))
        loaded = read_checkpoint(self.workspace, "shard-01")
        self.assertEqual(loaded["sequence"], 0)

    def test_stale_and_duplicate_publish_rejected(self):
        publish_checkpoint(self.workspace, record("shard-01", 0))
        with self.assertRaises(StaleCheckpointError) as ctx:
            publish_checkpoint(self.workspace, record("shard-01", 0))
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)
        with self.assertRaises(StaleCheckpointError):
            publish_checkpoint(self.workspace, record("shard-01", 5))

    def test_contiguous_sequence_advances(self):
        publish_checkpoint(self.workspace, record("shard-01", 0))
        publish_checkpoint(self.workspace,
                           record("shard-02", 1, prev="shard-01"))
        loaded = read_checkpoint(self.workspace, "shard-02")
        self.assertEqual(loaded["sequence"], 1)

    def test_fingerprint_change_rejected_as_stale(self):
        publish_checkpoint(self.workspace,
                           record("shard-01", 0, fingerprint="a"))
        with self.assertRaises(StaleCheckpointError):
            publish_checkpoint(self.workspace,
                               record("shard-01", 1, fingerprint="b"))

    def test_crash_before_rename_leaves_no_record(self):
        with self.assertRaises(RuntimeError):
            publish_checkpoint(self.workspace,
                               record("shard-01", 0),
                               crash_after_temp=True)
        self.assertIsNone(read_checkpoint(self.workspace, "shard-01"))
        leftovers = [name for name in
                     os.listdir(os.path.join(self.workspace,
                                             "checkpoints"))
                     if name.startswith(".tmp-")]
        self.assertEqual(leftovers, [])

    def test_file_lock_is_reentrant_across_handles(self):
        path = os.path.join(self.workspace, "state", "lock")
        with FileLock(path):
            self.assertTrue(os.path.exists(path))

    def test_reset_removes_generated_state(self):
        publish_checkpoint(self.workspace, record("shard-01", 0))
        reset_workspace(self.workspace)
        self.assertIsNone(read_checkpoint(self.workspace, "shard-01"))


class StageTest(unittest.TestCase):
    def test_unknown_stage(self):
        with self.assertRaises(StageError) as ctx:
            stage_index("nonsense")
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)

    def test_stage_gate(self):
        with self.assertRaises(StageError) as ctx:
            assert_stage({"stage": "plan"}, "aggregate")
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)
        assert_stage({"stage": "aggregate"}, "plan")  # no error

    def test_invalid_config_code(self):
        from src.config import BacktestConfig
        cfg = BacktestConfig(window_length=0)
        with self.assertRaises(ConfigError) as ctx:
            cfg.validate()
        self.assertEqual(ctx.exception.code, E_CONFIG_INVALID)


if __name__ == "__main__":
    unittest.main()
