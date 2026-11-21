import os
import tempfile
import threading
import unittest

from src.checkpoints import publish_checkpoint, read_checkpoint
from src.errors import E_STALE_CHECKPOINT, StaleCheckpointError


def make_record(shard_id, sequence):
    return {"shard_id": shard_id, "sequence": sequence,
            "prev_shard_id": "", "input_fingerprint": "fp",
            "config_fingerprint": "cfg", "completed_folds": 1,
            "product_fingerprint": "prod", "results": [],
            "failures": [], "plan": {}}


class ConcurrentPublishTest(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.mkdtemp(prefix="wb-concurrent-")
        publish_checkpoint(self.workspace, make_record("shard-01", 0))

    def test_only_contiguous_sequence_wins(self):
        errors = []
        barrier = threading.Barrier(4)

        def publish(sequence):
            barrier.wait()
            try:
                publish_checkpoint(self.workspace,
                                   make_record("shard-01", sequence))
            except StaleCheckpointError as exc:
                errors.append(exc.code)

        # sequences 1,1,2,9: the first 1 and the contiguous 2 may win under
        # the lock; every duplicate/stale/non-contiguous attempt is rejected
        threads = [threading.Thread(target=publish, args=(sequence,))
                   for sequence in (1, 1, 2, 9)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        record = read_checkpoint(self.workspace, "shard-01")
        # only contiguous advances are accepted; the jump to 9 never wins
        self.assertIn(record["sequence"], (1, 2))
        self.assertTrue(all(code == E_STALE_CHECKPOINT for code in errors))
        self.assertGreaterEqual(len(errors), 2)

    def test_lock_file_lives_in_state_dir(self):
        # publishing must not pollute the checkpoints directory with a lock
        entries = os.listdir(os.path.join(self.workspace, "checkpoints"))
        self.assertNotIn(".workspace.lock", entries)
        self.assertTrue(os.path.exists(
            os.path.join(self.workspace, "state", ".workspace.lock")))


if __name__ == "__main__":
    unittest.main()
