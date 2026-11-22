import multiprocessing as multiprocessing_module
import os
import unittest

from ragwb.checkpoint import (STAGE_ORDER, CheckpointStore, make_record)
from ragwb.errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, WorkbenchError
from ragwb.io_utils import FileLock, read_json, write_json
from tests.helpers import fresh_run_dir, make_workspace


def _publisher(run_dir, record, product, config_fp, queue):
    try:
        store = CheckpointStore(run_dir)
        store.publish(record, product, config_fingerprint=config_fp)
        queue.put(("ok", None))
    except WorkbenchError as exc:
        queue.put(("error", exc.code))


class CheckpointTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = make_workspace()
        self.run_dir = fresh_run_dir(self.data_dir)
        self.store = CheckpointStore(self.run_dir)
        self.config_fp = "cfg"

    def _publish_stage(self, stage, turn):
        product = {"fingerprint": f"{stage}-{turn}", "value": turn}
        record = make_record(stage, "sess-x", turn,
                             f"input-{stage}", product, turn * 4 +
                             STAGE_ORDER.index(stage))
        return self.store.publish(
            record, product, config_fingerprint=self.config_fp)

    def test_continuous_sequence_enforced(self):
        for stage in STAGE_ORDER:
            self._publish_stage(stage, 0)
        state = self.store.load_state("sess-x")
        self.assertEqual(state["next_turn"], 1)
        self.assertEqual(state["next_stage_seq"], 0)
        records = self.store.all_records()
        sequences = [record["sequence_number"] for record in records]
        self.assertEqual(sequences, list(range(4)))

    def test_stale_and_duplicate_rejected(self):
        self._publish_stage("query_rewrite", 0)
        with self.assertRaises(WorkbenchError) as caught:
            self._publish_stage("query_rewrite", 0)
        self.assertEqual(caught.exception.code, E_STALE_CHECKPOINT)

    def test_missing_predecessor_rejected(self):
        product = {"fingerprint": "retrieval", "value": 1}
        record = make_record("retrieval", "sess-y", 0, "input", product, 1)
        with self.assertRaises(WorkbenchError) as caught:
            self.store.publish(
                record, product, config_fingerprint=self.config_fp)
        self.assertEqual(caught.exception.code, E_INVALID_STAGE)

    def test_atomic_write_never_half_written(self):
        target = os.path.join(self.run_dir, "atomic.json")
        write_json(target, {"a": 1})
        self.assertEqual(read_json(target), {"a": 1})
        leftovers = [name for name in os.listdir(self.run_dir)
                     if name.startswith(".tmp-")]
        self.assertEqual(leftovers, [])

    def test_real_file_lock_is_exclusive(self):
        lock_path = os.path.join(self.run_dir, "x.lock")
        lock = FileLock(lock_path).acquire()
        queue = multiprocessing_module.Queue()
        process = multiprocessing_module.Process(
            target=_lock_child, args=(lock_path, queue))
        process.start()
        process.join(timeout=3)
        self.assertTrue(queue.empty())
        lock.release()
        process.join(timeout=3)
        self.assertFalse(queue.empty())
        self.assertEqual(queue.get(), ("acquired", None))
        process.close()


def _lock_child(lock_path, queue):
    with FileLock(lock_path):
        queue.put(("acquired", None))


if __name__ == "__main__":
    unittest.main()
