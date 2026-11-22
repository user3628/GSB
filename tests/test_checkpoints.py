import multiprocessing
import tempfile
import unittest
from pathlib import Path

from shotcheck.checkpoints import CheckpointStore
from shotcheck.errors import BusinessError
from shotcheck.util import atomic_write_json, canonical_bytes


def _race_worker(workspace, barrier, queue, marker):
    store = CheckpointStore(workspace)
    barrier.wait()
    try:
        record = store.publish(
            "normalize",
            {"stage": "normalize", "marker": marker, "shots": {}, "errors": []},
            input_fingerprint=f"fp-{marker}",
            config_version="test",
            config_fingerprint="cfg",
            expected_prev_sequence=0,
        )
        queue.put({"ok": True, "sequence": record["sequence"]})
    except BusinessError as exc:
        queue.put({"ok": False, "error": exc.to_dict()})


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp.name)
        self.store = CheckpointStore(self.workspace)

    def tearDown(self):
        self.temp.cleanup()

    def _publish(self, stage, marker, expected, upstream=None):
        return self.store.publish(
            stage,
            {"stage": stage, "marker": marker, "shots": {}, "errors": []},
            input_fingerprint=f"fp-{marker}",
            config_version="test",
            config_fingerprint="cfg",
            expected_prev_sequence=expected,
            upstream=upstream,
        )

    def test_sequence_duplicate_and_lineage(self):
        first = self._publish("normalize", "one", 0)
        self.assertEqual(first["sequence"], 1)
        with self.assertRaises(BusinessError) as duplicate:
            self._publish("normalize", "one", 1)
        self.assertEqual(duplicate.exception.code, "E_STALE_CHECKPOINT")
        self.assertEqual(duplicate.exception.details["reason"], "duplicate_publish")
        with self.assertRaises(BusinessError) as stale:
            self._publish("normalize", "two", 0)
        self.assertEqual(stale.exception.details["reason"], "sequence_not_contiguous")
        second = self._publish("normalize", "two", 1)
        self.assertEqual(second["sequence"], 2)

        with self.assertRaises(BusinessError) as wrong_upstream:
            self._publish(
                "duration",
                "duration",
                0,
                upstream={
                    "stage": "normalize",
                    "sequence": 99,
                    "input_fingerprint": "wrong",
                    "artifact_digest": "wrong",
                },
            )
        self.assertEqual(
            wrong_upstream.exception.details["reason"], "upstream_lineage_mismatch"
        )
        with self.assertRaises(BusinessError) as jump:
            self._publish("coverage", "coverage", 0)
        self.assertEqual(jump.exception.code, "E_INVALID_STAGE")

        current = self.store.read_checkpoint("normalize")
        self.assertEqual(current["sequence"], 2)
        self.assertEqual(current["artifact_digest"], second["artifact_digest"])

    def test_real_file_lock_allows_one_writer(self):
        context = multiprocessing.get_context("fork")
        barrier = context.Barrier(2)
        queue = context.Queue()
        processes = [
            context.Process(
                target=_race_worker,
                args=(str(self.workspace), barrier, queue, marker),
            )
            for marker in ("a", "b")
        ]
        for process in processes:
            process.start()
        results = [queue.get(timeout=10) for _ in processes]
        for process in processes:
            process.join(timeout=10)
        self.assertEqual(sum(1 for item in results if item["ok"]), 1)
        failures = [item for item in results if not item["ok"]]
        self.assertEqual(failures[0]["error"]["code"], "E_STALE_CHECKPOINT")
        self.assertEqual(self.store.read_checkpoint("normalize")["sequence"], 1)

    def test_artifact_tampering_is_detected(self):
        record = self._publish("normalize", "one", 0)
        artifact_path = self.workspace / "objects" / record["artifact_ref"]
        artifact_path.write_bytes(canonical_bytes({"tampered": True}))
        with self.assertRaises(BusinessError) as mismatch:
            self.store.verify_artifact(record)
        self.assertEqual(
            mismatch.exception.details["reason"], "artifact_digest_mismatch"
        )


if __name__ == "__main__":
    unittest.main()
