"""Publish protection: file lock, seq continuity, predecessor checks."""

import os
import shutil
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workbench.errors import InvalidStageError, StaleCheckpointError
from workbench.store import Store
from workbench.util import sha256_bytes


_META_DIGEST = sha256_bytes(b"meta")


def _record(seq, stage="infer", upstream="genesis:abc", stage_fp=None):
    return {
        "seq": seq,
        "stage": stage,
        "sample_fp": "sample",
        "config_version": 1,
        "stage_fp": stage_fp or "fp-%s" % seq,
        "cache_key": "key-%s-%s" % (stage, seq),
        "upstream_fp": upstream,
        "artifact_digest": _META_DIGEST,
    }


class TestPublishProtection(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb-conc-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.store = Store(os.path.join(self.tmp, "state"))
        self.store.ensure()
        for seq in range(1, 8):
            for stage in ("infer", "encode", "select", "summarize"):
                self.store.write_artifact("key-%s-%s" % (stage, seq), b"meta")

    def test_sequential_publish_ok(self):
        self.store.publish(_record(1, "infer"))
        self.store.publish(_record(2, "encode", upstream="fp-1"))
        self.assertEqual(self.store.head_seq(), 2)

    def test_duplicate_seq_rejected(self):
        self.store.publish(_record(1, "infer"))
        with self.assertRaises(StaleCheckpointError) as ctx:
            self.store.publish(_record(1, "infer"))
        self.assertEqual(ctx.exception.to_dict()["code"], "E_STALE_CHECKPOINT")

    def test_lagging_seq_rejected(self):
        self.store.publish(_record(1, "infer"))
        self.store.publish(_record(2, "encode", upstream="fp-1"))
        with self.assertRaises(StaleCheckpointError):
            self.store.publish(_record(2, "encode", upstream="fp-1"))

    def test_gap_seq_rejected(self):
        self.store.publish(_record(1, "infer"))
        with self.assertRaises(StaleCheckpointError):
            self.store.publish(_record(3, "encode", upstream="fp-1"))

    def test_missing_predecessor_rejected(self):
        self.store.publish(_record(1, "infer"))
        with self.assertRaises(InvalidStageError) as ctx:
            self.store.publish(_record(2, "select", upstream="fp-1"))
        err = ctx.exception.to_dict()
        self.assertEqual(err["code"], "E_INVALID_STAGE")
        self.assertEqual(err["details"]["requires"], "encode")

    def test_wrong_upstream_fp_rejected(self):
        self.store.publish(_record(1, "infer"))
        with self.assertRaises(InvalidStageError):
            self.store.publish(_record(2, "encode", upstream="bogus"))

    def test_error_codes_distinct(self):
        self.assertNotEqual(StaleCheckpointError.code, InvalidStageError.code)

    def test_concurrent_publishers_single_winner(self):
        results = {"ok": 0, "stale": 0, "other": 0}
        lock = threading.Lock()

        def worker():
            try:
                self.store.publish(_record(1, "infer"))
                with lock:
                    results["ok"] += 1
            except StaleCheckpointError:
                with lock:
                    results["stale"] += 1
            except Exception:
                with lock:
                    results["other"] += 1

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(results["ok"], 1)
        self.assertEqual(results["stale"], 7)
        self.assertEqual(results["other"], 0)
        self.assertEqual(self.store.head_seq(), 1)

    def test_concurrent_multi_stage_chain(self):
        outcomes = []

        def pipeline(tag):
            try:
                for seq, stage, up in (
                    (1, "infer", "genesis:abc"),
                    (2, "encode", "fp-1"),
                    (3, "select", "fp-2"),
                ):
                    self.store.publish(_record(seq, stage, upstream=up))
                outcomes.append((tag, "ok"))
            except (StaleCheckpointError, InvalidStageError) as exc:
                outcomes.append((tag, exc.code))

        threads = [threading.Thread(target=pipeline, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        winners = [o for o in outcomes if o[1] == "ok"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(self.store.head_seq(), 3)
        stages = [r["stage"] for r in self.store.recover()]
        self.assertEqual(stages, ["infer", "encode", "select"])


if __name__ == "__main__":
    unittest.main()
