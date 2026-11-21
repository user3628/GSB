import os
import unittest

from tests.support import feed, load_sample, new_engine


class CheckpointTest(unittest.TestCase):
    def setUp(self):
        self.root = os.path.join("artifacts", "test_runs", "checkpoint")
        self.small = load_sample("wal_sequence.json")
        self.engine = new_engine(self.root, fsync_wal=False)

    def tearDown(self):
        self.engine.close()

    def test_sequence_monotonic_and_state_replacement(self):
        feed(self.engine, self.small["records"][:40])
        cp1 = self.engine.checkpoint(incremental=False)["result"]
        feed(self.engine, self.small["records"][:20])
        cp2 = self.engine.checkpoint(incremental=True)["result"]
        self.assertEqual(cp2["seq"], cp1["seq"] + 1)
        active = self.engine.active_checkpoint()
        self.assertEqual(active.seq, cp2["seq"])
        # Whole-state replacement: page set equals the new checkpoint exactly.
        snap = self.engine.snapshot_pages()
        self.assertEqual(set(snap), set(active.pages))
        for page_no, page in active.pages.items():
            self.assertEqual(snap[page_no]["digest"], page.digest)
            self.assertEqual(snap[page_no]["version"], page.version)

    def test_incremental_only_flushes_changed_pages(self):
        feed(self.engine, self.small["records"][:32])
        self.engine.checkpoint(incremental=False)
        feed(self.engine, self.small["records"][:8])
        inc = self.engine.checkpoint(incremental=True)["result"]
        self.assertEqual(inc["mode"], "incremental")
        changed = {r["page_no"] for r in self.small["records"][:8]}
        self.assertEqual(inc["flushed_pages"], len(changed))

    def test_invalid_checksum_keeps_previous_state(self):
        from wckpt.errors import CheckpointInvalidError

        feed(self.engine, self.small["records"][:20])
        good = self.engine.checkpoint(incremental=False)["result"]
        # Tamper the persisted checkpoint payload.
        row = self.engine.store.conn.execute(
            "SELECT seq, payload FROM checkpoints WHERE seq=?", (good["seq"],)
        ).fetchone()
        import json
        payload = json.loads(row["payload"])
        payload["checksum"] = "deadbeef"
        self.engine.store.conn.execute(
            "UPDATE checkpoints SET payload=? WHERE seq=?",
            (json.dumps(payload, sort_keys=True), good["seq"]),
        )
        with self.assertRaises(CheckpointInvalidError) as ctx:
            self.engine._load_active_checkpoint()
        self.assertEqual(ctx.exception.code, "E_CHECKPOINT_INVALID")
        # Page table remains available and untouched.
        self.assertTrue(self.engine.snapshot_pages())

    def test_sequence_regression_rejected(self):
        from wckpt.models import Checkpoint

        feed(self.engine, self.small["records"][:10])
        cp1 = self.engine.checkpoint(incremental=False)["result"]
        self.assertEqual(cp1["seq"], 1)
        # Fabricate a committed checkpoint at a higher water mark (seq=5).
        high = Checkpoint(
            seq=5,
            start_lsn=11,
            pages=self.engine.store.all_pages(),
        ).seal()
        self.engine.store.insert_checkpoint(high, "COMMITTED", self.engine._clock)
        prev = self.engine._load_active_checkpoint()
        self.assertEqual(prev.seq, 5)
        # A lower-sequence checkpoint must never win the "latest committed" query.
        stale = Checkpoint(seq=2, start_lsn=1, pages=prev.pages).seal()
        self.engine.store.insert_checkpoint(stale, "COMMITTED", self.engine._clock)
        self.assertEqual(self.engine._load_active_checkpoint().seq, 5)
        # Fresh checkpoints always move forward from the highest known sequence.
        new_cp = self.engine.checkpoint(incremental=False)["result"]
        self.assertEqual(new_cp["seq"], 6)


if __name__ == "__main__":
    unittest.main()
