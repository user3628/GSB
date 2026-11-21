import os
import unittest

from tests.support import expected_pages_after, feed, load_sample, new_engine


class ReplayTest(unittest.TestCase):
    def setUp(self):
        self.small = load_sample("wal_sequence.json")

    def test_rebuild_matches_uninterrupted_baseline(self):
        root = os.path.join("artifacts", "test_runs", "replay_base")
        engine = new_engine(root, fsync_wal=False)
        feed(engine, self.small["records"])
        engine.checkpoint(incremental=False)
        engine.replay()
        snap = engine.snapshot_pages()
        expected = expected_pages_after(self.small["records"])
        for page_no, want in expected.items():
            self.assertEqual(snap[page_no]["digest"], want["digest"])
            self.assertEqual(snap[page_no]["version"], want["version"])
            self.assertEqual(snap[page_no]["last_lsn"], want["last_lsn"])
        engine.close()

    def test_replay_is_idempotent(self):
        root = os.path.join("artifacts", "test_runs", "replay_idem")
        engine = new_engine(root, fsync_wal=False)
        feed(engine, self.small["records"][:60])
        engine.checkpoint(incremental=False)
        feed(engine, self.small["records"][:30])
        engine.replay()
        first = engine.snapshot_pages()
        engine.replay()
        second = engine.snapshot_pages()
        self.assertEqual(first, second)
        engine.close()

    def test_page_versions_are_per_page_contiguous(self):
        root = os.path.join("artifacts", "test_runs", "replay_versions")
        engine = new_engine(root, fsync_wal=False)
        feed(engine, self.small["records"])
        engine.checkpoint(incremental=False)
        engine.replay()
        snap = engine.snapshot_pages()
        expected = expected_pages_after(self.small["records"])
        for page_no, want in expected.items():
            self.assertEqual(snap[page_no]["version"], want["version"])
        engine.close()

    def test_mismatch_reports_expected_and_actual_lsn(self):
        from wckpt.errors import ReplayMismatchError
        from wckpt.models import Checkpoint

        root = os.path.join("artifacts", "test_runs", "replay_mismatch")
        engine = new_engine(root, fsync_wal=False)
        feed(engine, self.small["records"][:40])
        cp = Checkpoint(seq=1, start_lsn=5, pages={}).seal()
        with self.assertRaises(ReplayMismatchError) as ctx:
            engine.replayer.replay(cp)
        self.assertEqual(ctx.exception.code, "E_REPLAY_MISMATCH")
        self.assertIn("expected_lsn", ctx.exception.details)
        self.assertIn("actual_lsn", ctx.exception.details)
        engine.close()


if __name__ == "__main__":
    unittest.main()
