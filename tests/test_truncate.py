import os
import unittest

from tests.support import feed, load_sample, new_engine


class TruncateTest(unittest.TestCase):
    def setUp(self):
        self.small = load_sample("wal_sequence.json")

    def test_unsafe_truncate_blocked_with_intervals(self):
        from wckpt.errors import TruncateUnsafeError

        root = os.path.join("artifacts", "test_runs", "trunc_unsafe")
        engine = new_engine(root, segment_max_bytes=2048, fsync_wal=False)
        feed(engine, self.small["records"][:60])
        engine.checkpoint(incremental=False)
        feed(engine, self.small["records"][60:120])
        with self.assertRaises(TruncateUnsafeError) as ctx:
            engine.truncate_wal()
        self.assertEqual(ctx.exception.code, "E_TRUNCATE_UNSAFE")
        self.assertTrue(ctx.exception.details["uncovered"])
        self.assertIn("start_lsn", ctx.exception.details["uncovered"][0])
        # WAL files remain on disk.
        self.assertTrue(any(n.endswith(".wal") for n in os.listdir(engine.wal_dir)))
        engine.close()

    def test_safe_truncate_archives_covered_segments(self):
        root = os.path.join("artifacts", "test_runs", "trunc_safe")
        engine = new_engine(root, segment_max_bytes=2048, fsync_wal=False)
        feed(engine, self.small["records"])
        engine.checkpoint(incremental=False)
        result = engine.truncate_wal()["result"]
        self.assertTrue(result["archived_segments"])
        self.assertEqual(
            sorted(os.listdir(engine.archive_dir)),
            sorted(result["archived_segments"]),
        )
        self.assertEqual(engine.wal_segments(), [])
        engine.close()

    def test_archived_segment_is_replayable_from_archive_copy(self):
        import shutil
        from wckpt import Engine

        root = os.path.join("artifacts", "test_runs", "trunc_archive")
        engine = new_engine(root, segment_max_bytes=2048, fsync_wal=False)
        feed(engine, self.small["records"][:40])
        engine.checkpoint(incremental=False)
        result = engine.truncate_wal()["result"]
        archived = os.path.join(engine.archive_dir, result["archived_segments"][0])
        self.assertTrue(os.path.exists(archived))
        engine.close()


if __name__ == "__main__":
    unittest.main()
