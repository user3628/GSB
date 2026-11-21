import os
import unittest

from tests.support import feed, load_sample, new_engine, test_config


class WalRotationTest(unittest.TestCase):
    def setUp(self):
        self.root = os.path.join("artifacts", "test_runs", "wal")
        self.small = load_sample("wal_sequence.json")
        self.engine = new_engine(self.root, segment_max_bytes=2048, fsync_wal=False)

    def tearDown(self):
        self.engine.close()

    def test_lsn_contiguous_across_segments(self):
        feed(self.engine, self.small["records"])
        self.engine.wal.close()
        lsns = [r.lsn for r in self.engine.wal.iter_records()]
        self.assertEqual(lsns, list(range(1, len(lsns) + 1)))
        self.assertGreater(len(self.engine.wal.segments()), 1)

    def test_segment_header_matches_first_record(self):
        feed(self.engine, self.small["records"])
        self.engine.wal.close()
        for seg in self.engine.wal.segments():
            self.assertFalse(seg.corrupt, seg.error)
            first = next(self.engine.wal.iter_records(seg.start_lsn, seg.start_lsn))
            self.assertEqual(first.lsn, seg.start_lsn)
            self.assertTrue(seg.sealed)

    def test_no_duplicate_lsn_after_reopen(self):
        feed(self.engine, self.small["records"][:50])
        self.engine.close()
        from wckpt import Engine
        engine2 = Engine(self.root, test_config(segment_max_bytes=2048, fsync_wal=False))
        engine2.open()
        before = engine2.wal.next_lsn
        feed(engine2, self.small["records"][50:60])
        lsns = [r.lsn for r in engine2.wal.iter_records()]
        self.assertEqual(len(lsns), len(set(lsns)))
        self.assertEqual(engine2.wal.next_lsn, before + 10)
        engine2.close()

    def test_range_query_returns_segment_identity(self):
        feed(self.engine, self.small["records"][:80])
        rows = self.engine.query_wal(10, 14)
        self.assertEqual([r["record"]["lsn"] for r in rows], [10, 11, 12, 13, 14])
        for row in rows:
            self.assertTrue(row["segment"].startswith("seg_"))
            self.assertIsInstance(row["seg_id"], int)


class WalCorruptionTest(unittest.TestCase):
    def test_corrupt_segment_excluded_and_code(self):
        from wckpt import Engine
        from wckpt.errors import WalCorruptError

        root = os.path.join("artifacts", "test_runs", "wal_corrupt")
        engine = new_engine(root, segment_max_bytes=2048, fsync_wal=False)
        feed(engine, load_sample("wal_sequence.json")["records"])
        engine.wal.close()
        engine.close()

        seg_dir = os.path.join(root, "wal")
        target = sorted(n for n in os.listdir(seg_dir) if n.endswith(".wal"))[1]
        with open(os.path.join(seg_dir, target), "a", encoding="utf-8") as fh:
            fh.write('{"magic":"REC","lsn": 555}\n')

        engine2 = Engine(root, test_config(segment_max_bytes=2048, fsync_wal=False))
        engine2.open()
        corrupt = [s for s in engine2.wal.segments() if s.corrupt]
        self.assertEqual([s.name for s in corrupt], [target])
        with self.assertRaises(WalCorruptError) as ctx:
            list(engine2.wal.iter_records())
        self.assertEqual(ctx.exception.code, "E_WAL_CORRUPT")
        engine2.close()


if __name__ == "__main__":
    unittest.main()
