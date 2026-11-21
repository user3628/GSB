import time
import unittest

from tests.support import load_sample, new_engine


class BackgroundFlushTest(unittest.TestCase):
    def test_flusher_drains_dirty_pages_without_losing_state(self):
        small = load_sample("wal_sequence.json")
        root = "artifacts/test_runs/bg_flush"
        engine = new_engine(
            root,
            fsync_wal=False,
            background_flush=True,
            background_flush_interval_ms=50,
        )
        for rec in small["records"][:64]:
            engine.apply_modification(
                rec["page_no"], rec["after_digest"], rec["txn_id"]
            )
        deadline = time.time() + 3
        while time.time() < deadline and engine._dirty:
            time.sleep(0.02)
        self.assertEqual(engine._dirty, set())
        # Durable page table still reflects every modification.
        snap = engine.snapshot_pages()
        touched = {r["page_no"] for r in small["records"][:64]}
        for page_no in touched:
            self.assertIn(page_no, snap)
            self.assertGreaterEqual(snap[page_no]["version"], 1)
        engine.close()


if __name__ == "__main__":
    unittest.main()
