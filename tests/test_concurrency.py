import os
import threading
import unittest

from tests.support import feed, load_sample, new_engine


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.small = load_sample("wal_sequence.json")
        self.root = os.path.join("artifacts", "test_runs", "concurrency")
        self.engine = new_engine(self.root, fsync_wal=False)

    def tearDown(self):
        self.engine.close()

    def test_readers_never_observe_partial_state(self):
        feed(self.engine, self.small["records"][:50])
        errors = []

        def reader():
            for _ in range(200):
                snap = self.engine.snapshot_pages()
                cp = self.engine.active_checkpoint()
                if cp is not None:
                    for page_no, page in cp.pages.items():
                        if page_no in snap:
                            if snap[page_no]["last_lsn"] > page.last_lsn and snap[page_no]["version"] < page.version:
                                errors.append("inconsistent page/checkpoint view")

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for i in range(50, 120):
            rec = self.small["records"][i]
            self.engine.apply_modification(
                rec["page_no"], rec["after_digest"], rec["txn_id"]
            )
        for t in threads:
            t.join()
        self.assertEqual(errors, [])

    def test_duplicate_request_executes_once(self):
        request_id = "req-xyz-001"
        rec = self.small["records"][0]
        call = lambda: self.engine.apply_modification(
            rec["page_no"], rec["after_digest"], rec["txn_id"],
            request_id=request_id,
        )
        first = call()
        self.assertFalse(first["reused"])
        second = call()
        self.assertTrue(second["reused"])
        self.assertEqual(second["result"], first["result"])
        lsns = [r.lsn for r in self.engine.wal.iter_records()]
        self.assertEqual(len(lsns), 1)

    def test_concurrent_duplicate_requests_single_execution(self):
        request_id = "req-concurrent-002"
        rec = self.small["records"][1]
        results = []
        barrier = threading.Barrier(5)

        def worker():
            barrier.wait()
            out = self.engine.apply_modification(
                rec["page_no"], rec["after_digest"], rec["txn_id"],
                request_id=request_id,
            )
            results.append(out)

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(results), 5)
        self.assertEqual(sum(1 for r in results if not r["reused"]), 1)
        self.assertEqual(sum(1 for r in results if r["reused"]), 4)
        lsns = [r.lsn for r in self.engine.wal.iter_records()]
        self.assertEqual(len(lsns), 1)


if __name__ == "__main__":
    unittest.main()
