import os
import unittest

from tests.support import feed, load_sample, new_engine, test_config
from wckpt import Config
from wckpt.models import Checkpoint


class AdaptivePathTest(unittest.TestCase):
    def setUp(self):
        self.large = load_sample("wal_sequence_large.json")
        self.initial = load_sample("initial_pages.json")

    def _build(self, root, cfg):
        engine = new_engine(root, **{
            "fsync_wal": False,
            "replay_record_threshold": cfg.replay_record_threshold,
            "replay_dirty_page_threshold": cfg.replay_dirty_page_threshold,
            "replay_batch_size": cfg.replay_batch_size,
            "flush_group_size": cfg.flush_group_size,
        })
        feed(engine, self.large["records"])
        cp0 = Checkpoint(seq=0, start_lsn=1, pages={}).seal()
        result = engine.replayer.replay(cp0)
        return engine, result

    def test_two_paths_produce_identical_pages(self):
        batched_cfg = test_config()
        engine_b, rb = self._build(
            os.path.join("artifacts", "test_runs", "adapt_batch"), batched_cfg
        )
        self.assertEqual(rb.decision.path, "batched")

        precise_cfg = test_config(
            replay_record_threshold=10 ** 9,
            replay_dirty_page_threshold=10 ** 9,
        )
        engine_p, rp = self._build(
            os.path.join("artifacts", "test_runs", "adapt_precise"), precise_cfg
        )
        self.assertEqual(rp.decision.path, "precise")

        pages_b = {k: v.to_dict() for k, v in rb.final_pages.items()}
        pages_p = {k: v.to_dict() for k, v in rp.final_pages.items()}
        self.assertEqual(pages_b, pages_p)
        for page_no, digest in self.large["expected_final_digests"].items():
            self.assertEqual(pages_b[int(page_no)]["digest"], digest)
        engine_b.close()
        engine_p.close()

    def test_decision_reports_path_params_and_reason(self):
        engine, result = self._build(
            os.path.join("artifacts", "test_runs", "adapt_decision"), test_config()
        )
        decision = result.decision.to_dict()
        self.assertEqual(decision["path"], "batched")
        self.assertGreater(decision["batch_size"], 0)
        self.assertGreater(decision["flush_group_size"], 0)
        self.assertIn("threshold", decision["reason"])
        engine.close()


if __name__ == "__main__":
    unittest.main()
