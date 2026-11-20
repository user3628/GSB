import copy
import tempfile
import time
import unittest
from pathlib import Path

from detpipe.config import PipelineConfig
from detpipe.generate import generate_dataset
from detpipe.pipeline import Pipeline
from detpipe.validator import report_bytes, validate_run


class TestPipelineE2E(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.dataset = generate_dataset()

    def tearDown(self):
        self.tmp.cleanup()

    def test_parallel_matches_sequential_byte_for_byte(self):
        seq_ws = self.root / "seq"
        par_ws = self.root / "par"
        Pipeline(self.dataset, PipelineConfig(workers=1, shard_size=2), seq_ws).run()
        Pipeline(self.dataset, PipelineConfig(workers=3, shard_size=2), par_ws).run()
        self.assertEqual(
            report_bytes(validate_run(seq_ws)),
            report_bytes(validate_run(par_ws)),
        )

    def test_stop_and_resume_matches_full_run(self):
        full_ws = self.root / "full"
        stop_ws = self.root / "stop"
        Pipeline(self.dataset, PipelineConfig(workers=2, shard_size=2), full_ws).run()
        expected = report_bytes(validate_run(full_ws))

        store = Pipeline(self.dataset, PipelineConfig(workers=2, shard_size=2), stop_ws).store
        original = Pipeline.process_shard

        def slow(self, plan):
            time.sleep(0.05)
            return original(self, plan)

        Pipeline.process_shard = slow
        import threading

        def trigger():
            time.sleep(0.04)
            store.request_stop()

        thread = threading.Thread(target=trigger)
        thread.start()
        first = Pipeline(self.dataset, PipelineConfig(workers=2, shard_size=2), stop_ws).run()
        thread.join()
        Pipeline.process_shard = original
        self.assertEqual(first["status"], "stopped")
        self.assertLess(first["shards_published"], first["shards_total"])

        store.clear_stop()
        second = Pipeline(self.dataset, PipelineConfig(workers=2, shard_size=2), stop_ws).run()
        self.assertEqual(second["status"], "completed")
        self.assertEqual(second["start_index"], first["shards_published"])
        self.assertEqual(report_bytes(validate_run(stop_ws)), expected)

    def test_budget_caps_evaluations(self):
        ws = self.root / "budget"
        result = Pipeline(
            self.dataset,
            PipelineConfig(workers=2, shard_size=2, max_evaluations=30),
            ws,
        ).run()
        self.assertLessEqual(result["scheduler_state"]["used"], 30)
        report = validate_run(ws)
        self.assertTrue(report["unprocessed_shards"])
        self.assertEqual(
            report["unprocessed_shards"][0]["code"], "E_BUDGET_EXHAUSTED"
        )

    def test_rebase_reuses_unaffected_shards(self):
        ws = self.root / "rebase"
        cfg = PipelineConfig(workers=2, shard_size=2)
        Pipeline(self.dataset, cfg, ws).run()
        mutated = copy.deepcopy(self.dataset)
        # img_004 belongs to shard index 2; earlier shards must be reused.
        mutated["images"][4]["candidates"][0]["score"] = 0.123456
        pipe = Pipeline(mutated, cfg, ws)
        report = pipe.rebase()
        self.assertEqual(report["affected_shard_indices"], [2])
        self.assertEqual(report["reused_shard_indices"], [0, 1])
        pipe.run()
        fresh_ws = self.root / "fresh"
        Pipeline(mutated, cfg, fresh_ws).run()
        self.assertEqual(
            report_bytes(validate_run(ws)),
            report_bytes(validate_run(fresh_ws)),
        )

    def test_dataset_has_required_shape(self):
        self.assertGreaterEqual(len(self.dataset["images"]), 4)
        self.assertGreaterEqual(len(self.dataset["sources"]), 3)
        ids0 = {c["candidate_id"] for c in self.dataset["images"][0]["candidates"]}
        ids3 = {c["candidate_id"] for c in self.dataset["images"][3]["candidates"]}
        # shared tracking block appears on both images, prefixed per image
        shared0 = {i for i in ids0 if "shared_track" in i}
        shared3 = {i for i in ids3 if "shared_track" in i}
        self.assertTrue(shared0 and shared3)


if __name__ == "__main__":
    unittest.main()
