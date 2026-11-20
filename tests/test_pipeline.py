"""Unit and integration tests for the pipeline (stdlib unittest)."""

import json
import os
import shutil
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.dedup import band_keys, dedup_records
from pipeline.errors import (E_INVALID_STAGE, E_STALE_CHECKPOINT,
                             PipelineError)
from pipeline.mixer import largest_remainder_allocation, stratified_mix
from pipeline.runner import Runner, StopRequested
from pipeline.stages import STAGES
from pipeline.store import CheckpointStore
from pipeline.textnorm import (hamming64, normalize_text, simhash64,
                               token_count, tokenize)


def write_jsonl(path, records):
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def make_workspace(root, extra_shards=None, config=None):
    shards = os.path.join(root, "data", "shards")
    os.makedirs(shards, exist_ok=True)
    write_jsonl(os.path.join(shards, "s1.jsonl"), [
        {"id": "x1", "source": "wiki", "lang": "en",
         "text": "The quick brown fox jumps over the lazy dog"},
        {"id": "x2", "source": "wiki", "lang": "zh",
         "text": "人工智能 是 计算机科学 的 一个 分支"},
        {"id": "x3", "source": "blog", "lang": "en",
         "text": "The quick brown fox jumps over the lazy dog"},
        {"id": "x4", "source": "blog", "lang": "en",
         "text": "Neural networks learn from data"},
    ])
    for name, records in (extra_shards or {}).items():
        write_jsonl(os.path.join(shards, name), records)
    cfg = config or {
        "dedup": {"hamming_threshold": 3, "bands": 4, "band_bits": 16},
        "mix": {"seed": 7, "total_samples": 2, "default_weight": 0.0,
                "weights": {"wiki|en": 1.0, "wiki|zh": 1.0,
                            "blog|en": 1.0}},
    }
    with open(os.path.join(root, "data", "mix_config.json"), "w",
              encoding="utf-8") as fh:
        json.dump(cfg, fh)
    return root


class TestNormalize(unittest.TestCase):
    def test_nfkc_fullwidth(self):
        self.assertEqual(normalize_text("Ｍachine ＬＥＡＲＮＩＮＧ"),
                         "machine learning")

    def test_whitespace_collapse_and_case(self):
        self.assertEqual(normalize_text("  The\tQuick\n\nBrown  "),
                         "the quick brown")

    def test_token_count(self):
        self.assertEqual(token_count("a b  c"), 3)
        self.assertEqual(token_count(""), 0)


class TestSimHash(unittest.TestCase):
    def test_deterministic(self):
        tokens = tokenize("the quick brown fox")
        self.assertEqual(simhash64(tokens), simhash64(list(tokens)))

    def test_identical_text_zero_distance(self):
        a = simhash64(tokenize("hello world"))
        b = simhash64(tokenize("hello world"))
        self.assertEqual(hamming64(a, b), 0)

    def test_band_keys_cover_all_bits(self):
        keys = band_keys(0x0123456789ABCDEF, bands=4, band_bits=16)
        self.assertEqual(len(keys), 4)
        self.assertEqual(keys[0], (0, 0xCDEF))
        self.assertEqual(keys[3], (3, 0x0123))


class TestDedup(unittest.TestCase):
    def test_first_occurrence_wins_and_order_preserved(self):
        base = simhash64(tokenize("some text"))
        near = base ^ 0b1  # hamming distance 1
        far = simhash64(tokenize("completely different text"))
        kept, dups = dedup_records([base, near, far, base], threshold=3)
        self.assertEqual(kept, [0, 2])
        self.assertEqual([(d[0], d[1]) for d in dups], [(1, 0), (3, 0)])

    def test_threshold_boundary(self):
        base = simhash64(tokenize("threshold test"))
        within = base ^ 0b111  # distance 3
        beyond = base ^ 0b1111  # distance 4
        kept, dups = dedup_records([base, within, beyond], threshold=3)
        self.assertEqual(kept, [0, 2])
        self.assertEqual(len(dups), 1)


class TestAllocation(unittest.TestCase):
    def test_largest_remainder_exact(self):
        alloc = largest_remainder_allocation(
            {"a": 1.0, "b": 1.0, "c": 2.0},
            {"a": 10, "b": 10, "c": 10}, total=8)
        self.assertEqual(sum(alloc.values()), 8)
        self.assertEqual(alloc["c"], 4)
        self.assertEqual(alloc["a"] + alloc["b"], 4)

    def test_allocation_capped_by_availability(self):
        alloc = largest_remainder_allocation(
            {"a": 3.0, "b": 1.0}, {"a": 1, "b": 5}, total=4)
        self.assertEqual(alloc["a"], 1)
        self.assertEqual(alloc["b"], 3)

    def test_allocation_deterministic_tie_break(self):
        first = largest_remainder_allocation(
            {"b": 1.0, "a": 1.0}, {"a": 5, "b": 5}, total=3)
        second = largest_remainder_allocation(
            {"a": 1.0, "b": 1.0}, {"b": 5, "a": 5}, total=3)
        self.assertEqual(first, second)
        self.assertEqual(first, {"a": 2, "b": 1})  # tie -> key order

    def test_zero_weights_allocate_nothing(self):
        alloc = largest_remainder_allocation(
            {"a": 0.0}, {"a": 5}, total=3)
        self.assertEqual(alloc["a"], 0)


class TestStratifiedMix(unittest.TestCase):
    def _records(self):
        return [{"order": i, "id": "r%d" % i,
                 "source": "s%d" % (i % 2), "lang": "en",
                 "tokens": i + 1, "text": "t%d" % i}
                for i in range(10)]

    def test_deterministic_same_seed(self):
        recs = self._records()
        sel1, _ = stratified_mix(recs, {"s0|en": 1, "s1|en": 1}, 0.0, 6, 42)
        sel2, _ = stratified_mix(recs, {"s0|en": 1, "s1|en": 1}, 0.0, 6, 42)
        self.assertEqual([r["id"] for r in sel1], [r["id"] for r in sel2])

    def test_output_in_original_order(self):
        sel, _ = stratified_mix(self._records(), {"s0|en": 1, "s1|en": 1},
                                0.0, 6, 42)
        orders = [r["order"] for r in sel]
        self.assertEqual(orders, sorted(orders))

    def test_stats_counts(self):
        _, stats = stratified_mix(self._records(),
                                  {"s0|en": 1, "s1|en": 1}, 0.0, 6, 42)
        self.assertEqual(stats["total_selected"], 6)
        self.assertEqual(
            sum(l["allocated"] for l in stats["layers"].values()), 6)


class TestStore(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.store = CheckpointStore(self.dir)

    def test_publish_and_read_back(self):
        seq = self.store.publish("normalize", {"shards": "v1"},
                                 "sha256:" + "a" * 64,
                                 "artifacts/normalize.jsonl", expected_seq=0)
        self.assertEqual(seq, 1)
        rec = self.store.latest_checkpoint("normalize")
        self.assertEqual(rec["input_versions"]["shards"], "v1")
        self.assertEqual(self.store.current_seq(), 1)

    def test_stale_publish_rejected(self):
        self.store.publish("normalize", {}, "sha256:" + "a" * 64, "p",
                           expected_seq=0)
        with self.assertRaises(PipelineError) as ctx:
            self.store.publish("dedup", {}, "sha256:" + "b" * 64, "p",
                               expected_seq=0)
        self.assertEqual(ctx.exception.code, E_STALE_CHECKPOINT)
        self.assertEqual(self.store.current_seq(), 1)

    def test_concurrent_publish_single_winner(self):
        results = []

        def worker(name):
            try:
                self.store.publish(name, {}, "sha256:" + "0" * 64, "p",
                                   expected_seq=0)
                results.append((name, "ok"))
            except PipelineError as exc:
                results.append((name, exc.code))

        threads = [threading.Thread(target=worker, args=("w%d" % i,))
                   for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        oks = [r for r in results if r[1] == "ok"]
        stale = [r for r in results if r[1] == E_STALE_CHECKPOINT]
        self.assertEqual(len(oks), 1)
        self.assertEqual(len(stale), 3)
        self.assertEqual(self.store.current_seq(), 1)

    def test_invalidation_marked_and_observable(self):
        self.store.mark_invalidated("mix", "mix_config version changed")
        states = self.store.stage_states()
        self.assertEqual(states["mix"]["state"], "invalidated")
        self.assertIn("mix_config", states["mix"]["detail"]["reason"])
        events = self.store.events()
        self.assertEqual(events[0]["kind"], "invalidate")


class TestRunnerRecovery(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        make_workspace(self.dir)

    def _artifact(self, name):
        with open(os.path.join(self.dir, "artifacts", name), "rb") as fh:
            return fh.read()

    def test_full_run_deterministic_bytes(self):
        Runner(self.dir, log=lambda *a: None).run()
        first = self._artifact("mix.jsonl")
        shutil.rmtree(os.path.join(self.dir, "state"))
        shutil.rmtree(os.path.join(self.dir, "artifacts"))
        Runner(self.dir, log=lambda *a: None).run()
        self.assertEqual(first, self._artifact("mix.jsonl"))

    def test_stop_then_resume_byte_identical(self):
        Runner(self.dir, log=lambda *a: None).run()
        reference = {n: self._artifact(n) for n in
                     ("normalize.jsonl", "dedup.jsonl", "mix.jsonl")}
        shutil.rmtree(os.path.join(self.dir, "state"))
        shutil.rmtree(os.path.join(self.dir, "artifacts"))
        runner = Runner(self.dir, log=lambda *a: None)
        with self.assertRaises(StopRequested):
            runner.run(stop_after="normalize")
        states = {s.stage: s.state for s in Runner(
            self.dir, log=lambda *a: None).plan()}
        self.assertEqual(states["normalize"], "completed")
        self.assertEqual(states["dedup"], "pending")
        Runner(self.dir, log=lambda *a: None).run()
        for name, data in reference.items():
            self.assertEqual(data, self._artifact(name), name)

    def test_config_change_invalidates_only_mix(self):
        Runner(self.dir, log=lambda *a: None).run()
        cfg_path = os.path.join(self.dir, "data", "mix_config.json")
        with open(cfg_path) as fh:
            cfg = json.load(fh)
        cfg["mix"]["total_samples"] = 1
        with open(cfg_path, "w") as fh:
            json.dump(cfg, fh)
        statuses = {s.stage: s for s in Runner(
            self.dir, log=lambda *a: None).refresh()}
        self.assertEqual(statuses["normalize"].state, "completed")
        self.assertEqual(statuses["dedup"].state, "completed")
        self.assertEqual(statuses["mix"].state, "invalidated")
        self.assertIn("mix_config", statuses["mix"].reason)

    def test_dedup_param_change_invalidates_dedup_and_mix(self):
        Runner(self.dir, log=lambda *a: None).run()
        cfg_path = os.path.join(self.dir, "data", "mix_config.json")
        with open(cfg_path) as fh:
            cfg = json.load(fh)
        cfg["dedup"]["hamming_threshold"] = 0
        with open(cfg_path, "w") as fh:
            json.dump(cfg, fh)
        statuses = {s.stage: s for s in Runner(
            self.dir, log=lambda *a: None).refresh()}
        self.assertEqual(statuses["normalize"].state, "completed")
        self.assertEqual(statuses["dedup"].state, "invalidated")
        self.assertEqual(statuses["mix"].state, "invalidated")

    def test_shard_change_invalidates_everything(self):
        Runner(self.dir, log=lambda *a: None).run()
        write_jsonl(os.path.join(self.dir, "data", "shards", "s2.jsonl"),
                    [{"id": "y1", "source": "wiki", "lang": "en",
                      "text": "brand new document"}])
        statuses = {s.stage: s for s in Runner(
            self.dir, log=lambda *a: None).refresh()}
        for stage in STAGES:
            self.assertEqual(statuses[stage].state, "invalidated", stage)

    def test_invalid_stage_errors(self):
        runner = Runner(self.dir, log=lambda *a: None)
        with self.assertRaises(PipelineError) as ctx:
            runner.run_single_stage("mix")
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)
        with self.assertRaises(PipelineError) as ctx:
            runner.run_single_stage("bogus")
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)
        with self.assertRaises(PipelineError) as ctx:
            runner.run(target="bogus")
        self.assertEqual(ctx.exception.code, E_INVALID_STAGE)

    def test_dedup_removes_cross_shard_duplicate(self):
        Runner(self.dir, log=lambda *a: None).run()
        with open(os.path.join(self.dir, "artifacts", "dedup.jsonl"),
                  encoding="utf-8") as fh:
            ids = [json.loads(line)["id"] for line in fh if line.strip()]
        self.assertNotIn("x3", ids)  # duplicate of x1
        self.assertEqual(ids[0], "x1")  # first occurrence kept, in order

    def test_no_half_written_artifact_from_tmp(self):
        Runner(self.dir, log=lambda *a: None).run()
        artifacts = os.path.join(self.dir, "artifacts")
        stray = os.path.join(artifacts, "mix.jsonl.tmp.999")
        with open(stray, "wb") as fh:
            fh.write(b"partial")
        mix = self._artifact("mix.jsonl")
        self.assertTrue(mix.startswith(b"{"))  # real artifact intact


class TestStopMechanisms(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        make_workspace(self.dir)

    def test_stop_file_halts_before_next_stage(self):
        runner = Runner(self.dir, log=lambda *a: None)
        runner.request_stop()
        with self.assertRaises(StopRequested):
            runner.run()
        self.assertEqual(runner.store.current_seq(), 0)  # nothing published
        runner.clear_stop()
        runner.run()
        self.assertEqual(runner.store.current_seq(), 3)

    def test_sigint_handler_sets_stop_flag(self):
        import signal
        from pipeline import runner as runner_mod
        previous = signal.getsignal(signal.SIGINT)
        old_flag = runner_mod._signal_stop_requested
        try:
            runner_mod._signal_stop_requested = False
            runner_mod._install_signal_handlers()
            os.kill(os.getpid(), signal.SIGINT)
            self.assertTrue(runner_mod._signal_stop_requested)
        finally:
            signal.signal(signal.SIGINT, previous)
            runner_mod._signal_stop_requested = old_flag


if __name__ == "__main__":
    unittest.main()
