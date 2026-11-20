"""End-to-end: determinism, cache identity, latency budget, incremental runs."""

import copy
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workbench.config import bump_config, default_config_store
from workbench.engine import Engine
from workbench.errors import LatencyBudgetExceeded
from workbench.sample import generate_csv
from workbench.store import Store
from workbench.util import sha256_file


class WorkspaceMixin(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="wb-pipe-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.sample = os.path.join(self.tmp, "sample.csv")
        generate_csv(self.sample, seed=20261120, rows=600)
        self.state = os.path.join(self.tmp, "state")
        store = Store(self.state)
        store.ensure()
        store.write_config_store(default_config_store())
        store.write_sample_registry(
            {"path": self.sample, "sha256": "", "rows": 600, "columns": []}
        )
        self.engine = Engine(self.state)

    def run_pipeline(self, **kw):
        return self.engine.run(**kw)

    def set_config(self, mutate):
        store = Store(self.state)
        obj = store.load_config_store()
        cfg = copy.deepcopy(obj["config"])
        mutate(cfg)
        store.write_config_store(bump_config(obj, cfg))

    def fresh_state(self, name, config_store=None):
        other = os.path.join(self.tmp, name)
        store = Store(other)
        store.ensure()
        store.write_config_store(config_store or default_config_store())
        store.write_sample_registry(
            {"path": self.sample, "sha256": "", "rows": 600, "columns": []}
        )
        return other


class TestDeterminism(WorkspaceMixin):
    def test_same_digest_across_fresh_states(self):
        r1 = self.run_pipeline()
        r2 = Engine(self.fresh_state("state2")).run()
        self.assertEqual(r1["summary_digest"], r2["summary_digest"])
        for s1, s2 in zip(r1["stages"], r2["stages"]):
            self.assertEqual(s1["cache_key"], s2["cache_key"])

    def test_artifacts_byte_identical_across_states(self):
        self.run_pipeline()
        other = self.fresh_state("state3")
        Engine(other).run()
        s1 = Store(self.state)
        s2 = Store(other)
        cfg_store = s1.load_config_store()
        for step in self.engine.plan(cfg_store, sha256_file(self.sample)):
            m1, x1 = s1.read_artifact(step["cache_key"])
            m2, x2 = s2.read_artifact(step["cache_key"])
            self.assertEqual(m1, m2, step["stage"])
            self.assertEqual(x1, x2, step["stage"])

    def test_rerun_is_noop_from_journal(self):
        self.run_pipeline()
        head = Store(self.state).head_seq()
        r2 = self.run_pipeline()
        self.assertTrue(all(s["source"] == "journal" for s in r2["stages"]))
        self.assertEqual(Store(self.state).head_seq(), head)


class TestCache(WorkspaceMixin):
    def test_cache_hit_byte_identical_to_computed(self):
        r1 = self.run_pipeline()
        other = os.path.join(self.tmp, "state-cache")
        shutil.copytree(
            os.path.join(self.state, "artifacts"), os.path.join(other, "artifacts")
        )
        store2 = Store(other)
        store2.ensure()
        store2.write_config_store(default_config_store())
        store2.write_sample_registry(
            {"path": self.sample, "sha256": "", "rows": 600, "columns": []}
        )
        r2 = Engine(other).run()
        self.assertTrue(all(s["source"] == "cache" for s in r2["stages"]))
        self.assertEqual(r1["summary_digest"], r2["summary_digest"])

    def test_no_stale_cache_after_config_change(self):
        self.run_pipeline()
        self.set_config(lambda c: c["selection"].__setitem__("method", "variance"))
        r2 = self.run_pipeline()
        sources = {s["stage"]: s["source"] for s in r2["stages"]}
        self.assertEqual(sources["select"], "computed")
        self.assertEqual(sources["summarize"], "computed")
        self.assertEqual(r2["summary"]["stages"]["select"]["method"], "variance")


class TestLatencyBudget(WorkspaceMixin):
    def test_zero_budget_raises(self):
        with self.assertRaises(LatencyBudgetExceeded) as ctx:
            self.run_pipeline(budget_ms=0)
        err = ctx.exception.to_dict()
        self.assertEqual(err["code"], "E_LATENCY_BUDGET")
        self.assertIn("budget_ms", err["details"])
        self.assertEqual(Store(self.state).head_seq(), 0)

    def test_generous_budget_completes(self):
        r = self.run_pipeline(budget_ms=60000)
        self.assertEqual(r["status"], "completed")
        self.assertLessEqual(r["elapsed_ms"], 60000)


class TestIncremental(WorkspaceMixin):
    def test_selection_change_recomputes_downstream_only(self):
        self.run_pipeline()
        self.set_config(lambda c: c["selection"]["mutual_info"].__setitem__("threshold", 0.01))
        r2 = self.run_pipeline()
        sources = {s["stage"]: s["source"] for s in r2["stages"]}
        self.assertEqual(sources["infer"], "journal")
        self.assertEqual(sources["encode"], "journal")
        self.assertEqual(sources["select"], "computed")
        self.assertEqual(sources["summarize"], "computed")

    def test_encoding_change_keeps_infer(self):
        self.run_pipeline()
        self.set_config(lambda c: c["encoding"]["hash"].__setitem__("n_buckets", 12))
        r2 = self.run_pipeline()
        sources = {s["stage"]: s["source"] for s in r2["stages"]}
        self.assertEqual(sources["infer"], "journal")
        self.assertEqual(sources["encode"], "computed")
        self.assertEqual(sources["select"], "computed")
        self.assertEqual(sources["summarize"], "computed")

    def test_incremental_equals_full_rebuild(self):
        self.run_pipeline()
        self.set_config(lambda c: c["encoding"]["per_column"].__setitem__("city", "target"))
        self.set_config(lambda c: c["selection"]["mutual_info"].__setitem__("top_k", 10))
        r_inc = self.run_pipeline()
        self.assertTrue(self.engine.verify()["consistent"])
        final_store = Store(self.state).load_config_store()
        r_full = Engine(self.fresh_state("state-full", final_store)).run()
        self.assertEqual(r_inc["summary_digest"], r_full["summary_digest"])
        self.assertEqual(r_inc["summary"], r_full["summary"])

    def test_section_versions_track_changes(self):
        self.run_pipeline()
        self.set_config(lambda c: c["selection"]["mutual_info"].__setitem__("threshold", 0.01))
        obj = Store(self.state).load_config_store()
        self.assertEqual(obj["version"], 2)
        self.assertEqual(obj["section_versions"]["selection"], 2)
        self.assertEqual(obj["section_versions"]["encoding"], 1)
        self.assertEqual(obj["section_versions"]["inference"], 1)


if __name__ == "__main__":
    unittest.main()
