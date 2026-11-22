import tempfile
import unittest

from qrwb.config import load_config, write_default_config
from qrwb.errors import (E_BUDGET_EXHAUSTED, E_CHANNEL_QUOTA,
                         E_LATENCY_BUDGET)
from qrwb.normalize import build_dispatch_plan
from qrwb.scheduler import (BudgetLedger, LatencyGuard, admit_queries,
                            build_waves, estimate_cost)


def _cfg(overrides=None):
    path = tempfile.mktemp(suffix=".yaml")
    write_default_config(path)
    cfg = load_config(path)
    if overrides:
        import yaml
        data = cfg.raw
        for dotted, value in overrides.items():
            cursor = data
            parts = dotted.split(".")
            for part in parts[:-1]:
                cursor = cursor[part]
            cursor[parts[-1]] = value
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            yaml.safe_dump(data, handle, sort_keys=True, allow_unicode=True)
        cfg = load_config(path)
    return cfg


class BudgetSchedulerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.cfg = _cfg({
            "budget.global_evaluations": 15,
            "budget.per_channel_quota": 8,
            "budget.max_concurrent_channels": 1,
        })
        from qrwb.util import fingerprint
        self.ledger = BudgetLedger(self.tmp, self.cfg.budget,
                                   fingerprint(self.cfg.budget))

    def _plans(self):
        records = [
            ("q1", "phone 手机", "app"),
            ("q2", "laptop 咖啡", "app"),
            ("q3", "shoes 鞋", "web"),
        ]
        return [build_dispatch_plan(
            {"query_id": qid, "raw_query": q, "lang": "mixed",
             "channel": ch}, self.cfg) for qid, q, ch in records]

    def test_global_and_channel_budget_enforced(self):
        manifest = {"total_rows": 3, "shards": []}
        admissions = admit_queries(self._plans(), self.ledger, manifest)
        codes = {a.query_id: (a.accepted, a.error["code"] if a.error else None)
                 for a in admissions}
        self.assertTrue(any(v[0] for v in codes.values()))
        self.assertTrue(any(v[1] in (E_BUDGET_EXHAUSTED, E_CHANNEL_QUOTA)
                            for v in codes.values()))
        snap = self.ledger.snapshot()
        self.assertLessEqual(snap["global_used"], snap["global_limit"])

    def test_never_exceeds_global_budget_under_repeat(self):
        plan = build_dispatch_plan(
            {"query_id": "q", "raw_query": "phone 手机", "lang": "mixed",
             "channel": "app"}, self.cfg)
        cost = estimate_cost(plan, 1)
        ledger = BudgetLedger(tempfile.mkdtemp(), self.cfg.budget, "fp")
        accepted = 0
        for idx in range(50):
            result = ledger.try_reserve(f"q{idx}", "app", cost)
            if result["ok"]:
                ledger.commit(f"q{idx}")
                accepted += 1
            else:
                self.assertIn(result["code"],
                              (E_BUDGET_EXHAUSTED, E_CHANNEL_QUOTA))
        snap = ledger.snapshot()
        self.assertLessEqual(snap["global_used"], snap["global_limit"])
        self.assertGreater(accepted, 0)

    def test_latency_guard_returns_structured_error(self):
        cfg = _cfg({"budget.per_request_latency_ms": 0.0})
        guard = LatencyGuard(cfg.budget["per_request_latency_ms"],
                             cfg.simulation["work_ms"],
                             cfg.simulation["jitter_ms"],
                             cfg.simulation["seed"])
        outcome = guard.simulate("q1")
        self.assertFalse(outcome["ok"])
        self.assertEqual(outcome["error"]["code"], E_LATENCY_BUDGET)
        self.assertEqual(outcome["error"]["query_id"], "q1")
        self.assertIn("elapsed_ms", outcome["error"]["context"])

    def test_waves_respect_concurrency(self):
        manifest = {"total_rows": 3, "shards": []}
        cfg = _cfg({"budget.max_concurrent_channels": 2,
                    "budget.global_evaluations": 100,
                    "budget.per_channel_quota": 100})
        ledger = BudgetLedger(tempfile.mkdtemp(), cfg.budget, "fp")
        admissions = admit_queries(self._plans(), ledger, manifest)
        waves = build_waves(admissions, 2)
        for wave in waves:
            self.assertLessEqual(
                len({a.channel for a in wave}), 2)

    def test_estimate_cost_is_upper_bound(self):
        plan = build_dispatch_plan(
            {"query_id": "q", "raw_query": "phone 手机", "lang": "mixed",
             "channel": "app"}, self.cfg)
        from qrwb.sampling import (build_bridge_dict, build_glyph_index,
                                   build_synonym_table)
        resources = {"synonym": build_synonym_table(self.cfg),
                     "glyph": build_glyph_index(self.cfg),
                     "bridge": build_bridge_dict(self.cfg)}
        from qrwb.candidates import generate_candidates
        bundle = generate_candidates(plan, self.cfg, resources)
        actual = len(bundle["candidates"])
        self.assertLessEqual(actual, estimate_cost(plan, 1))


if __name__ == "__main__":
    unittest.main()
