"""Budget-constrained admission scheduling."""

from __future__ import annotations

from _base import WorkspaceTestCase

from rerank_bench.config import Config
from rerank_bench.errors import E_BUDGET_EXCEEDED, E_QUOTA_EXCEEDED
from rerank_bench.generator import default_config
from rerank_bench.scheduler import plan_schedule


def _config(workers=4, budget=1000, quota=500):
    raw = default_config()
    raw["parallel"] = {
        "max_workers": workers,
        "global_eval_budget": budget,
        "per_worker_quota": quota,
        "shard_latency_budget_ms": 60000,
    }
    return Config(raw)


def _shards(counts):
    return [{
        "shard_id": f"shard-{i:03d}", "shard_index": i, "candidates": n,
    } for i, n in enumerate(counts)]


class TestScheduler(WorkspaceTestCase):
    def test_all_admitted_under_budget(self):
        plan = plan_schedule(_shards([10, 20, 30]), _config())
        self.assertEqual(len(plan["admitted"]), 3)
        self.assertEqual(plan["rejected"], [])
        self.assertEqual(plan["reserved_evaluations"], 60)
        self.assertEqual(plan["concurrency"], 3)

    def test_global_budget_rejects_overflow(self):
        plan = plan_schedule(_shards([10, 20, 30]), _config(budget=35))
        admitted = [a["shard_id"] for a in plan["admitted"]]
        self.assertEqual(admitted, ["shard-000", "shard-001"])
        self.assertEqual(plan["rejected"][0]["code"], E_BUDGET_EXCEEDED)
        self.assertLessEqual(plan["reserved_evaluations"], 35)

    def test_per_worker_quota_rejects_oversized_shard(self):
        plan = plan_schedule(_shards([10, 600, 30]), _config(quota=500))
        admitted = [a["shard_id"] for a in plan["admitted"]]
        self.assertEqual(admitted, ["shard-000", "shard-002"])
        rejected = plan["rejected"][0]
        self.assertEqual(rejected["shard_id"], "shard-001")
        self.assertEqual(rejected["code"], E_QUOTA_EXCEEDED)
        self.assertIn("quota", rejected["reason"])

    def test_concurrency_capped_by_max_workers(self):
        plan = plan_schedule(_shards([5, 5, 5, 5, 5]), _config(workers=2))
        self.assertEqual(plan["concurrency"], 2)

    def test_budget_invariant_holds_by_construction(self):
        plan = plan_schedule(_shards([40, 40, 40, 40]), _config(budget=100))
        self.assertLessEqual(plan["reserved_evaluations"],
                             plan["global_eval_budget"])
