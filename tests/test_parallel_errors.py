"""Parallel-mode error isolation and executor robustness."""

from __future__ import annotations

from _base import REPO_ROOT, WorkspaceTestCase

from rerank_bench import errors
from rerank_bench.runner import export_summary, run_pipeline


class TestParallelErrors(WorkspaceTestCase):
    def test_parallel_worker_failure_is_structured_and_reducible(self):
        shard_path = self.ws.shard_path("shard-001.jsonl")
        with open(shard_path, "w", encoding="utf-8") as fh:
            fh.write("{corrupt json}\n")
        report = run_pipeline(self.ws, serial=False, repo_root=REPO_ROOT)
        failed = report["shard_results"]["shard-001"]
        self.assertEqual(failed["status"], "error")
        self.assertIn("code", failed["error"])
        self.assertEqual(report["shard_results"]["shard-000"]["status"], "done")
        self.assertTrue(report["aggregated"])
        summary = export_summary(self.ws)
        statuses = {s["shard_id"]: s["status"] for s in summary["shards"]}
        self.assertEqual(statuses["shard-001"], "error")
        self.assertEqual(statuses["shard-000"], "done")
        self.assertEqual(len(summary["errors"]), 1)
        self.assertEqual(summary["errors"][0]["shard_id"], "shard-001")

    def test_parallel_latency_budget_error_object(self):
        import yaml

        with open(self.ws.config_path, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
        cfg["parallel"]["shard_latency_budget_ms"] = 0
        with open(self.ws.config_path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(cfg, fh, allow_unicode=True, sort_keys=False)
        report = run_pipeline(self.ws, serial=False, repo_root=REPO_ROOT)
        result = report["shard_results"]["shard-000"]
        self.assertEqual(result["status"], "error")
        error = result["error"]
        self.assertEqual(error["code"], errors.E_LATENCY_BUDGET)
        self.assertEqual(error["details"]["shard_id"], "shard-000")
        self.assertIn("elapsed_ms", error["details"])
        # other shards also fail fast; reduction still completes
        self.assertTrue(report["aggregated"])
