"""HTTP API surface over 127.0.0.1 (via the Api dispatch layer)."""

from __future__ import annotations

from _base import REPO_ROOT, WorkspaceTestCase

from rerank_bench.errors import PipelineError
from rerank_bench.server import Api


class TestApi(WorkspaceTestCase):
    def setUp(self):
        super().setUp()
        self.api = Api(self.ws_root, repo_root=REPO_ROOT)

    def test_full_http_flow(self):
        out = self.api.handle("POST", "/import", {"data_dir": self.data_dir})
        self.assertEqual(len(out["imported"]["shards"]), 4)
        run = self.api.handle("POST", "/run", {"serial": True})
        self.assertTrue(run["aggregated"])
        st = self.api.handle("GET", "/status", {})
        self.assertEqual(len(st["shards"]), 4)
        budget = self.api.handle("GET", "/budget", {})
        self.assertEqual(budget["consumed_evaluations"],
                         budget["reserved_evaluations"])
        export = self.api.handle("GET", "/export", {})
        self.assertEqual(export["schema"], "rerank-bench.summary.v1")
        fp = self.api.handle("GET", "/fingerprints/shard-000", {})
        self.assertIn("stages", fp)
        trace = self.api.handle("GET", "/shards/shard-000/trace", {})
        self.assertIn("trace", trace["query_groups"][0])

    def test_stop_and_resume_via_api(self):
        self.api.handle("POST", "/import", {"data_dir": self.data_dir})
        self.api.handle("POST", "/stop", {})
        run = self.api.handle("POST", "/run", {})
        self.assertTrue(run["stopped"])
        resumed = self.api.handle("POST", "/resume", {})
        self.assertTrue(resumed["aggregated"])

    def test_export_before_run_returns_structured_error(self):
        self.api.handle("POST", "/import", {"data_dir": self.data_dir})
        with self.assertRaises(PipelineError) as ctx:
            self.api.handle("GET", "/export", {})
        self.assertEqual(ctx.exception.code, "E_INVALID_STAGE")

    def test_budget_reports_unprocessed_reasons(self):
        self.api.handle("POST", "/import", {"data_dir": self.data_dir})
        import yaml
        with open(self.api.ws.config_path, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
        cfg["parallel"]["global_eval_budget"] = 100
        with open(self.api.ws.config_path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(cfg, fh, allow_unicode=True, sort_keys=False)
        self.api.handle("POST", "/run", {"serial": True})
        budget = self.api.handle("GET", "/budget", {})
        self.assertTrue(budget["unprocessed"])
        self.assertEqual(budget["unprocessed"][0]["code"], "E_BUDGET_EXCEEDED")
        self.assertLessEqual(budget["consumed_evaluations"], 100)
