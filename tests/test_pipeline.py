"""End-to-end pipeline behaviour: determinism, recovery, invalidation."""

from __future__ import annotations

import hashlib
import json
import os

import yaml

from _base import REPO_ROOT, WorkspaceTestCase

from rerank_bench import errors
from rerank_bench.errors import PipelineError
from rerank_bench.pipeline import Workspace, run_shard_stages
from rerank_bench.runner import export_summary, run_pipeline, status


def _export_digest(ws: Workspace) -> str:
    with open(os.path.join(ws.export_dir, "summary.json"), "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


class TestDeterminism(WorkspaceTestCase):
    def test_parallel_equals_serial_byte_identical(self):
        run_pipeline(self.ws, serial=False, repo_root=REPO_ROOT)
        parallel_digest = _export_digest(self.ws)
        ws2 = Workspace(os.path.join(self.tmp, "ws2"))
        ws2.import_data(self.data_dir)
        run_pipeline(ws2, serial=True, repo_root=REPO_ROOT)
        self.assertEqual(parallel_digest, _export_digest(ws2))

    def test_rerun_is_byte_identical(self):
        run_pipeline(self.ws, repo_root=REPO_ROOT)
        first = _export_digest(self.ws)
        run_pipeline(self.ws, repo_root=REPO_ROOT)
        self.assertEqual(first, _export_digest(self.ws))

    def test_stop_resume_equals_fresh_run(self):
        run_pipeline(self.ws, repo_root=REPO_ROOT, stop_after=2)
        self.assertFalse(os.path.exists(
            os.path.join(self.ws.export_dir, "summary.json")))
        run_pipeline(self.ws, repo_root=REPO_ROOT)  # resume
        resumed = _export_digest(self.ws)
        ws2 = Workspace(os.path.join(self.tmp, "ws2"))
        ws2.import_data(self.data_dir)
        run_pipeline(ws2, repo_root=REPO_ROOT)
        self.assertEqual(resumed, _export_digest(ws2))

    def test_stop_flag_halts_dispatch(self):
        self.ws.request_stop()
        report = run_pipeline(self.ws, repo_root=REPO_ROOT)
        self.assertTrue(report["stopped"])
        self.assertEqual(len(report["pending"]), 4)
        self.assertFalse(report["aggregated"])


class TestInvalidation(WorkspaceTestCase):
    def _rewrite_config(self, mutate):
        with open(self.ws.config_path, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
        mutate(cfg)
        with open(self.ws.config_path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(cfg, fh, allow_unicode=True, sort_keys=False)

    def test_weight_change_invalidates_score_and_downstream(self):
        run_pipeline(self.ws, repo_root=REPO_ROOT)
        self._rewrite_config(lambda c: c["precision"]["weights"].update(
            ctr=0.99))
        report = run_pipeline(self.ws, repo_root=REPO_ROOT)
        events = [e for e in report["invalidations"] if e["shard_id"] != "_global"]
        self.assertEqual(len(events), 4)
        for event in events:
            self.assertEqual(event["earliest_stage"], "score")
            self.assertEqual(event["invalidated_stages"], ["score", "rerank"])
            self.assertEqual(event["reason"], "config_changed:score")
        stages = report["shard_results"]["shard-000"]["stages"]
        self.assertEqual(stages["assemble"], "reused")
        self.assertEqual(stages["score"], "done")
        self.assertEqual(stages["rerank"], "done")

    def test_window_change_invalidates_rerank_only(self):
        run_pipeline(self.ws, repo_root=REPO_ROOT)
        self._rewrite_config(lambda c: c["rerank"].update(window_size=4))
        report = run_pipeline(self.ws, repo_root=REPO_ROOT)
        events = [e for e in report["invalidations"] if e["shard_id"] != "_global"]
        self.assertTrue(events)
        for event in events:
            self.assertEqual(event["earliest_stage"], "rerank")
            self.assertEqual(event["invalidated_stages"], ["rerank"])
        stages = report["shard_results"]["shard-000"]["stages"]
        self.assertEqual(stages["assemble"], "reused")
        self.assertEqual(stages["score"], "reused")
        self.assertEqual(stages["rerank"], "done")

    def test_shard_content_change_invalidates_from_assemble(self):
        run_pipeline(self.ws, repo_root=REPO_ROOT)
        shard_path = self.ws.shard_path("shard-000.jsonl")
        with open(shard_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps({
                "type": "candidate", "query_id": "q-s0-0",
                "doc_id": "doc-injected", "coarse_score": 0.5,
                "category": "news", "language": "zh",
                "features": {"ctr": 0.1, "dwell": 1.0, "freshness": 0.2,
                             "authority": 0.3},
            }, ensure_ascii=False, sort_keys=True) + "\n")
        report = run_pipeline(self.ws, repo_root=REPO_ROOT)
        events = [e for e in report["invalidations"]
                  if e["shard_id"] == "shard-000"]
        self.assertEqual(events[0]["earliest_stage"], "assemble")
        self.assertEqual(events[0]["reason"], "shard_content_changed")
        untouched = [e for e in report["invalidations"]
                     if e["shard_id"] == "shard-001"]
        self.assertEqual(untouched, [])

    def test_fingerprints_observable_in_status(self):
        run_pipeline(self.ws, repo_root=REPO_ROOT)
        st = status(self.ws)
        shard = st["shards"][0]
        for stage in ("assemble", "score", "rerank"):
            info = shard["stages"][stage]
            self.assertEqual(info["status"], "valid")
            self.assertEqual(info["expected_fingerprint"],
                             info["recorded_fingerprint"])


class TestErrorCodes(WorkspaceTestCase):
    def test_export_before_aggregate_raises_invalid_stage(self):
        with self.assertRaises(PipelineError) as ctx:
            export_summary(self.ws)
        self.assertEqual(ctx.exception.code, errors.E_INVALID_STAGE)

    def test_unknown_stage_raises_invalid_stage(self):
        with self.assertRaises(PipelineError) as ctx:
            run_pipeline(self.ws, stage_limit="fly", repo_root=REPO_ROOT)
        self.assertEqual(ctx.exception.code, errors.E_INVALID_STAGE)

    def test_latency_budget_with_injected_clock(self):
        times = iter([0.0, 10.0, 20.0, 30.0])
        result = run_shard_stages(
            self.ws_root, "shard-000", "shard-000.jsonl",
            self.ws.config().raw, 5000, clock=lambda: next(times))
        self.assertEqual(result["status"], "error")
        error = result["error"]
        self.assertEqual(error["code"], errors.E_LATENCY_BUDGET)
        self.assertEqual(error["details"]["shard_id"], "shard-000")
        self.assertEqual(error["details"]["elapsed_ms"], 10000)
        self.assertEqual(error["details"]["budget_ms"], 5000)

    def test_missing_feature_surfaces_in_shard_result(self):
        shard_path = self.ws.shard_path("shard-000.jsonl")
        with open(shard_path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        records = [json.loads(x) for x in lines if x.strip()]
        for rec in records:
            if rec["type"] == "candidate":
                rec["features"].pop("ctr", None)
                break
        with open(shard_path, "w", encoding="utf-8", newline="\n") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False, sort_keys=True))
                fh.write("\n")
        report = run_pipeline(self.ws, serial=True, repo_root=REPO_ROOT)
        result = report["shard_results"]["shard-000"]
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["error"]["code"], errors.E_MISSING_FEATURE)
        self.assertEqual(result["error"]["details"]["field"], "ctr")
        self.assertIn("query_id", result["error"]["details"])

    def test_worker_crash_is_structured_and_reduction_continues(self):
        shard_path = self.ws.shard_path("shard-001.jsonl")
        with open(shard_path, "w", encoding="utf-8") as fh:
            fh.write("{not json}\n")
        report = run_pipeline(self.ws, serial=True, repo_root=REPO_ROOT)
        result = report["shard_results"]["shard-001"]
        self.assertEqual(result["status"], "error")
        self.assertIn("code", result["error"])
        self.assertEqual(report["shard_results"]["shard-000"]["status"], "done")
