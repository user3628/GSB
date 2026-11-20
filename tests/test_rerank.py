"""Rerank stage: windowing, diversity penalties, degenerate cases, trace."""

from __future__ import annotations

from _base import WorkspaceTestCase

from rerank_bench.config import Config
from rerank_bench.generator import default_config
from rerank_bench.stages import rerank_stage


def _config(window=3, cat_pen=0.08, lang_pen=0.05, dedup=5.0):
    raw = default_config()
    raw["rerank"] = {
        "window_size": window,
        "dedup_penalty": dedup,
        "category_penalty": cat_pen,
        "language_penalty": lang_pen,
    }
    return Config(raw)


def _score_artifact(specs):
    cands = []
    for rank, (doc_id, score_micro, cat, lang) in enumerate(specs):
        cands.append({
            "rank": rank, "doc_id": doc_id, "category": cat, "language": lang,
            "score_micro": score_micro, "score": str(score_micro),
            "components_micro": {}, "components": {},
        })
    return {"query_groups": [{
        "query_id": "q1", "locale": "zh-CN", "channel": "web",
        "candidates": cands,
    }]}


class TestRerank(WorkspaceTestCase):
    def test_zero_penalty_degenerates_to_precision_order(self):
        config = _config(window=2, cat_pen=0.0, lang_pen=0.0, dedup=0.0)
        artifact = _score_artifact([
            ("d1", 900, "news", "zh"),
            ("d2", 800, "news", "zh"),
            ("d3", 700, "news", "zh"),
        ])
        out = rerank_stage(artifact, config)
        ids = [s["doc_id"] for s in out["query_groups"][0]["selected"]]
        self.assertEqual(ids, ["d1", "d2", "d3"])

    def test_diversity_penalty_reorders(self):
        config = _config(window=4, cat_pen=100, lang_pen=0.0, dedup=0.0)
        artifact = _score_artifact([
            ("d1", 1000, "news", "zh"),
            ("d2", 990, "news", "zh"),
            ("d3", 500, "docs", "zh"),
        ])
        out = rerank_stage(artifact, config)
        ids = [s["doc_id"] for s in out["query_groups"][0]["selected"]]
        self.assertEqual(ids[0], "d1")
        self.assertEqual(ids[1], "d3")  # diversity beats small score gap

    def test_window_truncated_and_recorded(self):
        config = _config(window=10)
        artifact = _score_artifact([
            ("d1", 100, "news", "zh"), ("d2", 90, "docs", "en"),
        ])
        out = rerank_stage(artifact, config)
        group = out["query_groups"][0]
        self.assertEqual(group["requested_window"], 10)
        self.assertEqual(group["effective_window"], 2)
        self.assertTrue(group["window_truncated"])

    def test_no_duplicate_selection(self):
        config = _config(window=2)
        artifact = _score_artifact([
            ("d1", 100, "news", "zh"), ("d2", 90, "docs", "en"),
            ("d3", 80, "video", "zh"),
        ])
        out = rerank_stage(artifact, config)
        ids = [s["doc_id"] for s in out["query_groups"][0]["selected"]]
        self.assertEqual(len(ids), len(set(ids)))

    def test_trace_records_decision_basis(self):
        config = _config(window=2, cat_pen=50, lang_pen=10)
        artifact = _score_artifact([
            ("d1", 1000, "news", "zh"),
            ("d2", 900, "news", "en"),
            ("d3", 800, "docs", "zh"),
        ])
        out = rerank_stage(artifact, config)
        trace = out["query_groups"][0]["trace"]
        self.assertEqual(len(trace), 3)
        step2 = trace[1]
        self.assertEqual(step2["step"], 1)
        self.assertIn("chosen", step2)
        self.assertEqual(len(step2["considered"]), 2)
        for view in step2["considered"]:
            self.assertIn("penalty", view)
            self.assertIn("adjusted_score", view)
            self.assertIn("category_hits", view)
            self.assertIn("language_hits", view)
