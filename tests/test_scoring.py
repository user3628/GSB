"""Precision scoring: determinism, quantisation, E_MISSING_FEATURE."""

from __future__ import annotations

import json

from _base import WorkspaceTestCase

from rerank_bench.config import Config
from rerank_bench.errors import E_MISSING_FEATURE, PipelineError
from rerank_bench.generator import default_config
from rerank_bench.stages import assemble_stage, score_stage


def _config(**overrides):
    raw = default_config()
    for key, value in overrides.items():
        raw["precision"][key] = value
    return Config(raw)


def _groups():
    return [{"query_id": "q1", "locale": "zh-CN", "channel": "web"}]


def _candidates(features=None):
    feats = features if features is not None else {
        "ctr": 0.5, "dwell": 120.0, "freshness": 0.9, "authority": 3.0}
    return {"q1": [{
        "doc_id": "d1", "coarse_score": 0.7, "category": "news",
        "language": "zh", "features": feats,
    }]}


class TestScoring(WorkspaceTestCase):
    def test_deterministic_byte_identical(self):
        config = _config()
        first = score_stage(assemble_stage(_groups(), _candidates(), config),
                            config)
        second = score_stage(assemble_stage(_groups(), _candidates(), config),
                             config)
        self.assertEqual(json.dumps(first, sort_keys=True),
                         json.dumps(second, sort_keys=True))

    def test_fixed_point_quantisation(self):
        config = _config()
        scored = score_stage(assemble_stage(_groups(), _candidates(), config),
                             config)
        cand = scored["query_groups"][0]["candidates"][0]
        self.assertIsInstance(cand["score_micro"], int)
        whole, _, frac = cand["score"].partition(".")
        self.assertEqual(len(frac), 6)
        self.assertEqual(cand["score_micro"],
                         int(whole) * 1_000_000 + int(frac))
        self.assertEqual(set(cand["components"]),
                         {"ctr", "dwell", "freshness", "authority", "embedding"})

    def test_missing_feature_raises_structured_error(self):
        config = _config()
        feats = {"ctr": 0.5, "dwell": 1.0, "freshness": 0.5}  # no authority
        with self.assertRaises(PipelineError) as ctx:
            assemble_stage(_groups(), _candidates(feats), config)
        self.assertEqual(ctx.exception.code, E_MISSING_FEATURE)
        self.assertEqual(ctx.exception.details["query_id"], "q1")
        self.assertEqual(ctx.exception.details["field"], "authority")

    def test_empty_candidate_group_raises(self):
        config = _config()
        with self.assertRaises(PipelineError) as ctx:
            assemble_stage(_groups(), {"q1": []}, config)
        self.assertEqual(ctx.exception.code, E_MISSING_FEATURE)
        self.assertEqual(ctx.exception.details["field"], "candidates")
        self.assertEqual(ctx.exception.details["query_id"], "q1")

    def test_ranking_order_and_tiebreak(self):
        config = _config()
        cands = {"q1": [
            {"doc_id": "b", "coarse_score": 0.1, "category": "news",
             "language": "zh",
             "features": {"ctr": 0.5, "dwell": 0.0, "freshness": 0.0,
                          "authority": 0.0}},
            {"doc_id": "a", "coarse_score": 0.1, "category": "news",
             "language": "zh",
             "features": {"ctr": 0.5, "dwell": 0.0, "freshness": 0.0,
                          "authority": 0.0}},
        ]}
        scored = score_stage(assemble_stage(_groups(), cands, config), config)
        ids = [c["doc_id"] for c in scored["query_groups"][0]["candidates"]]
        self.assertEqual(ids, ["a", "b"])  # doc_id tiebreak, deterministic
