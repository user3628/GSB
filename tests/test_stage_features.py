import numpy as np
import pytest

from prefalign.core.errors import FeatureInvalidError
from prefalign.core.stage_features import pair_feature, run, unit_embedding


def test_unit_embedding_norm_and_dim():
    vec = unit_embedding("alpha beta alpha gamma", 64, 104729)
    assert vec.shape == (64,)
    assert np.isclose(np.linalg.norm(vec), 1.0)


def test_empty_text_zero_vector():
    vec = unit_embedding("", 64, 1)
    assert np.count_nonzero(vec) == 0


def test_pair_rules_dimensions():
    chosen = np.ones(64) / 8.0
    rejected = np.zeros(64)
    assert pair_feature(chosen, rejected, "concat").shape == (128,)
    assert pair_feature(chosen, rejected, "diff").shape == (64,)
    norm = pair_feature(chosen, rejected, "normalized")
    assert np.isclose(np.linalg.norm(norm), 1.0)


def test_unknown_rule(built_pipeline):
    with pytest.raises(FeatureInvalidError):
        pair_feature(np.zeros(2), np.zeros(2), "bogus")


def test_folds_cover_all_groups_and_no_leakage(built_pipeline):
    features = built_pipeline.load_artifact("features")
    normalized = built_pipeline.load_artifact("normalize")
    group_to_fold = {item["group_id"]: item["fold"] for item in features["group_folds"]}
    for record in features["records"]:
        assert record["fold"] == group_to_fold[record["group_id"]]
    assert {fs["fold"] for fs in features["fold_stats"]} == set(range(features["n_folds"]))
    assert all(fs["group_count"] > 0 for fs in features["fold_stats"])
    # Every group lands entirely inside one fold.
    group_ids = {r["group_id"] for r in normalized["records"]}
    assert set(group_to_fold) == group_ids


def test_dimension_mismatch_diagnostic(built_pipeline, monkeypatch):
    normalized = built_pipeline.load_artifact("normalize")
    quality = built_pipeline.load_artifact("quality")
    cfg = built_pipeline.cfg.raw
    monkeypatch.setitem(cfg["features"], "rule", "concat")
    monkeypatch.setitem(cfg["features"], "embedding_dim", 32)
    result = run(normalized, quality, cfg)
    assert result["feature_dim"] == 64
    assert all(len(r["feature"]) == 64 for r in result["records"])


def test_empty_fold_emits_feature_invalid(built_pipeline):
    normalized = built_pipeline.load_artifact("normalize")
    quality = built_pipeline.load_artifact("quality")
    cfg_raw = built_pipeline.cfg.raw
    cfg_raw["folds"]["n_folds"] = 64  # more folds than groups -> empty folds
    result = run(normalized, quality, cfg_raw)
    diags = [d for d in result["diagnostics"] if d["code"] == "E_FEATURE_INVALID"]
    assert diags
    assert all(d["fold"] is not None for d in diags)
