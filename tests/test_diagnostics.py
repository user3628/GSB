import numpy as np

from evalbench.diagnostics import (brier_score, diagnose, log_loss,
                                   reliability_table)
from evalbench.models import EvalRecord


def make_records(n=40, langs=("zh", "en"), batches=("b1", "b2", "b3")):
    rng = np.random.default_rng(3)
    recs = []
    for i in range(n):
        recs.append(EvalRecord(
            model_id="m", sample_id=f"{i:04d}", score=float(rng.random()),
            label=int(rng.integers(0, 2)), lang=langs[i % 2],
            batch=batches[i % 3], bucket=0))
    return recs


def test_metric_formulas():
    labels = np.array([0, 1, 1, 0], dtype=float)
    probs = np.array([0.1, 0.9, 0.8, 0.4])
    assert abs(brier_score(labels, probs) - np.mean((probs - labels) ** 2)) < 1e-12
    expected_ll = -np.mean(labels * np.log(probs) + (1 - labels) * np.log(1 - probs))
    assert abs(log_loss(labels, probs) - expected_ll) < 1e-12


def test_reliability_table_deviation():
    labels = np.array([1, 0, 1, 1], dtype=float)
    probs = np.array([0.9, 0.2, 0.8, 0.7])
    buckets = np.array([0, 0, 1, 1])
    table = reliability_table(labels, probs, buckets, 2)
    assert table[0]["count"] == 2
    assert abs(table[0]["mean_predicted"] - 0.55) < 1e-12
    assert abs(table[0]["empirical_freq"] - 0.5) < 1e-12
    assert abs(table[0]["abs_deviation"] - 0.05) < 1e-12


def test_strata_and_insufficient_layers():
    recs = make_records(40)
    labels = np.array([r.label for r in recs], dtype=float)
    raw = np.array([r.score for r in recs])
    buckets = np.minimum((raw * 10).astype(int), 9)
    out = diagnose(recs, labels, raw, raw, buckets, 10, 15, (0.0, 1.0))
    # every batch stratum has <= 14 samples -> all listed as insufficient
    assert out["strata"]["by_batch"] == []
    assert {s["value"] for s in out["insufficient_strata"]} == {"b1", "b2", "b3"}
    assert all(s["status"] == "insufficient_samples" for s in out["insufficient_strata"])
    # lang strata have 20 samples each -> present with metrics and composition
    assert {s["value"] for s in out["strata"]["by_lang"]} == {"zh", "en"}
    for layer in out["strata"]["by_lang"]:
        assert layer["n_samples"] == 20
        assert layer["composition"]["by_lang"][layer["value"]] == 20
        assert "ece" in layer and "log_loss" in layer and "brier_score" in layer


def test_expected_positive_rate_flag():
    recs = make_records(10)
    labels = np.ones(10)
    raw = np.full(10, 0.9)
    buckets = np.zeros(10, dtype=int)
    out = diagnose(recs, labels, raw, raw, buckets, 10, 1, (0.0, 0.5))
    assert out["overall"]["expected_positive_rate"]["within_interval"] is False
    out2 = diagnose(recs, labels, raw, raw, buckets, 10, 1, (0.5, 1.0))
    assert out2["overall"]["expected_positive_rate"]["within_interval"] is True
