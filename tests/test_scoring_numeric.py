from __future__ import annotations

import math
from pathlib import Path

import pytest

from qworkbench.config import WorkbenchConfig
from qworkbench.dedup import deduplicate
from qworkbench.errors import BusinessError
from qworkbench.numeric import weighted_geometric_fixed
from qworkbench.scoring import score_records


def _score(config, dataset):
    normalized = deduplicate(dataset.samples, config.dedup)
    return score_records(normalized["records"], config)


def test_fixed_point_contributions_and_fallbacks(config, dataset) -> None:
    result = _score(config, dataset)
    assert result["records"]
    for record in result["records"]:
        assert 0.0 < record["quality_score"] <= 1.0
        assert math.isfinite(record["log_score"])
        assert record["effective_digits"] >= 0
        contribution_units = sum(
            factor["log_term_units"] for factor in record["factors"].values()
        )
        assert contribution_units == record["log_score_fixed_units"]
        assert sum(factor["weight_units"] for factor in record["factors"].values()) == (
            1 << config.quantization.fraction_bits
        )
    empty = next(record for record in result["records"] if record["id"] == "anomaly-empty-output")
    fallback_names = {item["factor"] for item in empty["fallback_reasons"]}
    assert {"length_penalty", "lexical_overlap"} <= fallback_names
    assert empty["quality_score"] > 0.0
    unknown = next(record for record in result["records"] if record["id"] == "anomaly-unknown-source")
    assert "source_prior" in unknown["factor_notes"]


def test_corpus_aggregation_compares_order_and_pairwise(config, dataset) -> None:
    result = _score(config, dataset)
    aggregation = result["diagnostics"]["corpus_aggregation"]
    assert aggregation["sequential_high_precision_delta"] <= aggregation["combined_deviation_bound"]
    assert aggregation["pairwise_high_precision_delta"] <= aggregation["combined_deviation_bound"]
    assert "Sequential summation is order-sensitive" in aggregation["note"]


@pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), -0.1, 1.1])
def test_numeric_errors_include_sample_and_factor(config, bad_value) -> None:
    factors = {
        "length_penalty": 0.5,
        "lexical_overlap": bad_value,
        "template_diversity": 0.8,
        "source_prior": 0.7,
    }
    with pytest.raises(BusinessError) as caught:
        weighted_geometric_fixed("sample-1", factors, config.scoring.weights, {}, config.quantization)
    assert caught.value.code == "E_NUMERIC"
    assert caught.value.details["sample_id"] == "sample-1"
    assert caught.value.details["factor"] == "lexical_overlap"


def test_zero_factor_without_fallback_is_numeric_error(config) -> None:
    factors = {
        "length_penalty": 0.0,
        "lexical_overlap": 0.5,
        "template_diversity": 0.8,
        "source_prior": 0.7,
    }
    with pytest.raises(BusinessError) as caught:
        weighted_geometric_fixed("sample-zero", factors, config.scoring.weights, {}, config.quantization)
    assert caught.value.code == "E_NUMERIC"
    assert caught.value.details["factor"] == "length_penalty"


def test_invalid_quantization_and_weights(config) -> None:
    raw = dict(config.raw)
    raw["quantization"] = {**raw["quantization"], "fraction_bits": 1}
    with pytest.raises(BusinessError) as caught:
        WorkbenchConfig.from_dict(raw)
    assert caught.value.code == "E_CONFIG"
    raw = dict(config.raw)
    weights = dict(raw["scoring"]["weights"])
    weights["length_penalty"] = -0.5
    raw["scoring"] = {**raw["scoring"], "weights": weights}
    with pytest.raises(BusinessError) as caught:
        WorkbenchConfig.from_dict(raw)
    assert caught.value.code == "E_CONFIG"
