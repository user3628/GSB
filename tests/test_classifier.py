from __future__ import annotations

import math

import pytest

from qiu.engine import QueryEngine
from qiu.errors import QueryError


def test_score_components_softmax_and_determinism(engine: QueryEngine, sample: dict[str, object], data_paths: dict) -> None:
    result = engine.parse(sample)
    candidates = result["intent"]["candidates"]
    assert [candidate["intent"] for candidate in candidates] == [
        "navigation",
        "informational",
        "transactional",
        "local_service",
    ]
    assert math.isclose(sum(candidate["probability"] for candidate in candidates), 1.0, abs_tol=1e-10)
    for candidate in candidates:
        components = candidate["components"]
        expected = components["prior_log"] + components["weighted_likelihood"] + components["weighted_feature"]
        assert math.isclose(components["score"], expected, abs_tol=1e-9)

    independent = QueryEngine(
        config_path=data_paths["config"],
        training_path=data_paths["training"],
        dictionary_path=data_paths["dictionary"],
    )
    assert result == independent.parse(sample)
    assert engine.parse(sample)["intent"]["candidates"] == candidates


def test_nan_confidence_injection_raises_stable_numeric_error(engine: QueryEngine, sample: dict[str, object]) -> None:
    with pytest.raises(QueryError) as caught:
        engine.parse(sample, {"nan_confidence": True})
    assert caught.value.code == "E_NUMERIC"
    assert caught.value.stage == "intent"
    assert caught.value.query_id == sample["query_id"]
