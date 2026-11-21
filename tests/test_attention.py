from __future__ import annotations

import numpy as np
import pytest

from rope_diag.attention import compute_scores_and_weights
from rope_diag.config import RopeConfig
from rope_diag.errors import RopeError


def test_scores_and_causal_softmax_are_correct() -> None:
    query = np.array([[[[1.0, 0.0], [0.0, 1.0]]]])
    key = np.array([[[[1.0, 0.0], [0.0, 1.0]]]])
    result = compute_scores_and_weights(
        query,
        key,
        RopeConfig(causal_mask=True),
        shard_id="unit",
        row_chunk_size=1,
    )
    np.testing.assert_array_equal(result["scores"][0, 0, 0], [1.0, -np.inf])
    np.testing.assert_array_equal(result["attention_weights"][0, 0, 0], [1.0, 0.0])
    np.testing.assert_array_equal(result["scores"][0, 0, 1], [0.0, 1.0])
    np.testing.assert_allclose(
        result["attention_weights"][0, 0, 1],
        np.array([1.0 / (1.0 + np.e), np.e / (1.0 + np.e)]),
        rtol=0,
        atol=0,
    )
    np.testing.assert_array_equal(
        np.sum(result["attention_weights"], axis=-1), np.ones((1, 1, 2))
    )


def test_non_causal_softmax_uses_all_keys() -> None:
    query = np.array([[[[1.0, 0.0], [1.0, 0.0], [1.0, 0.0]]]])
    key = np.array([[[[1.0, 0.0], [0.0, 1.0], [-1.0, 0.0]]]])
    result = compute_scores_and_weights(
        query,
        key,
        RopeConfig(causal_mask=False),
        shard_id="unit",
        row_chunk_size=1,
    )
    np.testing.assert_array_equal(result["scores"][0, 0, 0], [1.0, 0.0, -1.0])
    assert result["normalization_factors"][0, 0, 0] > 1.0
    np.testing.assert_allclose(
        np.sum(result["attention_weights"], axis=-1), np.ones((1, 1, 3))
    )


@pytest.mark.parametrize("bad_value", [np.nan, np.inf, -np.inf])
def test_numeric_errors_include_shard_row_block_and_step(bad_value: float) -> None:
    query = np.zeros((1, 1, 3, 2), dtype=np.float64)
    key = np.zeros_like(query)
    query[0, 0, 1, 0] = bad_value
    with pytest.raises(RopeError) as caught:
        compute_scores_and_weights(
            query,
            key,
            RopeConfig(causal_mask=True),
            shard_id="numeric-shard",
            row_chunk_size=2,
        )
    assert caught.value.code == "E_NUMERIC"
    assert caught.value.details["shard_id"] == "numeric-shard"
    assert caught.value.details["row_block_index"] == 0
    assert caught.value.details["step_index"] == 1


def test_zero_normalization_factor_is_structured_numeric_error(monkeypatch: pytest.MonkeyPatch) -> None:
    query = np.zeros((1, 1, 2, 2), dtype=np.float64)
    key = np.zeros_like(query)
    monkeypatch.setattr("rope_diag.attention.np.exp", lambda value: np.zeros_like(value))
    with pytest.raises(RopeError) as caught:
        compute_scores_and_weights(
            query,
            key,
            RopeConfig(causal_mask=False),
            shard_id="zero-z",
            row_chunk_size=1,
        )
    assert caught.value.code == "E_NUMERIC"
    assert caught.value.details["reason"] == "normalization_factor_zero_or_non_finite"
    assert {"shard_id", "row_block_index", "step_index"} <= set(caught.value.details)
