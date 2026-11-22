from __future__ import annotations

import numpy as np
import pytest

from feature_workbench.config import normalize_config
from feature_workbench.errors import WorkbenchError
from feature_workbench.selection import select_features


def _shard(matrix: np.ndarray, labels: np.ndarray, names: list[str]) -> dict:
    return {
        "summary": {
            "shard_index": 0,
            "tier": "small",
            "feature_names": names,
            "matrix_digest": "matrix",
        },
        "matrix": matrix,
        "labels": labels,
    }


def _data():
    names = ["signal", "constant", "noise"]
    x = np.asarray(
        [
            [0.0, 1.0, 0.9],
            [1.0, 1.0, 0.1],
            [2.0, 1.0, 0.8],
            [3.0, 1.0, 0.2],
            [4.0, 1.0, 0.7],
            [5.0, 1.0, 0.3],
        ]
    )
    labels = np.asarray([0, 0, 0, 1, 1, 1], dtype=np.int64)
    return names, x, labels


def test_variance_filter_reports_scores_and_eliminates_low_variance() -> None:
    names, x, labels = _data()
    config = normalize_config({"selection": {"method": "variance", "threshold": 0.5}})
    result = select_features([_shard(x, labels, names)], config)
    assert result["selected"] == ["signal"]
    assert result["eliminated"] == ["constant", "noise"]
    assert result["scores"]["constant"] == 0.0
    assert result["ranking"][0] == "signal"
    assert len(result["selection_digest"]) == 64


def test_mutual_information_ranks_signal_above_noise() -> None:
    names, x, labels = _data()
    config = normalize_config({"selection": {"method": "mutual_info", "threshold": 0.0}})
    first = select_features([_shard(x, labels, names)], config)
    second = select_features([_shard(x, labels, names)], config)
    assert first["selection_digest"] == second["selection_digest"]
    assert first["scores"]["signal"] > first["scores"]["noise"]
    assert first["ranking"][0] == "signal"


def test_rfe_threshold_controls_kept_count() -> None:
    names, x, labels = _data()
    config = normalize_config({"selection": {"method": "rfe", "threshold": 0.2}})
    result = select_features([_shard(x, labels, names)], config)
    assert len(result["selected"]) == 1
    assert set(result["selected"]).isdisjoint(result["eliminated"])
    assert result["score_basis"].startswith("ridge_linear")


def test_method_or_threshold_change_changes_selection_digest() -> None:
    names, x, labels = _data()
    variance = normalize_config({"selection": {"method": "variance", "threshold": 0.01}})
    variance_high = normalize_config({"selection": {"method": "variance", "threshold": 0.5}})
    mi = normalize_config({"selection": {"method": "mutual_info", "threshold": 0.0}})
    digests = {
        select_features([_shard(x, labels, names)], config)["selection_digest"]
        for config in (variance, variance_high, mi)
    }
    assert len(digests) == 3


def test_rfe_invalid_threshold_returns_business_error() -> None:
    names, x, labels = _data()
    config = normalize_config({"selection": {"method": "rfe", "threshold": 0.5}})
    config["selection"]["threshold"] = 0.0
    with pytest.raises(WorkbenchError) as raised:
        select_features([_shard(x, labels, names)], config)
    assert raised.value.code == "E_INVALID_THRESHOLD"

