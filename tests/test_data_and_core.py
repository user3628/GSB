from __future__ import annotations

import hashlib

import numpy as np
import pytest

from gsb.binning import build_binning_payload, fit_bins
from gsb.config import Config
from gsb.data import generate_datasets, verify_generated_data
from gsb.errors import WorkbenchError
from gsb.tree import fit_boosting_model, predict_model_raw


def raw_digest(values: np.ndarray) -> str:
    return hashlib.sha256(np.asarray(values, dtype="<f8").tobytes()).hexdigest()


def test_generated_data_is_byte_deterministic_and_declared(project_root):
    first = verify_generated_data(project_root / "data")
    second = generate_datasets(project_root / "data", force=True)
    assert first["manifest_sha256"] == second["manifest_sha256"]
    assert {key: value["sha256"] for key, value in first["datasets"].items()} == {
        key: value["sha256"] for key, value in second["datasets"].items()
    }
    assert second["datasets"]["small"]["rows"] == 800
    assert second["datasets"]["small"]["cols"] == 12
    assert second["datasets"]["medium"]["rows"] == 8000
    assert second["datasets"]["medium"]["cols"] == 32
    assert second["datasets"]["small"]["observed_missing_columns"] == [2, 7]
    assert second["datasets"]["medium"]["observed_missing_columns"] == [3, 9, 17, 25]
    assert 0.0 < second["datasets"]["small"]["observed_positive_rate"] < 1.0


@pytest.mark.parametrize(
    "patch",
    [
        {"bins": 0},
        {"learning_rate": 0.0},
        {"learning_rate": 1.01},
        {"max_depth": 0},
        {"l2": float("nan")},
        {"calibration_folds": 1},
        {"calibrator": "spline"},
    ],
)
def test_invalid_parameters_fail_before_training(patch):
    with pytest.raises(WorkbenchError) as caught:
        Config().with_patch(patch)
    assert caught.value.code == "E_INVALID_PARAM"


def test_binning_and_boosting_are_exactly_reproducible(small_dataset):
    config = Config(bins=12, rounds=5, max_depth=3, min_leaf=12).validate()
    payload_a, spec_a, binned_a = build_binning_payload(small_dataset.X, config.bins)
    payload_b, spec_b, binned_b = build_binning_payload(small_dataset.X, config.bins)
    assert payload_a == payload_b
    assert spec_a == spec_b
    assert np.array_equal(binned_a, binned_b)
    assert np.all(binned_a[:, 2][np.isnan(small_dataset.X[:, 2])] == 0)
    model_a, raw_a = fit_boosting_model(binned_a, small_dataset.y, config)
    model_b, raw_b = fit_boosting_model(binned_b, small_dataset.y, config)
    assert model_a.to_payload() == model_b.to_payload()
    assert raw_digest(raw_a) == raw_digest(raw_b)
    assert np.array_equal(raw_a, predict_model_raw(model_a, binned_a))


def test_bin_edges_handle_single_bin_and_missing_column(small_dataset):
    spec = fit_bins(small_dataset.X, 1)
    assert all(len(edges) == 0 for edges in spec.edges)
    binned = spec.transform(small_dataset.X)
    assert np.all(binned[np.isnan(small_dataset.X)] == 0)
    assert np.all(binned[~np.isnan(small_dataset.X)] == 1)
