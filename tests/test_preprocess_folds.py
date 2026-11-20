from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from conftest import DATA_DIR, fast_config
from linreg_workbench.canonical import digest
from linreg_workbench.data import DatasetRegistry
from linreg_workbench.errors import InvalidCV
from linreg_workbench.folds import make_folds
from linreg_workbench.preprocess import preprocess, preprocessing_fingerprint
from linreg_workbench.workbench import Workbench


def test_preprocessing_is_deterministic_and_onehot() -> None:
    registry = DatasetRegistry(DATA_DIR, Path("/tmp/unused-preprocess-workspace"))
    spec = registry.get("small")
    config = fast_config("small")
    first = preprocess(spec, config)
    second = preprocess(spec, config)
    assert digest(first) == digest(second)
    assert first["feature_count"] == 20
    assert first["row_count"] == 600
    assert first["fingerprint"] == preprocessing_fingerprint(spec, config)
    assert np.all(np.isfinite(np.asarray(first["X"], dtype=float)))


def test_target_encoding_is_deterministic() -> None:
    registry = DatasetRegistry(DATA_DIR, Path("/tmp/unused-preprocess-workspace-2"))
    spec = registry.get("medium")
    config = fast_config("medium", folds=2)
    config["preprocessing"]["categorical_encoding"] = "target"
    artifact = preprocess(spec, config)
    assert artifact["feature_count"] == 39
    assert artifact["row_count"] == 6000
    assert len(artifact["observed_missing_columns"]) == 5
    assert all(len(row) == 39 for row in artifact["X"])


def test_empty_feature_is_reported_and_others_continue(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty.csv"
    lines = ["signal,empty_column,label"]
    lines += [f"{index % 7},,{index % 2}" for index in range(40)]
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    wb = Workbench(tmp_path / "work", data_dir=DATA_DIR)
    wb.import_dataset(csv_path, "empty-demo", target="label")
    config = fast_config("empty-demo", folds=2)
    config["regularization"] = {"mode": "l2", "strengths": [0.01], "l1_ratios": [0.0]}
    wb.create_session("empty", config)
    result = wb.start_training("empty", background=False)["result"]
    assert [item["code"] for item in result["empty_feature_errors"]] == ["E_EMPTY_FEATURE"]
    assert result["empty_feature_errors"][0]["column"] == "empty_column"
    assert len(result["feature_names"]) == 1


def test_folds_are_deterministic_and_stratified() -> None:
    y = [index % 2 for index in range(120)]
    config = fast_config("small", folds=5)
    one = make_folds(y, config, "pre-fingerprint")
    two = make_folds(y, config, "pre-fingerprint")
    assert digest(one) == digest(two)
    for fold in one["folds"]:
        assert fold["validation_class_counts"] == {
            "0": 12,
            "1": 12,
        }
        assert len(fold["training_indices"]) + len(fold["validation_indices"]) == 120


def test_single_class_fold_is_invalid_cv() -> None:
    y = [0] * 20 + [1]
    config = fast_config("small", folds=3)
    with pytest.raises(InvalidCV) as caught:
        make_folds(y, config, "pre")
    assert caught.value.code == "E_INVALID_CV"
    assert caught.value.details["fold"] is not None
    assert "class" in caught.value.details["reason"]
    assert "absent" in caught.value.details["reason"]
