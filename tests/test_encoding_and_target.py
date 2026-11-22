from __future__ import annotations

import copy
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from feature_workbench.config import normalize_config
from feature_workbench.dataset import (
    build_shard_specs,
    infer_type_schema,
    load_manifest,
    load_table,
)
from feature_workbench.encoding import _target_encode, encode_table
from feature_workbench.errors import WorkbenchError

from conftest import read_config


def _small_context(root: Path):
    manifest = load_manifest(root / "data" / "manifest.json")
    specs = build_shard_specs(manifest, root / "data")
    tables = {spec.shard_index: load_table(spec) for spec in specs}
    schema = infer_type_schema(manifest, specs, tables)
    small_specs = sorted([spec for spec in specs if spec.tier == "small"], key=lambda spec: spec.shard_index)
    tier_schema = dict(schema["tiers"]["small"])
    tier_schema["schema_digest"] = schema["schema_digest"]
    return manifest, specs, tables, small_specs, tier_schema


def test_one_hot_encoding_is_deterministic_and_maps_columns(workspace: Path) -> None:
    manifest, specs, tables, small_specs, tier_schema = _small_context(workspace)
    config = normalize_config(read_config(workspace))
    table = tables[small_specs[0].shard_index]
    first = encode_table(table, tier_schema, config)
    second = encode_table(table, tier_schema, config)
    assert first["summary"]["encoding_digest"] == second["summary"]["encoding_digest"]
    assert np.array_equal(first["matrix"], second["matrix"])
    summary = first["summary"]
    assert first["matrix"].shape[0] == 300
    assert summary["encoded_feature_count"] == len(summary["feature_names"])
    assert len(summary["mapping"]) == len(summary["feature_names"])
    assert all(item["source_column"] in table.header for item in summary["mapping"])
    assert any(name == "ohe:city=__MISSING__" for name in summary["feature_names"])
    assert all(item["feature"] in summary["feature_names"] for item in summary["feature_domains"])
    assert len(summary["source_domains"]) == 9


def test_hash_encoding_has_fixed_width_and_is_deterministic(workspace: Path) -> None:
    _, _, tables, small_specs, tier_schema = _small_context(workspace)
    config = normalize_config(read_config(workspace))
    config["encoding"]["strategy"] = "hash"
    table = tables[small_specs[0].shard_index]
    first = encode_table(table, tier_schema, config)
    second = encode_table(table, tier_schema, config)
    assert first["summary"]["encoding_digest"] == second["summary"]["encoding_digest"]
    hash_features = [name for name in first["summary"]["feature_names"] if name.startswith("hash:")]
    assert len(hash_features) == 4 * config["encoding"]["hash"]["n_features"]


def test_one_hot_keeps_global_domain_when_a_shard_has_no_missing(workspace: Path) -> None:
    _, _, tables, small_specs, tier_schema = _small_context(workspace)
    config = normalize_config(read_config(workspace))
    table = tables[small_specs[0].shard_index]
    city_index = table.header.index("city")
    rows = [list(row) for row in table.rows]
    for row in rows:
        if row[city_index].strip().lower() in {"", "na", "null", "none", "nan"}:
            row[city_index] = "Beijing"
    without_missing = replace(table, rows=rows)
    result = encode_table(without_missing, tier_schema, config)
    assert "ohe:city=__MISSING__" in result["summary"]["feature_names"]
    assert len(result["summary"]["feature_names"]) == len(
        encode_table(table, tier_schema, config)["summary"]["feature_names"]
    )


def test_target_encoding_uses_only_training_fold_labels(workspace: Path) -> None:
    _, _, tables, small_specs, tier_schema = _small_context(workspace)
    config = normalize_config(read_config(workspace))
    config["encoding"]["strategy"] = "target"
    table = tables[small_specs[0].shard_index]
    original = encode_table(table, tier_schema, config)
    assert original["summary"]["target_encoding_diagnostics"]
    diagnostics = original["summary"]["target_encoding_diagnostics"][0]
    assert diagnostics["fold_assignment_seed"] == config["encoding"]["target"]["fold_seed"]
    category = diagnostics["categories"][0]
    encoded_values = [entry["encoded"] for entry in category["folds"]]
    assert len(set(encoded_values)) > 1
    assert all("fallback" in entry for entry in category["folds"])

    rows = [list(row) for row in table.rows]
    label_index = table.header.index("label")
    rows[0][label_index] = "0" if rows[0][label_index] == "1" else "1"
    changed_table = replace(table, rows=rows)
    changed = encode_table(changed_table, tier_schema, config)
    te_positions = [
        index
        for index, name in enumerate(original["summary"]["feature_names"])
        if name.startswith("te:")
    ]
    assert np.array_equal(original["matrix"][0, te_positions], changed["matrix"][0, te_positions])


def test_target_encoding_unseen_category_uses_training_global_mean() -> None:
    labels = np.asarray([index % 2 for index in range(30)], dtype=np.int64)
    values = ["a", "b", "c"] * 9 + ["rare", "a", "b"]
    config = normalize_config({"encoding": {"strategy": "target"}})
    encoded, diagnostics = _target_encode(
        values,
        labels,
        column_name="category",
        column_index=1,
        config=config,
    )
    assert encoded.shape == labels.shape
    unseen = [
        (category["value"], fold)
        for category in diagnostics["categories"]
        for fold in category["folds"]
        if not fold["seen_in_training"]
    ]
    assert unseen
    fold_priors = {
        item["fold"]: item["fallback_global_mean"] for item in diagnostics["folds"]
    }
    assert all(abs(fold["encoded"] - fold_priors[fold["fold"]]) < 1e-12 for _, fold in unseen)


def test_target_encoding_single_class_fold_returns_business_error() -> None:
    values = ["a", "b"] * 15
    labels = np.zeros(30, dtype=np.int64)
    config = normalize_config({"encoding": {"strategy": "target"}})
    with pytest.raises(WorkbenchError) as raised:
        _target_encode(values, labels, column_name="category", column_index=1, config=config)
    assert raised.value.code == "E_FOLD_INVALID"
    assert raised.value.details["column"] == "category"


def test_invalid_selection_thresholds_return_business_error() -> None:
    from feature_workbench.config import validate_threshold

    with pytest.raises(WorkbenchError) as negative:
        validate_threshold("variance", -0.1)
    assert negative.value.code == "E_INVALID_THRESHOLD"
    with pytest.raises(WorkbenchError) as rfe:
        validate_threshold("rfe", 1.1)
    assert rfe.value.code == "E_INVALID_THRESHOLD"

