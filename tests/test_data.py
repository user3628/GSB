from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from chunkops.data import generate_datasets, load_fragment


def test_generated_shapes_ranges_and_operations(dataset_manifest: dict) -> None:
    records = {record["fragment_id"]: record for record in dataset_manifest["fragments"]}
    assert records["small_base"]["shape"] == [128, 512]
    assert records["medium_base"]["shape"] == [2048, 1024]
    assert records["small_anomaly"]["shape"] == [128, 512]
    assert records["small_base"]["numeric_range"]["nan_count"] == 0
    assert records["small_anomaly"]["numeric_range"]["min"] == -10000.0
    assert records["small_anomaly"]["numeric_range"]["max"] == 10000.0
    assert records["small_anomaly"]["zero_row_indices"]
    operation_pairs = {
        (item["op"], item["axis"]) for item in records["small_base"]["operations"]
    }
    assert {("softmax", 1), ("layernorm", 0)} <= operation_pairs
    assert dataset_manifest["shape_and_range_catalog"]["small_base"]["shape"] == [128, 512]


def test_anomaly_fragment_contains_nan_pos_inf_neg_inf_and_zero_rows(
    dataset_manifest: dict, fragment_paths: dict[str, str]
) -> None:
    record = next(
        item for item in dataset_manifest["fragments"] if item["fragment_id"] == "small_anomaly"
    )
    assert record["nonfinite"]["nan_count"] > 0
    assert record["nonfinite"]["pos_inf_count"] > 0
    assert record["nonfinite"]["neg_inf_count"] > 0
    assert record["zero_row_indices"] == [3, 25, 42]
    x, gamma, beta, metadata = load_fragment(
        fragment_paths["small_anomaly"], fragment_id="small_anomaly"
    )
    assert gamma is not None and beta is not None
    assert metadata["shape"] == [128, 512]
    assert np.isnan(x[10, 128])
    assert np.all(x[record["zero_row_indices"], :] == 0)


def test_fixed_seed_generation_is_byte_deterministic(tmp_path: Path) -> None:
    first = generate_datasets(tmp_path / "a", seed=1234, include_large=False, overwrite=True)
    second = generate_datasets(tmp_path / "b", seed=1234, include_large=False, overwrite=True)
    first_by_id = {item["fragment_id"]: item for item in first["fragments"]}
    second_by_id = {item["fragment_id"]: item for item in second["fragments"]}
    for fragment_id in first_by_id:
        assert first_by_id[fragment_id]["file_sha256"] == second_by_id[fragment_id]["file_sha256"]
        assert first_by_id[fragment_id]["value_fingerprint"] == second_by_id[fragment_id]["value_fingerprint"]


def test_manifest_catalog_has_ranges_for_every_fragment(dataset_manifest: dict) -> None:
    catalog = dataset_manifest["shape_and_range_catalog"]
    assert set(catalog) == {item["fragment_id"] for item in dataset_manifest["fragments"]}
    for entry in catalog.values():
        assert len(entry["shape"]) == 2
        assert entry["numeric_range"]["element_count"] > 0
        assert "nonfinite_first_positions" in entry


def test_generation_upgrades_small_only_manifest(tmp_path: Path) -> None:
    root = tmp_path / "upgrade"
    first = generate_datasets(root, seed=7, include_large=False, overwrite=True)
    assert "large_base" not in {item["fragment_id"] for item in first["fragments"]}
    second = generate_datasets(root, seed=7, include_large=True, overwrite=False)
    ids = {item["fragment_id"] for item in second["fragments"]}
    assert {"large_base", "large_anomaly"} <= ids
    assert (root / "fragments" / "large_base.npz").exists()
