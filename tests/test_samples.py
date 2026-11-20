from __future__ import annotations

from pathlib import Path

import numpy as np

from spmm_ops.samples import generate_samples, load_manifest


def test_manifest_is_reproducible(tmp_path: Path, manifest: dict, sample_dir: Path) -> None:
    second = tmp_path / "again"
    regenerated = generate_samples(second)
    assert regenerated["manifest_digest"] == manifest["manifest_digest"]
    assert regenerated["samples"] == manifest["samples"]
    assert load_manifest(second / "manifest.json")["manifest_digest"] == manifest["manifest_digest"]
    assert (sample_dir / "square_10pct.npz").exists()


def test_required_sample_matrix_shapes_and_structure(manifest: dict) -> None:
    samples = {sample["name"]: sample for sample in manifest["samples"]}
    expected = {
        "square_1pct": ([96, 96], 0.01),
        "square_10pct": ([96, 96], 0.10),
        "square_40pct": ([96, 96], 0.40),
        "tall_1pct": ([160, 64], 0.01),
        "tall_10pct": ([160, 64], 0.10),
        "tall_40pct": ([160, 64], 0.40),
        "wide_1pct": ([64, 160], 0.01),
        "wide_10pct": ([64, 160], 0.10),
        "wide_40pct": ([64, 160], 0.40),
    }
    assert set(expected).issubset(samples)
    for name, (shape, density) in expected.items():
        sample = samples[name]
        assert sample["shape"] == shape
        assert abs(sample["actual_density"] - density) < 0.002
        assert sample["dense_shape"][0] == shape[1]

    degenerate = samples["degenerate_struct"]
    assert degenerate["empty_rows"] >= 5
    assert degenerate["duplicate_count"] >= 3
    assert degenerate["max_duplicate_extra_per_index"] <= 1
    assert degenerate["explicit_zero_count"] >= 10

    anomaly = samples["anomaly_nan_inf"]
    assert anomaly["has_nonfinite_sparse_data"]
    assert anomaly["has_nonfinite_dense_data"]


def test_high_precision_reference_files_are_explicit_and_complete(sample_dir: Path, manifest: dict) -> None:
    for sample in manifest["samples"]:
        with np.load(sample_dir / sample["reference_file"], allow_pickle=False) as archive:
            values = archive["values"]
            assert values.shape == (sample["shape"][0], sample["dense_shape"][1])
            assert int(archive["precision"]) == 80
            assert values.dtype.kind == "U"
        if sample["name"] == "anomaly_nan_inf":
            flattened = {str(value) for value in values.reshape(-1)}
            assert "NAN" in flattened
            assert "INF" in flattened

