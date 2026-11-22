from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from density_trace.canonical import canonical_json_bytes
from density_trace.config import Config
from density_trace.demo import demo_config, generate_demo_data, write_deterministic_npz
from density_trace.errors import BusinessError
from density_trace.manifest import load_batch, load_manifest
from density_trace.numerics import evaluate_batch_density
from density_trace.reference import load_reference
from density_trace.state import DensityState, merge_batch_state, merge_many_batch_states

from .conftest import make_engine


def test_kde_deterministic_compensated_and_chunk_tolerant(data_copy: Path) -> None:
    manifest = load_manifest(data_copy / "manifest.json")
    config = Config.from_dict(demo_config())
    batch = load_batch(manifest, 1)
    first = evaluate_batch_density(batch, config)
    second = evaluate_batch_density(batch, config)
    assert (
        first.density_units.astype("<i8").tobytes()
        == second.density_units.astype("<i8").tobytes()
    )
    assert canonical_json_bytes(first.error_report) == canonical_json_bytes(
        second.error_report
    )
    assert [block["block_index"] for block in first.blocks] == list(
        range(len(first.blocks))
    )
    assert all(
        len(block["partial_sum_units"]) == config.grid.nx * config.grid.ny
        for block in first.blocks
    )
    assert first.error_report["block_order"][0][0] == 0
    assert first.error_report["block_order"][-1][1] == batch.effective_samples.shape[0]
    assert (
        first.error_report["observed_error_density"]
        <= first.error_report["error_bound_density"]
    )
    assert first.error_report["significant_digits"] >= 8

    curves = []
    for chunk_size in (1, 7, 64, 1000):
        chunk_config = replace(
            config, density=replace(config.density, chunk_size=chunk_size)
        )
        density = evaluate_batch_density(batch, chunk_config)
        curves.append(density.density_curve(chunk_config.density.density_scale))
    for candidate in curves[1:]:
        assert (
            float(np.max(np.abs(curves[0] - candidate)))
            <= config.density.curve_abs_tolerance
        )


def test_empty_batch_errors(tmp_path: Path) -> None:
    npz_path = tmp_path / "empty.npz"
    write_deterministic_npz(
        npz_path,
        {
            "features": np.zeros((0, 2), dtype="<f8"),
            "source_batch": np.asarray([], dtype="<U8"),
        },
    )
    manifest = {
        "schema_version": 1,
        "batches": [
            {
                "batch_id": 0,
                "file": npz_path.name,
                "rows": 0,
                "cols": 2,
                "valid_columns": [0, 1],
            }
        ],
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    loaded_manifest = load_manifest(tmp_path / "manifest.json")
    with pytest.raises(BusinessError) as empty:
        load_batch(loaded_manifest, 0)
    assert empty.value.code == "E_EMPTY_BATCH"
    assert empty.value.details["batch_id"] == 0

    valid_columns_manifest = json.loads(json.dumps(manifest))
    valid_columns_manifest["batches"][0]["valid_columns"] = []
    (tmp_path / "manifest-invalid-columns.json").write_text(
        json.dumps(valid_columns_manifest), encoding="utf-8"
    )
    with pytest.raises(BusinessError) as no_columns:
        load_batch(load_manifest(tmp_path / "manifest-invalid-columns.json"), 0)
    assert no_columns.value.code == "E_EMPTY_BATCH"


def test_incremental_state_equals_one_shot_and_intermediate_export(
    data_copy: Path, tmp_path: Path
) -> None:
    config = Config.from_dict(demo_config())
    manifest = load_manifest(data_copy / "manifest.json")
    reference = load_reference(data_copy / "reference_quantiles.json")
    config_fingerprint = config.config_fingerprint(reference.digest)
    densities = [
        evaluate_batch_density(load_batch(manifest, batch_id), config)
        for batch_id in range(4)
    ]
    one_shot = merge_many_batch_states(densities, config, config_fingerprint)
    incremental = DensityState.empty(config)
    for density in densities:
        incremental = merge_batch_state(
            incremental,
            density,
            config,
            density.batch_id,
            config_fingerprint,
        )
    assert (
        one_shot.density_units.astype("<i8").tobytes()
        == incremental.density_units.astype("<i8").tobytes()
    )
    assert (
        one_shot.curve().astype("<f8").tobytes()
        == incremental.curve().astype("<f8").tobytes()
    )
    assert one_shot.summary(config_fingerprint) == incremental.summary(
        config_fingerprint
    )

    engine = make_engine(tmp_path / "workspace", data_copy)
    engine.resume()
    for batch_id in range(4):
        exported = engine.get_state(batch_id)
        expected = merge_many_batch_states(
            densities[: batch_id + 1], config, config_fingerprint
        )
        assert exported["curve_sha256"] == expected.curve_sha256()
        assert (
            exported["state"]["density_units"]
            == expected.density_units.astype(int).tolist()
        )


def test_numeric_errors_have_batch_grid_interval_and_anomaly(data_copy: Path) -> None:
    config = Config.from_dict(demo_config())
    manifest = load_manifest(data_copy / "manifest.json")
    batch = load_batch(manifest, 0)
    with pytest.raises(BusinessError) as zero_scale:
        evaluate_batch_density(
            batch, replace(config, density=replace(config.density, density_scale=0))
        )
    assert zero_scale.value.code == "E_NUMERIC"
    assert zero_scale.value.details["batch_id"] == 0
    assert zero_scale.value.details["grid_interval"] == [
        0,
        config.grid.nx * config.grid.ny - 1,
    ]
    assert "density_scale=0" in zero_scale.value.details["anomaly"]

    bad_samples = batch.effective_samples.copy()
    bad_samples[3, 0] = np.nan
    bad_batch = replace(batch, effective_samples=bad_samples)
    with pytest.raises(BusinessError) as nan_error:
        evaluate_batch_density(bad_batch, config)
    assert nan_error.value.code == "E_NUMERIC"
    assert nan_error.value.details["batch_id"] == 0
    assert (
        "NaN" in nan_error.value.details["anomaly"]
        or "nan" in nan_error.value.details["anomaly"].lower()
    )

    inf_data = Path(data_copy / "inf-data")
    generate_demo_data(inf_data, overwrite=True)
    features = np.zeros((4, 2), dtype="<f8")
    features[2, 1] = np.inf
    write_deterministic_npz(
        inf_data / "batches" / "batch-000.npz",
        {"features": features, "source_batch": np.asarray(["inf"] * 4, dtype="<U8")},
    )
    manifest_raw = json.loads((inf_data / "manifest.json").read_text(encoding="utf-8"))
    manifest_raw["batches"][0]["rows"] = 4
    (inf_data / "manifest.json").write_text(json.dumps(manifest_raw), encoding="utf-8")
    with pytest.raises(BusinessError) as inf_error:
        load_batch(load_manifest(inf_data / "manifest.json"), 0)
    assert inf_error.value.code == "E_NUMERIC"
    assert inf_error.value.details["batch_id"] == 0
    assert "Inf" in inf_error.value.details["anomaly"]


def test_state_summary_and_version_are_traceable(
    data_copy: Path, tmp_path: Path
) -> None:
    engine = make_engine(tmp_path / "workspace", data_copy)
    engine.resume()
    for batch_id in range(4):
        record = engine._record_for_batch(batch_id)
        summary = record["state_summary"]
        assert summary["state_version"] == batch_id + 1
        assert summary["covered_batches"] == batch_id + 1
        assert record["adjudication"]["state_version"] == summary["state_version"]
        assert (
            record["adjudication"]["state_version_fingerprint"]
            == summary["version_fingerprint"]
        )
        assert record["version_fingerprint"] == summary["version_fingerprint"]
