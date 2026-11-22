from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from blockgemm import (
    BlockGemmError,
    E_INVALID_SHAPE,
    E_NUMERIC_MISMATCH,
    compare_tile,
    gemm,
    generate_samples,
)
import blockgemm.engine as engine_module
from blockgemm.kernel import compute_block as original_compute_block
from blockgemm.manifest import load_manifest
from blockgemm.reference import power_of_two_sum_tile


def test_manifest_covers_required_sample_families(manifest_path) -> None:
    manifest = load_manifest(manifest_path)
    cases = {entry["id"]: entry for entry in manifest["cases"]}
    assert {"small_gemm", "medium_gemm", "large_gemm"}.issubset(cases)
    assert cases["small_gemm"]["M"] == 64 and cases["small_gemm"]["N"] == 64 and cases["small_gemm"]["K"] == 64
    assert cases["medium_gemm"]["M"] == 512 and cases["medium_gemm"]["N"] == 256 and cases["medium_gemm"]["K"] == 384
    assert cases["large_gemm"]["M"] == cases["large_gemm"]["N"] == cases["large_gemm"]["K"] == 1024
    assert cases["transpose_gemm"]["a_transpose"] is True
    assert cases["transpose_gemm"]["b_transpose"] is True
    assert any(entry["kind"] == "bmm" and entry["batch"] > 1 for entry in cases.values())
    assert any("nan" in entry["tags"] for entry in cases.values())
    assert any("inf" in entry["tags"] for entry in cases.values())
    shapes = json.loads((Path(manifest_path).parent / "shapes.json").read_text())
    assert len(shapes["cases"]) == len(cases)


def test_generator_is_byte_deterministic(tmp_path) -> None:
    one = tmp_path / "one"
    two = tmp_path / "two"
    generate_samples(one, force=True)
    generate_samples(two, force=True)
    for relative in ("manifest.json", "shapes.json", "matrices/small_gemm_a.npy", "matrices/nan_gemm_b.npy"):
        assert (one / relative).read_bytes() == (two / relative).read_bytes()


def test_invalid_shape_error_has_business_fields() -> None:
    rng = np.random.default_rng(1)
    a = rng.standard_normal((5, 7))
    b = rng.standard_normal((8, 9))
    with pytest.raises(BlockGemmError) as caught:
        gemm(a, b)
    error = caught.value
    assert error.code == E_INVALID_SHAPE
    assert error.shard_id == "gemm"
    assert error.block_coord == [0, 0]
    assert error.reason == "contract_dimension_mismatch"
    assert error.to_dict()["details"]["A_shape"] == [5, 7]


def test_numeric_mismatch_reports_ranges_and_errors() -> None:
    reference = np.ones((3, 4), dtype=np.float64)
    actual = reference.copy()
    actual[1, 2] = 1.5
    with pytest.raises(BlockGemmError) as caught:
        compare_tile(
            actual,
            reference,
            shard_id="gemm",
            block_coord=[2, 3],
            row_range=[16, 19],
            col_range=[24, 28],
            sequence=9,
            atol=1e-12,
            rtol=1e-12,
        )
    error = caught.value
    assert error.code == E_NUMERIC_MISMATCH
    assert error.shard_id == "gemm"
    assert error.block_coord == [2, 3]
    assert error.details["row_range"] == [16, 19]
    assert error.details["col_range"] == [24, 28]
    assert error.details["max_abs_error"] == 0.5
    assert error.details["max_rel_error"] == 0.5


def test_power_of_two_reference_matches_direct_long_double() -> None:
    rng = np.random.default_rng(8)
    a = rng.standard_normal((9, 21))
    b = rng.standard_normal((21, 7))
    reference = power_of_two_sum_tile(a, b, leaf_k=4)
    direct = np.asarray(a, dtype=np.longdouble) @ np.asarray(b, dtype=np.longdouble)
    np.testing.assert_allclose(np.asarray(reference, dtype=np.float64), np.asarray(direct, dtype=np.float64), rtol=1e-13, atol=1e-13)


def test_engine_numeric_mismatch_blocks_publication(tmp_path, monkeypatch) -> None:
    rng = np.random.default_rng(91)
    a = rng.standard_normal((40, 36))
    b = rng.standard_normal((36, 32))

    def corrupted_compute(problem, descriptor):
        product = original_compute_block(problem, descriptor)
        product = product.copy()
        product.flat[0] += 1.0
        return product

    monkeypatch.setattr(engine_module, "compute_block", corrupted_compute)
    workspace = tmp_path / "numeric"
    with pytest.raises(BlockGemmError) as caught:
        engine_module.run_gemm_job(workspace, a, b, verify_blocks=1)
    assert caught.value.code == E_NUMERIC_MISMATCH
    assert caught.value.shard_id == "gemm"
    assert caught.value.block_coord == [0, 0]
    assert caught.value.details["row_range"][0] == 0
    assert caught.value.details["col_range"][0] == 0
    assert caught.value.details["max_abs_error"] >= 1.0
    assert not (workspace / "checkpoints" / "0000000000.json").exists()
