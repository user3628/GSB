from __future__ import annotations

import numpy as np

from blockgemm import bmm, gemm, run_gemm_job
from blockgemm.manifest import load_case


def test_gemm_and_transposes_match_numpy(tmp_path) -> None:
    rng = np.random.default_rng(42)
    a = rng.standard_normal((37, 23))
    b = rng.standard_normal((23, 29))
    expected = a @ b
    np.testing.assert_allclose(gemm(a, b), expected, rtol=1e-11, atol=1e-11)
    np.testing.assert_allclose(gemm(a.T, b.T, a_transpose=True, b_transpose=True), expected, rtol=1e-11, atol=1e-11)


def test_bmm_batch_tail_and_transpose_match_numpy(tmp_path) -> None:
    rng = np.random.default_rng(43)
    a = rng.standard_normal((3, 11, 35))
    b = rng.standard_normal((3, 35, 19))
    expected = np.matmul(a, b)
    workspace = tmp_path / "bmm"
    result = run_gemm_job(workspace, a, b, verify_blocks=3)
    assert result.output is not None
    np.testing.assert_allclose(result.output, expected, rtol=1e-11, atol=1e-11)
    shards = {block["shard_id"] for block in result.summary["blocks"]}
    assert shards == {"batch-0000", "batch-0001", "batch-0002"}
    assert any(block["tail_flags"]["m"] or block["tail_flags"]["n"] or block["tail_flags"]["k"] for block in result.summary["blocks"])

    a_raw = np.swapaxes(a, -1, -2)
    b_raw = np.swapaxes(b, -1, -2)
    np.testing.assert_allclose(
        bmm(a_raw, b_raw, a_transpose=True, b_transpose=True),
        expected,
        rtol=1e-11,
        atol=1e-11,
    )


def test_nonfinite_values_propagate_and_blocks_are_marked(manifest_path, tmp_path) -> None:
    loaded_nan = load_case(manifest_path, "nan_gemm")
    nan_result = run_gemm_job(
        tmp_path / "nan",
        loaded_nan.a,
        loaded_nan.b,
        expected=loaded_nan.expected,
        verify_blocks=3,
    )
    assert np.isnan(nan_result.output).any()
    assert nan_result.summary["nonfinite_blocks"]
    assert nan_result.summary["numeric_verification"]["reports"][0]["status"] == "skipped_nonfinite"

    loaded_inf = load_case(manifest_path, "inf_bmm_batch2")
    inf_result = run_gemm_job(
        tmp_path / "inf",
        loaded_inf.a,
        loaded_inf.b,
        expected=loaded_inf.expected,
        verify_blocks=3,
    )
    assert np.isinf(inf_result.output).any() or np.isnan(inf_result.output).any()
    affected = {block["shard_id"] for block in inf_result.summary["nonfinite_blocks"]}
    assert "batch-0001" in affected
    assert "batch-0000" not in affected


def test_two_runs_with_same_input_have_same_output_bytes(tmp_path) -> None:
    rng = np.random.default_rng(44)
    a = rng.standard_normal((48, 40))
    b = rng.standard_normal((40, 33))
    first = run_gemm_job(tmp_path / "one", a, b, verify_blocks=1)
    second = run_gemm_job(tmp_path / "two", a, b, verify_blocks=1)
    assert first.output_path.read_bytes() == second.output_path.read_bytes()
    assert first.summary_path.read_bytes() == second.summary_path.read_bytes()
