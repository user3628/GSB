from __future__ import annotations

import numpy as np
import pytest

from gemm_lab import bmm, gemm


@pytest.mark.parametrize(
    ("m", "n", "k", "trans_a", "trans_b"),
    [
        (32, 24, 40, "N", "N"),
        (31, 23, 37, "T", "N"),
        (29, 27, 33, "N", "T"),
        (25, 21, 35, "T", "T"),
    ],
)
@pytest.mark.parametrize("strategy", ["sequential", "pairwise", "compensated"])
def test_gemm_transpose_and_nonsquare(m, n, k, trans_a, trans_b, strategy):
    rng = np.random.default_rng(1234 + m + n + k)
    a_orig = rng.standard_normal((k, m) if trans_a == "T" else (m, k))
    b_orig = rng.standard_normal((n, k) if trans_b == "T" else (k, n))
    a_logical = a_orig.T if trans_a == "T" else a_orig
    b_logical = b_orig.T if trans_b == "T" else b_orig
    result = gemm(a_orig, b_orig, trans_a=trans_a, trans_b=trans_b, sum_strategy=strategy)
    expected = a_logical @ b_logical
    assert result.output.shape == (m, n)
    np.testing.assert_allclose(result.output, expected, rtol=2e-12, atol=2e-12)
    assert result.error_report["passed"] is True
    assert result.error_report["comparison"].startswith("per-element absolute")


def test_bmm_batches_equal_standalone_gemm_and_are_isolated():
    rng = np.random.default_rng(99)
    a = rng.standard_normal((5, 17, 23))
    b = rng.standard_normal((5, 23, 19))
    batched = bmm(a, b, sum_strategy="compensated")
    for index in range(5):
        single = gemm(a[index], b[index], sum_strategy="compensated")
        assert np.array_equal(batched.output[index], single.output)
    assert not np.shares_memory(batched.output[0], batched.output[1])
    assert batched.kernel.batches_executed == 5


def test_bmm_transposed_batches_equal_logical_reference():
    rng = np.random.default_rng(101)
    a_orig = rng.standard_normal((4, 21, 13))
    b_orig = rng.standard_normal((4, 17, 21))
    result = bmm(a_orig, b_orig, trans_a="T", trans_b="T", sum_strategy="pairwise")
    for index in range(4):
        expected = a_orig[index].T @ b_orig[index].T
        np.testing.assert_allclose(result.output[index], expected, rtol=2e-12, atol=2e-12)


def test_nan_and_inf_propagate_with_position_statistics():
    rng = np.random.default_rng(7)
    a = rng.standard_normal((24, 20))
    b = rng.standard_normal((20, 18))
    a[0, 0] = np.nan
    a[3, 4] = np.inf
    b[2, 5] = -np.inf
    result = gemm(a, b, verify=True)
    assert result.nonfinite["A"]["nan_count"] == 1
    assert result.nonfinite["A"]["pos_inf_count"] == 1
    assert result.nonfinite["B"]["neg_inf_count"] == 1
    output = result.nonfinite["output"]
    assert output["total_nonfinite"] > 0
    assert output["positions_sample"]
    assert set(output["positions_sample"][0]) == {"row", "col", "kind"}


def test_sum_strategies_are_distinct_but_within_bounds():
    rng = np.random.default_rng(55)
    a = rng.standard_normal((64, 96))
    b = rng.standard_normal((96, 64))
    results = {
        strategy: gemm(a, b, sum_strategy=strategy)
        for strategy in ("sequential", "pairwise", "compensated")
    }
    assert not np.array_equal(results["sequential"].output, results["pairwise"].output)
    for result in results.values():
        assert result.kernel.accumulation_mode == "scalar"
        assert result.error_report["passed"] is True
        assert result.error_report["max_abs_error_vs_high_precision"] <= result.error_report[
            "theoretical_abs_bound_max"
        ] * (1.0 + 1e-12)


def test_tiny_matrix_verification_row_sampling_terminates():
    rng = np.random.default_rng(17)
    a = rng.standard_normal((2, 3))
    b = rng.standard_normal((3, 2))
    result = gemm(a, b, sum_strategy="pairwise")
    assert result.output.shape == (2, 2)
    assert result.error_report["passed"] is True

