from __future__ import annotations

import numpy as np
import pytest

from chunkops.errors import ChunkOpsError, E_MEMORY_BUDGET
from chunkops.ops import (
    build_schedule,
    execute_blocked_op,
    reference_layernorm,
    reference_softmax,
    reduce_sum,
    scan_nonfinite,
)
from chunkops.strategies import build_auto_plan, build_requested_plan
from chunkops.validation import validate_output


@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.parametrize("strategy", ["sequential", "pairwise", "compensated"])
def test_softmax_all_axes_and_strategies(axis: int, strategy: str) -> None:
    rng = np.random.default_rng(11)
    x = (rng.standard_normal((37, 53)) * 4.0).astype(np.float32)
    plan = build_requested_plan(
        op="softmax",
        axis=axis,
        rows=37,
        cols=53,
        row_block=9,
        col_block=11,
        sum_strategy=strategy,
    )
    first = execute_blocked_op(x, plan, memory_budget_bytes=32 * 1024 * 1024)
    second = execute_blocked_op(x, plan, memory_budget_bytes=32 * 1024 * 1024)
    assert first.output.shape == x.shape
    assert first.output.dtype == x.dtype
    assert first.output.tobytes() == second.output.tobytes()
    np.testing.assert_allclose(
        first.output.astype(np.float64),
        reference_softmax(x, axis),
        rtol=2e-6,
        atol=2e-6,
    )
    np.testing.assert_allclose(first.output.sum(axis=axis), 1.0, atol=2e-6)


@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.parametrize("strategy", ["sequential", "pairwise", "compensated"])
def test_layernorm_all_axes_and_strategies(axis: int, strategy: str) -> None:
    rng = np.random.default_rng(19)
    x = (rng.standard_normal((37, 53)) * 2.0).astype(np.float32)
    n = x.shape[axis]
    gamma = (1.0 + 0.1 * rng.standard_normal(n)).astype(np.float32)
    beta = (0.05 * rng.standard_normal(n)).astype(np.float32)
    plan = build_requested_plan(
        op="layernorm",
        axis=axis,
        rows=37,
        cols=53,
        row_block=7,
        col_block=13,
        sum_strategy=strategy,
    )
    result = execute_blocked_op(
        x,
        plan,
        gamma=gamma,
        beta=beta,
        eps=1e-5,
        memory_budget_bytes=32 * 1024 * 1024,
    )
    reference = reference_layernorm(x, axis, gamma=gamma, beta=beta, eps=1e-5)
    np.testing.assert_allclose(result.output, reference, rtol=3e-5, atol=3e-5)
    assert result.output.shape == x.shape


def test_extreme_values_are_stable() -> None:
    x = np.zeros((4, 8), dtype=np.float32)
    x[0, :4] = 1e4
    x[0, 4:] = -1e4
    plan = build_auto_plan(op="softmax", axis=1, rows=4, cols=8)
    result = execute_blocked_op(x, plan)
    assert np.all(np.isfinite(result.output))
    np.testing.assert_allclose(result.output.sum(axis=1), 1.0, atol=1e-6)
    np.testing.assert_allclose(result.output[0, :4], 0.25, atol=1e-7)
    np.testing.assert_allclose(result.output[0, 4:], 0.0, atol=1e-7)


def test_all_zero_rows_softmax_uniform() -> None:
    x = np.zeros((5, 13), dtype=np.float32)
    plan = build_auto_plan(op="softmax", axis=1, rows=5, cols=13)
    result = execute_blocked_op(x, plan)
    np.testing.assert_allclose(result.output, np.full_like(x, 1.0 / 13.0), atol=1e-7)
    assert result.zero_rows == [0, 1, 2, 3, 4]


def test_nan_inf_scan_and_ieee_propagation() -> None:
    x = np.arange(24, dtype=np.float32).reshape(4, 6)
    x[1, 2] = np.nan
    x[2, 4] = np.inf
    x[3, 0] = -np.inf
    scan = scan_nonfinite(x, fragment_id="frag")
    assert scan["first_nan"]["position"] == [1, 2]
    assert scan["first_pos_inf"]["position"] == [2, 4]
    assert scan["first_neg_inf"]["position"] == [3, 0]
    plan = build_auto_plan(op="softmax", axis=1, rows=4, cols=6)
    result = execute_blocked_op(x, plan, fragment_id="frag")
    assert np.isnan(result.output[1]).all()
    assert np.isnan(result.output[2]).all()
    assert np.isfinite(result.output[3]).all()
    assert result.output[3, 0] == 0.0
    assert np.isnan(result.output[0]).sum() == 0


def test_reduce_strategies_have_explicit_different_order() -> None:
    values = np.array([[1e16, 1.0, -1e16]], dtype=np.float64)
    sequential = reduce_sum(values, 1, "sequential")[0]
    pairwise = reduce_sum(values, 1, "pairwise")[0]
    compensated = reduce_sum(values, 1, "compensated")[0]
    assert sequential == 0.0
    assert pairwise == 0.0
    assert compensated == pytest.approx(1.0)


def test_validation_report_has_block_ranges_and_estimates() -> None:
    rng = np.random.default_rng(29)
    x = rng.standard_normal((65, 47), dtype=np.float32)
    plan = build_auto_plan(op="softmax", axis=1, rows=65, cols=47)
    result = execute_blocked_op(x, plan)
    report = validate_output(x, result.output, plan, raise_on_mismatch=False)
    assert report["status"] == "ok"
    assert report["sample_blocks"]
    for block in report["sample_blocks"]:
        assert len(block["row_range"]) == 2
        assert len(block["col_range"]) == 2
        assert "max_abs" in block
        assert "max_rel" in block
    assert len(report["error_estimates"]) >= 1
    estimate = report["error_estimates"][0]
    assert estimate["reduction_order"]
    assert estimate["sample_positions"]
    assert estimate["sum_strategy"] == plan.sum_strategy
    assert "estimated_error_bound" in estimate


def test_numeric_mismatch_business_code() -> None:
    rng = np.random.default_rng(31)
    x = rng.standard_normal((16, 19), dtype=np.float32)
    plan = build_auto_plan(op="softmax", axis=1, rows=16, cols=19)
    result = execute_blocked_op(x, plan)
    corrupted = result.output.copy()
    corrupted[0, 0] = np.float32(0.999)
    with pytest.raises(ChunkOpsError) as caught:
        validate_output(x, corrupted, plan, atol=1e-7, rtol=1e-7)
    assert caught.value.code == "E_NUMERIC_MISMATCH"
    failed = caught.value.details["failed_blocks"]
    assert failed
    assert "row_range" in failed[0]
    assert "col_range" in failed[0]
    assert "max_abs" in failed[0]
    assert "max_rel" in failed[0]


def test_memory_split_and_budget_error() -> None:
    plan = build_auto_plan(op="softmax", axis=1, rows=512, cols=512)
    generous = build_schedule(
        plan,
        input_itemsize=4,
        output_itemsize=4,
        memory_budget_bytes=64 * 1024 * 1024,
    )
    split = build_schedule(
        plan,
        input_itemsize=4,
        output_itemsize=4,
        memory_budget_bytes=generous.base_bytes + generous.workspace_bytes - 1,
    )
    assert split.split_decisions
    assert split.effective_row_block <= plan.row_block
    assert split.peak_bytes <= split.budget_bytes
    assert split.steps
    assert all(step.estimated_peak_bytes <= split.budget_bytes for step in split.steps)
    with pytest.raises(ChunkOpsError) as caught:
        build_schedule(
            plan,
            input_itemsize=4,
            output_itemsize=4,
            memory_budget_bytes=generous.base_bytes - 1,
        )
    assert caught.value.code == E_MEMORY_BUDGET
    assert caught.value.details["input_bytes"] > 0
    assert caught.value.details["output_bytes"] > 0


def test_gamma_beta_shape_validation() -> None:
    x = np.zeros((8, 9), dtype=np.float32)
    plan = build_auto_plan(op="layernorm", axis=1, rows=8, cols=9)
    with pytest.raises(ChunkOpsError) as caught:
        execute_blocked_op(x, plan, gamma=np.ones(8, dtype=np.float32))
    assert caught.value.code == "E_INVALID_SHAPE"


def test_top_level_convenience_api() -> None:
    from chunkops import layernorm, softmax

    rng = np.random.default_rng(41)
    x = rng.standard_normal((21, 23), dtype=np.float32)
    softmax_output = softmax(x, axis=1)
    layernorm_output = layernorm(x, axis=0)
    assert softmax_output.shape == x.shape
    assert layernorm_output.shape == x.shape
    np.testing.assert_allclose(softmax_output.sum(axis=1), 1.0, atol=2e-6)
    np.testing.assert_allclose(
        layernorm_output.astype(np.float64),
        reference_layernorm(x, 0),
        rtol=2e-6,
        atol=2e-6,
    )


def test_every_sampled_block_links_to_covering_error_bound() -> None:
    rng = np.random.default_rng(43)
    x = rng.standard_normal((64, 37), dtype=np.float32)
    plan = build_auto_plan(op="layernorm", axis=0, rows=64, cols=37)
    result = execute_blocked_op(x, plan)
    report = validate_output(x, result.output, plan, raise_on_mismatch=False)
    for block in report["sample_blocks"]:
        assert block["error_estimate_block_indices"]
        assert block["estimated_error_bound"] is not None
        assert block["bound_covers_observed"] is True


def test_plan_shape_mismatch_business_code() -> None:
    x = np.zeros((8, 9), dtype=np.float32)
    plan = build_auto_plan(op="softmax", axis=1, rows=8, cols=10)
    with pytest.raises(ChunkOpsError) as caught:
        execute_blocked_op(x, plan)
    assert caught.value.code == "E_INVALID_SHAPE"
    assert caught.value.details["plan_shape"] == [8, 10]
    assert caught.value.details["input_shape"] == [8, 9]


def test_validation_handles_mixed_infinities_in_reference() -> None:
    x = np.ones((6, 7), dtype=np.float32)
    x[1, 2] = np.inf
    x[4, 2] = -np.inf
    plan = build_auto_plan(op="layernorm", axis=0, rows=6, cols=7)
    result = execute_blocked_op(x, plan)
    assert np.isnan(result.output[:, 2]).all()
    report = validate_output(x, result.output, plan, raise_on_mismatch=False)
    assert report["status"] == "ok"
    assert report["input_nonfinite"]["pos_inf_count"] == 1
    assert report["input_nonfinite"]["neg_inf_count"] == 1


def test_error_estimates_follow_effective_split_blocks() -> None:
    rng = np.random.default_rng(47)
    x = rng.standard_normal((96, 128), dtype=np.float32)
    plan = build_auto_plan(op="softmax", axis=1, rows=96, cols=128)
    generous = build_schedule(
        plan,
        input_itemsize=x.dtype.itemsize,
        output_itemsize=x.dtype.itemsize,
        memory_budget_bytes=64 * 1024 * 1024,
    )
    split = execute_blocked_op(
        x,
        plan,
        memory_budget_bytes=generous.base_bytes + generous.workspace_bytes // 2,
    )
    assert split.schedule.effective_row_block <= plan.row_block
    report = validate_output(
        x,
        split.output,
        plan,
        raise_on_mismatch=False,
        effective_row_block=split.schedule.effective_row_block,
        effective_col_block=split.schedule.effective_col_block,
    )
    first = report["error_estimates"][0]
    assert first["reduction_order"][0][1] <= split.schedule.effective_col_block
    assert report["effective_row_block"] == split.schedule.effective_row_block
    assert report["effective_col_block"] == split.schedule.effective_col_block
