from __future__ import annotations

import numpy as np

from chunkops.strategies import (
    ChunkPlan,
    SUM_STRATEGIES,
    build_auto_plan,
    build_requested_plan,
    softmax_plan_is_reasonable,
)


def test_plan_changes_with_scale_and_small_is_bounded() -> None:
    small = build_auto_plan(op="softmax", axis=1, rows=128, cols=512)
    medium = build_auto_plan(op="softmax", axis=1, rows=2048, cols=1024)
    large = build_auto_plan(op="softmax", axis=1, rows=8192, cols=4096)
    signatures = {
        (plan.row_block, plan.col_block, plan.sum_strategy)
        for plan in (small, medium, large)
    }
    assert len(signatures) >= 2
    assert small.row_block <= 32
    assert small.col_block <= 64
    assert softmax_plan_is_reasonable(small)
    assert large.row_block > small.row_block or large.col_block > small.col_block
    assert small.sum_strategy == "sequential"
    assert medium.sum_strategy == "pairwise"
    assert large.sum_strategy == "pairwise"


def test_anomaly_ratio_changes_strategy_to_compensated() -> None:
    normal = build_auto_plan(op="layernorm", axis=0, rows=512, cols=512, anomaly_ratio=0.0)
    anomalous = build_auto_plan(
        op="layernorm", axis=0, rows=512, cols=512, anomaly_ratio=0.001
    )
    assert anomalous.sum_strategy == "compensated"
    assert normal.sum_strategy != "compensated"


def test_plan_records_reasons_scores_and_rejections() -> None:
    plan = build_auto_plan(op="softmax", axis=1, rows=257, cols=263)
    assert plan.auto_selected is True
    assert plan.selection_reason
    assert "traffic_score" in plan.score_details
    assert "error_score" in plan.score_details
    assert plan.score_details["rejected"]
    assert all(item["reasons"] for item in plan.score_details["rejected"])
    assert plan.candidates
    assert all(
        {
            "row_block",
            "col_block",
            "sum_strategy",
            "score",
            "reasons",
        }
        <= set(item)
        for item in plan.candidates
    )
    assert set(item["sum_strategy"] for item in plan.candidates) <= set(SUM_STRATEGIES)
    assert plan.workspace_bytes > 0
    assert plan.estimated_traffic_bytes > 0
    assert plan.predicted_error_bound > 0


def test_rejected_candidates_include_scale_cap_reason() -> None:
    plan = build_auto_plan(op="softmax", axis=1, rows=128, cols=512)
    reasons = {
        reason
        for item in plan.candidates
        for reason in item["reasons"]
    }
    assert "row_block_exceeds_scale_cap" in reasons or "col_block_exceeds_scale_cap" in reasons


def test_user_plan_is_audited_not_auto_selected() -> None:
    plan = build_requested_plan(
        op="softmax",
        axis=1,
        rows=37,
        cols=53,
        row_block=9,
        col_block=11,
        sum_strategy="compensated",
    )
    assert plan.auto_selected is False
    assert plan.row_block == 9
    assert plan.col_block == 11
    assert plan.sum_strategy == "compensated"
    assert plan.score_details["candidate_count"] == 1


def test_plan_round_trip_and_cache_key_components() -> None:
    plan = build_auto_plan(op="softmax", axis=1, rows=128, cols=512)
    restored = ChunkPlan.from_dict(plan.to_dict())
    assert restored.row_block == plan.row_block
    assert restored.col_block == plan.col_block
    assert restored.sum_strategy == plan.sum_strategy
    kwargs = dict(
        shape_fp="shape-a",
        value_fp="value-a",
        input_dtype="float32",
        output_dtype="float32",
        eps=1e-5,
        gamma_fp=None,
        beta_fp=None,
        memory_budget_bytes=512 * 1024 * 1024,
        effective_row_block=plan.row_block,
        effective_col_block=plan.col_block,
    )
    key = plan.cache_key(**kwargs)
    assert len(key) == 64
    assert int(key, 16) >= 0
    changed_shape = plan.cache_key(**{**kwargs, "shape_fp": "shape-b"})
    changed_budget = plan.cache_key(**{**kwargs, "memory_budget_bytes": 1})
    assert key != changed_shape
    assert key != changed_budget
    other = build_requested_plan(
        op="softmax",
        axis=1,
        rows=128,
        cols=512,
        row_block=plan.row_block,
        col_block=plan.col_block,
        sum_strategy="compensated",
    )
    assert key != other.cache_key(**kwargs)


def test_non_power_of_two_plans_cover_all_rows_and_columns() -> None:
    for rows, cols in ((1, 1), (13, 17), (257, 263), (1000, 777)):
        plan = build_auto_plan(op="softmax", axis=1, rows=rows, cols=cols)
        assert 1 <= plan.row_block <= rows
        assert 1 <= plan.col_block <= cols
        assert np.arange(0, rows, plan.row_block)[-1] < rows
