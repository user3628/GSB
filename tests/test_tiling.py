from __future__ import annotations

import pytest

from blockgemm import query_plan, select_tiling


def test_plan_selection_is_deterministic() -> None:
    first = select_tiling(m=512, n=256, k=384)
    second = select_tiling(m=512, n=256, k=384)
    assert first.plan == second.plan
    assert first.plan.fingerprint() == second.plan.fingerprint()
    assert first.to_dict()["selected"] == second.to_dict()["selected"]


def test_selected_plan_fits_cache_budgets() -> None:
    selection = select_tiling(m=512, n=256, k=384, l1_cache_kib=64, l2_cache_kib=1024)
    plan = selection.plan
    assert plan.l1_working_set_bytes(8) <= 64 * 1024
    assert plan.l2_working_set_bytes(8) <= 1024 * 1024
    details = selection.selected.details
    assert details["l1_working_set_bytes"] <= 64 * 1024
    assert details["l2_working_set_bytes"] <= 1024 * 1024


def test_small_problem_does_not_choose_oversized_cache_plan() -> None:
    selection = select_tiling(m=64, n=64, k=64, l1_cache_kib=32, l2_cache_kib=64)
    plan = selection.plan
    assert plan.mc <= 64
    assert plan.nc <= 64
    assert plan.l2_working_set_bytes(8) <= 64 * 1024


def test_plan_query_contains_reason_and_score_breakdown() -> None:
    plan = query_plan(m=1024, n=1024, k=1024)
    assert plan["reason"]
    assert plan["top_candidates"]
    details = plan["selected"]["details"]
    for key in (
        "predicted_traffic_bytes",
        "normalized_traffic",
        "l2_working_set_bytes",
        "parallel_blocks",
        "microkernel_call_overhead",
    ):
        assert key in details
    assert details["parallel_blocks"] == 16


def test_tiny_budget_reports_stable_config_error() -> None:
    from blockgemm import BlockGemmError, E_INVALID_CONFIG

    with pytest.raises(BlockGemmError) as caught:
        select_tiling(m=64, n=64, k=64, l1_cache_kib=1, l2_cache_kib=1)
    assert caught.value.code == E_INVALID_CONFIG
    assert caught.value.reason == "no_cache_feasible_tiling"
