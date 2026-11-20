from __future__ import annotations

import pytest

from gemm_lab import select_plan
from gemm_lab.blocking import BlockPlan, explicit_selection
from gemm_lab.errors import BusinessCode, GemmError


def test_small_plan_avoids_overblocking():
    selection = select_plan(64, 64, 64, dtype="float64")
    plan = selection.plan
    assert selection.workload_class == "small"
    assert plan.mc >= 64 and plan.nc >= 64 and plan.kc >= 64
    assert selection.metrics["microkernel_calls"] <= 16
    assert selection.reason.startswith("small workload")
    assert selection.normalized_components["microkernel_calls"] == 1.0


def test_large_plan_stays_in_l2_and_records_score_details():
    l2 = 1024 * 1024
    selection = select_plan(1024, 1024, 1024, l1_bytes=32 * 1024, l2_bytes=l2)
    assert selection.workload_class == "large"
    assert selection.metrics["cache_workspace_bytes"] <= l2
    assert selection.metrics["microkernel_calls"] < 100_000
    assert selection.candidates_considered > 100
    payload = selection.to_dict()
    assert payload["reason"]
    assert payload["weights"]["traffic"] > 0
    assert "normalized_components" in payload


def test_non_square_shape_plan():
    selection = select_plan(512, 256, 384)
    plan = selection.plan
    assert plan.mr <= plan.mc and plan.nr <= plan.nc
    assert plan.mc % plan.mr == 0
    assert plan.nc % plan.nr == 0


def test_explicit_plan_requires_internal_divisibility():
    with pytest.raises(GemmError) as error:
        explicit_selection(
            BlockPlan(mr=8, nr=16, mc=60, kc=32, nc=64),
            m=128,
            n=128,
            k=64,
            itemsize=8,
            l1_bytes=32 * 1024,
            l2_bytes=1024 * 1024,
        )
    assert error.value.code == BusinessCode.E_INVALID_SHAPE
    assert error.value.details["reason"] == "cache_register_divisibility"


def test_explicit_plan_size_limit():
    with pytest.raises(GemmError) as error:
        explicit_selection(
            BlockPlan(mr=64, nr=16, mc=128, kc=32, nc=64),
            m=128,
            n=128,
            k=64,
            itemsize=8,
            l1_bytes=32 * 1024,
            l2_bytes=1024 * 1024,
        )
    assert error.value.code == BusinessCode.E_INVALID_SHAPE
    assert error.value.details["reason"] == "block_size_limit"


def test_explicit_plan_allows_partial_edge_tiles():
    selection = explicit_selection(
        BlockPlan(mr=4, nr=8, mc=64, kc=32, nc=64),
        m=67,
        n=69,
        k=35,
        itemsize=8,
        l1_bytes=32 * 1024,
        l2_bytes=1024 * 1024,
    )
    assert selection.metrics["m_tiles"] == 2
    assert selection.metrics["n_tiles"] == 2
    assert selection.metrics["k_tiles"] == 2

