from __future__ import annotations

import numpy as np
import pytest

from gemm_lab import bmm, gemm
from gemm_lab.errors import BusinessCode, GemmError


def test_k_mismatch_is_invalid_shape():
    rng = np.random.default_rng(1)
    with pytest.raises(GemmError) as error:
        gemm(rng.standard_normal((8, 9)), rng.standard_normal((8, 9)))
    assert error.value.code == BusinessCode.E_INVALID_SHAPE


def test_batch_mismatch_is_distinct_code():
    rng = np.random.default_rng(2)
    with pytest.raises(GemmError) as error:
        bmm(rng.standard_normal((2, 8, 9)), rng.standard_normal((3, 9, 7)))
    assert error.value.code == BusinessCode.E_BATCH_MISMATCH


def test_gemm_rejects_batched_operands():
    rng = np.random.default_rng(22)
    with pytest.raises(GemmError) as error:
        gemm(rng.standard_normal((2, 8, 9)), rng.standard_normal((2, 9, 7)))
    assert error.value.code == BusinessCode.E_BATCH_MISMATCH


def test_transpose_flag_shape_conflict():
    rng = np.random.default_rng(3)
    with pytest.raises(GemmError) as error:
        gemm(rng.standard_normal((8, 9)), rng.standard_normal((7, 9)), trans_a="T")
    assert error.value.code == BusinessCode.E_INVALID_SHAPE


def test_numeric_mismatch_reports_block_intervals_and_raises():
    rng = np.random.default_rng(4)
    a = rng.standard_normal((64, 64))
    b = rng.standard_normal((64, 64))
    with pytest.raises(GemmError) as error:
        gemm(a, b, sum_strategy="sequential", verify=True, atol=0.0, rtol=0.0)
    assert error.value.code == BusinessCode.E_NUMERIC_MISMATCH
    report = error.value.details["report"]
    assert report["passed"] is False
    block = report["matrices"][0]["block_comparisons"][0]
    assert len(block["row_interval"]) == 2
    assert len(block["col_interval"]) == 2
    assert "max_deviation" in block


def test_bmm_slicing_is_exact_and_budget_failure_has_breakdown():
    rng = np.random.default_rng(5)
    a = rng.standard_normal((6, 19, 23))
    b = rng.standard_normal((6, 23, 17))
    full = bmm(a, b, sum_strategy="compensated")
    base = (
        full.memory.output_bytes
        + full.memory.cache_workspace_bytes
        + full.memory.register_workspace_bytes
        + full.memory.fixed_overhead_bytes
    )
    tight = base + 2 * full.memory.per_batch_operand_bytes
    sliced = bmm(a, b, sum_strategy="compensated", memory_budget_bytes=tight)
    assert sliced.memory.sliced is True
    assert sliced.memory.slice_size == 2
    assert np.array_equal(full.output, sliced.output)
    for shard in sliced.memory.shards:
        assert shard.estimated_peak_bytes <= tight
        assert shard.budget_remaining_bytes >= 0
    with pytest.raises(GemmError) as error:
        bmm(a, b, memory_budget_bytes=512)
    assert error.value.code == BusinessCode.E_MEMORY_BUDGET
    assert error.value.details["required_minimum_bytes"] > 512
    assert "breakdown" in error.value.details


def test_api_cache_hit_and_version_invalidation(tmp_path):
    from gemm_lab import OperatorWorkspace

    rng = np.random.default_rng(8)
    a = rng.standard_normal((32, 24))
    b = rng.standard_normal((24, 28))
    workspace = OperatorWorkspace(tmp_path / "workspace")
    first = gemm(
        a,
        b,
        workspace=workspace,
        use_cache=True,
        publish_config=True,
        sum_strategy="pairwise",
    )
    assert first.cache["lookup_status"] == "miss"
    hit = gemm(
        a,
        b,
        workspace=workspace,
        use_cache=True,
        publish_config=False,
        sum_strategy="pairwise",
    )
    assert hit.cache["status"] == "hit"
    assert hit.execution["source"] == "cache"
    assert np.array_equal(first.output, hit.output)

    workspace.config_store.publish(
        fingerprint=first.shape.fingerprint,
        plan=first.plan_selection.plan,
        sum_strategy="pairwise",
        cache_budget={"l1_bytes": 32 * 1024, "l2_bytes": 1024 * 1024},
        selection_source="test",
    )
    rebuilt = gemm(
        a,
        b,
        workspace=workspace,
        use_cache=True,
        publish_config=False,
        sum_strategy="pairwise",
    )
    assert rebuilt.cache["lookup_status"] == "invalidated"
    assert rebuilt.cache["lookup_reason"] == "config_version_mismatch"
    assert rebuilt.execution["source"] == "computed"
    assert np.array_equal(first.output, rebuilt.output)

