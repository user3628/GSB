from __future__ import annotations

import pytest

from rope_diag.config import RopeConfig
from rope_diag.data import load_manifest
from rope_diag.errors import RopeError
from rope_diag.memory import plan_rotation, plan_scoring


def _long_shard(assets: dict):
    shards, _ = load_manifest(assets["manifest"])
    return next(shard for shard in shards if shard.shard_id == "long")


def test_scoring_plan_splits_rows_under_tight_budget(assets: dict) -> None:
    shard = _long_shard(assets)
    baseline = plan_scoring(shard, RopeConfig())
    budget = baseline.fixed_bytes + baseline.detail["per_row_workspace_bytes"] * 4 + 1
    decision = plan_scoring(shard, RopeConfig(memory_budget_bytes=budget))
    assert decision.mode == "row_blocks"
    assert decision.row_chunk_size == 4
    assert decision.estimated_peak_bytes <= budget


def test_rotation_plan_splits_batches_under_tight_budget(assets: dict) -> None:
    shard = _long_shard(assets)
    baseline = plan_rotation(shard, RopeConfig())
    budget = baseline.fixed_bytes + baseline.detail["per_batch_workspace_bytes"] + 1
    decision = plan_rotation(shard, RopeConfig(memory_budget_bytes=budget))
    assert decision.mode == "batch_chunks"
    assert decision.batch_chunk_size == 1
    assert decision.chunk_count == shard.batch


def test_impossible_memory_budget_returns_budget_details(assets: dict) -> None:
    shard = _long_shard(assets)
    with pytest.raises(RopeError) as caught:
        plan_scoring(shard, RopeConfig(memory_budget_bytes=1))
    assert caught.value.code == "E_MEMORY_BUDGET"
    assert caught.value.details["shard_id"] == "long"
    assert caught.value.details["required_bytes"] > 1
    assert "fixed_output_bytes" in caught.value.details
