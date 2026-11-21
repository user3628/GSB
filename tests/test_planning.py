from __future__ import annotations

import pytest

from chunk_engine.errors import BusinessError, E_KEY_SET_TOO_LARGE, E_MEMORY_BUDGET_EXCEEDED
from chunk_engine.planning import plan_query
from chunk_engine.snapshot import load_snapshot


def _plan(snapshot, keys, columns, **overrides):
    settings = {
        "max_keys": 1000,
        "total_memory_budget_bytes": 2 * 1024 * 1024,
        "remaining_memory_budget_bytes": 2 * 1024 * 1024,
        "single_task_budget_bytes": 64 * 1024,
        "max_parallel_blocks": 4,
        "execution_mode": "auto",
    }
    settings.update(overrides)
    return plan_query(snapshot, keys, columns, **settings)


def test_empty_key_set_is_not_an_error(sample_suite: dict) -> None:
    snapshot = load_snapshot(sample_suite["v1_small"]["root"])
    plan = _plan(snapshot, [], ["id", "score"])
    assert plan.keys == ()
    assert plan.hit_blocks == ()
    assert plan.fetch_tasks == ()
    assert plan.peak_memory_estimate_bytes == 0


def test_key_set_limit_returns_business_error(sample_suite: dict) -> None:
    snapshot = load_snapshot(sample_suite["v1_small"]["root"])
    with pytest.raises(BusinessError) as raised:
        _plan(snapshot, [1, 2, 3], ["id"], max_keys=2)
    assert raised.value.code == E_KEY_SET_TOO_LARGE
    assert raised.value.context == {"key_count": 3, "max_keys": 2}


def test_adjacent_hit_blocks_merge_and_disjoint_ranges(sample_suite: dict) -> None:
    snapshot = load_snapshot(sample_suite["v1_small"]["root"])
    adjacent = _plan(snapshot, [1, 600, 1100], ["id"])
    assert len(adjacent.fetch_tasks) == 1
    assert adjacent.fetch_tasks[0].block_nos == (0, 1, 2)
    assert adjacent.fetch_tasks[0].key_start == 1
    assert adjacent.fetch_tasks[0].key_end == 1100
    separated = _plan(snapshot, [1, 1200], ["id"])
    assert len(separated.fetch_tasks) == 2
    first, second = separated.fetch_tasks
    assert first.block_nos == (0,)
    assert second.block_nos == (2,)
    assert first.key_end < second.key_start
    for left, right in zip(separated.fetch_tasks, separated.fetch_tasks[1:]):
        assert left.block_end < right.block_start
        assert left.key_end < right.key_start


def test_large_blocks_are_split_before_decode(sample_suite: dict) -> None:
    snapshot = load_snapshot(sample_suite["v2_large"]["root"])
    plan = _plan(
        snapshot,
        [1],
        ["id"],
        total_memory_budget_bytes=1024 * 1024,
        remaining_memory_budget_bytes=1024 * 1024,
        single_task_budget_bytes=4096,
    )
    assert plan.split_records
    record = plan.split_records[0]
    assert record.block_no == 0
    assert record.column == "id"
    assert record.chunk_rows < record.row_count
    assert record.subblock_count > 1
    assert plan.peak_memory_estimate_bytes <= 1024 * 1024


def test_memory_exceeded_before_decode(sample_suite: dict) -> None:
    snapshot = load_snapshot(sample_suite["v2_large"]["root"])
    with pytest.raises(BusinessError) as raised:
        _plan(
            snapshot,
            [1],
            ["id"],
            total_memory_budget_bytes=8,
            remaining_memory_budget_bytes=8,
            single_task_budget_bytes=8,
        )
    assert raised.value.code == E_MEMORY_BUDGET_EXCEEDED
    assert raised.value.context["bytes_per_row"] > raised.value.context["effective_task_budget_bytes"]


def test_path_decision_uses_parallel_lanes_and_budget(sample_suite: dict) -> None:
    snapshot = load_snapshot(sample_suite["v2_large"]["root"])
    plan = _plan(
        snapshot,
        [1, 5000, 10000, 15000, 20000],
        ["id", "region", "score"],
        single_task_budget_bytes=16 * 1024,
    )
    assert plan.selected_path == "batch"
    assert plan.parallel_blocks >= 2
    assert plan.peak_memory_estimate_bytes <= plan.remaining_budget_bytes
    forced = _plan(
        snapshot,
        [1],
        ["id"],
        execution_mode="stream",
        single_task_budget_bytes=16 * 1024,
    )
    assert forced.selected_path == "stream"
    assert forced.parallel_blocks == 1
