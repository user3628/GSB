import pytest

from pkq.errors import QueryTooLarge
from pkq.planner import plan_query
from pkq.columnar import SnapshotStore


@pytest.fixture()
def snap(sample_root):
    store = SnapshotStore()
    return store.load(__import__("os").path.join(sample_root, "snapshot_v1"))


def test_empty_keys_returns_empty(snap):
    assert plan_query(snap, [], 64) == []


def test_too_large(snap):
    with pytest.raises(QueryTooLarge) as ei:
        plan_query(snap, list(range(100)), 64)
    assert ei.value.limit == 64
    assert ei.value.to_payload()["error"]["details"]["limit"] == 64


def test_task_ranges_non_overlap_and_cover(snap):
    keys = [0, 7, 1005, 2000, 5005, 11000]
    tasks = plan_query(snap, keys, 64)
    hit_blocks = {b.block_id for b in snap.locate_blocks(keys)}
    covered = {b.block_id for t in tasks for b in t.blocks}
    assert covered == hit_blocks  # 覆盖全部命中块
    for i in range(len(tasks) - 1):
        assert tasks[i].pk_hi < tasks[i + 1].pk_lo  # 区间不重叠且升序


def test_adjacent_blocks_merged(snap):
    # 块 0 与块 1 索引相邻 -> 合并为一个任务；块 0 与块 2 间隔未命中块 -> 不合并
    assert len(plan_query(snap, [0, 1000], 64)) == 1
    assert len(plan_query(snap, [0, 2000], 64)) == 2
