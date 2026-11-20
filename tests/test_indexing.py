from logindex.indexing import (
    aggregate_batch_stats,
    merge_intervals,
    partition_summary,
    rebuild_equivalent,
    append_intervals,
)


def test_merge_intervals_basic():
    assert merge_intervals([]) == []
    assert merge_intervals([42]) == [(42, 42)]
    assert merge_intervals([1, 2, 3, 5, 6, 10]) == [(1, 3), (5, 6), (10, 10)]


def test_merge_intervals_unsorted_and_duplicates():
    assert merge_intervals([10, 1, 2, 2, 3]) == [(1, 3), (10, 10)]


def test_append_and_full_merge_equivalent_when_ordered():
    existing = merge_intervals([1, 2, 3])
    new_points = [4, 5, 10]
    fast = append_intervals(existing, new_points)
    full = merge_intervals([1, 2, 3, 4, 5, 10])
    assert fast == full
    assert rebuild_equivalent(existing, [1, 2, 3], new_points)


def test_append_detects_non_append_input():
    existing = merge_intervals([100, 200])
    # 早于最大点的输入：快路径结果与全量重建不同，调用方必须改走重建
    fast = append_intervals(existing, [150])
    full = merge_intervals([100, 200, 150])
    assert fast != full
    assert not rebuild_equivalent(existing, [100, 200], [150])


def test_partition_summary():
    rows = [("svc", 1, "INFO"), ("svc", 2, "INFO"), ("svc", 9, "ERROR")]
    counts, intervals = partition_summary(rows)
    assert counts == {"INFO": 2, "ERROR": 1}
    assert intervals == [(1, 2), (9, 9)]


def test_aggregate_batch_stats():
    stats = aggregate_batch_stats([("a", "INFO"), ("a", "INFO"), ("b", "ERROR")])
    assert stats == {("a", "INFO"): 2, ("b", "ERROR"): 1}
