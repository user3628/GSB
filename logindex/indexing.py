"""派生产物的纯计算算子（numpy 实现）。

这里只负责“给定一组已提交记录，算出应有索引内容”，不触碰数据库，
从而让仅追加路径与重建路径共享同一套合并语义，保证两条路径对同一
数据产生完全一致的索引内容。

- merge_intervals: 点时间戳排序后合并相邻/重叠区间（含端点相接）
- partition_summary: 某服务分区的分类计数与合并区间
- aggregate_batch_stats: 批次级 (service, level) 计数
"""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List, Sequence, Tuple

import numpy as np


def merge_intervals(timestamps_ms: Sequence[int]) -> List[Tuple[int, int]]:
    """把一组点时间戳合并为互不相交、端点相接即合并的整数区间。

    点数据每个点是 ``[t, t]``；排序后，若下一个点不超过当前区间
    右端点 + 1，则视为连续/重叠并扩展右端点，否则开启新区间。
    """
    if not timestamps_ms:
        return []
    pts = np.sort(np.asarray(list(timestamps_ms), dtype=np.int64))
    merged: List[Tuple[int, int]] = []
    start = int(pts[0])
    end = start
    for value in pts[1:]:
        value = int(value)
        if value <= end + 1:
            end = max(end, value)
        else:
            merged.append((start, end))
            start = end = value
    merged.append((start, end))
    return merged


def append_intervals(
    existing: Sequence[Tuple[int, int]], new_points: Sequence[int]
) -> List[Tuple[int, int]]:
    """仅追加快路径：新区间点都严格晚于现有最大时间戳时使用。

    与 ``merge_intervals`` 数学等价，只是避免读取该服务的全部历史点。
    若前提不成立（出现早于最大时间戳的点），调用方必须改走重建路径。
    """
    if not existing and not new_points:
        return []
    ordered: List[Tuple[int, int]] = list(existing)
    for point in sorted(new_points):
        point = int(point)
        if ordered and point <= ordered[-1][1] + 1:
            prev_start, prev_end = ordered[-1]
            ordered[-1] = (prev_start, max(prev_end, point))
        else:
            ordered.append((point, point))
    return ordered


def partition_summary(
    rows: Iterable[Tuple[str, int, str]]
) -> Tuple[Dict[str, int], List[Tuple[int, int]]]:
    """根据 (service, ts, level) 行计算单分区分类计数与合并区间。"""
    counts: Dict[str, int] = defaultdict(int)
    timestamps: List[int] = []
    for _service, ts, level in rows:
        counts[level] += 1
        timestamps.append(ts)
    return dict(counts), merge_intervals(timestamps)


def aggregate_batch_stats(
    records: Sequence[Tuple[str, str]]
) -> Dict[Tuple[str, str], int]:
    """按 (service, level) 聚合批内计数。"""
    stats: Dict[Tuple[str, str], int] = defaultdict(int)
    for service, level in records:
        stats[(service, level)] += 1
    return dict(stats)


def rebuild_equivalent(
    existing: Sequence[Tuple[int, int]],
    historical_points: Sequence[int],
    new_points: Sequence[int],
) -> bool:
    """自检：快路径结果必须等于全量重建结果。"""
    fast = append_intervals(existing, new_points)
    full = merge_intervals(list(historical_points) + list(new_points))
    return fast == full
