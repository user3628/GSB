"""资源规格与快照信封校验。

校验失败统一抛出 :class:`SpecInvalid` / :class:`SnapshotInvalid`，
调用方保证在校验通过前不触碰任何已提交状态。
"""

from __future__ import annotations

from collections import Counter
from typing import Iterable

from .errors import SnapshotInvalid, SpecInvalid
from .models import Resource, Snapshot


def validate_resources(resources: Iterable[Resource], *, known_ids: set[str] | None = None) -> None:
    """校验一批资源的标识唯一性、父引用存在性与权重区间。

    ``known_ids`` 用于增量场景：除本批资源外，当前快照中仍然存在的资源标识集合。
    """

    resource_list = list(resources)
    issues: list[dict[str, str]] = []

    id_counter = Counter(item.id for item in resource_list)
    duplicate_ids = sorted(rid for rid, count in id_counter.items() if count > 1)
    for rid in duplicate_ids:
        issues.append({"rule": "unique_id", "resource_id": rid, "message": "资源标识在提交内容中重复"})

    local_ids = set(id_counter)
    visible_ids = set(known_ids or set()) | local_ids

    for item in resource_list:
        if not 1 <= item.weight_milli <= 1000:
            issues.append(
                {
                    "rule": "weight_range",
                    "resource_id": item.id,
                    "message": f"weight_milli={item.weight_milli} 超出 [1, 1000] 千分比区间",
                }
            )
        parent_counter = Counter(item.parents)
        for parent, count in sorted(parent_counter.items()):
            if count > 1:
                issues.append(
                    {
                        "rule": "duplicate_parent",
                        "resource_id": item.id,
                        "parent_id": parent,
                        "message": "父资源引用重复",
                    }
                )
            if parent not in visible_ids:
                issues.append(
                    {
                        "rule": "parent_exists",
                        "resource_id": item.id,
                        "parent_id": parent,
                        "message": "父资源引用不存在",
                    }
                )
            if parent == item.id:
                issues.append(
                    {
                        "rule": "self_parent",
                        "resource_id": item.id,
                        "parent_id": parent,
                        "message": "资源不能依赖自身",
                    }
                )

    if issues:
        raise SpecInvalid("资源规格校验失败", {"issues": issues})


def validate_removals(removals: Iterable[str], resource_map: dict[str, Resource]) -> None:
    unknown = sorted(rid for rid in removals if rid not in resource_map)
    if unknown:
        raise SpecInvalid(
            "增量删除了不存在的资源",
            {"issues": [{"rule": "removal_exists", "resource_id": rid, "message": "待删除资源不存在"} for rid in unknown]},
        )


def validate_snapshot_envelope(snapshot: Snapshot, current_version: int | None) -> None:
    """快照级校验：摘要一致 + 版本号严格单调递增。"""

    problems: list[str] = []
    if snapshot.supplied_checksum is not None and snapshot.supplied_checksum != snapshot.checksum:
        problems.append("checksum_mismatch")
    if current_version is not None and snapshot.version <= current_version:
        problems.append("version_not_monotonic")
    if problems:
        raise SnapshotInvalid(
            "快照信封校验失败，旧快照保持可用",
            {
                "problems": problems,
                "submitted_version": snapshot.version,
                "current_version": current_version,
                "expected_checksum": snapshot.checksum,
                "supplied_checksum": snapshot.supplied_checksum,
            },
        )
