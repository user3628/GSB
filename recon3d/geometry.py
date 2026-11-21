"""阶段三：几何一致性裁剪与确定性体素降采样。

1. 按可配置重投影误差阈值剔除不一致轨道，记录被剔除点与原因；
2. 按清单声明的体素尺寸（采样密度）做确定性体素降采样：
   体素键 = floor(坐标 / 尺寸)，同一体素保留重投影误差最小（平局按
   track_id）的代表点，结果按体素键排序；
3. 裁剪后点数低于下限时返回 E_GEOMETRY_DEGENERATE；可配置严格模式下，
   一旦出现重复体素键（发生合并）同样返回该错误并保留诊断。
"""

from __future__ import annotations

from .canonical import round_float
from .errors import GeometryDegenerateError

GEOMETRY_STAGE = "geometry"


def _voxel_key(point: list[float], voxel_size: float) -> tuple[int, int, int]:
    import math

    return tuple(int(math.floor(coord / voxel_size)) for coord in point)


def cull_and_downsample(
    triangulation: dict,
    reprojection_threshold: float,
    min_points: int,
    voxel_size: float,
    strict_duplicate_voxel: bool = False,
) -> dict:
    if voxel_size <= 0.0:
        raise GeometryDegenerateError(
            "voxel size must be positive",
            stage=GEOMETRY_STAGE,
            tile_id="global",
            voxel_size=round_float(voxel_size),
        )

    kept = []
    rejected = []
    for track in triangulation["tracks"]:
        if track["max_reprojection_error"] > reprojection_threshold:
            rejected.append(
                {
                    "track_id": track["track_id"],
                    "reason": "reprojection_exceeded",
                    "max_reprojection_error": track["max_reprojection_error"],
                    "threshold": round_float(reprojection_threshold),
                }
            )
        else:
            kept.append(track)

    diagnostics = {
        "input_track_count": len(triangulation["tracks"]),
        "kept_after_cull": len(kept),
        "rejected_after_cull": len(rejected),
        "reprojection_threshold": round_float(reprojection_threshold),
        "min_points": int(min_points),
        "voxel_size": round_float(voxel_size),
    }

    if len(kept) < min_points:
        raise GeometryDegenerateError(
            "too few points remain after culling",
            stage=GEOMETRY_STAGE,
            tile_id="global",
            diagnostics=diagnostics,
            rejected=rejected,
        )

    # 确定性体素降采样：同体素保留误差最小、平局 track_id 更小的点
    buckets: dict[tuple[int, int, int], dict] = {}
    duplicate_keys = set()
    for track in kept:
        key = _voxel_key(track["point"], voxel_size)
        if key in buckets:
            duplicate_keys.add(key)
            incumbent = buckets[key]
            candidate_rank = (track["max_reprojection_error"], track["track_id"])
            incumbent_rank = (incumbent["max_reprojection_error"], incumbent["track_id"])
            if candidate_rank < incumbent_rank:
                buckets[key] = track
        else:
            buckets[key] = track

    if strict_duplicate_voxel and duplicate_keys:
        diagnostics["duplicate_voxel_keys"] = [list(key) for key in sorted(duplicate_keys)]
        raise GeometryDegenerateError(
            "duplicate voxel keys detected under strict downsampling",
            stage=GEOMETRY_STAGE,
            tile_id="global",
            diagnostics=diagnostics,
            rejected=rejected,
        )

    downsampled = []
    merge_log = []
    for key in sorted(buckets):
        representative = buckets[key]
        members = [
            track["track_id"]
            for track in kept
            if _voxel_key(track["point"], voxel_size) == key
        ]
        downsampled.append(
            {
                "voxel_key": list(key),
                "track_id": representative["track_id"],
                "point": representative["point"],
                "max_reprojection_error": representative["max_reprojection_error"],
                "view_count": representative["view_count"],
                "merged_track_ids": sorted(members),
            }
        )
        if len(members) > 1:
            merge_log.append({"voxel_key": list(key), "merged_track_ids": sorted(members)})

    if len(downsampled) < min_points:
        diagnostics["downsampled_count"] = len(downsampled)
        raise GeometryDegenerateError(
            "too few points remain after downsampling",
            stage=GEOMETRY_STAGE,
            tile_id="global",
            diagnostics=diagnostics,
            rejected=rejected,
        )

    return {
        "stage": GEOMETRY_STAGE,
        "counts": {
            "before_cull": len(triangulation["tracks"]),
            "after_cull": len(kept),
            "after_downsample": len(downsampled),
            "rejected": len(rejected),
            "merged_voxels": len(merge_log),
        },
        "rejected": rejected,
        "merge_log": merge_log,
        "duplicate_voxel_keys": [list(key) for key in sorted(duplicate_keys)],
        "points": downsampled,
        "diagnostics": diagnostics,
    }
