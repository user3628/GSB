"""阶段二：多视角三角化。

流程：
1. 校验每个参与视角的位姿（旋转矩阵正交性）与内参（正焦距）；
2. 把特征像素坐标经内参/位姿转为世界系视线（原点 + 单位方向）；
3. 确定性全局匹配：枚举跨视角特征组合，按最小二乘交会后的最大重投影
   误差排序，贪心做一对一排他分配，构建多视角轨道；
4. 每条轨道用视线最小二乘交会求三维坐标，记录参与视角数与逐视角/最大
   重投影误差。

位姿非正交、零/负焦距、或某点仅单视角可见时返回 E_INVALID_POSE，
且不输出任何未验证坐标。
"""

from __future__ import annotations

import math

import numpy as np

from .canonical import round_float
from .errors import InvalidPoseError

ORTHO_TOL = 1e-6
POSE_STAGE = "triangulation"


def validate_camera(tile: dict) -> None:
    tile_id = tile["tile_id"]
    rotation = np.asarray(tile["rotation"], dtype=np.float64)
    if rotation.shape != (3, 3):
        raise InvalidPoseError(
            "rotation matrix must be 3x3", stage=POSE_STAGE, tile_id=tile_id
        )
    orthogonality = np.abs(rotation @ rotation.T - np.eye(3)).max()
    determinant = float(np.linalg.det(rotation))
    # 仅要求正交且保持单位体积（det 为 +1 或 -1，相机基约定可能给出 -1）
    if orthogonality > ORTHO_TOL or abs(abs(determinant) - 1.0) > 1e-6:
        raise InvalidPoseError(
            "rotation matrix is not orthogonal (R@R.T != I or |det(R)| != 1)",
            stage=POSE_STAGE,
            tile_id=tile_id,
            orthogonality=round_float(float(orthogonality)),
            determinant=round_float(determinant),
        )
    intrinsics = np.asarray(tile["intrinsics"], dtype=np.float64)
    if intrinsics.shape != (3, 3):
        raise InvalidPoseError(
            "intrinsics must be 3x3", stage=POSE_STAGE, tile_id=tile_id
        )
    fx, fy = float(intrinsics[0, 0]), float(intrinsics[1, 1])
    if fx <= 0.0 or fy <= 0.0:
        raise InvalidPoseError(
            "focal length must be positive",
            stage=POSE_STAGE,
            tile_id=tile_id,
            fx=round_float(fx),
            fy=round_float(fy),
        )


def world_ray(tile: dict, x: float, y: float) -> tuple[np.ndarray, np.ndarray]:
    rotation = np.asarray(tile["rotation"], dtype=np.float64)
    translation = np.asarray(tile["translation"], dtype=np.float64)
    intrinsics = np.asarray(tile["intrinsics"], dtype=np.float64)
    direction_cam = np.array(
        [
            (x - intrinsics[0, 2]) / intrinsics[0, 0],
            (y - intrinsics[1, 2]) / intrinsics[1, 1],
            1.0,
        ]
    )
    direction_cam /= np.linalg.norm(direction_cam)
    direction_world = rotation.T @ direction_cam
    origin_world = -rotation.T @ translation
    return origin_world, direction_world


def _lstq_intersect(rays: list[tuple[np.ndarray, np.ndarray]]) -> np.ndarray:
    matrix = []
    vector = []
    for origin, direction in rays:
        projector = np.eye(3) - np.outer(direction, direction)
        matrix.append(projector)
        vector.append(projector @ origin)
    point, *_ = np.linalg.lstsq(np.vstack(matrix), np.concatenate(vector), rcond=None)
    return point


def _reproject(tile: dict, point: np.ndarray, x: float, y: float) -> float:
    rotation = np.asarray(tile["rotation"], dtype=np.float64)
    translation = np.asarray(tile["translation"], dtype=np.float64)
    intrinsics = np.asarray(tile["intrinsics"], dtype=np.float64)
    cam_point = rotation @ point + translation
    if cam_point[2] <= 0.0:
        return float("inf")
    uv = cam_point[:2] / cam_point[2]
    projected_x = intrinsics[0, 0] * uv[0] + intrinsics[0, 2]
    projected_y = intrinsics[1, 1] * uv[1] + intrinsics[1, 2]
    return math.hypot(projected_x - x, projected_y - y)


def triangulate_tracks(tiles: list[dict], feature_sets: dict) -> dict:
    """根据清单位姿/内参与各分片特征，构建轨道并三角化。

    ``feature_sets`` 映射 ``tile_id -> 阶段一特征清单``。
    返回带轨道、每点三维坐标、重投影误差与参与视角数的结构化结果。
    """
    for tile in tiles:
        validate_camera(tile)
    by_id = {tile["tile_id"]: tile for tile in tiles}
    # 仅使用至少在两个视角出现特征的分片集合
    view_ids = [tile["tile_id"] for tile in tiles if feature_sets.get(tile["tile_id"], {}).get("features")]
    if len(view_ids) < 2:
        raise InvalidPoseError(
            "a triangulated point needs at least two views",
            stage=POSE_STAGE,
            tile_id=view_ids[0] if view_ids else None,
            view_ids=view_ids,
        )

    feature_lists = {vid: feature_sets[vid]["features"] for vid in view_ids}

    def observations_of(combo: tuple[int, ...]):
        return [
            (by_id[vid], feature_lists[vid][index])
            for vid, index in zip(view_ids, combo)
        ]

    def cost_of(combo: tuple[int, ...]) -> tuple[float, np.ndarray]:
        rays = [world_ray(tile, feature["x"], feature["y"]) for tile, feature in observations_of(combo)]
        point = _lstq_intersect(rays)
        errors = [
            _reproject(tile, point, feature["x"], feature["y"])
            for tile, feature in observations_of(combo)
        ]
        if any(not math.isfinite(err) for err in errors):
            return float("inf"), point
        return max(errors), point

    # 枚举跨视角组合（小分片特征数少，组合规模可控），按最大重投影误差贪心排他
    ranges = [range(len(feature_lists[vid])) for vid in view_ids]
    edges = []
    for combo in _product(ranges):
        cost, point = cost_of(combo)
        if math.isfinite(cost):
            edges.append((cost, combo))
    edges.sort(key=lambda item: (item[0], item[1]))

    used = [set() for _ in view_ids]
    assignments = []
    for cost, combo in edges:
        if any(index in used[position] for position, index in enumerate(combo)):
            continue
        for position, index in enumerate(combo):
            used[position].add(index)
        assignments.append((cost, combo))

    tracks = []
    for track_index, (_, combo) in enumerate(assignments):
        observations = observations_of(combo)
        rays = [world_ray(tile, feature["x"], feature["y"]) for tile, feature in observations]
        point = _lstq_intersect(rays)
        per_view = []
        for tile, feature in observations:
            error = _reproject(tile, point, feature["x"], feature["y"])
            per_view.append(
                {
                    "tile_id": tile["tile_id"],
                    "feature_id": feature["feature_id"],
                    "x": int(feature["x"]),
                    "y": int(feature["y"]),
                    "reprojection_error": round_float(error),
                }
            )
        if len(per_view) < 2:
            raise InvalidPoseError(
                "a triangulated point has only one view",
                stage=POSE_STAGE,
                tile_id=per_view[0]["tile_id"],
                view_ids=[row["tile_id"] for row in per_view],
            )
        errors = [row["reprojection_error"] for row in per_view]
        tracks.append(
            {
                "track_id": f"track_{track_index:02d}",
                "point": [round_float(float(v)) for v in point],
                "view_count": len(per_view),
                "max_reprojection_error": round_float(max(errors)),
                "mean_reprojection_error": round_float(sum(errors) / len(errors)),
                "observations": per_view,
            }
        )
    tracks.sort(key=lambda row: row["track_id"])
    return {
        "stage": POSE_STAGE,
        "view_ids": view_ids,
        "track_count": len(tracks),
        "tracks": tracks,
    }


def _product(ranges):
    if not ranges:
        return []
    combos = [()]
    for indices in ranges:
        combos = [prefix + (index,) for prefix in combos for index in indices]
    return combos
