"""本地确定性样本生成器。

固定随机种子 -> 同一几何体（8 个真值 3D 点，分布在两个错开的方形环上）
与 5 个相机分片：
* 3 个 32x32 宽视角（不同方位角/俯仰角光心），8 个点全部可见且像素可分；
* 1 个 24x24 与 1 个 16x16 变焦近景色（与宽视角同光心、不同焦距），
  体现平移/缩放/明暗变化，只覆盖可分点子集。

每个 3D 点渲染为带唯一主方向的亮斑（暗瓣对），供梯度方向直方图区分；
每视角注入确定性的缩放与明暗（增益/偏置）变化。清单声明位姿（R, t）、
内参 K、每视角可见真值点、采样密度与算法偏好。真值点云仅由本生成器按
种子可复现产出，是结果验证的唯一真值来源。
"""

from __future__ import annotations

import math
import os

import numpy as np

from .canonical import write_json, write_png

SEED = 20261121
MARGIN = 2.0
MIN_SEPARATION = 2.6

# (方位角, 俯仰角, 光心距离, 焦距, 分片边长)
CAMERA_CONFIGS = [
    (25.0, 25.0, 4.0, 24.0, 32),
    (145.0, -28.0, 4.0, 24.0, 32),
    (265.0, 40.0, 4.0, 24.0, 32),
    (25.0, 25.0, 4.0, 34.0, 24),
    (145.0, -28.0, 4.0, 44.0, 16),
]
N_TILES = len(CAMERA_CONFIGS)
MIN_VISIBLE_VIEWS = 2


def _square_ring(radius: float, rotation_deg: float, z: float) -> np.ndarray:
    rotation = math.radians(rotation_deg)
    rows = []
    for angle_deg in (45, 135, 225, 315):
        angle = rotation + math.radians(angle_deg)
        rows.append([radius * math.cos(angle), radius * math.sin(angle), z])
    return np.array(rows)


def _make_points(rng: np.random.Generator) -> np.ndarray:
    # 两个旋转 45 度、不同半径/高度的方形环；小幅种子相关抖动保持可复现性
    outer = _square_ring(1.8, 0.0, 0.6)
    inner = _square_ring(1.3, 45.0, -0.6)
    points = np.vstack([outer, inner])
    points += rng.uniform(-0.04, 0.04, size=points.shape)
    return points


def _camera(azimuth_deg: float, elevation_deg: float, radius: float, focal: float, size: int) -> dict:
    azimuth = math.radians(azimuth_deg)
    elevation = math.radians(elevation_deg)
    position = np.array(
        [
            radius * math.cos(elevation) * math.cos(azimuth),
            radius * math.cos(elevation) * math.sin(azimuth),
            radius * math.sin(elevation),
        ]
    )
    forward = -position / np.linalg.norm(position)
    world_up = np.array([0.0, 0.0, 1.0])
    right = np.cross(world_up, forward)
    right /= np.linalg.norm(right)
    up = np.cross(forward, right)
    rotation = np.vstack([right, -up, forward])
    translation = -rotation @ position
    principal = (size - 1) / 2.0
    intrinsics = np.array(
        [[focal, 0.0, principal], [0.0, focal, principal], [0.0, 0.0, 1.0]]
    )
    return {"position": position, "R": rotation, "t": translation, "K": intrinsics, "size": size}


def _project(points: np.ndarray, cam: dict):
    cam_points = points @ cam["R"].T + cam["t"]
    depth = cam_points[:, 2]
    uv = cam_points[:, :2] / depth[:, None]
    k = cam["K"]
    px = k[0, 0] * uv[:, 0] + k[0, 2]
    py = k[1, 1] * uv[:, 1] + k[1, 2]
    return np.column_stack([px, py]), depth


def _visibility(points: np.ndarray, cams: list[dict]):
    visible = []
    for cam in cams:
        xy, depth = _project(points, cam)
        size = cam["size"]
        ids = [
            int(i)
            for i in range(len(points))
            if depth[i] > 0.0
            and MARGIN <= xy[i, 0] < size - MARGIN
            and MARGIN <= xy[i, 1] < size - MARGIN
        ]
        # 像素可分性：冲突时保留编号更小的点（确定性），近景色自然只保留子集
        blocked = set()
        for i in ids:
            for j in ids:
                if j < i and np.linalg.norm(xy[i] - xy[j]) < MIN_SEPARATION:
                    blocked.add(i)
        visible.append(sorted(i for i in ids if i not in blocked))
    counts = [sum(1 for vis in visible if i in vis) for i in range(len(points))]
    return visible, counts


def _render_tile(
    cam: dict,
    points: np.ndarray,
    visible_ids: list[int],
    rng: np.random.Generator,
    tile_index: int,
) -> np.ndarray:
    size = cam["size"]
    image = np.full((size, size), 50, dtype=np.float64)
    xy, _ = _project(points, cam)
    # 每视角确定性的亮斑缩放与明暗（增益/偏置）变化
    scales = (1.0, 0.9, 1.05, 0.8, 1.15)
    gains = (1.0, 0.8, 1.15, 0.9, 1.1)
    biases = (0.0, 8.0, -6.0, 5.0, -4.0)
    radius = max(1.1, 1.7 * scales[tile_index])
    yy, xx = np.mgrid[0:size, 0:size]
    for pid in visible_ids:
        cx, cy = xy[pid]
        # 亚像素抖动：保留平移变化，同时避免亮斑峰偏移超过一个像素
        jitter = rng.uniform(-0.18, 0.18, size=2)
        cx += jitter[0]
        cy += jitter[1]
        angle = 2.0 * math.pi * pid / 16.0 + rng.uniform(-0.05, 0.05)
        dist2 = (xx - cx) ** 2 + (yy - cy) ** 2
        blob = np.exp(-dist2 / (2.0 * radius * radius))
        along = (xx - cx) * math.cos(angle) + (yy - cy) * math.sin(angle)
        across = -(xx - cx) * math.sin(angle) + (yy - cy) * math.cos(angle)
        oriented = -0.85 * np.exp(-(across**2) / (2.0 * 0.6**2)) * (
            np.exp(-((along - 1.3 * radius) ** 2) / (2.0 * 0.8**2))
            + np.exp(-((along + 1.3 * radius) ** 2) / (2.0 * 0.8**2))
        )
        image += 105.0 * (blob + np.clip(oriented, -1.0, 0.0) * blob)
    image = np.clip(image * gains[tile_index] + biases[tile_index], 0.0, 255.0)
    return np.rint(image).astype(np.uint8)


def generate_samples(out_dir: str, seed: int = SEED) -> dict:
    """生成样本与清单，返回写入的 manifest 字典。"""
    rng = np.random.default_rng(seed)
    points = _make_points(rng)
    cams = [_camera(*cfg) for cfg in CAMERA_CONFIGS]
    visible, counts = _visibility(points, cams)
    if min(counts) < MIN_VISIBLE_VIEWS:
        raise RuntimeError("sample geometry does not satisfy 2-view visibility; adjust seed")

    tiles = []
    os.makedirs(os.path.join(out_dir, "images"), exist_ok=True)
    for index, cam in enumerate(cams):
        tile_id = f"tile_{index:02d}"
        image = _render_tile(cam, points, visible[index], rng, index)
        img_path = os.path.join("images", f"{tile_id}.png")
        write_png(os.path.join(out_dir, img_path), image)
        truth = [
            {
                "point_id": f"p{pid:02d}",
                "world": [float(v) for v in points[pid]],
                "pixel": [float(v) for v in _project(points[None, pid], cam)[0][0]],
            }
            for pid in visible[index]
        ]
        tiles.append(
            {
                "tile_id": tile_id,
                "image": img_path,
                "width": int(cam["size"]),
                "height": int(cam["size"]),
                "rotation": cam["R"].tolist(),
                "translation": cam["t"].tolist(),
                "intrinsics": cam["K"].tolist(),
                "visible_points": truth,
                "voxel_size": 0.09,
                "algorithm": "hog-lstq",
            }
        )

    manifest = {
        "manifest_version": 1,
        "seed": seed,
        "config_version": "cfg-1",
        "reprojection_threshold": 1.25,
        "min_points": 3,
        "sample_density": 0.09,
        "algorithm_preference": "hog-lstq",
        "truth_points": [
            {"point_id": f"p{i:02d}", "world": [float(v) for v in points[i]]}
            for i in range(len(points))
        ],
        "tiles": tiles,
    }
    write_json(os.path.join(out_dir, "manifest.json"), manifest)
    return manifest
