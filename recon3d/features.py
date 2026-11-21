"""阶段一：图像特征提取。

固定规则：
1. 灰度分片做零均值/单位方差规范化（全恒定图视为空白）；
2. 在 3x3 邻域内检测局部强度峰作为角点候选，阈值相对规范化图像确定；
3. 以角点为中心的 9x9 窗计算 Sobel 梯度，按梯度幅值对 16 个方向箱
   做三线性/线性插值投票，L2 归一化得到 HOG 描述子。

输出带分片标识、整数像素坐标与定长描述子的特征清单，规范化序列化后
对同一输入与配置逐字节一致。空白/零梯度图返回 E_EMPTY_FEATURE。
"""

from __future__ import annotations

import math

import numpy as np

from .canonical import FLOAT_SIG, round_float
from .errors import EmptyFeatureError

N_BINS = 16
WINDOW_RADIUS = 4
PEAK_THRESHOLD = 0.6
MIN_PEAK_DISTANCE = 2.0
DESCRIPTION = "hog16-zscore-localmax"


def normalize_image(image: np.ndarray) -> np.ndarray:
    arr = image.astype(np.float64)
    mean = float(arr.mean())
    std = float(arr.std())
    if std <= 1e-12:
        return np.zeros_like(arr)
    return (arr - mean) / std


def _sobel(norm: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    padded = np.pad(norm, 1, mode="edge")
    gx = (
        padded[2:, 2:]
        + 2 * padded[1:-1, 2:]
        + padded[:-2, 2:]
        - padded[2:, :-2]
        - 2 * padded[1:-1, :-2]
        - padded[:-2, :-2]
    )
    gy = (
        padded[2:, 2:]
        + 2 * padded[2:, 1:-1]
        + padded[2:, :-2]
        - padded[:-2, 2:]
        - 2 * padded[:-2, 1:-1]
        - padded[:-2, :-2]
    )
    return gx / 8.0, gy / 8.0


def _detect_peaks(norm: np.ndarray) -> list[tuple[int, int]]:
    height, width = norm.shape
    candidates: list[tuple[float, int, int]] = []
    for y in range(1, height - 1):
        for x in range(1, width - 1):
            center = norm[y, x]
            if center < PEAK_THRESHOLD:
                continue
            neighborhood = np.concatenate(
                [norm[y - 1, x - 1 : x + 2], norm[y, x - 1 : x + 2 : 2], norm[y + 1, x - 1 : x + 2]]
            )
            if center >= neighborhood.max() and (center > neighborhood).sum() >= 4:
                candidates.append((float(center), y, x))
    # 非极大值抑制：按强度降序，过近的弱峰被强峰吸收，保证每个亮斑一个峰
    candidates.sort(key=lambda item: (-item[0], item[1], item[2]))
    peaks: list[tuple[int, int]] = []
    for _, y, x in candidates:
        if all(math.hypot(y - py, x - px) >= MIN_PEAK_DISTANCE for py, px in peaks):
            peaks.append((y, x))
    peaks.sort()
    return peaks


def _hog_descriptor(gx: np.ndarray, gy: np.ndarray, cy: int, cx: int) -> list[float]:
    height, width = gx.shape
    histogram = np.zeros(N_BINS, dtype=np.float64)
    for y in range(cy - WINDOW_RADIUS, cy + WINDOW_RADIUS + 1):
        for x in range(cx - WINDOW_RADIUS, cx + WINDOW_RADIUS + 1):
            if not (0 <= y < height and 0 <= x < width):
                continue
            vx, vy = gx[y, x], gy[y, x]
            magnitude = math.hypot(vx, vy)
            if magnitude <= 1e-12:
                continue
            angle = math.atan2(vy, vx)
            if angle < 0:
                angle += 2 * math.pi
            position = angle / (2 * math.pi) * N_BINS
            lower = int(math.floor(position)) % N_BINS
            upper = (lower + 1) % N_BINS
            fraction = position - math.floor(position)
            histogram[lower] += magnitude * (1 - fraction)
            histogram[upper] += magnitude * fraction
    norm_value = float(np.linalg.norm(histogram))
    if norm_value <= 1e-12:
        return [0.0] * N_BINS
    histogram = histogram / norm_value
    return [round_float(float(v), FLOAT_SIG) for v in histogram]


def extract_features(image: np.ndarray, tile_id: str) -> dict:
    """对单分片提取特征，返回 ``{tile_id, descriptor, features}``。"""
    norm = normalize_image(image)
    if float(np.abs(norm).sum()) <= 1e-12:
        raise EmptyFeatureError(
            "blank or constant image has no usable gradient",
            tile_id=tile_id,
            stage="features",
        )
    gx, gy = _sobel(norm)
    if np.allclose(gx, 0.0) and np.allclose(gy, 0.0):
        raise EmptyFeatureError(
            "all-zero gradient image",
            tile_id=tile_id,
            stage="features",
        )
    peaks = _detect_peaks(norm)
    features = []
    for index, (y, x) in enumerate(peaks):
        descriptor = _hog_descriptor(gx, gy, y, x)
        if all(abs(v) <= 1e-12 for v in descriptor):
            continue
        features.append(
            {
                "feature_id": f"{tile_id}#f{index:02d}",
                "tile_id": tile_id,
                "x": int(x),
                "y": int(y),
                "descriptor": descriptor,
            }
        )
    if not features:
        raise EmptyFeatureError(
            "no corner feature detected",
            tile_id=tile_id,
            stage="features",
        )
    return {
        "tile_id": tile_id,
        "descriptor": DESCRIPTION,
        "n_bins": N_BINS,
        "features": features,
    }
