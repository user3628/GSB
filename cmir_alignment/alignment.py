"""Deterministic cross-modal projection into the shared 64-D space."""

from __future__ import annotations

import hashlib
import math
from typing import Any

import numpy as np

from .canonical import digest_obj
from .configs import FeatureConfig, ProjectionConfig
from .descriptors import feature_hash
from .errors import CMIRError, E_PROJECTION_INVALID, numeric_error


CONCEPT_DIMS = {
    "red": 0,
    "orange": 1,
    "yellow": 2,
    "green": 3,
    "cyan": 4,
    "blue": 5,
    "purple": 6,
    "magenta": 7,
    "dark": 8,
    "bright": 9,
    "neutral": 10,
    "saturated": 11,
    "horizontal": 12,
    "vertical": 13,
    "diagonal_up": 14,
    "diagonal_down": 15,
    "textured": 16,
    "smooth": 17,
    "contrast": 18,
    "gradient": 19,
    "square": 20,
    "circle": 21,
    "triangle": 22,
    "stripes": 23,
}

LEXICON: dict[str, list[str]] = {
    "red": ["red", "红", "红色", "crimson"],
    "orange": ["orange", "橙", "橙色"],
    "yellow": ["yellow", "黄", "黄色"],
    "green": ["green", "绿", "绿色"],
    "cyan": ["cyan", "青", "青色", "teal"],
    "blue": ["blue", "蓝", "蓝色"],
    "purple": ["purple", "violet", "紫", "紫色"],
    "magenta": ["magenta", "pink", "洋红", "洋红色", "粉"],
    "dark": ["dark", "dim", "暗", "深色", "黑"],
    "bright": ["bright", "light", "明", "明亮", "亮"],
    "neutral": ["neutral", "gray", "grey", "white", "灰", "白"],
    "saturated": ["saturated", "vivid", "鲜艳", "饱和"],
    "horizontal": ["horizontal", "水平", "横", "横向"],
    "vertical": ["vertical", "垂直", "竖", "纵向"],
    "diagonal_up": ["diagonal", "斜", "斜向"],
    "diagonal_down": ["diagonal-down", "反斜"],
    "textured": ["texture", "textured", "纹理", "质感"],
    "smooth": ["smooth", "平滑", "柔和"],
    "contrast": ["contrast", "对比", "高对比"],
    "gradient": ["gradient", "渐变", "过渡"],
    "square": ["square", "block", "方块", "方形", "矩形", "方"],
    "circle": ["circle", "round", "圆形", "圆"],
    "triangle": ["triangle", "三角", "三角形"],
    "stripes": ["stripe", "stripes", "条纹", "条带"],
}

COLOR_PROTOTYPES = {
    "red": np.array([1.0, -0.5, -0.5]),
    "orange": np.array([0.65, 0.35, -1.0]),
    "yellow": np.array([0.45, 0.45, -0.9]),
    "green": np.array([-0.7, 0.7, -0.2]),
    "cyan": np.array([-0.8, 0.6, 0.6]),
    "blue": np.array([-0.5, -0.5, 1.0]),
    "purple": np.array([0.5, -0.5, 0.9]),
    "magenta": np.array([0.9, -0.6, 0.4]),
}


def _shake_uniform(count: int, seed: int, tag: str) -> np.ndarray:
    """Cross-NumPy-version deterministic U(0,1) stream."""
    material = f"cmir-projection:{seed}:{tag}".encode("utf-8")
    raw = hashlib.shake_256(material).digest(count * 8)
    values = np.frombuffer(raw, dtype="<u8").astype(np.float64)
    return (values + 0.5) / float(1 << 64)


def deterministic_normal(*, rows: int, cols: int, seed: int, tag: str) -> np.ndarray:
    count = rows * cols
    u1 = _shake_uniform(count, seed, tag + ":u1")
    u2 = _shake_uniform(count, seed, tag + ":u2")
    radius = np.sqrt(-2.0 * np.log(u1))
    matrix = radius * np.cos(2.0 * np.pi * u2)
    return matrix.reshape(rows, cols)


def _block_layout(config: FeatureConfig) -> tuple[int, int, int, int]:
    block_width = 3 + 3 + config.grad_bins
    mean_offset = 0
    std_offset = 3
    gradient_offset = 6
    return block_width, mean_offset, std_offset, gradient_offset


def _add_text_lexicon(matrix: np.ndarray, config: FeatureConfig) -> None:
    for concept, terms in LEXICON.items():
        dimension = CONCEPT_DIMS[concept]
        for term in terms:
            if any(ch.isascii() and (ch.isalpha() or ch.isdigit()) for ch in term):
                column = feature_hash("word", term, config.text_dim)
                matrix[dimension, column] += 3.5
            for ngram_size in range(config.char_ngram_min, config.char_ngram_max + 1):
                if len(term) == ngram_size:
                    column = feature_hash(f"char:{ngram_size}", term, config.text_dim)
                    matrix[dimension, column] += 3.5


def _add_visual_semantics(matrix: np.ndarray, config: FeatureConfig) -> None:
    block_width, mean_offset, std_offset, gradient_offset = _block_layout(config)
    block_count = config.grid_rows * config.grid_cols
    color_gain = 1.4 / math.sqrt(block_count)
    for block_index in range(block_count):
        base = block_index * block_width
        for concept, prototype in COLOR_PROTOTYPES.items():
            centered = prototype - float(prototype.mean())
            norm = float(np.linalg.norm(centered))
            matrix[CONCEPT_DIMS[concept], base + mean_offset : base + mean_offset + 3] += (
                centered / max(norm, 1e-12) * color_gain
            )
        brightness = np.ones(3, dtype=np.float64) / math.sqrt(3.0)
        matrix[CONCEPT_DIMS["bright"], base + mean_offset : base + mean_offset + 3] += (
            brightness * 0.8 / math.sqrt(block_count)
        )
        matrix[CONCEPT_DIMS["dark"], base + mean_offset : base + mean_offset + 3] -= (
            brightness * 0.8 / math.sqrt(block_count)
        )
        texture = np.ones(3, dtype=np.float64) / math.sqrt(3.0)
        matrix[CONCEPT_DIMS["textured"], base + std_offset : base + std_offset + 3] += (
            texture * 0.7 / math.sqrt(block_count)
        )
        matrix[CONCEPT_DIMS["smooth"], base + std_offset : base + std_offset + 3] -= (
            texture * 0.7 / math.sqrt(block_count)
        )
        matrix[CONCEPT_DIMS["contrast"], base + std_offset : base + std_offset + 3] += (
            texture * 0.5 / math.sqrt(block_count)
        )
        row = block_index // config.grid_cols
        col = block_index % config.grid_cols
        vertical_position = (row + 0.5) / config.grid_rows - 0.5
        horizontal_position = (col + 0.5) / config.grid_cols - 0.5
        matrix[CONCEPT_DIMS["gradient"], base + mean_offset : base + mean_offset + 3] += (
            brightness * vertical_position * 1.0 / math.sqrt(block_count)
        )
        matrix[CONCEPT_DIMS["horizontal"], base + mean_offset : base + mean_offset + 3] += (
            brightness * horizontal_position * 0.35 / math.sqrt(block_count)
        )

    bins = config.grad_bins
    def gradient_bin(angle_ratio: float) -> int:
        return min(bins - 1, int(angle_ratio * bins))

    orientation_weights = {
        "horizontal": [0.0, 0.08, 0.62, 0.95],
        "vertical": [1.0, 0.65, 0.1, 0.0],
        "diagonal_up": [1.0, 0.45, 0.05, 0.0],
        "diagonal_down": [0.0, 0.05, 0.45, 1.0],
    }
    for block_index in range(block_count):
        base = block_index * block_width + gradient_offset
        for concept, ratios in orientation_weights.items():
            for ratio, gain in zip(ratios, (1.0, 0.7, 0.7, 1.0)):
                bin_index = gradient_bin(ratio)
                matrix[CONCEPT_DIMS[concept], base + bin_index] += gain / math.sqrt(block_count)
        # Shape sketches: square has axis-aligned edges, triangle has diagonal edges,
        # circle spreads energy over all bins, stripes emphasize vertical edges.
        matrix[CONCEPT_DIMS["square"], base + gradient_bin(0.0)] += 0.9 / math.sqrt(block_count)
        matrix[CONCEPT_DIMS["square"], base + gradient_bin(0.5)] += 0.9 / math.sqrt(block_count)
        matrix[CONCEPT_DIMS["stripes"], base + gradient_bin(0.0)] += 1.1 / math.sqrt(block_count)
        matrix[CONCEPT_DIMS["triangle"], base + gradient_bin(0.25)] += 0.9 / math.sqrt(block_count)
        matrix[CONCEPT_DIMS["triangle"], base + gradient_bin(0.75)] += 0.9 / math.sqrt(block_count)
        for bin_index in range(bins):
            matrix[CONCEPT_DIMS["circle"], base + bin_index] += (
                0.18 / math.sqrt(max(1, block_count * bins))
            )


def _default_visual_matrix(config: ProjectionConfig, feature: FeatureConfig) -> np.ndarray:
    matrix = deterministic_normal(
        rows=config.alignment_dim,
        cols=feature.visual_dim,
        seed=config.seed,
        tag="visual",
    ) * 0.045
    _add_visual_semantics(matrix, feature)
    return matrix


def _default_text_matrix(config: ProjectionConfig, feature: FeatureConfig) -> np.ndarray:
    matrix = deterministic_normal(
        rows=config.alignment_dim,
        cols=feature.text_dim,
        seed=config.seed,
        tag="text",
    ) * 0.055
    _add_text_lexicon(matrix, feature)
    return matrix


def _matrix_from_input(value: Any, *, name: str, expected_shape: tuple[int, int]) -> np.ndarray:
    try:
        matrix = np.asarray(value, dtype=np.float64)
    except (TypeError, ValueError) as exc:
        raise CMIRError(
            E_PROJECTION_INVALID,
            f"{name} is not numeric",
            item_id="__global__",
            dimensions={"expected": list(expected_shape)},
            stage="projection_config",
        ) from exc
    if matrix.shape != expected_shape:
        raise CMIRError(
            E_PROJECTION_INVALID,
            f"{name} has an incompatible shape",
            item_id="__global__",
            dimensions={
                "expected": list(expected_shape),
                "actual": list(matrix.shape),
                "matrix": name,
            },
            stage="projection_config",
        )
    if not np.all(np.isfinite(matrix)):
        nonfinite = int(matrix.size - np.isfinite(matrix).sum())
        raise CMIRError(
            E_PROJECTION_INVALID,
            f"{name} contains NaN or Inf",
            item_id="__global__",
            dimensions=list(matrix.shape),
            nonfinite_values=nonfinite,
            stage="projection_config",
        )
    return matrix


def build_projection_matrices(
    config: ProjectionConfig, feature_config: FeatureConfig
) -> tuple[np.ndarray, np.ndarray]:
    visual_shape = (config.alignment_dim, feature_config.visual_dim)
    text_shape = (config.alignment_dim, feature_config.text_dim)
    visual = (
        _matrix_from_input(config.visual_matrix, name="visual_matrix", expected_shape=visual_shape)
        if config.visual_matrix is not None
        else _default_visual_matrix(config, feature_config)
    )
    text = (
        _matrix_from_input(config.text_matrix, name="text_matrix", expected_shape=text_shape)
        if config.text_matrix is not None
        else _default_text_matrix(config, feature_config)
    )
    return visual, text


def projection_matrix_digest(
    config: ProjectionConfig, feature_config: FeatureConfig
) -> str:
    visual, text = build_projection_matrices(config, feature_config)
    return digest_obj(
        {
            "schema": "cmir.projection-matrices.v1",
            "visual": visual.tolist(),
            "text": text.tolist(),
        }
    )


def project_vector(
    vector: np.ndarray,
    matrix: np.ndarray,
    *,
    item_id: str,
    component: str,
) -> np.ndarray:
    array = np.asarray(vector, dtype=np.float64)
    expected = matrix.shape[1]
    if array.ndim != 1 or array.size != expected:
        raise CMIRError(
            E_PROJECTION_INVALID,
            "query/input feature dimension does not match projection matrix",
            item_id=item_id,
            dimensions={"expected": expected, "actual": list(array.shape)},
            component=component,
            stage="projection",
        )
    if not np.all(np.isfinite(array)):
        raise CMIRError(
            E_PROJECTION_INVALID,
            "input feature vector contains NaN or Inf",
            item_id=item_id,
            dimensions=list(array.shape),
            component=component,
            stage="projection",
        )
    projected = matrix @ array
    if not np.all(np.isfinite(projected)):
        raise numeric_error(
            item_id,
            "projection",
            "projected vector contains NaN or Inf",
            component=component,
        )
    norm = float(np.linalg.norm(projected))
    if not math.isfinite(norm) or norm <= 0.0:
        raise numeric_error(
            item_id,
            "projection",
            "projected vector has zero or non-finite norm",
            component=component,
            norm=norm,
        )
    return projected / norm
