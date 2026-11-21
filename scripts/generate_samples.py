"""Deterministically regenerate the local RGB PNG dataset and metadata."""

import json
import os
import sys

import numpy as np
from PIL import Image

SEED = 20261122
WIDTH = 48
HEIGHT = 36


def _save(path, array):
    Image.fromarray(array.astype(np.uint8), mode="RGB").save(path, format="PNG", optimize=True)


def geometric_blocks(rng):
    array = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
    palette = [
        (220, 60, 60),
        (60, 180, 90),
        (50, 90, 220),
        (230, 200, 70),
        (180, 80, 200),
        (70, 200, 210),
    ]
    for index, color in enumerate(palette):
        top = (index // 3) * (HEIGHT // 2)
        left = (index % 3) * (WIDTH // 3)
        array[top:top + HEIGHT // 2, left:left + WIDTH // 3] = color
    array = np.clip(array.astype(np.int16) + rng.integers(-8, 9, array.shape), 0, 255)
    return array.astype(np.uint8)


def gradient_image(rng):
    column = np.linspace(0, 255, WIDTH, dtype=np.float64)
    row = np.linspace(0, 180, HEIGHT, dtype=np.float64)[:, None]
    array = np.zeros((HEIGHT, WIDTH, 3), dtype=np.float64)
    array[:, :, 0] = column
    array[:, :, 1] = row
    array[:, :, 2] = 255 - column
    array += rng.normal(0, 3, array.shape)
    return np.clip(array, 0, 255).astype(np.uint8)


def stripes_texture(rng):
    array = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
    for x in range(WIDTH):
        color = 230 if (x // 6) % 2 == 0 else 40
        array[:, x] = (color, max(color - 30, 0), 255 - color)
    array = np.clip(array.astype(np.int16) + rng.integers(-5, 6, array.shape), 0, 255)
    return array.astype(np.uint8)


def noise_texture(rng):
    base = rng.integers(0, 256, (HEIGHT, WIDTH, 3), dtype=np.int16)
    smooth = np.zeros((HEIGHT, WIDTH, 3), dtype=np.int16)
    smooth[:, :, 0] = np.linspace(40, 210, WIDTH)[None, :]
    smooth[:, :, 1] = np.linspace(210, 40, HEIGHT)[:, None]
    smooth[:, :, 2] = 128
    blended = (0.65 * base + 0.35 * smooth).astype(np.int16)
    return np.clip(blended, 0, 255).astype(np.uint8)


def black_image(_rng):
    return np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)


def reference_image(rng):
    array = np.zeros((HEIGHT, WIDTH, 3), dtype=np.uint8)
    array[: HEIGHT // 2, :, 0] = 200
    array[HEIGHT // 2:, :, 1] = 200
    array[:, :, 2] = rng.integers(60, 120, (HEIGHT, WIDTH))
    return array


def main(work_dir):
    rng = np.random.default_rng(SEED)
    images_dir = os.path.join(work_dir, "images")
    reference_dir = os.path.join(work_dir, "references")
    os.makedirs(images_dir, exist_ok=True)
    os.makedirs(reference_dir, exist_ok=True)

    builders = [
        ("img_blocks.png", geometric_blocks),
        ("img_gradient.png", gradient_image),
        ("img_stripes.png", stripes_texture),
        ("img_noise.png", noise_texture),
        ("img_black.png", black_image),
        ("img_dim.png", geometric_blocks),
    ]
    for filename, builder in builders:
        _save(os.path.join(images_dir, filename), builder(rng))
    _save(os.path.join(reference_dir, "ref_main.png"), reference_image(rng))

    metadata = [
        {
            "image_id": "img_001_blocks",
            "file": "img_blocks.png",
            "prompt": "红色 蓝色 geometric blocks 色块与 green 几何图案",
            "declared_width": WIDTH,
            "declared_height": HEIGHT,
            "reference_file": "ref_main.png",
            "expected_range": [0.05, 0.95],
            "operator": "op_geometric_v1",
        },
        {
            "image_id": "img_002_gradient",
            "file": "img_gradient.png",
            "prompt": "smooth gradient 渐变 blue sky 天空",
            "declared_width": WIDTH,
            "declared_height": HEIGHT,
            "reference_file": None,
            "expected_range": [0.90, 0.99],
            "operator": "op_gradient_v1",
        },
        {
            "image_id": "img_003_stripes",
            "file": "img_stripes.png",
            "prompt": "stripes 条纹 red 与 white 白色 pattern",
            "declared_width": WIDTH,
            "declared_height": HEIGHT,
            "reference_file": "ref_main.png",
            "expected_range": [0.05, 0.95],
            "operator": "op_texture_v1",
        },
        {
            "image_id": "img_004_noise",
            "file": "img_noise.png",
            "prompt": "noise 噪声 texture 纹理 random 随机",
            "declared_width": WIDTH,
            "declared_height": HEIGHT,
            "reference_file": None,
            "expected_range": [0.05, 0.95],
            "operator": "op_noise_v1",
        },
        {
            "image_id": "img_005_black",
            "file": "img_black.png",
            "prompt": "black 黑色 smooth 平滑 background",
            "declared_width": WIDTH,
            "declared_height": HEIGHT,
            "reference_file": None,
            "expected_range": [0.60, 0.99],
            "operator": "op_edge_v1",
        },
        {
            "image_id": "img_006_dim_mismatch",
            "file": "img_dim.png",
            "prompt": "green blocks 绿色几何色块",
            "declared_width": WIDTH + 16,
            "declared_height": HEIGHT,
            "reference_file": None,
            "expected_range": [0.05, 0.95],
            "operator": "op_dim_v1",
        },
    ]
    metadata_path = os.path.join(work_dir, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True))
        handle.write("\n")
    print(f"generated {len(builders)} images and 1 reference in {work_dir}")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "..", "work")
    main(os.path.abspath(target))
