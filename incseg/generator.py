"""Deterministic sample generator: fixed-seed RGB PNG tiles + JSON config.

Uses ``numpy.random.RandomState`` (guaranteed stable across NumPy versions)
so the same seed always produces byte-identical tiles and config, offline.
Every tile contains multiple semantic classes and at least two disconnected
instance regions of the same class.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
from PIL import Image

from .config import WorkspaceConfig
from .stages.common import PNG_SAVE_KWARGS

CLASSES = (
    (1, "red", (220, 40, 40)),
    (2, "green", (40, 200, 60)),
    (3, "blue", (50, 80, 220)),
)

BACKGROUND_RGB = (28, 28, 32)


def _draw_rect(img, y0, x0, height, width, rgb):
    img[y0:y0 + height, x0:x0 + width] = rgb


def _draw_disc(img, cy, cx, radius, rgb):
    yy, xx = np.mgrid[0:img.shape[0], 0:img.shape[1]]
    mask = (yy - cy) ** 2 + (xx - cx) ** 2 <= radius * radius
    img[mask] = rgb


def generate_tile_image(rng, size):
    """One tile: background + 2 disconnected red rects, 1 green rect, 1 blue disc."""
    img = np.empty((size, size, 3), dtype=np.float64)
    img[:] = BACKGROUND_RGB
    margin = 8
    side_a = int(rng.randint(14, 21))
    y_a = margin + int(rng.randint(0, 5))
    x_a = margin + int(rng.randint(0, 5))
    _draw_rect(img, y_a, x_a, side_a, side_a, CLASSES[0][2])
    side_b = int(rng.randint(14, 21))
    y_b = size - margin - side_b - int(rng.randint(0, 5))
    x_b = size - margin - side_b - int(rng.randint(0, 5))
    _draw_rect(img, y_b, x_b, side_b, side_b, CLASSES[0][2])
    g_h = int(rng.randint(12, 18))
    g_w = int(rng.randint(16, 24))
    g_y = size - margin - g_h - int(rng.randint(0, 4))
    g_x = margin + int(rng.randint(0, 4))
    _draw_rect(img, g_y, g_x, g_h, g_w, CLASSES[1][2])
    radius = int(rng.randint(9, 13))
    c_y = margin + radius + int(rng.randint(0, 4))
    c_x = size - margin - radius - int(rng.randint(0, 4))
    _draw_disc(img, c_y, c_x, radius, CLASSES[2][2])
    noise = rng.normal(0.0, 9.0, img.shape)
    return np.clip(img + noise, 0, 255).astype(np.uint8)


def tile_config_dict(tile_id, image_rel):
    return {
        "tile_id": tile_id,
        "image": image_rel,
        "color_map": dict(
            [("#{:02x}{:02x}{:02x}".format(*BACKGROUND_RGB), 0)]
            + [(f"#{r:02x}{g:02x}{b:02x}", cid) for cid, _, (r, g, b) in CLASSES]
        ),
        "class_names": dict(
            [("0", "background")]
            + [(str(cid), name) for cid, name, _ in CLASSES]
        ),
        "class_priority": [cid for cid, _, _ in CLASSES] + [0],
        "connectivity": 8,
        "min_area": 24,
        "smooth_weight": 0.6,
        "smooth_iterations": 2,
        "refine_iterations": 2,
        "fusion_weight": 0.5,
    }


def generate_workspace(workspace, tiles=4, seed=42, size=96):
    """Generate tiles + config.json into ``workspace`` (idempotent, deterministic)."""
    workspace = Path(workspace)
    tiles_dir = workspace / "tiles"
    tiles_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.RandomState(seed)
    tile_entries = []
    for index in range(tiles):
        tile_id = f"tile_{index:03d}"
        image_rel = f"tiles/{tile_id}.png"
        image = generate_tile_image(rng, size)
        Image.fromarray(image, mode="RGB").save(tiles_dir / f"{tile_id}.png", **PNG_SAVE_KWARGS)
        tile_entries.append(tile_config_dict(tile_id, image_rel))
    config = WorkspaceConfig.from_dict({
        "config_version": "1.0.0",
        "seed": seed,
        "tiles": tile_entries,
    })
    config.save(workspace / "config.json")
    return config
