"""Stage 4: quantized outputs (semantic mask, instance mask, summary).

The semantic mask is a palette PNG whose palette is derived deterministically
from the configured class colors (index == class id, 0 = background). The
instance mask is a 16-bit grayscale PNG of instance ids. The summary reports
per-class pixel ratios (every configured class is reported, never silently
dropped), instance counts and the boundary pixel count.
"""

from __future__ import annotations

import numpy as np
from PIL import Image

from .. import ENGINE_VERSION
from ..config import BACKGROUND_CLASS, parse_hex_color
from ..util import round6
from .common import PNG_SAVE_KWARGS


def build_palette_image(labels: np.ndarray, tile) -> Image.Image:
    palette = [0] * (256 * 3)
    for color, class_id in tile.color_map.items():
        r, g, b = parse_hex_color(color)
        palette[class_id * 3: class_id * 3 + 3] = [r, g, b]
    img = Image.fromarray(labels.astype(np.uint8), mode="P")
    img.putpalette(palette)
    return img


def boundary_pixel_count(labels: np.ndarray) -> int:
    """Pixels whose 4-neighborhood contains a different class."""
    boundary = np.zeros(labels.shape, dtype=bool)
    diff_down = labels[:-1, :] != labels[1:, :]
    boundary[:-1, :] |= diff_down
    boundary[1:, :] |= diff_down
    diff_right = labels[:, :-1] != labels[:, 1:]
    boundary[:, :-1] |= diff_right
    boundary[:, 1:] |= diff_right
    return int(boundary.sum())


def build_summary(tile, refined_labels: np.ndarray, instance_items: list) -> dict:
    total = int(refined_labels.size)
    classes = []
    # Every configured class is reported (never silently dropped); background
    # is always present even if it is not in the color map.
    for class_id in sorted(set(tile.class_ids) | {BACKGROUND_CLASS}):
        pixels = int((refined_labels == class_id).sum())
        default_name = "background" if class_id == BACKGROUND_CLASS else f"class_{class_id}"
        classes.append({
            "class_id": class_id,
            "name": tile.class_names.get(class_id, default_name),
            "pixels": pixels,
            "ratio": round6(pixels / total),
        })
    items = [dict(item) for item in instance_items]
    return {
        "tile_id": tile.tile_id,
        "engine_version": ENGINE_VERSION,
        "width": int(refined_labels.shape[1]),
        "height": int(refined_labels.shape[0]),
        "classes": classes,
        "instances": {"count": len(items), "items": items},
        "boundary_pixels": boundary_pixel_count(refined_labels),
    }


def write_semantic_mask(path, labels: np.ndarray, tile) -> None:
    build_palette_image(labels, tile).save(path, **PNG_SAVE_KWARGS)
