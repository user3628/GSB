"""Shared deterministic image / label-map IO and neighborhood helpers."""

from __future__ import annotations

import numpy as np
from PIL import Image

PNG_SAVE_KWARGS = {"format": "PNG", "compress_level": 6}


def read_rgb(path) -> np.ndarray:
    with Image.open(path) as img:
        return np.asarray(img.convert("RGB"), dtype=np.uint8)


def write_label_map(path, labels: np.ndarray) -> None:
    """Persist a class-id label map as an 8-bit grayscale PNG."""
    Image.fromarray(labels.astype(np.uint8), mode="L").save(path, **PNG_SAVE_KWARGS)


def read_label_map(path) -> np.ndarray:
    with Image.open(path) as img:
        return np.asarray(img, dtype=np.uint8)


def write_instance_mask(path, mask: np.ndarray) -> None:
    """Persist an instance-id mask as a 16-bit grayscale PNG."""
    Image.fromarray(mask.astype(np.uint16)).save(path, **PNG_SAVE_KWARGS)


def read_instance_mask(path) -> np.ndarray:
    with Image.open(path) as img:
        return np.asarray(img, dtype=np.uint16)


def neighbor_count_map(indicator: np.ndarray, connectivity: int = 8) -> np.ndarray:
    """Count, per pixel, how many of its neighbors are set in ``indicator``."""
    src = indicator.astype(np.int32)
    count = np.zeros_like(src)
    count[:-1, :] += src[1:, :]
    count[1:, :] += src[:-1, :]
    count[:, :-1] += src[:, 1:]
    count[:, 1:] += src[:, :-1]
    if connectivity == 8:
        count[:-1, :-1] += src[1:, 1:]
        count[1:, 1:] += src[:-1, :-1]
        count[:-1, 1:] += src[1:, :-1]
        count[1:, :-1] += src[:-1, 1:]
    return count


def valid_neighbor_count(shape, connectivity: int = 8) -> np.ndarray:
    """Number of existing neighbors per pixel (border pixels have fewer)."""
    return neighbor_count_map(np.ones(shape, dtype=np.int32), connectivity)
