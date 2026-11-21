"""Fixed-seed local shard and manifest generator (no external data required)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from .util import digest_obj, sha256_file, write_atomic


DEFAULT_SEED = 20261122
IMAGE_SIZE = 64


def _base_gradient(size: int, start: tuple[float, float, float], end: tuple[float, float, float], *, diagonal: bool = False) -> np.ndarray:
    y, x = np.mgrid[0:size, 0:size]
    t = (x + y) / (2.0 * (size - 1)) if diagonal else y / (size - 1)
    t = t[:, :, None]
    start_array = np.asarray(start, dtype=np.float64).reshape(1, 1, 3) / 255.0
    end_array = np.asarray(end, dtype=np.float64).reshape(1, 1, 3) / 255.0
    return start_array * (1.0 - t) + end_array * t


def _rectangle(image: np.ndarray, bounds: tuple[int, int, int, int], color: tuple[int, int, int]) -> None:
    x0, y0, x1, y1 = bounds
    image[y0:y1, x0:x1, :] = np.asarray(color, dtype=np.float64) / 255.0


def _circle(image: np.ndarray, center: tuple[int, int], radius: int, color: tuple[int, int, int]) -> None:
    cy, cx = center
    y, x = np.mgrid[0 : image.shape[0], 0 : image.shape[1]]
    mask = (x - cx) ** 2 + (y - cy) ** 2 <= radius**2
    image[mask, :] = np.asarray(color, dtype=np.float64) / 255.0


def _diagonal_band(image: np.ndarray, width: int, color: tuple[int, int, int], *, offset: int = 0) -> None:
    y, x = np.mgrid[0 : image.shape[0], 0 : image.shape[1]]
    mask = ((x + y + offset) // width) % 2 == 0
    image[mask, :] = np.asarray(color, dtype=np.float64) / 255.0


def _shade(image: np.ndarray, factor: float) -> np.ndarray:
    return np.clip(image * factor, 0.0, 1.0)


def _scene(index: int, size: int = IMAGE_SIZE) -> np.ndarray:
    if index in (1, 6):
        image = _base_gradient(size, (18, 42, 146), (64, 184, 244), diagonal=True)
        _rectangle(image, (8, 10, 37, 45), (220, 48, 55))
        _circle(image, (48, 27), 7, (250, 216, 94))
        return _shade(image, 1.16 if index == 6 else 1.0)
    if index == 2:
        image = _base_gradient(size, (7, 15, 42), (57, 91, 126))
        _rectangle(image, (0, 38, 64, 64), (15, 24, 38))
        _circle(image, (24, 43), 11, (222, 232, 226))
        return image
    if index == 3:
        image = _base_gradient(size, (62, 132, 89), (232, 190, 116), diagonal=True)
        _rectangle(image, (0, 0, 19, 25), (24, 114, 65))
        _rectangle(image, (0, 48, 64, 64), (236, 154, 82))
        return image
    if index in (4, 8):
        image = _base_gradient(size, (196, 104, 46), (240, 203, 99), diagonal=True)
        _diagonal_band(image, 9, (89, 46, 32))
        _circle(image, (47, 17), 8, (252, 245, 199))
        return image
    if index == 5:
        image = _base_gradient(size, (74, 22, 128), (208, 90, 178), diagonal=True)
        _rectangle(image, (0, 0, 7, 64), (239, 188, 70))
        _rectangle(image, (57, 0, 64, 64), (236, 206, 93))
        _circle(image, (32, 32), 12, (36, 20, 66))
        return image
    if index == 7:
        image = _base_gradient(size, (15, 135, 158), (97, 219, 215))
        for x0 in range(0, size, 16):
            _rectangle(image, (x0, 0, min(x0 + 7, size), size), (190, 241, 226))
        _circle(image, (45, 42), 10, (235, 119, 32))
        return image
    raise ValueError(f"unknown scene index {index}")


def _write_png(path: Path, image: np.ndarray) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    rgb = np.clip(np.rint(image * 255.0), 0, 255).astype(np.uint8)
    pil_image = Image.fromarray(rgb, mode="RGB")
    tmp = path.with_suffix(".png.tmp")
    pil_image.save(tmp, format="PNG", optimize=False, compress_level=6)
    tmp.replace(path)
    return sha256_file(path)


def generate_samples(output_dir: str | Path, *, seed: int = DEFAULT_SEED) -> dict[str, Any]:
    root = Path(output_dir)
    images_dir = root / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    # The RNG is intentionally instantiated even though scene geometry is anchored to it.
    rng = np.random.default_rng(seed)
    brightness_jitter = rng.uniform(-0.004, 0.004, size=8)
    rows: list[dict[str, Any]] = []
    definitions = [
        ("item-01", 1, "红色方块与蓝色渐变 red block over blue gradient", "zh-en", "shard-00"),
        ("item-02", 2, "冷色暗部与白色圆形 cool shadow, pale circle", "en-zh", "shard-01"),
        ("item-03", 3, "绿色转角与暖色地平线 green corner + warm horizon", "zh-en", "shard-02"),
        ("item-04", 4, "暖色斜线与明暗对比 warm diagonals and strong contrast", "en-zh", "shard-03"),
        ("item-05", 5, "紫罗兰渐变遇见金色边缘 violet gradient meets golden edge", "zh-en", "shard-04"),
        ("item-06", 6, "近似重复：红色方块 blue gradient brighter view", "zh-en", "shard-05"),
        ("item-07", 7, "青色竖条与橙色圆点 cyan stripes 和 orange dot", "zh-en", "shard-06"),
        ("item-08", 8, "", "zh-en", "shard-07"),
    ]
    for position, (item_id, scene_index, caption, lang, source) in enumerate(definitions):
        image = _scene(scene_index)
        image = _shade(image, 1.0 + float(brightness_jitter[position]))
        image_file = f"images/{item_id}.png"
        image_sha = _write_png(root / image_file, image)
        row: dict[str, Any] = {
            "item_id": item_id,
            "image_file": image_file,
            "caption": caption,
            "lang": lang,
            "source": source,
        }
        if item_id == "item-08":
            row["enabled"] = False
            row["expected_error"] = "E_EMPTY_CAPTION"
        else:
            row["enabled"] = True
        row["_image_sha256"] = image_sha
        rows.append(row)

    public_rows = [{key: value for key, value in row.items() if not key.startswith("_")} for row in rows]
    manifest_bytes = b"".join(
        (json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        for row in public_rows
    )
    write_atomic(root / "manifest.jsonl", manifest_bytes)
    strict_rows = [row for row in public_rows if row.get("enabled", True)]
    strict_bytes = b"".join(
        (json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        for row in strict_rows
    )
    write_atomic(root / "manifest_strict.jsonl", strict_bytes)

    summary = {
        "format": "shardseek-sample-generator-v1",
        "seed": seed,
        "image_size": [IMAGE_SIZE, IMAGE_SIZE],
        "image_count": len(rows),
        "enabled_count": len(strict_rows),
        "quarantined_count": len(rows) - len(strict_rows),
        "manifest_sha256": digest_obj(public_rows),
        "manifest_bytes_sha256": sha256_file(root / "manifest.jsonl"),
        "strict_manifest_sha256": digest_obj(strict_rows),
        "images": [
            {
                "item_id": row["item_id"],
                "source": row["source"],
                "image_file": row["image_file"],
                "sha256": row["_image_sha256"],
            }
            for row in rows
        ],
    }
    write_atomic(root / "generation_summary.json", (json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8"))
    return summary

