"""Deterministic local PNG + JSONL sample generator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw

from .canonical import atomic_write_bytes, atomic_write_json, digest_obj


PALETTE = [
    ("red", "红色", (210, 48, 48), (245, 126, 90)),
    ("orange", "橙色", (226, 126, 42), (250, 190, 92)),
    ("yellow", "黄色", (220, 196, 58), (250, 236, 120)),
    ("green", "绿色", (52, 160, 92), (126, 220, 146)),
    ("cyan", "青色", (52, 174, 188), (126, 228, 232)),
    ("blue", "蓝色", (54, 92, 204), (116, 158, 240)),
    ("purple", "紫色", (126, 64, 182), (184, 126, 220)),
    ("magenta", "洋红色", (192, 64, 126), (238, 126, 176)),
]
SHAPES = [
    ("square", "方块", "square"),
    ("circle", "圆形", "circle"),
    ("triangle", "三角", "triangle"),
    ("stripes", "条纹", "stripes"),
    ("gradient", "渐变", "gradient"),
]


def _row_text(primary: tuple[str, str, tuple[int, int, int], tuple[int, int, int]],
              secondary: tuple[str, str, tuple[int, int, int], tuple[int, int, int]],
              shape: tuple[str, str, str], bright: bool) -> str:
    zh_primary, en_primary = primary[1], primary[0]
    zh_secondary, en_secondary = secondary[1], secondary[0]
    zh_shape, en_shape = shape[1], shape[0]
    if bright:
        light_zh, light_en = "明亮", "bright"
    else:
        light_zh, light_en = "深色", "dark"
    return (
        f"{zh_primary} {en_primary} {zh_shape} {en_shape}，"
        f"配 {zh_secondary} {en_secondary} {light_zh} {light_en} 视角"
    )


def _make_image(
    width: int,
    height: int,
    primary: tuple[int, int, int],
    secondary: tuple[int, int, int],
    shape_code: str,
    bright: bool,
) -> np.ndarray:
    y = np.linspace(0.0, 1.0, height, dtype=np.float64)[:, None, None]
    x = np.linspace(0.0, 1.0, width, dtype=np.float64)[None, :, None]
    mix = np.clip(0.65 * x + 0.35 * y, 0.0, 1.0)
    first = np.asarray(primary, dtype=np.float64).reshape(1, 1, 3)
    second = np.asarray(secondary, dtype=np.float64).reshape(1, 1, 3)
    pixels = first * (1.0 - mix) + second * mix
    image = Image.fromarray(np.rint(pixels).astype(np.uint8), mode="RGB")
    draw = ImageDraw.Draw(image)
    margin_x = max(4, width // 8)
    margin_y = max(4, height // 8)
    box = (margin_x, margin_y, width - margin_x - 1, height - margin_y - 1)
    accent = tuple(int(value) for value in (np.asarray(secondary) * 0.75).tolist())
    outline = tuple(int(value) for value in (np.asarray(primary) * 0.45).tolist())
    if shape_code == "square":
        draw.rectangle(box, fill=accent, outline=outline, width=2)
    elif shape_code == "circle":
        draw.ellipse(box, fill=accent, outline=outline, width=2)
    elif shape_code == "triangle":
        draw.polygon(
            [(width // 2, margin_y), (width - margin_x - 1, height - margin_y - 1), (margin_x, height - margin_y - 1)],
            fill=accent,
            outline=outline,
        )
    elif shape_code == "stripes":
        stripe_width = max(3, width // 10)
        for offset in range(margin_x, width - margin_x, stripe_width * 2):
            draw.rectangle(
                [offset, margin_y, min(offset + stripe_width, width - margin_x), height - margin_y],
                fill=accent,
            )
    else:
        draw.line(
            [(margin_x, height - margin_y - 1), (width - margin_x - 1, margin_y)],
            fill=accent,
            width=max(3, height // 10),
        )
    array = np.asarray(image, dtype=np.float64)
    factor = 0.58 if not bright else 1.0
    array = np.clip(array * factor, 0.0, 255.0)
    return np.rint(array).astype(np.uint8)


def generate_dataset(
    output_dir: str | Path,
    *,
    seed: int = 20261122,
    shards: int = 3,
    items_per_shard: int = 6,
    width: int = 64,
    height: int = 48,
) -> dict[str, Any]:
    if shards < 3:
        raise ValueError("shards must be >= 3")
    if items_per_shard < 2:
        raise ValueError("items_per_shard must be >= 2")
    root = Path(output_dir).resolve()
    (root / "shards").mkdir(parents=True, exist_ok=True)
    shard_entries: list[dict[str, str]] = []
    combined_rows: list[dict[str, Any]] = []
    scene_counter = 0
    for shard_index in range(shards):
        shard_id = f"shard-{shard_index:02d}"
        shard_dir = root / "shards" / shard_id
        images_dir = shard_dir / "images"
        images_dir.mkdir(parents=True, exist_ok=True)
        rows: list[dict[str, str]] = []
        for item_index in range(items_per_shard):
            item_id = f"{shard_id}-item-{item_index:03d}"
            primary = PALETTE[(scene_counter * 3 + shard_index + item_index) % len(PALETTE)]
            secondary = PALETTE[(scene_counter * 5 + shard_index + item_index + 3) % len(PALETTE)]
            if secondary[0] == primary[0]:
                secondary = PALETTE[(PALETTE.index(secondary) + 1) % len(PALETTE)]
            shape = SHAPES[(scene_counter + shard_index) % len(SHAPES)]
            bright = item_index % 2 == 0
            if item_index % 3 == 0:
                # Exercise a fixed pair of bright/dark views with identical geometry.
                bright = True
            elif item_index % 3 == 1:
                bright = False
            pixels = _make_image(width, height, primary[2], secondary[2], shape[2], bright)
            relative = Path("shards") / shard_id / "images" / f"{item_id}.png"
            absolute = root / relative
            # Pillow's PNG encoder has no time-dependent chunk in this workflow.
            from io import BytesIO

            buffer = BytesIO()
            Image.fromarray(pixels, mode="RGB").save(buffer, format="PNG", optimize=False)
            atomic_write_bytes(absolute, buffer.getvalue())
            caption = _row_text(primary, secondary, shape, bright)
            row = {
                "item_id": item_id,
                "image_file": relative.as_posix(),
                "caption": caption,
                "lang": "zh-en",
                "source": f"synthetic-seed-{seed}",
            }
            rows.append(row)
            combined_rows.append({**row, "shard_id": shard_id})
            scene_counter += 1
        manifest_relative = Path("shards") / shard_id / "data.jsonl"
        manifest_bytes = "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows
        ).encode("utf-8")
        atomic_write_bytes(root / manifest_relative, manifest_bytes)
        shard_entries.append(
            {"shard_id": shard_id, "manifest": manifest_relative.as_posix()}
        )
    descriptor = {
        "schema": "cmir.dataset.v1",
        "seed": seed,
        "shards": shard_entries,
    }
    atomic_write_json(root / "dataset.json", descriptor)
    combined_bytes = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in combined_rows
    ).encode("utf-8")
    atomic_write_bytes(root / "manifest.jsonl", combined_bytes)
    return {
        "ok": True,
        "dataset_dir": str(root),
        "seed": seed,
        "shards": shards,
        "items_per_shard": items_per_shard,
        "image_size": [width, height],
        "item_count": len(combined_rows),
        "digest": digest_obj(
            {
                "dataset": descriptor,
                "items": combined_rows,
                "images": [
                    {
                        "item_id": row["item_id"],
                        "image_sha256": digest_of_item(root / row["image_file"]),
                    }
                    for row in combined_rows
                ],
            }
        ),
    }


def digest_of_item(path: Path) -> str:
    from .canonical import digest_file

    return digest_file(path)
