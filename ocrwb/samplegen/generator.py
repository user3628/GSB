"""Deterministic local sample generator.

All randomness comes from seeded :class:`random.Random` instances keyed by the
global seed and the shard index, so regenerating the dataset byte-for-byte
reproduces both the PNG shards and the JSON task manifest.
"""

from __future__ import annotations

import json
import os
import random

from ..glyphs import CELL_H, CELL_W, render_shard
from ..hashing import canonical_json

GROUPS = ("small", "medium", "large")

GROUP_SPECS = {
    "small": {"shards": 4, "widths": (96, 128), "heights": (72, 96)},
    "medium": {"shards": 16, "widths": (128, 176), "heights": (96, 128)},
    "large": {"shards": 40, "widths": (160, 224), "heights": (112, 160)},
}

EN_WORDS = ["OCR", "TEXT", "IMAGE", "SCAN", "PIXEL", "BENCH", "SAMPLE", "DETECT"]
ZH_CHARS = list("文字识别检测样本工作诊断数据分片图像模板匹配中英混排")
MIXED_WORDS = ["OCR文字", "TEXT识别", "图像SCAN", "检测BENCH", "SAMPLE数据"]

BAND_COLORS = [
    (226, 236, 255),
    (255, 238, 221),
    (224, 246, 230),
    (248, 231, 245),
    (238, 238, 248),
]
BACKGROUNDS = [
    (248, 248, 248),
    (245, 247, 250),
    (250, 248, 245),
]


def _make_line_text(rng: random.Random) -> tuple[str, str]:
    kind = rng.randrange(3)
    if kind == 0:
        return rng.choice(EN_WORDS), "en"
    if kind == 1:
        length = rng.randint(3, 5)
        return "".join(rng.choice(ZH_CHARS) for _ in range(length)), "zh"
    return rng.choice(MIXED_WORDS), "mixed"


def _line_language(text: str) -> str:
    if any("一" <= ch <= "鿿" for ch in text):
        return "mixed" if any(ch.isascii() and ch.isalpha() for ch in text) else "zh"
    return "en"


def _build_shard(index: int, seed: int) -> dict:
    rng = random.Random(seed * 1000003 + index)
    group = next(name for name in GROUPS if index < sum(GROUP_SPECS[g]["shards"] for g in GROUPS[: GROUPS.index(name) + 1]))
    spec = GROUP_SPECS[group]
    width = rng.randrange(spec["widths"][0], spec["widths"][1] + 1, 8)
    height = rng.randrange(spec["heights"][0], spec["heights"][1] + 1, 8)
    scale = rng.choice([1, 1, 2])
    line_h = CELL_H * scale + 8
    max_lines = max(2, (height - 8) // line_h)
    line_count = rng.randint(2, min(4, max_lines))
    lines: list[dict] = []
    y = 5
    for line_index in range(line_count):
        text, _ = _make_line_text(rng)
        scale_line = scale
        line_w = CELL_W * scale_line * len(text)
        if line_w > width - 10:
            text = text[: max(1, (width - 10) // (CELL_W * scale_line))]
            line_w = CELL_W * scale_line * len(text)
        tilt = rng.choice([-12, -8, 0, 0, 0, 8, 12])
        x = rng.randint(4, max(4, width - line_w - 2))
        lines.append(
            {
                "text": text,
                "scale": scale_line,
                "tilt": tilt,
                "band_color": rng.choice(BAND_COLORS),
                "x": x,
                "y": y,
            }
        )
        y += line_h
        if y + line_h > height:
            break
    image, boxes = render_shard(
        width,
        height,
        lines,
        background=rng.choice(BACKGROUNDS),
        text_color=(28, 28, 28),
    )
    image_id = f"shard_{index:04d}"
    path = f"images/{image_id}.png"
    image.save(path, format="PNG", compress_level=9)
    truth = []
    languages = set()
    for line, box in zip(lines, boxes):
        language = _line_language(line["text"])
        languages.add(language)
        truth.append(
            {
                "text": line["text"],
                "language": language,
                "tilt": line["tilt"],
                "box": {
                    "x0": round(box["x0"] / width, 6),
                    "y0": round(box["y0"] / height, 6),
                    "x1": round(box["x1"] / width, 6),
                    "y1": round(box["y1"] / height, 6),
                },
            }
        )
    return {
        "image_id": image_id,
        "path": path,
        "width": width,
        "height": height,
        "group": group,
        "languages": sorted(languages),
        "truth": truth,
        "char_count": sum(len(item["text"]) for item in truth),
    }


def generate_dataset(output_dir: str, seed: int = 20261121) -> dict:
    """Generate all shards plus the manifest under ``output_dir``."""
    images_dir = os.path.join(output_dir, "images")
    os.makedirs(images_dir, exist_ok=True)
    old_cwd = os.getcwd()
    os.chdir(output_dir)
    try:
        total = sum(spec["shards"] for spec in GROUP_SPECS.values())
        shards = [_build_shard(index, seed) for index in range(total)]
    finally:
        os.chdir(old_cwd)
    groups = []
    offset = 0
    for name in GROUPS:
        count = GROUP_SPECS[name]["shards"]
        members = shards[offset : offset + count]
        groups.append(
            {
                "name": name,
                "shard_count": count,
                "total_chars": sum(item["char_count"] for item in members),
                "image_ids": [item["image_id"] for item in members],
            }
        )
        offset += count
    manifest = {
        "manifest_version": "ocrwb-manifest-v1",
        "generator_seed": seed,
        "groups": groups,
        "shards": shards,
    }
    manifest_path = os.path.join(output_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as handle:
        handle.write(canonical_json(manifest))
    return manifest
