from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw

from .canonical import atomic_write_bytes, atomic_write_json, digest_file, digest_value
from .config import DEFAULT_CONFIG, normalize_config


SCALES = {
    "small": {"shot_count": 5, "seed": 1307, "width": 96, "height": 54},
    "large": {"shot_count": 36, "seed": 7301, "width": 96, "height": 54},
}


PROMPT_FRAGMENTS = [
    "雨后的霓虹街角，slow orbital light",
    "纸鹤掠过窗台，gentle morning haze",
    "旧车站的长镜头，warm tungsten glow",
    "海面微光与远山，quiet cinematic depth",
    "未来市场的人流，vivid teal and amber",
    "山谷里的薄雾，soft volumetric light",
]


def _frame_image(seed: int, index: int, width: int, height: int) -> Image.Image:
    rng = np.random.default_rng(seed + index * 7919)
    yy, xx = np.indices((height, width), dtype=np.float32)
    phase = (index * 0.37) % 1.0
    red = (xx / max(width - 1, 1) * 190 + seed % 47 + 24) % 256
    green = (yy / max(height - 1, 1) * 170 + index * 13 + 36) % 256
    blue = ((xx + yy) / max(width + height - 2, 1) * 210 + phase * 90 + 20) % 256
    array = np.stack([red, green, blue], axis=-1)
    wave = (np.sin(xx * 0.14 + index) * 16 + np.cos(yy * 0.19 - index) * 10).astype(np.float32)
    array = np.clip(array + wave[..., None], 0, 255).astype(np.uint8)
    image = Image.fromarray(array)
    draw = ImageDraw.Draw(image)
    for _ in range(9):
        x0 = int(rng.integers(0, width))
        y0 = int(rng.integers(0, height))
        x1 = min(width - 1, x0 + int(rng.integers(4, 24)))
        y1 = min(height - 1, y0 + int(rng.integers(3, 18)))
        color = tuple(int(value) for value in rng.integers(24, 241, size=3))
        draw.rectangle([x0, y0, x1, y1], outline=color, width=1)
    for _ in range(5):
        cx = int(rng.integers(0, width))
        cy = int(rng.integers(0, height))
        radius = int(rng.integers(2, 9))
        color = tuple(int(value) for value in rng.integers(32, 235, size=3))
        draw.ellipse([cx - radius, cy - radius, cx + radius, cy + radius], fill=color)
    draw.line([0, height // 2 + index % 5, width - 1, height // 3], fill=(245, 236, 190), width=1)
    return image


def _png_bytes(image: Image.Image) -> bytes:
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", compress_level=9, optimize=False)
    return buffer.getvalue()


def sample_records(scale: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if scale not in SCALES:
        raise ValueError(f"unknown scale {scale}")
    spec = SCALES[scale]
    count = int(spec["shot_count"])
    moves = ["static", "pan", "zoom"]
    fps_values = [24, 29.97, 25, 30, 23.976]
    transitions = ["crossfade", "hard_cut", "crossfade", "hard_cut", "crossfade"]
    records: list[dict[str, Any]] = []
    for index in range(count):
        duration = 10 + ((index * 7 + int(spec["seed"])) % 9)
        records.append(
            {
                "shot_id": f"{scale}-shot-{index:03d}",
                "image": f"shot_{index:03d}.png",
                "prompt": f"{PROMPT_FRAGMENTS[index % len(PROMPT_FRAGMENTS)]} / take {index:02d}",
                "camera_move": moves[index % len(moves)],
                "duration_frames": duration,
                "fps": fps_values[index % len(fps_values)],
                "transition": transitions[index % len(transitions)],
            }
        )
    config = normalize_config(DEFAULT_CONFIG)
    return records, config


def generate_scale(scale: str, destination: Path) -> dict[str, Any]:
    records, config = sample_records(scale)
    spec = SCALES[scale]
    destination.mkdir(parents=True, exist_ok=True)
    images_dir = destination / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    for index, record in enumerate(records):
        image = _frame_image(int(spec["seed"]), index, int(spec["width"]), int(spec["height"]))
        atomic_write_bytes(images_dir / record["image"], _png_bytes(image))
    script_bytes = b"".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        + b"\n"
        for record in records
    )
    atomic_write_bytes(destination / "shots.jsonl", script_bytes)
    atomic_write_json(destination / "config.json", config)
    digest_records = {
        "script": digest_file(destination / "shots.jsonl"),
        "config": digest_file(destination / "config.json"),
        "images": {
            record["image"]: digest_file(images_dir / record["image"])
            for record in records
        },
    }
    manifest = {
        "format": "storypipe-sample-v1",
        "scale": scale,
        "seed": spec["seed"],
        "shot_count": len(records),
        "width": spec["width"],
        "height": spec["height"],
        "digests": digest_records,
        "sample_digest": digest_value(digest_records),
    }
    atomic_write_json(destination / "sample_manifest.json", manifest)
    return manifest


def generate_samples(root: Path) -> dict[str, Any]:
    manifests = {}
    for scale in SCALES:
        manifests[scale] = generate_scale(scale, root / scale)
    aggregate = {
        "format": "storypipe-sample-set-v1",
        "scales": manifests,
        "set_digest": digest_value(
            {scale: manifest["sample_digest"] for scale, manifest in manifests.items()}
        ),
    }
    atomic_write_json(root / "sample_set_manifest.json", aggregate)
    return aggregate


def stable_manifest(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
