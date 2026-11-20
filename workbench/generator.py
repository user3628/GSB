from __future__ import annotations

import hashlib
import io
import random
import shutil
from pathlib import Path
from typing import Any

from PIL import Image

from .config import write_default_config
from .util import atomic_write_bytes, atomic_write_json, ensure_dir, read_json, relpath, stable_round


DEFAULT_SEED = 20261121
DEFAULT_CLASSES = ["cat", "dog", "vehicle", "indoor", "outdoor", "day", "night"]


SAMPLE_SPECS: list[dict[str, Any]] = [
    {
        "image_id": "s01_dual",
        "labels": ["cat", "indoor"],
        "boosts": {"cat": 0.58, "indoor": 0.52, "vehicle": 0.50},
        "purpose": "合法双标签；同时包含一个高分误报标签 vehicle",
    },
    {
        "image_id": "s02_single",
        "labels": ["vehicle"],
        "boosts": {"vehicle": 0.55},
        "miss_labels": ["vehicle"],
        "purpose": "合法单标签；车辆分数高于默认阈值但低于真值下界，产生漏报",
    },
    {
        "image_id": "s03_empty",
        "labels": [],
        "boosts": {},
        "purpose": "空标签合法样本；所有分数低于默认阈值",
    },
    {
        "image_id": "s04_unknown",
        "labels": ["cat"],
        "extra_truth": [{"label": "unicorn", "lower_bound": 0.5}],
        "boosts": {"cat": 0.52},
        "purpose": "真值含类别表外标签，触发 E_UNKNOWN_LABEL",
    },
    {
        "image_id": "s05_nan",
        "labels": ["cat"],
        "boosts": {"cat": 0.52},
        "numeric_anomaly": {"label": "cat", "value": "NaN"},
        "purpose": "打分表含 NaN，触发 E_NUMERIC",
    },
    {
        "image_id": "s06_dim_mismatch",
        "labels": ["outdoor"],
        "boosts": {"outdoor": 0.54},
        "declared_size_delta": [7, 2],
        "purpose": "清单声明尺寸与实际 PNG 尺寸不符，触发 E_DIM_MISMATCH",
    },
    {
        "image_id": "s07_conflict",
        "labels": ["cat", "dog"],
        "boosts": {"cat": 0.56, "dog": 0.55},
        "purpose": "预测同时命中互斥标签 cat/dog，产生 E_LABEL_CONFLICT 警告",
    },
    {
        "image_id": "s08_multi",
        "labels": ["day", "outdoor"],
        "boosts": {"day": 0.55, "outdoor": 0.53, "vehicle": 0.48},
        "purpose": "合法双标签；含一个 vehicle 误报用于指标与共现统计",
    },
]


def generate_demo(root: Path, *, seed: int = DEFAULT_SEED, clean: bool = False) -> dict[str, Any]:
    """Generate deterministic PNG files and a self-describing local manifest."""

    root = root.resolve()
    samples_dir = root / "samples"
    images_dir = samples_dir / "images"
    configs_dir = root / "configs"
    ensure_dir(samples_dir)
    ensure_dir(configs_dir)
    ensure_dir(root / "checkpoints")
    ensure_dir(root / "reports")
    if clean and images_dir.exists():
        shutil.rmtree(images_dir)
    ensure_dir(images_dir)
    if clean:
        for stale in (samples_dir / "manifest.json", configs_dir / "injection.json"):
            if stale.exists():
                stale.unlink()
    write_default_config(root, configs_dir / "default.json")
    config_path = configs_dir / "default.json"
    config_data = read_json(config_path)
    config_data.setdefault("dataset", {})["manifest"] = "samples/manifest.json"
    config_data.setdefault("dataset", {})["injection"] = "configs/injection.json"
    atomic_write_json(config_path, config_data, pretty=True)
    if not (configs_dir / "injection.json").exists():
        atomic_write_json(configs_dir / "injection.json", {"format": 1, "operations": []}, pretty=True)

    samples: list[dict[str, Any]] = []
    for index, spec in enumerate(SAMPLE_SPECS):
        image_id = spec["image_id"]
        width, height = 32, 32
        image_bytes = _render_png(seed, index, width, height)
        image_path = images_dir / f"{image_id}.png"
        atomic_write_bytes(image_path, image_bytes)
        image_sha = hashlib.sha256(image_bytes).hexdigest()

        boosts = dict(spec.get("boosts", {}))
        scores: dict[str, Any] = {}
        for label in DEFAULT_CLASSES:
            score = _pseudo_score(image_sha, label, seed) + float(boosts.get(label, 0.0))
            scores[label] = stable_round(min(0.99, max(0.01, score)), 6)

        truth: list[dict[str, Any]] = []
        for label in spec.get("labels", []):
            score = float(scores[label])
            if label in set(spec.get("miss_labels", [])):
                lower = min(0.99, score + 0.08)
            else:
                lower = max(0.01, score - 0.08)
            truth.append({"label": label, "lower_bound": stable_round(lower, 6)})
        truth.extend(spec.get("extra_truth", []))

        numeric = spec.get("numeric_anomaly")
        if numeric:
            scores[numeric["label"]] = numeric["value"]

        delta = spec.get("declared_size_delta", [0, 0])
        declared_width = width + int(delta[0])
        declared_height = height + int(delta[1])
        samples.append(
            {
                "image_id": image_id,
                "path": f"images/{image_id}.png",
                "width": declared_width,
                "height": declared_height,
                "truth": truth,
                "scores": scores,
                "purpose": spec.get("purpose", ""),
            }
        )

    manifest = {
        "schema_version": 1,
        "dataset_id": "deterministic-cv-demo-v1",
        "seed": seed,
        "source": {
            "generator": "workbench.generator",
            "rule": "stdlib-random-pixels + sha256-seeded pseudo-inference",
        },
        "classes": DEFAULT_CLASSES,
        "samples": samples,
    }
    manifest_path = samples_dir / "manifest.json"
    atomic_write_json(manifest_path, manifest, pretty=True)
    (samples_dir / "README.md").write_text(
        "# Deterministic demo samples\n\n"
        "Generated by `python -m workbench generate`. PNG bytes, truth tables, and\n"
        "pseudo-inference score tables are stable for the configured seed.\n",
        encoding="utf-8",
    )
    return {
        "dataset_id": manifest["dataset_id"],
        "seed": seed,
        "manifest": relpath(root, manifest_path),
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "sample_count": len(samples),
        "samples": [sample["image_id"] for sample in samples],
    }


def _render_png(seed: int, index: int, width: int, height: int) -> bytes:
    rng = random.Random(f"pixels:{seed}:{index}")
    palettes = [
        (216, 84, 72),
        (52, 140, 211),
        (84, 184, 112),
        (218, 166, 62),
        (142, 98, 201),
        (45, 177, 173),
        (202, 81, 149),
        (106, 128, 65),
    ]
    base = palettes[index % len(palettes)]
    pixels: list[list[tuple[int, int, int]]] = []
    for y in range(height):
        row: list[tuple[int, int, int]] = []
        for x in range(width):
            checker = 18 if ((x // 4) + (y // 4)) % 2 == 0 else -12
            noise = rng.randint(-22, 22)
            row.append(
                tuple(
                    max(0, min(255, channel + checker + noise))
                    for channel in base
                )
            )
        pixels.append(row)
    for y in range(6, 14):
        for x in range(6, 14):
            pixels[y][x] = (245, 245, 245)
    for y in range(height // 2 - 1, height // 2 + 2):
        for x in range(width // 2 - 1, width // 2 + 2):
            pixels[y][x] = (18, 18, 22)
    image = Image.new("RGB", (width, height))
    image.putdata([channel for row in pixels for channel in row])
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", compress_level=9, optimize=False)
    return buffer.getvalue()


def _pseudo_score(image_sha256: str, label: str, seed: int) -> float:
    material = f"pseudo-inference:{seed}:{image_sha256}:{label}".encode("utf-8")
    integer = int.from_bytes(hashlib.sha256(material).digest()[:8], "big")
    unit = integer / float(2**64)
    return 0.04 + 0.30 * unit


__all__ = ["DEFAULT_CLASSES", "DEFAULT_SEED", "SAMPLE_SPECS", "generate_demo"]
