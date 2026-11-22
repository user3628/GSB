from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from .canonical import atomic_write_json, canonical_bytes, round_vector, sha256_bytes
from .config import Config
from .dataio import Dataset, load_dataset, write_dataset
from .stages import hash_embedding, normalize_text, tokenize_offsets


DEMO_TEXTS: tuple[dict[str, str], ...] = (
    {
        "text_id": "t01",
        "text": "多模态 fusion 需要可复现的版本指纹。",
        "lang": "zh-en",
        "source_channel": "primary_text",
    },
    {
        "text_id": "t02",
        "text": "Checkpoint restore keeps the local pipeline reproducible.",
        "lang": "en",
        "source_channel": "review_text",
    },
    {
        "text_id": "t03",
        "text": "增量失效与恢复一致性 comparative test。",
        "lang": "zh-en",
        "source_channel": "admin_text",
    },
    {
        "text_id": "t04",
        "text": "孤立片段 orphan fragment without any image.",
        "lang": "zh-en",
        "source_channel": "orphan_text",
    },
    {
        "text_id": "t05",
        "text": "Checkpoint restore keeps the local pipeline reproducible.",
        "lang": "en",
        "source_channel": "duplicate_text",
    },
)


DEMO_IMAGES: tuple[dict[str, Any], ...] = (
    {"image_id": "i01", "text_id": "t01", "source_operator": "vision", "target_cosine": 0.98, "noise_index": 1},
    {"image_id": "i02", "text_id": "t01", "source_operator": "caption", "target_cosine": -0.15, "noise_index": 2},
    {"image_id": "i03", "text_id": "t02", "source_operator": "ocr", "target_cosine": -0.98, "noise_index": 3},
    {"image_id": "i04", "text_id": "t03", "source_operator": "vision", "target_cosine": 0.90, "noise_index": 4},
    {"image_id": "i05", "text_id": "t02", "source_operator": "ocr", "target_cosine": 0.90, "noise_index": 5},
    {"image_id": "i06", "text_id": "t05", "source_operator": "caption", "target_cosine": 0.85, "noise_index": 6},
)


def _text_embedding(record: dict[str, str], config: Config) -> np.ndarray:
    normalized = normalize_text(record["text"])
    tokens = tokenize_offsets(normalized)
    return np.asarray(hash_embedding(tokens, config.hash_salt), dtype=np.float64)


def _feature_for_target(
    text_embedding: np.ndarray,
    matrix: np.ndarray,
    target_cosine: float,
    noise_index: int,
    seed: int,
) -> np.ndarray:
    target = text_embedding / np.linalg.norm(text_embedding)
    rng = np.random.default_rng(seed + noise_index * 7919)
    noise = rng.normal(size=target.shape[0])
    noise -= np.dot(noise, target) * target
    noise /= np.linalg.norm(noise)
    amount = max(0.0, 1.0 - target_cosine * target_cosine) ** 0.5
    projected_target = target * target_cosine + noise * amount
    projected_target /= np.linalg.norm(projected_target)
    inverse = np.linalg.pinv(matrix)
    feature = inverse @ projected_target
    return np.asarray(round_vector(feature, 12), dtype=np.float64)


def _write_png(path: Path, feature: np.ndarray, index: int, seed: int) -> None:
    rng = np.random.default_rng(seed + 104729 * index)
    width, height = 24, 24
    base = np.clip((feature[:3] + 0.03) * 255.0, 0, 255).astype(np.uint8)
    texture = rng.integers(0, 32, size=(height, width, 3), dtype=np.uint8)
    pixels = np.clip(texture + base.reshape(1, 1, 3), 0, 255).astype(np.uint8)
    image = Image.fromarray(pixels, mode="RGB")
    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path, format="PNG", optimize=False)


def generate_demo(data_dir: str | Path, seed: int, config: Config) -> Dataset:
    root = Path(data_dir)
    root.mkdir(parents=True, exist_ok=True)
    (root / "png").mkdir(parents=True, exist_ok=True)
    matrix = np.asarray(config.matrix, dtype=np.float64)
    text_map = {record["text_id"]: record for record in DEMO_TEXTS}
    features: dict[str, np.ndarray] = {}
    for index, image_spec in enumerate(DEMO_IMAGES, start=1):
        text_embedding = _text_embedding(text_map[str(image_spec["text_id"])], config)
        feature = _feature_for_target(
            text_embedding,
            matrix,
            float(image_spec["target_cosine"]),
            int(image_spec["noise_index"]),
            seed,
        )
        features[str(image_spec["image_id"])] = feature
        _write_png(root / "png" / f"{image_spec['image_id']}.png", feature, index, seed)

    images: list[dict[str, Any]] = []
    for index, image_spec in enumerate(DEMO_IMAGES, start=1):
        image_id = str(image_spec["image_id"])
        images.append(
            {
                "image_id": image_id,
                "png": f"png/{image_id}.png",
                "feature": round_vector(features[image_id], 12),
                "text_id": image_spec["text_id"],
                "source_operator": image_spec["source_operator"],
                "sample_order": index,
            }
        )
    # The loader deliberately keeps only the business fields. sample_order is
    # removed by the canonical writer so the visible fixture stays compact.
    compact_images = [
        {key: value for key, value in record.items() if key != "sample_order"}
        for record in images
    ]
    write_dataset(root, list(DEMO_TEXTS), compact_images)
    dataset = load_dataset(root)
    file_hashes = {
        "texts.jsonl": sha256_bytes((root / "texts.jsonl").read_bytes()),
        "images.json": sha256_bytes((root / "images.json").read_bytes()),
    }
    file_hashes.update(
        {
            f"png/{image['image_id']}.png": sha256_bytes((root / str(image["png"])).read_bytes())
            for image in dataset.images
        }
    )
    manifest = {
        "generator": "mmcoop.generator.v1",
        "seed": seed,
        "text_count": len(dataset.texts),
        "image_count": len(dataset.images),
        "one_to_many_text_ids": sorted(
            {text_id for text_id in (image.get("text_id") for image in dataset.images) if text_id and sum(1 for image in dataset.images if image.get("text_id") == text_id) > 1}
        ),
        "missing_link_text_ids": sorted(
            set(dataset.text_map()) - {str(image.get("text_id")) for image in dataset.images if image.get("text_id") is not None}
        ),
        "duplicate_content_groups": list(dataset.duplicate_content_groups),
        "file_sha256": dict(sorted(file_hashes.items())),
    }
    manifest["manifest_content_sha256"] = hashlib.sha256(canonical_bytes(manifest)).hexdigest()
    atomic_write_json(root / "manifest.json", manifest, indent=2)
    return dataset
