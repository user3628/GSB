"""Deterministic local sample generator.

Everything is produced by a seeded ``numpy.random.Generator``; no network, no
real dataset and no pretrained model are involved.  The dataset contains:

* 6 images of varying sizes
* 3 object categories and 3 candidate source operators
* one strong candidate and one duplicate candidate per ground truth
* high-score background candidates and low-confidence noise candidates
* a cross-image duplicate block: the same normalized candidate geometry is
  emitted on two different images by the tracking operator
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

CATEGORIES = ["person", "car", "dog"]
SOURCES = ["op_rpn", "op_fpn", "op_track"]
SIZES = [
    (640, 480),
    (1280, 720),
    (800, 600),
    (1024, 768),
    (640, 640),
    (960, 540),
]
DEFAULT_SEED = 20260520


def _clamp_box(x: float, y: float, w: float, h: float) -> list[float]:
    w = min(max(w, 0.02), 0.9)
    h = min(max(h, 0.02), 0.9)
    x = min(max(x, 0.0), 1.0 - w)
    y = min(max(y, 0.0), 1.0 - h)
    return [round(x, 6), round(y, 6), round(w, 6), round(h, 6)]


def _jitter(rng: np.random.Generator, bbox: list[float], shift: float,
            scale_lo: float, scale_hi: float) -> list[float]:
    x, y, w, h = bbox
    return _clamp_box(
        x + float(rng.uniform(-shift, shift)),
        y + float(rng.uniform(-shift, shift)),
        w * float(rng.uniform(scale_lo, scale_hi)),
        h * float(rng.uniform(scale_lo, scale_hi)),
    )


def generate_dataset(seed: int = DEFAULT_SEED, num_images: int = 6) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    images: list[dict[str, Any]] = []

    for image_index in range(num_images):
        width, height = SIZES[image_index % len(SIZES)]
        image_id = f"img_{image_index:03d}"
        num_gt = int(rng.integers(2, 5))
        ground_truth: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []

        for gt_index in range(num_gt):
            category = CATEGORIES[int(rng.integers(0, len(CATEGORIES)))]
            bbox = _clamp_box(
                float(rng.uniform(0.02, 0.7)),
                float(rng.uniform(0.02, 0.7)),
                float(rng.uniform(0.12, 0.28)),
                float(rng.uniform(0.12, 0.28)),
            )
            ground_truth.append(
                {
                    "gt_id": f"{image_id}_gt{gt_index}",
                    "category": category,
                    "bbox": bbox,
                }
            )

            # Strong candidate: small jitter, high score -> likely match.
            primary = _jitter(rng, bbox, 0.012, 0.94, 1.06)
            candidates.append(
                {
                    "candidate_id": f"{image_id}_c{len(candidates)}",
                    "category": category,
                    "score": round(float(rng.uniform(0.62, 0.95)), 6),
                    "bbox": primary,
                    "source": SOURCES[int(rng.integers(0, len(SOURCES)))],
                }
            )
            # Duplicate candidate: still overlaps the GT but loses to primary.
            duplicate = _jitter(rng, bbox, 0.02, 0.88, 1.12)
            candidates.append(
                {
                    "candidate_id": f"{image_id}_c{len(candidates)}",
                    "category": category,
                    "score": round(float(rng.uniform(0.55, 0.9)), 6),
                    "bbox": duplicate,
                    "source": SOURCES[int(rng.integers(0, len(SOURCES)))],
                }
            )

        # High-score background candidates (random geometry, weak overlap).
        for _ in range(int(rng.integers(2, 4))):
            category = CATEGORIES[int(rng.integers(0, len(CATEGORIES)))]
            bbox = _clamp_box(
                float(rng.uniform(0.0, 0.85)),
                float(rng.uniform(0.0, 0.85)),
                float(rng.uniform(0.05, 0.15)),
                float(rng.uniform(0.05, 0.15)),
            )
            candidates.append(
                {
                    "candidate_id": f"{image_id}_c{len(candidates)}",
                    "category": category,
                    "score": round(float(rng.uniform(0.5, 0.88)), 6),
                    "bbox": bbox,
                    "source": SOURCES[int(rng.integers(0, len(SOURCES)))],
                }
            )

        # Low-confidence noise.
        for _ in range(int(rng.integers(1, 3))):
            category = CATEGORIES[int(rng.integers(0, len(CATEGORIES)))]
            bbox = _clamp_box(
                float(rng.uniform(0.0, 0.9)),
                float(rng.uniform(0.0, 0.9)),
                float(rng.uniform(0.04, 0.12)),
                float(rng.uniform(0.04, 0.12)),
            )
            candidates.append(
                {
                    "candidate_id": f"{image_id}_c{len(candidates)}",
                    "category": category,
                    "score": round(float(rng.uniform(0.05, 0.45)), 6),
                    "bbox": bbox,
                    "source": SOURCES[int(rng.integers(0, len(SOURCES)))],
                }
            )

        images.append(
            {
                "image_id": image_id,
                "width": width,
                "height": height,
                "ground_truth": ground_truth,
                "candidates": candidates,
            }
        )

    # Cross-image duplicate pattern: img_000 and img_003 share two identical
    # tracking-operator candidates (same normalized geometry and score).
    shared_block = [
        {
            "candidate_id": "shared_track_0",
            "category": "car",
            "score": 0.7321,
            "bbox": [0.31, 0.22, 0.18, 0.14],
            "source": "op_track",
        },
        {
            "candidate_id": "shared_track_1",
            "category": "person",
            "score": 0.6142,
            "bbox": [0.55, 0.6, 0.1, 0.22],
            "source": "op_track",
        },
    ]
    for target in (0, 3):
        for cand in shared_block:
            clone = dict(cand)
            clone["candidate_id"] = f"{images[target]['image_id']}_{cand['candidate_id']}"
            images[target]["candidates"].append(clone)

    dataset = {
        "schema_version": 1,
        "seed": seed,
        "categories": CATEGORIES,
        "sources": SOURCES,
        "images": images,
    }
    return dataset


def write_dataset(path: str | Path, seed: int = DEFAULT_SEED,
                  num_images: int = 6) -> dict[str, Any]:
    dataset = generate_dataset(seed=seed, num_images=num_images)
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    import json

    with open(out, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(dataset, fh, sort_keys=True, indent=2, ensure_ascii=False)
        fh.write("\n")
    return dataset
