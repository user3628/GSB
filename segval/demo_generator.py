"""Deterministic local JSON shard generator. No external data or model is used."""

from __future__ import annotations

import math
import random
from pathlib import Path
from typing import Any

from .canonical import atomic_write_bytes, canonical_bytes


CATEGORIES = ("person", "car", "bicycle", "dog")
SOURCES = ("baseline", "tile-infer", "augment-rotate", "ensemble")
DENSITIES = ("sparse", "medium", "dense", "mixed")


def _q(value: float) -> float:
    return round(value, 6)


def rectangle(
    x: float,
    y: float,
    width: float,
    height: float,
    rotation_degrees: float = 0.0,
) -> list[list[float]]:
    cx = x + width / 2.0
    cy = y + height / 2.0
    angle = math.radians(rotation_degrees)
    cos_a = math.cos(angle)
    sin_a = math.sin(angle)
    corners = [
        (-width / 2.0, -height / 2.0),
        (width / 2.0, -height / 2.0),
        (width / 2.0, height / 2.0),
        (-width / 2.0, height / 2.0),
    ]
    points = []
    for px, py in corners:
        rx = px * cos_a - py * sin_a
        ry = px * sin_a + py * cos_a
        points.append([_q(min(max(cx + rx, 0.01), 0.99)), _q(min(max(cy + ry, 0.01), 0.99))])
    return points


def _jitter_polygon(polygon: list[list[float]], rng: random.Random) -> list[list[float]]:
    cx = sum(point[0] for point in polygon) / len(polygon)
    cy = sum(point[1] for point in polygon) / len(polygon)
    scale = rng.uniform(0.88, 1.04)
    dx = rng.uniform(-0.006, 0.006)
    dy = rng.uniform(-0.006, 0.006)
    return [
        [_q(min(max(cx + (x - cx) * scale + dx, 0.005), 0.995)), _q(min(max(cy + (y - cy) * scale + dy, 0.005), 0.995))]
        for x, y in polygon
    ]


def _prediction(
    shard_index: int,
    sample_index: int,
    prediction_index: int,
    class_name: str,
    confidence: float,
    polygon: list[list[float]],
    source: str,
) -> dict[str, Any]:
    return {
        "prediction_id": f"p-{shard_index:04d}-{sample_index:04d}-{prediction_index:04d}",
        "class_name": class_name,
        "confidence": round(confidence, 6),
        "polygon": polygon,
        "source_operator": source,
    }


def _background_polygon(rng: random.Random) -> list[list[float]]:
    x = rng.uniform(0.02, 0.78)
    y = rng.uniform(0.03, 0.72)
    width = rng.uniform(0.03, 0.14)
    height = rng.uniform(0.03, 0.14)
    return rectangle(x, y, width, height, rng.choice([0.0, 0.0, 7.0, -11.0]))


def _make_sample(
    *,
    shard_index: int,
    sample_index: int,
    density: str,
    rng: random.Random,
) -> dict[str, Any]:
    width = 640
    height = 480
    image_id = f"img-{shard_index:04d}-{sample_index:04d}"
    class_count = {"sparse": 2, "medium": 2, "dense": 3, "mixed": 3}[density]
    selected_categories = list(CATEGORIES)
    rng.shuffle(selected_categories)
    selected_categories = selected_categories[:class_count]
    ground_truth: dict[str, list[list[list[float]]]] = {}
    predictions: list[dict[str, Any]] = []
    prediction_index = 0

    for class_ordinal, class_name in enumerate(selected_categories):
        instance_count = 1 if density == "sparse" else rng.randint(1, 2)
        ground_truth[class_name] = []
        for instance_ordinal in range(instance_count):
            cell_x = 0.06 + class_ordinal * 0.28 + instance_ordinal * 0.08
            cell_y = 0.10 + instance_ordinal * 0.34 + rng.uniform(-0.02, 0.02)
            box_width = rng.uniform(0.12, 0.22)
            box_height = rng.uniform(0.16, 0.30)
            polygon = rectangle(
                min(max(cell_x, 0.03), 0.74),
                min(max(cell_y, 0.03), 0.66),
                box_width,
                box_height,
                rng.choice([0.0, 0.0, 0.0, 5.0, -6.0]),
            )
            ground_truth[class_name].append(polygon)

            should_hit = (density == "sparse" and instance_ordinal == 0) or rng.random() < 0.82
            if should_hit:
                source = SOURCES[(shard_index + sample_index + prediction_index) % len(SOURCES)]
                predictions.append(
                    _prediction(
                        shard_index,
                        sample_index,
                        prediction_index,
                        class_name,
                        rng.uniform(0.55, 0.97),
                        _jitter_polygon(polygon, rng),
                        source,
                    )
                )
                prediction_index += 1

            extra_count = {"sparse": 0, "medium": 1, "dense": 2, "mixed": 2}[density]
            for _ in range(extra_count):
                source = SOURCES[(shard_index + prediction_index + 2) % len(SOURCES)]
                if rng.random() < 0.58:
                    predictions.append(
                        _prediction(
                            shard_index,
                            sample_index,
                            prediction_index,
                            class_name,
                            rng.uniform(0.61, 0.94),
                            _jitter_polygon(polygon, rng),
                            source,
                        )
                    )
                else:
                    predictions.append(
                        _prediction(
                            shard_index,
                            sample_index,
                            prediction_index,
                            class_name,
                            rng.uniform(0.08, 0.39),
                            _background_polygon(rng),
                            source,
                        )
                    )
                prediction_index += 1

    if density in {"medium", "dense", "mixed"}:
        for _ in range({"medium": 1, "dense": 3, "mixed": 2}[density]):
            class_name = rng.choice(selected_categories)
            source = SOURCES[(shard_index + prediction_index + 1) % len(SOURCES)]
            predictions.append(
                _prediction(
                    shard_index,
                    sample_index,
                    prediction_index,
                    class_name,
                    rng.uniform(0.04, 0.34),
                    _background_polygon(rng),
                    source,
                )
            )
            prediction_index += 1

    if shard_index == 4 and sample_index == 0:
        invalid_specs = [
            (
                "too_few_vertices",
                "person",
                0.88,
                [[0.10, 0.10], [0.20, 0.18]],
                "broken-decoder",
            ),
            (
                "self_intersection",
                "car",
                0.77,
                [[0.40, 0.20], [0.55, 0.35], [0.40, 0.35], [0.55, 0.20]],
                "augment-rotate",
            ),
            (
                "zero_area",
                "bicycle",
                0.66,
                [[0.70, 0.20], [0.76, 0.20], [0.82, 0.20]],
                "tile-infer",
            ),
            (
                "out_of_bounds",
                "dog",
                0.72,
                [[-0.08, 0.70], [0.04, 0.70], [0.04, 0.84], [-0.08, 0.84]],
                "baseline",
            ),
        ]
        for _, class_name, confidence, polygon, source in invalid_specs:
            predictions.append(
                _prediction(
                    shard_index,
                    sample_index,
                    prediction_index,
                    class_name,
                    confidence,
                    polygon,
                    source,
                )
            )
            prediction_index += 1

    predictions.sort(key=lambda item: item["prediction_id"])
    return {
        "image_id": image_id,
        "width": width,
        "height": height,
        "ground_truth": ground_truth,
        "predictions": predictions,
    }


def generate_shard(shard_index: int, seed: int = 20261123) -> dict[str, Any]:
    density = DENSITIES[(shard_index - 1) % len(DENSITIES)]
    rng = random.Random(seed + shard_index * 10007)
    sample_count = {"sparse": 2, "medium": 3, "dense": 3, "mixed": 2}[density]
    samples = [
        _make_sample(
            shard_index=shard_index,
            sample_index=sample_index,
            density=density,
            rng=rng,
        )
        for sample_index in range(sample_count)
    ]
    return {
        "schema_version": 1,
        "shard_id": f"shard-{shard_index:04d}",
        "shard_index": shard_index,
        "density_label": density,
        "generator": {
            "name": "local-deterministic-instance-generator",
            "seed": seed,
        },
        "samples": samples,
    }


def generate_demo_shards(output_dir: Path, *, seed: int = 20261123, shard_count: int = 4) -> list[dict[str, Any]]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    generated = []
    for shard_index in range(1, shard_count + 1):
        shard = generate_shard(shard_index, seed=seed)
        path = output_dir / f"shard-{shard_index:04d}.json"
        atomic_write_bytes(path, canonical_bytes(shard) + b"\n")
        generated.append(
            {
                "shard_id": shard["shard_id"],
                "shard_index": shard["shard_index"],
                "density_label": shard["density_label"],
                "sample_count": len(shard["samples"]),
                "prediction_count": sum(len(sample["predictions"]) for sample in shard["samples"]),
                "path": str(path),
            }
        )
    return generated

