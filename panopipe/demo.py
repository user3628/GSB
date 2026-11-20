"""Deterministic local demo data generator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from .manifest import ParseOptions, parse_manifest


DEMO_CATEGORIES = [
    ("background", "#202020", False, "FF202020"),
    ("road", "#505050", False, "FF505050"),
    ("sky", "#6080C0", False, "FF6080C0"),
    ("person", "#E04040", True, "FFE04040"),
    ("car", "#4060E0", True, "FF4060E0"),
]


def _hex_rgb(value: str) -> tuple[int, int, int]:
    value = value.lstrip("#")
    return (int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16))


def _make_layout(seed: int) -> np.ndarray:
    """Create a small RGB label image using a deterministic seeded layout."""

    rng = np.random.default_rng(seed)
    width, height = 64, 48
    colors = {
        name: _hex_rgb(color)
        for name, color, _thing, _argb in DEMO_CATEGORIES
    }
    image = np.empty((height, width, 3), dtype=np.uint8)
    image[:, :] = colors["background"]
    image[0:14, :] = colors["sky"]
    image[32:48, :] = colors["road"]

    # Two adjacent same-class instances, separated by one background column.
    person_y = int(rng.integers(18, 22))
    person_w = int(rng.integers(5, 8))
    image[person_y : person_y + 8, 6 : 6 + person_w] = colors["person"]
    image[person_y : person_y + 8, 7 + person_w : 7 + person_w * 2] = colors["person"]

    # Two adjacent car instances in the road area.
    car_y = int(rng.integers(36, 39))
    image[car_y : car_y + 5, 26:35] = colors["car"]
    image[car_y : car_y + 5, 36:46] = colors["car"]

    # A tiny component below the minimum area exercises filtering and makes
    # segment id 0 (background) observable.
    tiny_x = int(rng.integers(52, 60))
    image[18, tiny_x] = colors["person"]
    return image


def _palette_index_image(rgb: np.ndarray, seed: int) -> Image.Image:
    palette = [_hex_rgb(color) for _name, color, _thing, _argb in DEMO_CATEGORIES]
    key_to_index = {color: index for index, color in enumerate(palette)}
    indices = np.empty(rgb.shape[:2], dtype=np.uint8)
    for y in range(rgb.shape[0]):
        for x in range(rgb.shape[1]):
            indices[y, x] = key_to_index[tuple(int(v) for v in rgb[y, x])]
    if seed % 2:
        # Vary the palette ordering without changing the rendered colors.
        order = list(range(len(palette)))
        order = order[1:] + order[:1]
        remap = {old: new for new, old in enumerate(order)}
        indices = np.vectorize(remap.__getitem__, otypes=[np.uint8])(indices)
        palette = [palette[index] for index in order]
    image = Image.fromarray(indices, mode="P")
    image.putpalette([channel for color in palette for channel in color])
    return image


def _base_shard(shard_id: str, image_ref: str, mask_format: str, encoding: str) -> dict[str, Any]:
    return {
        "id": shard_id,
        "image": image_ref,
        "width": 64,
        "height": 48,
        "instance_start_id": 100,
        "min_instance_area": 4,
        "confidence_threshold": 0.6,
        "mask_format": mask_format,
        "encoding": encoding,
    }


def generate_demo(output_dir: Path, *, seed: int = 20261120) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    images_dir = output_dir / "images"
    errors_dir = output_dir / "errors"
    images_dir.mkdir(parents=True, exist_ok=True)
    errors_dir.mkdir(parents=True, exist_ok=True)

    formats = ["png_palette", "rle", "coco_json", "png_palette"]
    encodings = ["indexed", "rgb", "indexed", "rgb"]
    shard_ids = [f"shard_{index + 1}_{encoding}" for index, encoding in enumerate(encodings)]
    for index, (shard_id, mask_format, encoding) in enumerate(zip(shard_ids, formats, encodings)):
        rgb = _make_layout(seed + index * 101)
        image_ref = f"images/{shard_id}.png"
        image_path = output_dir / image_ref
        if encoding == "indexed":
            _palette_index_image(rgb, seed + index).save(image_path, format="PNG", optimize=False)
        else:
            Image.fromarray(rgb, mode="RGB").save(image_path, format="PNG", optimize=False)

    categories = [
        {"id": index + 1, "name": name, "color": color, "thing": thing, "confidence": 1.0}
        for index, (name, color, thing, _argb) in enumerate(DEMO_CATEGORIES)
    ]
    v2_shards = []
    for index, (shard_id, mask_format, encoding) in enumerate(zip(shard_ids, formats, encodings)):
        shard = _base_shard(shard_id, f"images/{shard_id}.png", mask_format, encoding)
        shard["categories"] = categories
        v2_shards.append(shard)
    manifest_v2 = {
        "schema_version": 2,
        "project": "local-panoptic-demo",
        "defaults": {
            "instance_start_id": 100,
            "min_instance_area": 4,
            "confidence_threshold": 0.6,
            "mask_format": "png_palette",
        },
        "shards": v2_shards,
    }
    (output_dir / "manifest_v2.json").write_text(
        json.dumps(manifest_v2, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )

    v1_shards = []
    old_formats = ["png", "rle", "coco", "png"]
    for shard_id, old_format, encoding in zip(shard_ids, old_formats, encodings):
        v1_shards.append(
            {
                "id": shard_id,
                "image": f"images/{shard_id}.png",
                "width": 64,
                "height": 48,
                "categories": {name: argb for name, _color, _thing, argb in DEMO_CATEGORIES},
                "confidence_threshold": 60,
                "min_instance_area": 4,
                "mask_format": old_format,
                "encoding": encoding,
            }
        )
    manifest_v1 = {
        "schema_version": 1,
        "project": "local-panoptic-demo",
        "shards": v1_shards,
    }
    (output_dir / "manifest_v1.json").write_text(
        json.dumps(manifest_v1, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )

    # Convert v1 through the compatibility parser and export the new structure.
    parsed_v1 = parse_manifest(
        manifest_v1,
        base_dir=output_dir,
        source_path=output_dir / "manifest_v1.json",
        options=ParseOptions(
            default_instance_start_id=100,
            min_instance_area=4,
            confidence_threshold=0.6,
            mask_format="png_palette",
        ),
    )
    (output_dir / "manifest_v1_upgraded.json").write_text(
        parsed_v1.to_json(version=2), encoding="utf-8"
    )

    # Error sample: an RGB pixel uses magenta, which is absent from the map.
    error_rgb = _make_layout(seed + 900)
    error_rgb[24, 24] = (255, 0, 255)
    unknown_image = errors_dir / "unknown_color.png"
    Image.fromarray(error_rgb, mode="RGB").save(unknown_image, format="PNG", optimize=False)
    unknown_manifest = {
        "schema_version": 2,
        "project": "error-unknown-color",
        "shards": [
            {
                **_base_shard("unknown_color", "unknown_color.png", "png_palette", "rgb"),
                "categories": categories,
            }
        ],
    }
    (errors_dir / "unknown_color_manifest.json").write_text(
        json.dumps(unknown_manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )

    # Error sample: configured instance start is beyond the 24-bit id limit.
    overflow_manifest = {
        "schema_version": 2,
        "project": "error-segment-overflow",
        "shards": [
            {
                **_base_shard("segment_overflow", "../images/shard_2_rgb.png", "png_palette", "rgb"),
                "instance_start_id": 1 << 24,
                "categories": categories,
            }
        ],
    }
    (errors_dir / "segment_overflow_manifest.json").write_text(
        json.dumps(overflow_manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )

    unknown_schema = json.loads(json.dumps(manifest_v2))
    unknown_schema["schema_version"] = 999
    unknown_schema["project"] = "error-unsupported-schema"
    for shard in unknown_schema["shards"]:
        shard["image"] = f"../{shard['image']}"
    (errors_dir / "unknown_schema_manifest.json").write_text(
        json.dumps(unknown_schema, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )

    readme = """Local deterministic panoptic demo\n\n- manifest_v1.json: legacy schema v1 (ARGB, integer percent threshold, no instance start)\n- manifest_v2.json: current schema v2\n- manifest_v1_upgraded.json: schema v1 compatibility conversion output\n- images/: 2 indexed-color PNG and 2 RGB PNG shards\n- errors/: unknown color, out-of-range segment id, unknown schema\n"""
    (output_dir / "README.txt").write_text(readme, encoding="utf-8")
    return {
        "seed": seed,
        "output": str(output_dir),
        "shards": shard_ids,
        "schema_versions": [1, 2],
        "error_samples": [
            "errors/unknown_color_manifest.json",
            "errors/segment_overflow_manifest.json",
            "errors/unknown_schema_manifest.json",
        ],
    }
