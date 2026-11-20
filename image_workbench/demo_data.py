from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from .atomic import atomic_write_bytes, atomic_write_json
from .hashing import digest_array, digest_json, sha256_bytes

SEED = 20261120


def _pattern(
    width: int,
    height: int,
    *,
    phase: int,
    palette: tuple[tuple[int, int, int], tuple[int, int, int]],
    rng: np.random.Generator,
) -> np.ndarray:
    yy, xx = np.mgrid[0:height, 0:width]
    first = np.asarray(palette[0], dtype=np.int64)
    second = np.asarray(palette[1], dtype=np.int64)
    mix = ((xx * 17 + yy * 13 + phase * 11) % 97)[:, :, None]
    base = (first[None, None, :] * (96 - mix) + second[None, None, :] * mix) // 96
    tile = (((xx // 3) + (yy // 3) + phase) % 3 == 0).astype(np.int64) * 7
    base = base + tile[:, :, None]
    noise = rng.integers(-7, 8, size=(height, width, 3), dtype=np.int16)
    return np.clip(base + noise, 0, 255).astype(np.uint8)


def _encode_image(pixels: np.ndarray, image_format: str, quality: int = 92) -> bytes:
    image = Image.fromarray(pixels, mode="RGB")
    output = io.BytesIO()
    if image_format == "png":
        image.save(output, format="PNG", optimize=False, compress_level=6)
    else:
        image.save(output, format="JPEG", quality=quality, subsampling=0, optimize=False)
    return output.getvalue()


def generate_demo_data(out_dir: str | Path) -> dict[str, Any]:
    root = Path(out_dir).resolve()
    images_dir = root / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(SEED)
    specs = [
        ("style_ref", 24, 24, 3, ((232, 78, 118), (55, 211, 180)), "png", None, "self", 0.0, 0, "png"),
        ("fragment_a", 20, 20, 7, ((37, 106, 168), (230, 188, 87)), "png", (0.25, 0.25, 0.50, 0.50), "style_ref", 0.65, 6, "png"),
        ("fragment_b", 22, 18, 11, ((112, 58, 157), (68, 205, 231)), "jpeg", (0.20, 0.25, 0.55, 0.60), "style_ref", 0.35, 4, "jpeg"),
        ("fragment_c", 18, 22, 17, ((42, 150, 91), (242, 132, 74)), "png", (0.10, 0.40, 0.50, 0.75), "style_ref", 0.82, 8, "png"),
        ("fragment_d", 21, 19, 23, ((56, 72, 155), (228, 95, 127)), "jpeg", (0.45, 0.10, 0.85, 0.45), "style_ref", 0.50, 5, "jpeg"),
    ]
    image_records: dict[str, Any] = {}
    manifest_fragments: list[dict[str, Any]] = []
    for name, width, height, phase, palette, source_format, rect, reference, strength, iterations, output_format in specs:
        pixels = _pattern(width, height, phase=phase, palette=palette, rng=rng)
        extension = "png" if source_format == "png" else "jpg"
        relative = f"images/{name}.{extension}"
        payload = _encode_image(pixels, source_format)
        atomic_write_bytes(root / relative, payload)
        image_records[name] = {
            "path": relative,
            "format": source_format,
            "width": width,
            "height": height,
            "pixel_digest": digest_array(pixels, role="rgb"),
            "sha256": sha256_bytes(payload),
        }
        mask_rects = [] if rect is None else [list(rect)]
        manifest_fragments.append(
            {
                "id": name,
                "image": relative,
                "mask": {"coordinate_space": "normalized-xyxy-half-open", "rects": mask_rects},
                "style_reference": reference,
                "style_strength": strength,
                "repair_iterations": iterations,
                "output_format": output_format,
                "metadata": {"fixture": name, "generator_seed": SEED},
            }
        )
    manifest = {
        "format": "image-workbench-manifest",
        "schema_version": 2,
        "project_name": "deterministic-demo",
        "metadata": {
            "schema_version": 2,
            "codec_version": "2.0.0",
            "workbench_version": "2.0.0",
            "generator": "image_workbench.demo_data",
            "generator_seed": SEED,
            "fixture_schema": "v2",
        },
        "format_config": {
            "png": {"compress_level": 6, "mask_channel": "alpha"},
            "jpeg": {"quality": 92, "subsampling": 0, "mask_transport": "companion-json"},
            "json": {"descriptor_version": 1},
        },
        "fragments": manifest_fragments,
    }
    legacy_mask = {"x": 5, "y": 5, "width": 5, "height": 5}
    legacy_manifest = {
        "schema_version": 1,
        "project_name": "legacy-demo-v1",
        "fragments": [
            {
                "image": "images/style_ref.png",
                "mask_pixels": {"x": 0, "y": 0, "width": 1, "height": 1},
                "style_reference": "images/style_ref.png",
                "style_strength_percent": 0,
                "repair_iterations": 1,
                "output_format": "png",
            },
            {
                "image": "images/fragment_a.png",
                "mask_pixels": legacy_mask,
                "style_reference": "images/style_ref.png",
                "style_strength_percent": 60,
                "repair_iterations": 5,
                "output_format": "png",
            },
        ],
    }
    legacy_mask_descriptor = {
        "schema_version": 1,
        "coordinate_space": "pixels-xywh",
        "image": "images/fragment_a.png",
        "image_size": [20, 20],
        "mask_pixels": legacy_mask,
        "style_reference": "images/style_ref.png",
        "style_strength_percent": 60,
    }
    atomic_write_json(root / "manifest.json", manifest)
    atomic_write_json(root / "legacy_manifest.json", legacy_manifest)
    atomic_write_json(root / "legacy_mask_descriptor.json", legacy_mask_descriptor)
    manifest_payload = (root / "manifest.json").read_bytes()
    legacy_payload = (root / "legacy_manifest.json").read_bytes()
    sample_digest = digest_json(
        {
            "seed": SEED,
            "images": {
                name: {
                    "path": record["path"],
                    "format": record["format"],
                    "width": record["width"],
                    "height": record["height"],
                    "pixel_digest": record["pixel_digest"],
                }
                for name, record in image_records.items()
            },
            "manifest_sha256": sha256_bytes(manifest_payload),
            "legacy_manifest_sha256": sha256_bytes(legacy_payload),
        }
    )
    summary = {
        "generator_seed": SEED,
        "sample_digest": sample_digest,
        "fragment_count": len(manifest_fragments),
        "image_count": len(image_records),
        "formats": ["png", "jpeg"],
        "manifest": str(root / "manifest.json"),
        "legacy_manifest": str(root / "legacy_manifest.json"),
        "legacy_mask_descriptor": str(root / "legacy_mask_descriptor.json"),
        "images": image_records,
    }
    atomic_write_json(root / "sample_summary.json", summary)
    return summary


if __name__ == "__main__":
    print(json.dumps(generate_demo_data(Path(__file__).resolve().parents[1] / "data"), indent=2))

