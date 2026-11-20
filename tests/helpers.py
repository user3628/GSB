from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import yaml

from detdiff.quantization import encode_png_rgb8


WIDTH = 16
HEIGHT = 16


def create_image(path: Path, seed: int, *, width: int = WIDTH, height: int = HEIGHT) -> None:
    yy, xx = np.mgrid[0:height, 0:width]
    array = np.stack(
        [
            ((xx * (channel + 3) + yy * (seed + 1) + seed * 17) % 256).astype(np.uint8)
            for channel in range(3)
        ],
        axis=-1,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encode_png_rgb8(array))


def config_data(root: Path, *, missing_reference: bool = False, with_reference: bool = True) -> dict[str, Any]:
    reference = "references/guide.png"
    if missing_reference:
        reference = "references/missing.png"
    shards: list[dict[str, Any]] = [
        {
            "id": "alpha",
            "prompt": "Alpha deterministic test tile",
            "guidance_scale": 2.5,
            "steps": 4,
            "seed": 101,
        },
        {
            "id": "beta",
            "prompt": "乙 测试 分片 with mixed text",
            "guidance_scale": 4.0,
            "steps": 5,
            "seed": 202,
        },
    ]
    if with_reference:
        shards[1]["reference"] = reference
        shards[1]["blend_strength"] = 0.35
    return {
        "schema_version": 1,
        "global": {
            "encoder": {
                "name": "sha256-xof-16",
                "version": 1,
                "dimension": 16,
                "domain": "detdiff.tests.v1",
            },
            "image": {"width": WIDTH, "height": HEIGHT, "channels": 3},
            "quantize": {"version": 1, "levels": 256, "png_compression": 9},
            "trace_version": 1,
        },
        "shards": shards,
    }


def make_workspace(
    root: Path,
    *,
    missing_reference: bool = False,
    with_reference: bool = True,
    reference_size: tuple[int, int] = (WIDTH, HEIGHT),
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    create_image(root / "inputs" / "alpha.png", 3)
    create_image(root / "inputs" / "beta.png", 7)
    if not missing_reference:
        create_image(root / "references" / "guide.png", 11, width=reference_size[0], height=reference_size[1])
    data = config_data(root, missing_reference=missing_reference, with_reference=with_reference)
    (root / "config.yaml").write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=True),
        encoding="utf-8",
    )
    return root

