#!/usr/bin/env python3
"""Generate the deterministic local sample recipes, cards, and anomalies."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from PIL import Image


MASK64 = (1 << 64) - 1
SEED = 20261122
MULTIPLIER = 6364136223846793005
INCREMENT = 1442695040888963407


class Lcg:
    def __init__(self, seed: int) -> None:
        self.state = seed & MASK64

    def next_u64(self) -> int:
        self.state = (self.state * MULTIPLIER + INCREMENT) & MASK64
        return self.state

    def next_byte(self) -> int:
        return (self.next_u64() >> 32) & 0xFF


def build_card_image() -> Image.Image:
    generator = Lcg(SEED)
    pixels = np.zeros((8, 16, 3), dtype=np.uint8)
    bands = [
        ((128, 58, 32), (36, 18, 12)),
        ((78, 90, 108), (20, 28, 40)),
        ((38, 28, 92), (12, 8, 34)),
        ((52, 84, 38), (16, 30, 12)),
    ]
    for card_index, (bright, dark) in enumerate(bands):
        left = (card_index % 2) * 8
        top = (card_index // 2) * 4
        for local_y in range(4):
            for local_x in range(8):
                amount = generator.next_u64() / float(1 << 64)
                blend = 0.20 + 0.75 * ((local_x + 0.5) / 8.0) * (0.65 + 0.35 * amount)
                color = [
                    int(round(dark[channel] + (bright[channel] - dark[channel]) * blend))
                    for channel in range(3)
                ]
                pixels[top + local_y, left + local_x] = color
    return Image.fromarray(pixels)


def octave_definition(index: int, amplitude_scale: float) -> dict[str, float]:
    return {
        "frequency": float(1 << index),
        "amplitude": round(amplitude_scale / (1.0 + 0.42 * index), 12),
    }


def recipe(
    *,
    material_id: str,
    description: str,
    base_color: list[float],
    roughness: float,
    metalness: float,
    resolution: list[int],
    base_resolution: list[int],
    seed: int,
    octave_count: int,
    palette_card: str | None,
) -> dict:
    payload = {
        "material_id": material_id,
        "description": description,
        "base_color": base_color,
        "roughness": roughness,
        "metalness": metalness,
        "resolution": resolution,
        "noise": {
            "seed": seed,
            "base_resolution": base_resolution,
            "octaves": [octave_definition(index, 0.62) for index in range(octave_count)],
        },
    }
    if palette_card is not None:
        payload["palette_card"] = palette_card
    return payload


def valid_recipes() -> list[dict]:
    return [
        recipe(
            material_id="terra_cotta",
            description="粗糙 陶土 Terracotta 暖色 Surface",
            base_color=[0.72, 0.34, 0.22],
            roughness=0.82,
            metalness=0.01,
            resolution=[24, 18],
            base_resolution=[40, 30],
            seed=104729,
            octave_count=4,
            palette_card="earth",
        ),
        recipe(
            material_id="brushed_steel",
            description="拉丝 Brushed 不锈钢 Steel 冷灰",
            base_color=[0.58, 0.62, 0.67],
            roughness=0.28,
            metalness=0.95,
            resolution=[32, 20],
            base_resolution=[56, 36],
            seed=130363,
            octave_count=6,
            palette_card="metal",
        ),
        recipe(
            material_id="indigo_velvet",
            description="柔软 Soft 靛蓝 Indigo 丝绒 Velvet",
            base_color=[0.18, 0.16, 0.42],
            roughness=0.65,
            metalness=0.02,
            resolution=[20, 14],
            base_resolution=[30, 22],
            seed=155921,
            octave_count=5,
            palette_card="textile",
        ),
        recipe(
            material_id="moss_stone",
            description="苔藓 Moss 石面 Stone 自然 Natural",
            base_color=[0.30, 0.40, 0.22],
            roughness=0.88,
            metalness=0.00,
            resolution=[26, 18],
            base_resolution=[44, 28],
            seed=196613,
            octave_count=7,
            palette_card=None,
        ),
    ]


def invalid_recipes() -> list[dict]:
    base = recipe(
        material_id="bad_octaves",
        description="非法 Octave 样本",
        base_color=[0.4, 0.4, 0.4],
        roughness=0.5,
        metalness=0.5,
        resolution=[12, 10],
        base_resolution=[16, 12],
        seed=17,
        octave_count=2,
        palette_card="metal",
    )
    empty_octaves = json.loads(json.dumps(base))
    empty_octaves["noise"]["octaves"] = []

    empty_palette = json.loads(json.dumps(base))
    empty_palette["material_id"] = "empty_palette"
    empty_palette["description"] = "空调色板 Empty Palette"
    empty_palette["palette_card"] = "empty"

    nan_noise = json.loads(json.dumps(base))
    nan_noise["material_id"] = "nan_noise"
    nan_noise["description"] = "NaN 噪声 Noise"
    nan_noise["noise"]["octaves"][1]["amplitude"] = float("nan")

    dimension_mismatch = json.loads(json.dumps(base))
    dimension_mismatch["material_id"] = "dimension_mismatch"
    dimension_mismatch["description"] = "尺寸错误 Dimension Mismatch"
    dimension_mismatch["palette_card"] = "broken_size"

    empty_prompt = json.loads(json.dumps(base))
    empty_prompt["material_id"] = "empty_prompt"
    empty_prompt["description"] = "！！！？？？"

    return [empty_octaves, empty_palette, nan_noise, dimension_mismatch, empty_prompt]


def write_json(path: Path, payload: object, *, indent: int | None = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        indent=indent,
        separators=(",", ":") if indent is None else None,
        allow_nan=True,
    )
    path.write_text(text + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    samples = args.root / "samples"
    samples.mkdir(parents=True, exist_ok=True)

    image = build_card_image()
    image.save(samples / "color_cards.png", format="PNG", optimize=False, compress_level=9)
    manifest = {
        "schema": "matgen.color_cards.v1",
        "image": "color_cards.png",
        "image_size": [16, 8],
        "cards": {
            "earth": {"box": [0, 0, 8, 4], "declared_size": [8, 4]},
            "metal": {"box": [8, 0, 16, 4], "declared_size": [8, 4]},
            "textile": {"box": [0, 4, 8, 8], "declared_size": [8, 4]},
            "moss": {"box": [8, 4, 16, 8], "declared_size": [8, 4]},
            "empty": {"box": [0, 0, 0, 0], "declared_size": [0, 0], "empty": True},
            "broken_size": {"box": [0, 0, 8, 4], "declared_size": [7, 4]},
        },
    }
    write_json(samples / "color_cards.json", manifest)
    recipes = valid_recipes()
    write_json(samples / "recipes.json", recipes)
    write_json(samples / "invalid_recipes.json", invalid_recipes())
    write_json(
        samples / "expected_errors.json",
        {
            "schema": "matgen.expected_errors.v1",
            "cases": [
                {"material_id": "bad_octaves", "code": "E_OCTAVE_RANGE"},
                {"material_id": "empty_palette", "code": "E_PALETTE_INVALID"},
                {"material_id": "nan_noise", "code": "E_NUMERIC"},
                {"material_id": "dimension_mismatch", "code": "E_DIM_MISMATCH"},
                {"material_id": "empty_prompt", "code": "E_EMPTY_PROMPT"},
            ],
        },
    )
    write_json(
        samples / "config.json",
        {
            "schema": "matgen.config.v1",
            "algorithm_version": "matgen-core-1.0.0",
            "hash_seed": 243001,
            "sample_count": 12,
            "sample_seed": 99173,
            "octave_error_budget": 5e-13,
            "total_error_budget": 0.01,
            "max_threads": 4,
            "noise_base_scale": 2,
            "noise_base_min": 16,
            "noise_base_max": 64,
        },
    )
    print(json.dumps({"samples": str(samples), "recipe_count": len(recipes)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

