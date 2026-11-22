"""Fixed-seed local demo dataset generator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from .chain import parse_chain
from .config import Config
from .imaging import encode_png_rgb, load_rgb, pixel_digest
from .models import TaskSpec
from .operators import apply_chain
from .util import atomic_write_bytes, ensure_dir, reset_runtime_dirs


DEMO_SEED = 20261123


def demo_config_mapping() -> dict[str, Any]:
    return {
        "version": "demo-v1",
        "max_workers": 3,
        "global_step_budget": 30,
        "worker_step_quota": 12,
        "task_timeout_seconds": 30.0,
        "max_chain_length": 8,
        "reachability": {
            "rotate_angle_min": -180.0,
            "rotate_angle_max": 180.0,
            "max_dimension": 512,
        },
        "cost_model": {
            "steps_per_operator": 2,
            "pixels_per_step": 1024,
            "min_task_steps": 1,
        },
        "export": {"format": "png-rgb-v1"},
    }


def demo_config_yaml() -> str:
    return """version: demo-v1
max_workers: 3
global_step_budget: 30
worker_step_quota: 12
task_timeout_seconds: 30.0
max_chain_length: 8
reachability:
  rotate_angle_min: -180.0
  rotate_angle_max: 180.0
  max_dimension: 512
cost_model:
  steps_per_operator: 2
  pixels_per_step: 1024
  min_task_steps: 1
export:
  format: png-rgb-v1
"""


def _shard_pixels(index: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed + index * 7919)
    height, width = 64, 64
    y = np.arange(height, dtype=np.float64)[:, None]
    x = np.arange(width, dtype=np.float64)[None, :]
    base = np.empty((height, width, 3), dtype=np.float64)
    base[:, :, 0] = 35 + 2.1 * x + 0.8 * y
    base[:, :, 1] = 25 + 1.4 * y + index * 9.0
    base[:, :, 2] = 55 + 1.1 * (x + y) + (3 - index) * 7.0
    cx = 12 + index * 11
    cy = 18 + (index % 2) * 22
    radius = 9 + index * 2
    mask = (x - cx) ** 2 + (y - cy) ** 2 <= radius**2
    base[:, :, 0] = np.where(mask, 220 - index * 12, base[:, :, 0])
    base[:, :, 1] = np.where(mask, 90 + index * 18, base[:, :, 1])
    base[:, :, 2] = np.where(mask, 70 + (index % 2) * 65, base[:, :, 2])
    stripe = ((x.astype(int) // (7 + index)) + (y.astype(int) // (9 + index))) % 2 == 0
    base[:, :, 1] += np.where(stripe, 14.0, -8.0)
    base += rng.normal(0.0, 2.5, size=base.shape)
    return np.clip(np.floor(base + 0.5), 0, 255).astype(np.uint8)


TASK_DEFINITIONS: list[dict[str, Any]] = [
    {
        "task_id": "t010",
        "input": "shard_000.png",
        "instruction": "Crop 左上局部区域，light warm color shift，然后 bilinear 缩放到 48x32。",
        "allowed_operators": ["crop", "color_shift", "resize"],
        "operations": [
            {"op": "crop", "x": 4, "y": 6, "width": 52, "height": 44},
            {"op": "color_shift", "red": 18.5, "green": 7.25, "blue": -12.75},
            {"op": "resize", "width": 48, "height": 32, "mode": "bilinear"},
        ],
        "priority": 0,
    },
    {
        "task_id": "t020",
        "input": "shard_001.png",
        "instruction": "Rotate 15.5 度并 resize 成 40x40，随后 cool tone color shift。",
        "allowed_operators": ["rotate", "resize", "color_shift"],
        "operations": [
            {"op": "rotate", "angle": 15.5},
            {"op": "resize", "width": 40, "height": 40, "mode": "bilinear"},
            {"op": "color_shift", "red": -8.0, "green": 12.0, "blue": 20.0},
        ],
        "priority": 0,
    },
    {
        "task_id": "t030",
        "input": "shard_000.png",
        "instruction": "Shared shard reuse：先 nearest resize 到 36x36，调色后 crop 中央 32x32。",
        "allowed_operators": ["resize", "color_shift", "crop"],
        "operations": [
            {"op": "resize", "width": 36, "height": 36, "mode": "nearest"},
            {"op": "color_shift", "red": -15.0, "green": 4.5, "blue": 15.0},
            {"op": "crop", "x": 2, "y": 2, "width": 32, "height": 32},
        ],
        "priority": 0,
    },
    {
        "task_id": "t040",
        "input": "shard_002.png",
        "instruction": "Seven-step deterministic chain：局部裁切、缩放、冷暖调节与旋转组合；该任务预计超出 worker quota。",
        "allowed_operators": ["crop", "resize", "color_shift", "rotate"],
        "operations": [
            {"op": "crop", "x": 0, "y": 0, "width": 60, "height": 60},
            {"op": "resize", "width": 48, "height": 48, "mode": "bilinear"},
            {"op": "color_shift", "red": 5.0, "green": -3.0, "blue": 2.0},
            {"op": "rotate", "angle": -7.25},
            {"op": "resize", "width": 40, "height": 40, "mode": "nearest"},
            {"op": "color_shift", "red": 3.0, "green": 3.0, "blue": -4.0},
            {"op": "crop", "x": 4, "y": 4, "width": 32, "height": 32},
        ],
        "priority": 0,
    },
    {
        "task_id": "t050",
        "input": "shard_003.png",
        "instruction": "不可达示例：对 64x64 分片请求 (80,80) 的 crop，必须返回 E_CHAIN_INVALID。",
        "allowed_operators": ["crop", "resize"],
        "operations": [
            {"op": "crop", "x": 80, "y": 80, "width": 8, "height": 8},
            {"op": "resize", "width": 20, "height": 20, "mode": "nearest"},
        ],
        "priority": 0,
    },
]


def parse_task_text(line: str) -> TaskSpec:
    return TaskSpec.from_mapping(json.loads(line))


def generate_demo(output_dir: str | Path, *, seed: int = DEMO_SEED) -> dict[str, Any]:
    output_dir = Path(output_dir).resolve()
    reset_runtime_dirs(output_dir)
    inputs_dir = ensure_dir(output_dir / "inputs")
    shard_names = [f"shard_{index:03d}.png" for index in range(4)]
    for index, name in enumerate(shard_names):
        pixels = _shard_pixels(index, seed)
        atomic_write_bytes(inputs_dir / name, encode_png_rgb(pixels))
    config_path = output_dir / "config.yaml"
    atomic_write_bytes(config_path, demo_config_yaml().encode("utf-8"))
    config = Config.load(config_path)
    tasks: list[dict[str, Any]] = []
    for definition in TASK_DEFINITIONS:
        task = dict(definition)
        try:
            spec = TaskSpec.from_mapping(task)
            loaded = load_rgb(inputs_dir / task["input"])
            parsed = parse_chain(spec, (loaded.width, loaded.height), config)
            output = apply_chain(loaded.pixels, parsed.public_operations(), task_id=spec.task_id)
            expected_size = [int(output.shape[1]), int(output.shape[0])]
            expected_digest = pixel_digest(output)
        except Exception:
            expected_size = [20, 20]
            expected_digest = "0" * 64
        task["expected_output"] = {
            "width": expected_size[0],
            "height": expected_size[1],
            "sha256": expected_digest,
        }
        tasks.append(task)
    tasks_path = output_dir / "tasks.jsonl"
    manifest = b"".join(
        (json.dumps(task, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
        for task in tasks
    )
    atomic_write_bytes(tasks_path, manifest)
    return {
        "output_dir": str(output_dir),
        "seed": seed,
        "shards": shard_names,
        "tasks": [task["task_id"] for task in tasks],
        "config": str(config_path),
        "manifest": str(tasks_path),
    }
