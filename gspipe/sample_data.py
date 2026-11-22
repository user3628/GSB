"""Deterministic local sample shards and mixed-language edit tasks."""

from __future__ import annotations

import base64
from typing import Any

import numpy as np

from .pngio import encode_rgb_png


SAMPLE_SEED = 20261123

SAMPLE_SHARDS: dict[str, tuple[int, int]] = {
    "shard_01.png": (160, 120),
    "shard_02.png": (144, 112),
    "shard_03.png": (176, 128),
    "shard_04.png": (128, 96),
}


def _make_shard(width: int, height: int, seed: int) -> np.ndarray:
    yy, xx = np.mgrid[0:height, 0:width]
    rng = np.random.default_rng(seed)
    red = (xx * 149 + yy * 37 + seed * 11) % 256
    green = (xx * 23 + yy * 181 + seed * 7) % 256
    blue = (255 - xx * 97 - yy * 59 + seed * 13) % 256
    noise = ((xx * 73 + yy * 151 + seed * 17) % 17 - 8).astype(np.int16)
    image = np.stack([red, green, blue], axis=2).astype(np.int16) + noise[:, :, None]

    # Add deterministic geometric blocks in addition to the global gradient.
    block_count = 5
    for _ in range(block_count):
        block_width = int(rng.integers(max(8, width // 12), max(10, width // 3)))
        block_height = int(rng.integers(max(6, height // 14), max(8, height // 3)))
        x = int(rng.integers(0, max(1, width - block_width + 1)))
        y = int(rng.integers(0, max(1, height - block_height + 1)))
        color = rng.integers(0, 256, size=3, dtype=np.int16)
        image[y : y + block_height, x : x + block_width, :] = color

    diagonal = ((xx + 2 * yy + seed) // 18) % 2 == 0
    stripe = np.array([24, -18, 31], dtype=np.int16)
    image[diagonal] = np.clip(image[diagonal] + stripe[None, :], 0, 255)
    return np.clip(image, 0, 255).astype(np.uint8)


def generate_samples() -> tuple[dict[str, bytes], dict[str, Any], dict[str, Any]]:
    shards: dict[str, bytes] = {}
    for index, (name, (width, height)) in enumerate(SAMPLE_SHARDS.items(), start=1):
        image = _make_shard(width, height, SAMPLE_SEED + index * 101)
        shards[name] = encode_rgb_png(image)

    tasks = [
        {
            "task_id": "task_005",
            "input": "shard_01.png",
            "instruction": "调色并轻微模糊 color_shift dr=-10 dg=8 db=18 blur radius=1",
            "allowed_ops": ["color_shift", "blur"],
            "expected_output": {"width": 160, "height": 120},
            "expected_summary": "color_shift(dr=-10,dg=8,db=18)>blur(radius=1)",
            "latency_budget_ms": 2000,
        },
        {
            "task_id": "task_001",
            "input": "shard_01.png",
            "instruction": "将背景调暖 color_shift dr=12 dg=6 db=-8 并裁剪右上区域 crop x=20 y=12 w=110 h=82",
            "allowed_ops": ["crop", "color_shift"],
            "expected_output": {"width": 110, "height": 82},
            "expected_summary": "color_shift(dr=12,dg=6,db=-8)>crop(x=20,y=12,w=110,h=82)",
            "latency_budget_ms": 2000,
        },
        {
            "task_id": "task_003",
            "input": "shard_03.png",
            "instruction": "顺时针 rotate angle=90 后 blur radius=2",
            "allowed_ops": ["rotate", "blur"],
            "expected_output": {"width": 176, "height": 128},
            "expected_summary": "rotate(angle=90)>blur(radius=2)",
            "latency_budget_ms": 2000,
        },
        {
            "task_id": "task_002",
            "input": "shard_02.png",
            "instruction": "先 crop x=40 y=8 w=96 h=96，再 resize w=120 h=120 mode=bilinear",
            "allowed_ops": ["crop", "resize"],
            "expected_output": {"width": 120, "height": 120},
            "expected_summary": "crop(x=40,y=8,w=96,h=96)>resize(w=120,h=120,mode=bilinear)",
            "latency_budget_ms": 2000,
        },
        {
            "task_id": "task_006",
            "input": "shard_02.png",
            "instruction": "延迟压力测试 旋转并模糊 rotate angle=30 blur radius=8 color_shift dr=5 dg=-3 db=2",
            "allowed_ops": ["rotate", "blur", "color_shift"],
            "expected_output": {"width": 144, "height": 112},
            "expected_summary": "rotate(angle=30)>blur(radius=8)>color_shift(dr=5,dg=-3,db=2)",
            "latency_budget_ms": 0,
        },
        {
            "task_id": "task_004",
            "input": "shard_04.png",
            "instruction": "西侧区域 crop x=0 y=0 w=96 h=80 然后 resize w=144 h=120 mode=nearest 最后 blur radius=1",
            "allowed_ops": ["crop", "resize", "blur"],
            "expected_output": {"width": 144, "height": 120},
            "expected_summary": "crop(x=0,y=0,w=96,h=80)>resize(w=144,h=120,mode=nearest)>blur(radius=1)",
            "latency_budget_ms": 2000,
        },
    ]
    config = {
        "parse": {"max_chain_length": 5},
        "execution": {
            "scheduler": {
                "total_step_budget": 20000,
                "worker_step_quota": 8000,
                "max_workers": 3,
            },
            "cache_enabled": True,
        },
    }
    return shards, {"schema": "gspipe-tasks-v1", "tasks": tasks}, config


def sample_import_payload() -> dict[str, Any]:
    shards, tasks_manifest, config = generate_samples()
    return {
        "shards": [
            {"name": name, "png_base64": base64.b64encode(data).decode("ascii")}
            for name, data in sorted(shards.items())
        ],
        "tasks": tasks_manifest["tasks"],
        "config": config,
    }
