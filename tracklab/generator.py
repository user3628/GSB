"""Deterministic synthetic video sequence generator (no external assets)."""

from __future__ import annotations

import math
import shutil
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import atomic_write_bytes, atomic_write_json, digest, sha256_bytes
from .config import Settings
from .geometry import validate_box
from .manifest import validate_manifest


SEQUENCE_SPECS: list[dict[str, Any]] = [
    {
        "sequence_id": "seq_uniform",
        "motion_type": "uniform_translation",
        "initial_box": [0.080, 0.380, 0.160, 0.240],
        "velocity": [0.0110, 0.0015],
        "scale_rate": 0.0,
        "direction_switch_frame": None,
        "occlusion_frames": None,
    },
    {
        "sequence_id": "seq_scaling",
        "motion_type": "slow_scaling",
        "initial_box": [0.400, 0.360, 0.120, 0.180],
        "velocity": [-0.0020, 0.0025],
        "scale_rate": 0.0105,
        "direction_switch_frame": None,
        "occlusion_frames": None,
    },
    {
        "sequence_id": "seq_direction",
        "motion_type": "direction_change_with_occlusion",
        "initial_box": [0.100, 0.320, 0.150, 0.220],
        "velocity": [0.0120, 0.0010],
        "direction_switch_frame": 20,
        "post_switch_velocity": [-0.0160, 0.0020],
        "scale_rate": 0.0,
        "occlusion_frames": [18, 23],
    },
]


def _box_at(spec: dict[str, Any], frame_index: int) -> list[float]:
    x, y, w, h = (float(v) for v in spec["initial_box"])
    vx, vy = (float(v) for v in spec["velocity"])
    switch = spec.get("direction_switch_frame")
    if switch is None or frame_index < int(switch):
        cx = x + w / 2.0 + vx * frame_index
        cy = y + h / 2.0 + vy * frame_index
    else:
        vx2, vy2 = (float(v) for v in spec["post_switch_velocity"])
        cx = x + w / 2.0 + vx * int(switch) + vx2 * (frame_index - int(switch))
        cy = y + h / 2.0 + vy * int(switch) + vy2 * (frame_index - int(switch))
    factor = 1.0 + float(spec["scale_rate"]) * frame_index
    bw = w * factor
    bh = h * factor
    return [cx - bw / 2.0, cy - bh / 2.0, bw, bh]


def _make_background(height: int, width: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    yy, xx = np.mgrid[0:height, 0:width]
    base = (
        31.0
        + 10.0 * np.sin(xx / 13.0 + 0.4)
        + 8.0 * np.cos(yy / 9.0 - 0.7)
        + 4.0 * np.sin((xx + 2.0 * yy) / 23.0)
    )
    base += rng.normal(0.0, 1.5, size=(height, width))
    return base


def _draw_target(image: np.ndarray, box: list[float], rng: np.random.Generator, phase: float) -> tuple[int, int, int, int]:
    height, width = image.shape
    x0 = int(math.floor(box[0] * width + 0.5))
    y0 = int(math.floor(box[1] * height + 0.5))
    x1 = int(math.floor((box[0] + box[2]) * width + 0.5))
    y1 = int(math.floor((box[1] + box[3]) * height + 0.5))
    x0 = max(0, min(width - 1, x0))
    y0 = max(0, min(height - 1, y0))
    x1 = max(x0 + 1, min(width, x1))
    y1 = max(y0 + 1, min(height, y1))
    h = y1 - y0
    w = x1 - x0
    yy, xx = np.mgrid[0:h, 0:w]
    u = xx / max(1.0, w - 1.0)
    v = yy / max(1.0, h - 1.0)
    checker = np.where(((xx // max(2, w // 5)) + (yy // max(2, h // 5))) % 2 == 0, 10.0, -10.0)
    texture = (
        158.0
        + 52.0 * np.sin(2.0 * math.pi * (1.15 * u + 0.55 * v) + phase)
        + 27.0 * np.cos(2.0 * math.pi * (3.0 * u - 1.7 * v) - phase * 0.7)
        + checker
        + rng.normal(0.0, 3.5, size=(h, w))
    )
    border = np.zeros((h, w), dtype=bool)
    border[:2, :] = True
    border[-2:, :] = True
    border[:, :2] = True
    border[:, -2:] = True
    texture = np.where(border, 238.0 + rng.normal(0.0, 2.0, size=(h, w)), texture)
    image[y0:y1, x0:x1] = np.clip(texture, 0.0, 255.0)
    return x0, y0, x1, y1


def _draw_occluder(image: np.ndarray, frame_index: int, rng: np.random.Generator) -> None:
    height, width = image.shape
    x0 = int(round(0.22 * width))
    x1 = int(round(0.50 * width))
    y0 = int(round(0.08 * height))
    y1 = int(round(0.92 * height))
    h = y1 - y0
    w = x1 - x0
    yy, xx = np.mgrid[0:h, 0:w]
    stripe = 20.0 + 24.0 * ((yy // 4 + frame_index) % 3)
    texture = stripe + 8.0 * np.sin(xx / 3.0) + rng.normal(0.0, 2.0, size=(h, w))
    texture[:, :3] = 12.0
    texture[:, -3:] = 12.0
    image[y0:y1, x0:x1] = np.clip(texture, 0.0, 255.0)


def _render_sequence(
    spec: dict[str, Any],
    *,
    seed: int,
    frame_count: int,
    height: int,
    width: int,
    frame_rate: float,
    sample_count_upper_bound: int,
) -> tuple[dict[str, Any], list[np.ndarray], list[list[float]]]:
    sequence_id = spec["sequence_id"]
    seq_seed = int(seed) + (sum(ord(ch) for ch in sequence_id) * 7919)
    bg = _make_background(height, width, seq_seed)
    occl = spec.get("occlusion_frames")
    boxes: list[list[float]] = []
    arrays: list[np.ndarray] = []
    for frame_index in range(frame_count):
        rng = np.random.default_rng(seq_seed + frame_index * 104729)
        box = _box_at(spec, frame_index)
        validate_box(box, sequence_id=sequence_id, frame_index=frame_index, label="generated_reference_box")
        boxes.append(box)
        yy, xx = np.mgrid[0:height, 0:width]
        frame = (
            bg
            + 3.0 * np.sin((xx + 0.7 * frame_index) / 8.0)
            + 2.5 * np.cos((yy - 0.4 * frame_index) / 7.0)
            + rng.normal(0.0, 2.2, size=(height, width))
        )
        _draw_target(frame, box, rng, phase=(0.3 + 0.07 * frame_index))
        if occl is not None and int(occl[0]) <= frame_index <= int(occl[1]):
            _draw_occluder(frame, frame_index, rng)
        arrays.append(np.clip(np.rint(frame), 0, 255).astype(np.uint8))

    sequence = {
        "sequence_id": sequence_id,
        "image_shape": [height, width],
        "frame_rate": frame_rate,
        "visible_frames": frame_count,
        "sample_count_upper_bound": int(sample_count_upper_bound),
        "initial_box": [round(float(v), 12) for v in spec["initial_box"]],
        "search_window_expansion": 0.75,
        "appearance_update_threshold": 0.90,
        "trace_confidence_floor": 0.60,
        "motion_model": {
            "type": spec["motion_type"],
            "velocity_per_frame": [round(float(v), 12) for v in spec["velocity"]],
            "scale_rate_per_frame": float(spec["scale_rate"]),
            "direction_switch_frame": spec.get("direction_switch_frame"),
            "post_switch_velocity_per_frame": spec.get("post_switch_velocity"),
        },
        "generator": {
            "sequence_seed": seq_seed,
            "noise_stream": "numpy.default_rng(sequence_seed + frame_index * 104729)",
        },
        "occlusion_intervals": (
            [{"start_frame": int(occl[0]), "end_frame": int(occl[1])}] if occl is not None else []
        ),
        "frames": [],
    }
    return sequence, arrays, boxes


def generate_demo(settings: Settings, *, force: bool = False) -> dict[str, Any]:
    gcfg = settings.generator
    seed = int(gcfg["seed"])
    height = int(gcfg["image_height"])
    width = int(gcfg["image_width"])
    frame_count = int(gcfg["frames_per_sequence"])
    frame_rate = float(gcfg["frame_rate"])
    bound = int(gcfg["sample_count_upper_bound"])
    if frame_count < 8:
        raise ValueError("frames_per_sequence must be at least 8")
    sequence_root = settings.data_dir / "sequences"
    if sequence_root.exists():
        if not force:
            raise FileExistsError(f"generated data already exists at {sequence_root}; pass force=True")
        shutil.rmtree(sequence_root)
    sequence_root.mkdir(parents=True, exist_ok=True)

    manifest_sequences: list[dict[str, Any]] = []
    summary_sequences: list[dict[str, Any]] = []
    total_frames = 0
    for spec in SEQUENCE_SPECS:
        seq, arrays, boxes = _render_sequence(
            spec,
            seed=seed,
            frame_count=frame_count,
            height=height,
            width=width,
            frame_rate=frame_rate,
            sample_count_upper_bound=bound,
        )
        sequence_id = seq["sequence_id"]
        frame_dir = sequence_root / sequence_id / "frames"
        frame_dir.mkdir(parents=True, exist_ok=True)
        for frame_index, (array, box) in enumerate(zip(arrays, boxes, strict=True)):
            relative = Path("sequences") / sequence_id / "frames" / f"{frame_index:06d}.npy"
            target = settings.data_dir / relative
            import io

            buffer = io.BytesIO()
            np.save(buffer, array, allow_pickle=False)
            payload = buffer.getvalue()
            atomic_write_bytes(target, payload)
            seq["frames"].append(
                {
                    "frame_index": frame_index,
                    "path": relative.as_posix(),
                    "box_xywh_norm": [round(float(v), 12) for v in box],
                    "sha256": sha256_bytes(payload),
                    "pixel_sum": int(array.sum(dtype=np.uint64)),
                }
            )
        manifest_sequences.append(seq)
        summary_sequences.append(
            {
                "sequence_id": sequence_id,
                "motion_type": seq["motion_model"]["type"],
                "visible_frames": len(seq["frames"]),
                "sample_count_upper_bound": bound,
                "occlusion_intervals": seq["occlusion_intervals"],
                "initial_box": seq["initial_box"],
                "final_reference_box": seq["frames"][-1]["box_xywh_norm"],
                "frame_bytes_sha256": digest([f["sha256"] for f in seq["frames"]]),
            }
        )
        total_frames += len(seq["frames"])

    manifest = {
        "schema_version": 1,
        "generator": {
            "name": "tracklab.deterministic_generator",
            "version": 1,
            "seed": seed,
            "image_shape": [height, width],
            "frame_rate": frame_rate,
            "frames_per_sequence": frame_count,
            "sample_count_upper_bound": bound,
            "declaration": "one grayscale uint8 frame is one visible sample",
        },
        "sequences": manifest_sequences,
    }
    public_summary = {
        "schema_version": 1,
        "seed": seed,
        "sequence_count": len(manifest_sequences),
        "total_visible_frames": total_frames,
        "sample_count_upper_bound_per_sequence": bound,
        "manifest_fingerprint": digest(manifest),
        "sequences": summary_sequences,
    }
    manifest_path = settings.data_dir / "tracking_manifest.json"
    summary_path = settings.data_dir / "reference_summary.json"
    manifest_bytes = atomic_write_json(manifest_path, manifest, pretty=True)
    summary_bytes = atomic_write_json(summary_path, public_summary, pretty=True)
    # Validate the just-written manifest and expose the generated bytes for verification.
    view = validate_manifest(settings, verify_frame_files=True)
    result = {
        "ok": True,
        "manifest_path": str(manifest_path),
        "summary_path": str(summary_path),
        "manifest_sha256": sha256_bytes(manifest_bytes),
        "summary_sha256": sha256_bytes(summary_bytes),
        "manifest_fingerprint": view.fingerprint,
        "sequences": [
            {
                "sequence_id": seq["sequence_id"],
                "visible_frames": seq["visible_frames"],
                "sample_count_upper_bound": seq["sample_count_upper_bound"],
            }
            for seq in manifest_sequences
        ],
        "total_visible_frames": total_frames,
    }
    atomic_write_json(settings.artifacts_dir / "generation_report.json", result, pretty=True)
    return result
