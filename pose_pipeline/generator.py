"""Deterministic local stick-figure dataset generator.

The generator intentionally uses only Pillow, NumPy-compatible plain Python,
and the standard library. No model, network, GPU, or external data is used.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from PIL import Image, ImageDraw

from .constants import COORDINATE_CONVENTION, SCHEMA_VERSION
from .util import atomic_write_pretty_json, digest_json, file_sha256


SKELETON: tuple[tuple[str, str], ...] = (
    ("head", "neck"),
    ("neck", "left_shoulder"),
    ("neck", "right_shoulder"),
    ("left_shoulder", "left_elbow"),
    ("right_shoulder", "right_elbow"),
    ("left_elbow", "left_wrist"),
    ("right_elbow", "right_wrist"),
    ("neck", "hip_center"),
    ("hip_center", "left_hip"),
    ("hip_center", "right_hip"),
    ("left_hip", "left_knee"),
    ("right_hip", "right_knee"),
    ("left_knee", "left_ankle"),
    ("right_knee", "right_ankle"),
)

KEYPOINT_NAMES: tuple[str, ...] = (
    "head",
    "neck",
    "left_shoulder",
    "right_shoulder",
    "left_elbow",
    "right_elbow",
    "left_wrist",
    "right_wrist",
    "hip_center",
    "left_hip",
    "right_hip",
    "left_knee",
    "right_knee",
    "left_ankle",
    "right_ankle",
)

# Coordinates are relative to the pose centre and intentionally symmetric.
BASE_POSE: dict[str, tuple[float, float]] = {
    "head": (0.0, -0.39),
    "neck": (0.0, -0.275),
    "left_shoulder": (-0.105, -0.255),
    "right_shoulder": (0.105, -0.255),
    "left_elbow": (-0.165, -0.10),
    "right_elbow": (0.165, -0.10),
    "left_wrist": (-0.205, 0.055),
    "right_wrist": (0.205, 0.055),
    "hip_center": (0.0, 0.005),
    "left_hip": (-0.085, 0.02),
    "right_hip": (0.085, 0.02),
    "left_knee": (-0.105, 0.22),
    "right_knee": (0.105, 0.22),
    "left_ankle": (-0.115, 0.405),
    "right_ankle": (0.115, 0.405),
}


@dataclass(frozen=True)
class InstanceLayout:
    instance_id: str
    center_x: float
    center_y: float
    scale: float
    angle_degrees: float
    invisible: tuple[str, ...] = ()
    color: tuple[int, int, int] = (38, 70, 130)


@dataclass(frozen=True)
class ShardLayout:
    shard_id: str
    source_label: str
    instances: tuple[InstanceLayout, ...]
    expected_confidence: tuple[float, float] = (0.40, 1.0)


DEFAULT_LAYOUTS: tuple[ShardLayout, ...] = (
    ShardLayout(
        shard_id="shard_01",
        source_label="中文源-Alpha / studio-alpha",
        instances=(InstanceLayout("pose_alpha", 0.50, 0.51, 0.64, -6.0),),
    ),
    ShardLayout(
        shard_id="shard_02",
        source_label="中文源-Beta / studio-beta",
        instances=(
            InstanceLayout(
                "pose_beta",
                0.49,
                0.50,
                0.69,
                8.0,
                invisible=("left_wrist",),
                color=(118, 57, 84),
            ),
        ),
    ),
    ShardLayout(
        shard_id="shard_03",
        source_label="混合源-Gamma / studio-gamma",
        instances=(
            InstanceLayout("pose_gamma_left", 0.25, 0.51, 0.53, 4.0),
            InstanceLayout(
                "pose_gamma_right", 0.75, 0.50, 0.56, -7.0, color=(144, 78, 32)
            ),
        ),
    ),
    ShardLayout(
        shard_id="shard_04",
        source_label="中文源-Delta / studio-delta",
        instances=(
            InstanceLayout(
                "pose_delta",
                0.51,
                0.50,
                0.62,
                -10.0,
                invisible=("right_ankle",),
                color=(45, 103, 84),
            ),
        ),
    ),
    ShardLayout(
        shard_id="shard_05",
        source_label="混合源-Epsilon / studio-epsilon",
        instances=(
            InstanceLayout("pose_epsilon_left", 0.25, 0.51, 0.50, 6.0),
            InstanceLayout(
                "pose_epsilon_right",
                0.75,
                0.50,
                0.51,
                -5.0,
                color=(102, 49, 126),
            ),
        ),
    ),
)


def generate_dataset(
    out_dir: str | Path,
    *,
    seed: int = 20261122,
    width: int = 128,
    height: int = 128,
    layouts: Iterable[ShardLayout] = DEFAULT_LAYOUTS,
) -> dict[str, Any]:
    """Generate PNG shards, manifest.json, and ground_truth.json.

    The same seed, dimensions, layouts, Pillow version, and output path layout
    produce byte-identical files.
    """

    target = Path(out_dir).resolve()
    images_dir = target / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    generation_params = {
        "seed": seed,
        "width": width,
        "height": height,
        "renderer": "stick-figure-v1",
        "supersample": 4,
    }
    shard_entries: list[dict[str, Any]] = []
    truth_entries: list[dict[str, Any]] = []
    image_digests: dict[str, str] = {}

    for layout in layouts:
        image = Image.new("RGB", (width, height), (247, 246, 240))
        scale_factor = 4
        canvas = Image.new(
            "RGB",
            (width * scale_factor, height * scale_factor),
            (247, 246, 240),
        )
        draw = ImageDraw.Draw(canvas)
        instance_entries: list[dict[str, Any]] = []
        truth_instances: list[dict[str, Any]] = []
        for instance in layout.instances:
            # Per-view jitter is deterministic, but does not affect symmetry.
            jitter_x = rng.uniform(-0.004, 0.004)
            jitter_y = rng.uniform(-0.004, 0.004)
            jitter_angle = rng.uniform(-0.6, 0.6)
            angle = math.radians(instance.angle_degrees + jitter_angle)
            cos_angle = math.cos(angle)
            sin_angle = math.sin(angle)
            points: dict[str, tuple[float, float]] = {}
            for name in KEYPOINT_NAMES:
                base_x, base_y = BASE_POSE[name]
                scaled_x = base_x * instance.scale
                scaled_y = base_y * instance.scale
                rotated_x = scaled_x * cos_angle - scaled_y * sin_angle
                rotated_y = scaled_x * sin_angle + scaled_y * cos_angle
                x = instance.center_x + jitter_x + rotated_x
                y = instance.center_y + jitter_y + rotated_y
                points[name] = (round(x, 8), round(y, 8))

            invisible = set(instance.invisible)
            for left, right in SKELETON:
                if left in invisible or right in invisible:
                    continue
                start = points[left]
                end = points[right]
                draw.line(
                    (
                        round(start[0] * (width - 1) * scale_factor),
                        round(start[1] * (height - 1) * scale_factor),
                        round(end[0] * (width - 1) * scale_factor),
                        round(end[1] * (height - 1) * scale_factor),
                    ),
                    fill=instance.color,
                    width=3 * scale_factor,
                )
            for name in KEYPOINT_NAMES:
                if name in invisible:
                    continue
                x, y = points[name]
                radius = 2.4 * scale_factor
                center = (
                    round(x * (width - 1) * scale_factor),
                    round(y * (height - 1) * scale_factor),
                )
                draw.ellipse(
                    (
                        center[0] - radius,
                        center[1] - radius,
                        center[0] + radius,
                        center[1] + radius,
                    ),
                    fill=instance.color,
                    outline=(24, 30, 40),
                    width=max(1, scale_factor // 2),
                )
            keypoint_entries = {
                name: {
                    "x": points[name][0],
                    "y": points[name][1],
                    "visible": name not in invisible,
                }
                for name in KEYPOINT_NAMES
            }
            instance_entries.append(
                {
                    "instance_id": instance.instance_id,
                    "keypoints": keypoint_entries,
                    "expected_pose_confidence": {
                        "min": layout.expected_confidence[0],
                        "max": layout.expected_confidence[1],
                    },
                }
            )
            truth_instances.append(
                {
                    "instance_id": instance.instance_id,
                    "keypoints": keypoint_entries,
                }
            )

        image = canvas.resize((width, height), Image.Resampling.LANCZOS)
        image_path = images_dir / f"{layout.shard_id}.png"
        image.save(image_path, format="PNG", optimize=False, compress_level=9)
        image_digests[layout.shard_id] = file_sha256(image_path)
        shard_entries.append(
            {
                "shard_id": layout.shard_id,
                "image": f"images/{layout.shard_id}.png",
                "width": width,
                "height": height,
                "source_label": layout.source_label,
                "keypoints": list(KEYPOINT_NAMES),
                "skeleton": [list(edge) for edge in SKELETON],
                "instances": instance_entries,
                "expected_pose_confidence": {
                    "min": layout.expected_confidence[0],
                    "max": layout.expected_confidence[1],
                },
                "generation": {
                    **generation_params,
                    "shard_id": layout.shard_id,
                    "instances": [
                        {
                            "instance_id": instance.instance_id,
                            "center_x": instance.center_x,
                            "center_y": instance.center_y,
                            "scale": instance.scale,
                            "angle_degrees": instance.angle_degrees,
                            "invisible": list(instance.invisible),
                        }
                        for instance in layout.instances
                    ],
                },
            }
        )
        truth_entries.append(
            {
                "shard_id": layout.shard_id,
                "source_label": layout.source_label,
                "image_sha256": image_digests[layout.shard_id],
                "instances": truth_instances,
            }
        )

    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "coordinate_convention": COORDINATE_CONVENTION,
        "generation": generation_params,
        "keypoints": list(KEYPOINT_NAMES),
        "skeleton": [list(edge) for edge in SKELETON],
        "shards": shard_entries,
        "dataset_digest": digest_json(
            {"generation": generation_params, "images": image_digests}
        ),
    }
    ground_truth = {
        "schema_version": SCHEMA_VERSION,
        "source_manifest": "manifest.json",
        "coordinate_convention": COORDINATE_CONVENTION,
        "shards": truth_entries,
    }
    atomic_write_pretty_json(target / "manifest.json", manifest)
    atomic_write_pretty_json(target / "ground_truth.json", ground_truth)
    return {
        "ok": True,
        "out_dir": str(target),
        "manifest": str(target / "manifest.json"),
        "ground_truth": str(target / "ground_truth.json"),
        "shard_count": len(shard_entries),
        "dataset_digest": manifest["dataset_digest"],
        "image_digests": image_digests,
    }
