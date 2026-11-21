"""Deterministic local sample generator.

Produces, from a fixed seed:
  * ``shards/view_XX.png``  - small RGB view shards of one synthetic geometry
    (cube corners + sphere points + plane grid) rendered at fixed poses with
    per-view lighting;
  * ``shards_alt/view_XX.png`` - an alternate render of one view used by the
    demo to replace a shard wholesale;
  * ``manifest.json`` - the scene project manifest declaring per-view camera
    extrinsics/intrinsics, visible landmark projections (with declared depth),
    node ownership, and the global voxelization parameters.

Everything is derived from numpy RandomState(seed) with a fixed draw order,
so re-running the generator yields byte-identical output. No GPU, no network,
no external assets.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from PIL import Image

DEFAULT_SEED = 20240501


def _normalize(vec: np.ndarray) -> np.ndarray:
    return vec / np.linalg.norm(vec)


def look_at(eye: np.ndarray, target: np.ndarray, up=(0.0, 0.0, 1.0)):
    """World->camera rotation R and translation t with +z as viewing axis."""
    forward = _normalize(target - eye)
    right = _normalize(np.cross(forward, np.asarray(up, dtype=np.float64)))
    cam_up = np.cross(right, forward)
    rot = np.stack([right, cam_up, forward], axis=0)
    trans = -rot @ eye
    return rot, trans


def project(point: np.ndarray, rot, trans, fx, fy, cx, cy):
    cam = rot @ point + trans
    if cam[2] <= 0:
        return cam[2], None
    return cam[2], (fx * cam[0] / cam[2] + cx, fy * cam[1] / cam[2] + cy)


def build_scene_landmarks(rng: np.random.RandomState):
    """Return list of dicts: id, node, position(3), base_color(3)."""
    landmarks = []

    def add(node, position, color):
        landmarks.append(
            {
                "landmark_id": f"lm_{len(landmarks):03d}",
                "node": node,
                "position": np.asarray(position, dtype=np.float64),
                "base_color": np.asarray(color, dtype=np.float64),
            }
        )

    for sx in (-1.2, 1.2):
        for sy in (-1.2, 1.2):
            for sz in (-1.2, 1.2):
                add("node_cube", (sx, sy, sz), rng.uniform(40, 230, size=3))
    sphere_center = np.array([2.2, 1.0, 0.8])
    dirs = rng.normal(size=(8, 3))
    for direction in dirs:
        add("node_sphere", sphere_center + 1.3 * _normalize(direction), rng.uniform(40, 230, size=3))
    for gx in (-1.5, 0.0, 1.5):
        for gy in (-1.5, 0.0, 1.5):
            add("node_plane", (gx, gy, -2.0), rng.uniform(40, 230, size=3))
    # Outliers: declared as visible projections but fall outside the image for
    # several views, exercising frustum/image-boundary clipping in stage 1.
    for pos in ((0.0, 6.5, 0.0), (0.0, -6.5, 0.0), (0.0, 0.0, 6.0)):
        add("node_outlier", pos, rng.uniform(40, 230, size=3))
    return landmarks


def render_view(
    rng: np.random.RandomState,
    width: int,
    height: int,
    landmarks,
    rot,
    trans,
    fx: float,
    fy: float,
    cx: float,
    cy: float,
    lighting: float,
    view_index: int,
):
    """Render one deterministic view; return (image uint8 HxWx3, visible list)."""
    grad = np.linspace(0.0, 1.0, height, dtype=np.float64)[:, None, None]
    top = np.array([30.0 + 4 * view_index, 40.0, 70.0 + 3 * view_index])
    bottom = np.array([90.0, 110.0 - 2 * view_index, 140.0])
    image = top[None, None, :] * (1.0 - grad) + bottom[None, None, :] * grad
    image = np.broadcast_to(image, (height, width, 3)).copy()

    projected = []
    for lm in landmarks:
        depth, uv = project(lm["position"], rot, trans, fx, fy, cx, cy)
        if uv is None:
            continue
        projected.append((depth, uv, lm))
    projected.sort(key=lambda item: -item[0])  # far -> near so near overwrites

    yy, xx = np.mgrid[0:height, 0:width]
    for depth, (u, v), lm in projected:
        if not (0.0 <= u <= width - 1 and 0.0 <= v <= height - 1):
            continue
        radius = 2.0
        mask = (xx - u) ** 2 + (yy - v) ** 2 <= radius * radius
        shade = min(1.0, max(0.6, 1.0 - 0.03 * depth))
        image[mask] = lm["base_color"][None, :] * (lighting * shade)

    noise = rng.normal(0.0, 1.1, size=image.shape)
    image = np.clip(image + noise, 0.0, 255.0).astype(np.uint8)

    margin = 16.0  # declared-visible window extends past the image border
    visible = []
    for lm in landmarks:
        depth, uv = project(lm["position"], rot, trans, fx, fy, cx, cy)
        if uv is None or depth <= 0.3:
            continue
        u, v = uv
        if -margin <= u <= width - 1 + margin and -margin <= v <= height - 1 + margin:
            visible.append(
                {
                    "landmark_id": lm["landmark_id"],
                    "pixel": [round(float(u), 3), round(float(v), 3)],
                    "depth": round(float(depth), 6),
                    "node": lm["node"],
                }
            )
    return image, visible


def _camera_for_view(index: int, num_views: int, rng: np.random.RandomState):
    angle = 2.0 * np.pi * index / num_views + 0.35
    radius = 8.5 + 0.4 * ((index % 3) - 1)
    eye = np.array(
        [radius * np.cos(angle), radius * np.sin(angle), 1.5 + 0.5 * (index % 2)],
        dtype=np.float64,
    )
    rot, trans = look_at(eye, np.zeros(3, dtype=np.float64))
    return rot, trans


def _scene_bounds(landmarks):
    positions = np.array(
        [lm["position"] for lm in landmarks], dtype=np.float64
    )
    margin = 1.5
    origin = np.floor((positions.min(axis=0) - margin) * 4) / 4.0
    top = np.ceil((positions.max(axis=0) + margin) * 4) / 4.0
    return origin.tolist(), (top - origin).tolist()


def generate_dataset(
    out_dir,
    seed: int = DEFAULT_SEED,
    num_views: int = 5,
    width: int = 64,
    height: int = 48,
    config_overrides: dict | None = None,
) -> dict:
    """Generate the deterministic sample dataset; returns the manifest dict."""
    if num_views < 4:
        raise ValueError("num_views must be >= 4")
    rng = np.random.RandomState(seed)
    out = Path(out_dir)
    shards_dir = out / "shards"
    alt_dir = out / "shards_alt"
    shards_dir.mkdir(parents=True, exist_ok=True)
    alt_dir.mkdir(parents=True, exist_ok=True)

    landmarks = build_scene_landmarks(rng)
    origin, size = _scene_bounds(landmarks)

    views = []
    intrinsics = []
    for index in range(num_views):
        rot, trans = _camera_for_view(index, num_views, rng)
        jitter = rng.uniform(-0.5, 0.5, size=2)
        fx = 0.9 * width + float(jitter[0])
        fy = 0.9 * width + float(jitter[1])
        cx = (width - 1) / 2.0
        cy = (height - 1) / 2.0
        lighting = 0.72 + 0.07 * index
        image, visible = render_view(
            rng, width, height, landmarks, rot, trans, fx, fy, cx, cy, lighting, index
        )
        image_name = f"view_{index:02d}.png"
        Image.fromarray(image, "RGB").save(shards_dir / image_name)
        intrinsics.append((fx, fy, cx, cy))
        views.append(
            {
                "view_id": f"view_{index:02d}",
                "image": f"shards/{image_name}",
                "width": width,
                "height": height,
                "intrinsic": {"fx": fx, "fy": fy, "cx": cx, "cy": cy},
                "extrinsic": {
                    "rotation": [float(x) for x in rot.reshape(-1)],
                    "translation": [float(x) for x in trans],
                },
                "landmarks": visible,
            }
        )

    # Alternate render of one view (different lighting + noise stream) used to
    # demonstrate wholesale shard replacement.
    alt_index = 2 if num_views > 2 else num_views - 1
    alt_rng = np.random.RandomState(seed + 777)
    rot, trans = _camera_for_view(alt_index, num_views, alt_rng)
    fx, fy, cx, cy = intrinsics[alt_index]
    alt_image, _ = render_view(
        alt_rng, width, height, landmarks, rot, trans, fx, fy, cx, cy, 0.41, alt_index
    )
    Image.fromarray(alt_image, "RGB").save(alt_dir / f"view_{alt_index:02d}.png")

    config = {
        "voxel_resolution": 0.5,
        "truncation_distance": 0.75,
        "fusion_radius": 1.5,
        "sparsity_threshold": 0.02,
        "voxel_feature_dim": 3,
        "ray_step": 0.25,
        "max_ray_depth": 14.0,
        "scene_origin": origin,
        "scene_size": size,
    }
    if config_overrides:
        config.update(config_overrides)

    manifest = {
        "schema_version": 1,
        "dataset": {"name": "synthetic-deterministic", "seed": seed},
        "config": config,
        "views": views,
    }
    (out / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )
    return manifest
