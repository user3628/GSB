"""Scene project manifest: schema, validation and typed access.

The manifest is the immutable-declaration input of the engine. It declares,
per view shard, the camera extrinsics/intrinsics (treated as immutable truth,
never optimized), the visible landmark projections (with declared depth), the
scene node ownership, and the global voxelization parameters.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from .errors import E_MANIFEST_INVALID, EngineError

SCHEMA_VERSION = 1

CONFIG_KEYS = (
    "voxel_resolution",
    "truncation_distance",
    "fusion_radius",
    "sparsity_threshold",
    "voxel_feature_dim",
    "ray_step",
    "max_ray_depth",
    "scene_origin",
    "scene_size",
)


@dataclass(frozen=True)
class Intrinsic:
    fx: float
    fy: float
    cx: float
    cy: float


@dataclass(frozen=True)
class Extrinsic:
    rotation: tuple  # 9 floats, row-major, world -> camera
    translation: tuple  # 3 floats


@dataclass(frozen=True)
class Landmark:
    landmark_id: str
    pixel: tuple  # (u, v) float pixel coordinates
    depth: float  # declared depth along the camera z axis
    node: str


@dataclass(frozen=True)
class ViewSpec:
    view_id: str
    image: str  # path of the PNG shard, relative to the manifest directory
    width: int
    height: int
    intrinsic: Intrinsic
    extrinsic: Extrinsic
    landmarks: tuple


@dataclass(frozen=True)
class Manifest:
    schema_version: int
    dataset_name: str
    seed: int
    config: dict
    views: tuple
    raw: dict = field(compare=False)


def _fail(msg: str, details: dict | None = None):
    raise EngineError(E_MANIFEST_INVALID, msg, details or {})


def _require(cond: bool, msg: str, details: dict | None = None):
    if not cond:
        _fail(msg, details)


def _parse_view(obj: dict, index: int) -> ViewSpec:
    for key in ("view_id", "image", "width", "height", "intrinsic", "extrinsic", "landmarks"):
        _require(key in obj, f"view[{index}] missing key '{key}'", {"view_index": index})
    intr = obj["intrinsic"]
    extr = obj["extrinsic"]
    _require(
        all(k in intr for k in ("fx", "fy", "cx", "cy")),
        f"view[{index}] intrinsic incomplete",
    )
    _require(
        isinstance(extr.get("rotation"), list) and len(extr["rotation"]) == 9,
        f"view[{index}] extrinsic.rotation must have 9 elements",
    )
    _require(
        isinstance(extr.get("translation"), list) and len(extr["translation"]) == 3,
        f"view[{index}] extrinsic.translation must have 3 elements",
    )
    landmarks = []
    for lm in obj["landmarks"]:
        _require(
            all(k in lm for k in ("landmark_id", "pixel", "depth", "node")),
            f"view[{index}] landmark entry incomplete",
        )
        _require(len(lm["pixel"]) == 2, "landmark pixel must be [u, v]")
        landmarks.append(
            Landmark(
                landmark_id=str(lm["landmark_id"]),
                pixel=(float(lm["pixel"][0]), float(lm["pixel"][1])),
                depth=float(lm["depth"]),
                node=str(lm["node"]),
            )
        )
    return ViewSpec(
        view_id=str(obj["view_id"]),
        image=str(obj["image"]),
        width=int(obj["width"]),
        height=int(obj["height"]),
        intrinsic=Intrinsic(
            fx=float(intr["fx"]), fy=float(intr["fy"]), cx=float(intr["cx"]), cy=float(intr["cy"])
        ),
        extrinsic=Extrinsic(
            rotation=tuple(float(x) for x in extr["rotation"]),
            translation=tuple(float(x) for x in extr["translation"]),
        ),
        landmarks=tuple(landmarks),
    )


def parse_manifest(obj: dict) -> Manifest:
    _require(isinstance(obj, dict), "manifest must be a JSON object")
    _require(obj.get("schema_version") == SCHEMA_VERSION, "unsupported schema_version")
    _require("config" in obj and isinstance(obj["config"], dict), "missing config")
    config = dict(obj["config"])
    for key in CONFIG_KEYS:
        _require(key in config, f"config missing '{key}'", {"missing_key": key})
    _require(float(config["voxel_resolution"]) > 0, "voxel_resolution must be > 0")
    _require(float(config["truncation_distance"]) >= 0, "truncation_distance must be >= 0")
    _require(float(config["fusion_radius"]) >= 0, "fusion_radius must be >= 0")
    _require(int(config["voxel_feature_dim"]) == 3, "voxel_feature_dim must be 3 (RGB)")
    _require(float(config["ray_step"]) > 0, "ray_step must be > 0")
    _require(len(config["scene_origin"]) == 3, "scene_origin must have 3 elements")
    _require(len(config["scene_size"]) == 3, "scene_size must have 3 elements")
    _require(
        all(float(s) > 0 for s in config["scene_size"]), "scene_size elements must be > 0"
    )
    views_raw = obj.get("views")
    _require(isinstance(views_raw, list) and len(views_raw) >= 1, "manifest needs >= 1 view")
    views = tuple(_parse_view(v, i) for i, v in enumerate(views_raw))
    view_ids = [v.view_id for v in views]
    _require(len(set(view_ids)) == len(view_ids), "duplicate view_id in manifest")
    dataset = obj.get("dataset", {})
    return Manifest(
        schema_version=SCHEMA_VERSION,
        dataset_name=str(dataset.get("name", "dataset")),
        seed=int(dataset.get("seed", 0)),
        config=config,
        views=views,
        raw=obj,
    )


def load_manifest(path) -> Manifest:
    path = Path(path)
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _fail(f"cannot read manifest: {exc}", {"path": str(path)})
    return parse_manifest(obj)
