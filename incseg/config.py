"""Workspace / tile configuration model with strict validation (E_INVALID_CONFIG)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

from .errors import InvalidConfigError, WorkspaceError
from .util import atomic_write_text, canonical_json, pretty_json, sha256_text

_HEX_COLOR = re.compile(r"^#[0-9a-fA-F]{6}$")
_TILE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")

BACKGROUND_CLASS = 0
MAX_CLASS_ID = 254  # label maps are stored as 8-bit PNG


def parse_hex_color(text: str) -> tuple[int, int, int]:
    return (int(text[1:3], 16), int(text[3:5], 16), int(text[5:7], 16))


def _fail(field: str, message: str) -> None:
    raise InvalidConfigError(message, {"field": field})


def _check_weight(name: str, value) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(name, f"{name} must be a number in [0, 1]")
    value = float(value)
    if not 0.0 <= value <= 1.0:
        _fail(name, f"{name} must be within [0, 1], got {value}")
    return value


def _check_int(name: str, value, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(name, f"{name} must be an integer")
    if not low <= value <= high:
        _fail(name, f"{name} must be within [{low}, {high}], got {value}")
    return value


@dataclass
class TileConfig:
    tile_id: str
    image: str
    color_map: dict  # "#rrggbb" -> class id (int)
    class_names: dict  # class id (int) -> human name
    class_priority: list  # class ids, most preferred first
    connectivity: int
    min_area: int
    smooth_weight: float
    smooth_iterations: int
    refine_iterations: int
    fusion_weight: float

    @staticmethod
    def from_dict(data: dict) -> "TileConfig":
        if not isinstance(data, dict):
            _fail("tile", "tile entry must be an object")
        tile_id = data.get("tile_id")
        if not isinstance(tile_id, str) or not _TILE_ID.match(tile_id):
            _fail("tile_id", f"invalid tile_id: {tile_id!r}")
        image = data.get("image")
        if not isinstance(image, str) or not image:
            _fail("image", f"tile {tile_id}: image path is required")

        color_map = data.get("color_map")
        if not isinstance(color_map, dict) or not color_map:
            _fail("color_map", f"tile {tile_id}: color_map is required and must be non-empty")
        parsed_map: dict[str, int] = {}
        used_ids: set[int] = set()
        for raw_color, raw_id in color_map.items():
            if not isinstance(raw_color, str) or not _HEX_COLOR.match(raw_color):
                _fail("color_map", f"tile {tile_id}: invalid color key {raw_color!r}")
            color_id = _check_int("color_map", raw_id, 0, MAX_CLASS_ID)
            if color_id in used_ids:
                _fail("color_map", f"tile {tile_id}: duplicate class id {color_id}")
            used_ids.add(color_id)
            parsed_map[raw_color.lower()] = color_id

        raw_names = data.get("class_names", {})
        if not isinstance(raw_names, dict):
            _fail("class_names", f"tile {tile_id}: class_names must be an object")
        class_names = {int(key): str(val) for key, val in raw_names.items()}
        for class_id in used_ids:
            class_names.setdefault(class_id, f"class_{class_id}")

        priority = data.get("class_priority")
        if priority is None:
            priority = sorted(used_ids)
        if not isinstance(priority, list) or sorted(priority) != sorted(used_ids):
            _fail("class_priority", f"tile {tile_id}: class_priority must list each class id exactly once")

        connectivity = _check_int("connectivity", data.get("connectivity"), 4, 8)
        if connectivity not in (4, 8):
            _fail("connectivity", f"tile {tile_id}: connectivity must be 4 or 8, got {connectivity}")
        min_area = _check_int("min_area", data.get("min_area"), 0, 1_000_000)
        smooth_iterations = _check_int("smooth_iterations", data.get("smooth_iterations", 1), 0, 50)
        refine_iterations = _check_int("refine_iterations", data.get("refine_iterations", 1), 0, 100)
        smooth_weight = _check_weight("smooth_weight", data.get("smooth_weight", 0.0))
        fusion_weight = _check_weight("fusion_weight", data.get("fusion_weight", 0.5))

        return TileConfig(
            tile_id=tile_id,
            image=image,
            color_map=parsed_map,
            class_names=class_names,
            class_priority=[int(c) for c in priority],
            connectivity=connectivity,
            min_area=min_area,
            smooth_weight=smooth_weight,
            smooth_iterations=smooth_iterations,
            refine_iterations=refine_iterations,
            fusion_weight=fusion_weight,
        )

    def to_dict(self) -> dict:
        return {
            "tile_id": self.tile_id,
            "image": self.image,
            "color_map": dict(sorted(self.color_map.items())),
            "class_names": {str(k): self.class_names[k] for k in sorted(self.class_names)},
            "class_priority": list(self.class_priority),
            "connectivity": self.connectivity,
            "min_area": self.min_area,
            "smooth_weight": self.smooth_weight,
            "smooth_iterations": self.smooth_iterations,
            "refine_iterations": self.refine_iterations,
            "fusion_weight": self.fusion_weight,
        }

    @property
    def class_ids(self) -> list:
        return sorted(self.color_map.values())

    def stage_config(self, stage: str) -> dict:
        """The exact config subset a stage depends on (drives its fingerprint)."""
        if stage == "classify":
            return {
                "color_map": sorted(self.color_map.items()),
                "smooth_iterations": self.smooth_iterations,
                "smooth_weight": self.smooth_weight,
            }
        if stage == "instances":
            return {"connectivity": self.connectivity, "min_area": self.min_area}
        if stage == "postprocess":
            return {
                "class_priority": list(self.class_priority),
                "fusion_weight": self.fusion_weight,
                "refine_iterations": self.refine_iterations,
            }
        if stage == "quantize":
            palette = [
                [class_id, list(parse_hex_color(color))]
                for color, class_id in sorted(self.color_map.items(), key=lambda kv: kv[1])
            ]
            return {"palette": palette, "summary_format": 1}
        raise WorkspaceError(f"unknown stage {stage!r}")

    def stage_config_version(self, stage: str) -> str:
        return sha256_text(canonical_json(self.stage_config(stage)))


@dataclass
class WorkspaceConfig:
    config_version: str
    seed: int
    tiles: list = field(default_factory=list)

    @staticmethod
    def from_dict(data: dict) -> "WorkspaceConfig":
        if not isinstance(data, dict):
            _fail("config", "config root must be an object")
        config_version = data.get("config_version")
        if not isinstance(config_version, str) or not config_version:
            _fail("config_version", "config_version is required")
        seed = _check_int("seed", data.get("seed"), 0, 2**31 - 1)
        raw_tiles = data.get("tiles")
        if not isinstance(raw_tiles, list) or not raw_tiles:
            _fail("tiles", "tiles must be a non-empty list")
        tiles = [TileConfig.from_dict(item) for item in raw_tiles]
        seen = set()
        for tile in tiles:
            if tile.tile_id in seen:
                _fail("tiles", f"duplicate tile_id {tile.tile_id!r}")
            seen.add(tile.tile_id)
        return WorkspaceConfig(config_version=config_version, seed=seed, tiles=tiles)

    @staticmethod
    def load(path) -> "WorkspaceConfig":
        import json

        path = Path(path)
        if not path.exists():
            raise WorkspaceError(f"config not found: {path}")
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise InvalidConfigError(f"cannot parse config {path}: {exc}")
        return WorkspaceConfig.from_dict(data)

    def to_dict(self) -> dict:
        return {
            "config_version": self.config_version,
            "seed": self.seed,
            "tiles": [tile.to_dict() for tile in self.tiles],
        }

    def save(self, path) -> None:
        atomic_write_text(path, pretty_json(self.to_dict()))

    def tile(self, tile_id: str) -> TileConfig:
        for tile in self.tiles:
            if tile.tile_id == tile_id:
                return tile
        raise WorkspaceError(f"unknown tile {tile_id!r}", {"tile_id": tile_id})

    def replace_tile(self, new_tile: TileConfig) -> None:
        for idx, tile in enumerate(self.tiles):
            if tile.tile_id == new_tile.tile_id:
                self.tiles[idx] = new_tile
                return
        self.tiles.append(new_tile)
