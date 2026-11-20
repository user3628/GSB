"""Manifest compatibility parser for schema versions 1 and 2."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from .errors import E_INVALID_CONFIG, E_NOT_FOUND, E_UNSUPPORTED_SCHEMA, PipelineError
from .fingerprint import digest_json


CURRENT_SCHEMA_VERSION = 2
SUPPORTED_SCHEMA_VERSIONS = (1, 2)

MASK_FORMATS = ("png_palette", "rle", "coco_json")
MASK_FORMAT_ALIASES = {
    "png": "png_palette",
    "indexed_png": "png_palette",
    "png_indexed": "png_palette",
    "png_palette": "png_palette",
    "rle": "rle",
    "rle_string": "rle",
    "coco": "coco_json",
    "coco_json": "coco_json",
    "json": "coco_json",
}

KNOWN_STUFF_NAMES = {
    "background",
    "wall",
    "floor",
    "road",
    "sidewalk",
    "sky",
    "grass",
    "vegetation",
    "water",
    "building",
}
KNOWN_THING_NAMES = {
    "person",
    "car",
    "bicycle",
    "motorcycle",
    "bus",
    "truck",
    "dog",
    "cat",
    "animal",
    "rider",
}


@dataclass(frozen=True)
class ParseOptions:
    default_instance_start_id: int = 100
    min_instance_area: int = 4
    confidence_threshold: float = 0.5
    mask_format: str = "png_palette"
    v1_drop_alpha: bool = True
    stuff_names: tuple[str, ...] = tuple(sorted(KNOWN_STUFF_NAMES))
    thing_names: tuple[str, ...] = tuple(sorted(KNOWN_THING_NAMES))

    def derived_thing(self, name: str) -> bool:
        lowered = name.strip().lower()
        if lowered in set(self.stuff_names):
            return False
        if lowered in set(self.thing_names):
            return True
        # Unknown names are treated as instances by default; manifests can
        # always make the choice explicit with a boolean "thing" field.
        return True


@dataclass(frozen=True)
class CategoryDef:
    name: str
    color: tuple[int, int, int]
    category_id: int
    thing: bool = True
    confidence: float = 1.0

    @property
    def color_hex(self) -> str:
        return "#{:02X}{:02X}{:02X}".format(*self.color)

    @property
    def argb_hex(self) -> str:
        return "#FF{:02X}{:02X}{:02X}".format(*self.color)

    def to_v2_dict(self) -> dict[str, Any]:
        return {
            "id": self.category_id,
            "name": self.name,
            "color": self.color_hex,
            "thing": bool(self.thing),
            "confidence": round(float(self.confidence), 8),
        }

    def fingerprint_dict(self) -> dict[str, Any]:
        return {
            "id": self.category_id,
            "name": self.name,
            "color": self.color_hex,
        }

    def semantics_dict(self) -> dict[str, Any]:
        return {"thing": bool(self.thing), "confidence": round(float(self.confidence), 8)}


@dataclass(frozen=True)
class ShardSpec:
    shard_id: str
    image_path: Path
    image_ref: str
    categories: tuple[CategoryDef, ...]
    instance_start_id: int
    min_instance_area: int
    confidence_threshold: float
    mask_format: str
    source_encoding: str = "auto"
    width: int | None = None
    height: int | None = None

    @property
    def category_by_name(self) -> dict[str, CategoryDef]:
        return {category.name: category for category in self.categories}

    @property
    def category_by_color(self) -> dict[tuple[int, int, int], CategoryDef]:
        return {category.color: category for category in self.categories}

    @property
    def max_category_id(self) -> int:
        return max(category.category_id for category in self.categories)

    def validate(self) -> None:
        if not self.shard_id or "/" in self.shard_id or "\\" in self.shard_id:
            raise PipelineError(E_INVALID_CONFIG, "Shard id must be a simple non-empty name")
        if not self.categories:
            raise PipelineError(
                E_INVALID_CONFIG,
                "Manifest category mapping cannot be empty",
                {"shard_id": self.shard_id},
            )
        names = [category.name for category in self.categories]
        ids = [category.category_id for category in self.categories]
        colors = [category.color for category in self.categories]
        if len(names) != len(set(names)):
            raise PipelineError(E_INVALID_CONFIG, "Duplicate category names", {"shard_id": self.shard_id})
        if len(ids) != len(set(ids)):
            raise PipelineError(E_INVALID_CONFIG, "Duplicate category ids", {"shard_id": self.shard_id})
        if len(colors) != len(set(colors)):
            raise PipelineError(
                E_INVALID_CONFIG,
                "Category colors must be unique after normalization",
                {"shard_id": self.shard_id},
            )
        if any(category_id <= 0 for category_id in ids):
            raise PipelineError(E_INVALID_CONFIG, "Category ids must be positive; 0 is reserved for background")
        if self.instance_start_id <= self.max_category_id or self.instance_start_id <= 0:
            raise PipelineError(
                E_INVALID_CONFIG,
                "instance_start_id conflicts with reserved/category ids",
                {
                    "shard_id": self.shard_id,
                    "instance_start_id": self.instance_start_id,
                    "max_category_id": self.max_category_id,
                },
            )
        if self.min_instance_area < 1:
            raise PipelineError(E_INVALID_CONFIG, "min_instance_area must be at least 1")
        if not 0.0 <= self.confidence_threshold <= 1.0:
            raise PipelineError(E_INVALID_CONFIG, "confidence_threshold must be in [0, 1]")
        if any(not 0.0 <= category.confidence <= 1.0 for category in self.categories):
            raise PipelineError(E_INVALID_CONFIG, "Category confidence must be in [0, 1]")
        if self.mask_format not in MASK_FORMATS:
            raise PipelineError(E_INVALID_CONFIG, "Unsupported mask format", {"format": self.mask_format})
        if self.width is not None and self.width <= 0:
            raise PipelineError(E_INVALID_CONFIG, "Image width must be positive")
        if self.height is not None and self.height <= 0:
            raise PipelineError(E_INVALID_CONFIG, "Image height must be positive")

    def category_fingerprint(self) -> str:
        return digest_json([category.fingerprint_dict() for category in self.categories])

    def semantics_fingerprint(self) -> str:
        return digest_json([category.semantics_dict() for category in self.categories])

    def axes(self) -> dict[str, Any]:
        return {
            "schema_version": None,  # filled by Manifest.version_axes
            "category_fingerprint": self.category_fingerprint(),
            "semantics_fingerprint": self.semantics_fingerprint(),
            "instance_start_id": self.instance_start_id,
            "min_instance_area": self.min_instance_area,
            "confidence_threshold": round(float(self.confidence_threshold), 8),
            "mask_format": self.mask_format,
            "source_encoding": self.source_encoding,
        }

    def to_v2_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "id": self.shard_id,
            "image": self.image_ref,
            "width": self.width,
            "height": self.height,
            "categories": [category.to_v2_dict() for category in self.categories],
            "instance_start_id": self.instance_start_id,
            "min_instance_area": self.min_instance_area,
            "confidence_threshold": round(float(self.confidence_threshold), 8),
            "mask_format": self.mask_format,
            "encoding": self.source_encoding,
        }
        return value

    def to_v1_dict(self) -> dict[str, Any]:
        return {
            "id": self.shard_id,
            "image": self.image_ref,
            "width": self.width,
            "height": self.height,
            "categories": {
                category.name: category.argb_hex
                for category in sorted(self.categories, key=lambda item: item.category_id)
            },
            # These are optional, backwards-compatible extensions. A bare v1
            # manifest still receives the documented deterministic defaults.
            "thing_categories": [category.name for category in self.categories if category.thing],
            "category_confidences": {
                category.name: round(float(category.confidence), 8) for category in self.categories
            },
            "confidence_threshold": int(round(float(self.confidence_threshold) * 100)),
            "min_instance_area": self.min_instance_area,
            "mask_format": self.mask_format,
            "encoding": self.source_encoding,
        }


@dataclass
class Manifest:
    project: str
    schema_version: int
    shards: tuple[ShardSpec, ...]
    source_path: Path | None = None

    def __post_init__(self) -> None:
        if not self.shards:
            raise PipelineError(E_INVALID_CONFIG, "Manifest must contain at least one shard")
        ids = [shard.shard_id for shard in self.shards]
        if len(ids) != len(set(ids)):
            raise PipelineError(E_INVALID_CONFIG, "Duplicate shard ids in manifest")
        for shard in self.shards:
            shard.validate()

    @property
    def shard_by_id(self) -> dict[str, ShardSpec]:
        return {shard.shard_id: shard for shard in self.shards}

    def version_axes(self, shard: ShardSpec) -> dict[str, Any]:
        axes = shard.axes()
        axes["schema_version"] = int(self.schema_version)
        return axes

    def manifest_version_fingerprint(self, shard: ShardSpec) -> str:
        return digest_json(
            {
                "schema_version": int(self.schema_version),
                "project": self.project,
                "shard": self.version_axes(shard),
            }
        )

    def overall_fingerprint(self) -> str:
        return digest_json(
            {
                "schema_version": int(self.schema_version),
                "project": self.project,
                "shards": [self.version_axes(shard) for shard in self.shards],
            }
        )

    def to_dict(self, *, version: int = CURRENT_SCHEMA_VERSION, for_workspace: bool = False) -> dict[str, Any]:
        if version == 2:
            value: dict[str, Any] = {
                "schema_version": 2,
                "project": self.project,
                "defaults": {
                    "instance_start_id": 100,
                    "min_instance_area": 4,
                    "confidence_threshold": 0.5,
                    "mask_format": "png_palette",
                },
                "shards": [shard.to_v2_dict() for shard in self.shards],
            }
            if for_workspace:
                value["import_metadata"] = {"schema_version": int(self.schema_version)}
            elif self.schema_version != 2:
                value["import_metadata"] = {"schema_version": int(self.schema_version)}
            return value
        if version == 1:
            return {
                "schema_version": 1,
                "project": self.project,
                "shards": [shard.to_v1_dict() for shard in self.shards],
            }
        raise PipelineError(
            E_UNSUPPORTED_SCHEMA,
            "Unsupported target manifest schema version",
            {"requested": version, "supported": list(SUPPORTED_SCHEMA_VERSIONS)},
        )

    def to_json(self, *, version: int = CURRENT_SCHEMA_VERSION, for_workspace: bool = False) -> str:
        return json.dumps(self.to_dict(version=version, for_workspace=for_workspace), sort_keys=True, indent=2) + "\n"


def _normalize_mask_format(value: Any) -> str:
    key = str(value).strip().lower()
    if key not in MASK_FORMAT_ALIASES:
        raise PipelineError(
            E_INVALID_CONFIG,
            "Unsupported mask encoding preference",
            {"value": value, "supported": list(MASK_FORMATS)},
        )
    return MASK_FORMAT_ALIASES[key]


def _parse_hex_color(value: Any, *, schema_version: int, drop_alpha: bool) -> tuple[int, int, int]:
    if not isinstance(value, str):
        raise PipelineError(E_INVALID_CONFIG, "Category color must be a hex string", {"value": value})
    raw = value.strip().upper()
    if raw.startswith("#"):
        raw = raw[1:]
    elif raw.startswith("0X"):
        raw = raw[2:]
    if len(raw) == 8:
        try:
            alpha = int(raw[0:2], 16)
        except ValueError as exc:
            raise PipelineError(E_INVALID_CONFIG, "Invalid hexadecimal color", {"value": value}) from exc
        if not drop_alpha and alpha != 0xFF:
            raise PipelineError(
                E_INVALID_CONFIG,
                "ARGB alpha must be FF when alpha dropping is disabled",
                {"value": value},
            )
        raw = raw[2:]
    elif len(raw) != 6:
        raise PipelineError(
            E_INVALID_CONFIG,
            "Color must be #AARRGGBB (v1) or #RRGGBB (v2)",
            {"value": value, "schema_version": schema_version},
        )
    try:
        return (int(raw[0:2], 16), int(raw[2:4], 16), int(raw[4:6], 16))
    except ValueError as exc:
        raise PipelineError(E_INVALID_CONFIG, "Invalid hexadecimal color", {"value": value}) from exc


def _parse_confidence(value: Any, *, schema_version: int, default: float) -> float:
    if value is None:
        return default
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise PipelineError(E_INVALID_CONFIG, "Confidence threshold must be numeric", {"value": value}) from exc
    if schema_version == 1:
        is_percentage_int = (isinstance(value, int) and not isinstance(value, bool)) or (
            isinstance(value, str) and value.strip().lstrip("+").isdigit()
        )
        if is_percentage_int or number > 1.0:
            number /= 100.0
    elif number > 1.0:
        number /= 100.0
    if not 0.0 <= number <= 1.0:
        raise PipelineError(E_INVALID_CONFIG, "Confidence threshold must normalize to [0, 1]", {"value": value})
    return number


def _category_entries(value: Any) -> list[tuple[str, Any]]:
    if isinstance(value, Mapping):
        return [(str(name), item) for name, item in value.items()]
    if isinstance(value, list):
        entries: list[tuple[str, Any]] = []
        for item in value:
            if not isinstance(item, Mapping) or "name" not in item:
                raise PipelineError(E_INVALID_CONFIG, "Category list entries require a name", {"value": item})
            entries.append((str(item["name"]), item))
        return entries
    raise PipelineError(E_INVALID_CONFIG, "Category mapping must be an object or list", {"value": value})


def _parse_categories(
    value: Any,
    *,
    schema_version: int,
    options: ParseOptions,
    thing_categories: Iterable[str] = (),
    category_confidences: Mapping[str, Any] | None = None,
) -> tuple[CategoryDef, ...]:
    entries = _category_entries(value)
    if not entries:
        raise PipelineError(E_INVALID_CONFIG, "Manifest category mapping cannot be empty")
    if schema_version == 1:
        # A bare v1 category map is order-insensitive JSON. Lexicographic name
        # ordering makes category id assignment deterministic across exporters.
        entries.sort(key=lambda item: item[0])

    explicit_ids: list[int] = []
    for _, item in entries:
        if isinstance(item, Mapping) and item.get("id") is not None:
            try:
                explicit_ids.append(int(item["id"]))
            except (TypeError, ValueError) as exc:
                raise PipelineError(E_INVALID_CONFIG, "Category id must be an integer", {"value": item}) from exc
    if explicit_ids and (len(explicit_ids) != len(entries) or len(explicit_ids) != len(set(explicit_ids))):
        raise PipelineError(E_INVALID_CONFIG, "Explicit category ids must be present and unique for every category")

    next_unused = 1
    used_ids = set(explicit_ids)
    thing_set = {name.lower() for name in thing_categories}
    confidences = category_confidences or {}
    result: list[CategoryDef] = []
    for name, item in entries:
        if isinstance(item, str):
            color_value = item
            thing_value: bool | None = None
            confidence_value: Any = confidences.get(name)
            category_id_value: int | None = None
        elif isinstance(item, Mapping):
            color_value = item.get("color", item.get("argb", item.get("value")))
            thing_value = item.get("thing")
            confidence_value = item.get("confidence", confidences.get(name))
            category_id_value = int(item["id"]) if item.get("id") is not None else None
        else:
            raise PipelineError(E_INVALID_CONFIG, "Malformed category declaration", {"name": name, "value": item})
        if color_value is None:
            raise PipelineError(E_INVALID_CONFIG, "Category declaration is missing a color", {"name": name})
        color = _parse_hex_color(color_value, schema_version=schema_version, drop_alpha=options.v1_drop_alpha)
        if category_id_value is None:
            while next_unused in used_ids:
                next_unused += 1
            category_id_value = next_unused
            used_ids.add(next_unused)
        if thing_value is None:
            if thing_set:
                thing = name.lower() in thing_set
            else:
                thing = options.derived_thing(name)
        elif isinstance(thing_value, bool):
            thing = thing_value
        else:
            raise PipelineError(E_INVALID_CONFIG, "Category thing flag must be boolean", {"name": name})
        confidence = _parse_confidence(confidence_value, schema_version=2, default=1.0)
        result.append(
            CategoryDef(
                name=name,
                color=color,
                category_id=category_id_value,
                thing=thing,
                confidence=confidence,
            )
        )
    return tuple(sorted(result, key=lambda category: category.category_id))


def _shard_items(value: Any) -> list[tuple[str, Mapping[str, Any]]]:
    if isinstance(value, list):
        items = []
        for item in value:
            if not isinstance(item, Mapping) or "id" not in item:
                raise PipelineError(E_INVALID_CONFIG, "Each shard requires an id")
            items.append((str(item["id"]), item))
        return items
    if isinstance(value, Mapping):
        items = []
        for shard_id, item in value.items():
            if not isinstance(item, Mapping):
                raise PipelineError(E_INVALID_CONFIG, "Malformed shard declaration", {"shard_id": shard_id})
            merged = dict(item)
            merged.setdefault("id", shard_id)
            items.append((str(shard_id), merged))
        return items
    raise PipelineError(E_INVALID_CONFIG, "Manifest shards must be a list or object")


def _collect_thing_categories(value: Any, shard_value: Any = None) -> list[str]:
    result: list[str] = []
    if isinstance(value, list):
        result.extend(str(item) for item in value)
    if isinstance(shard_value, list):
        result.extend(str(item) for item in shard_value)
    return result


def parse_manifest(
    data: Mapping[str, Any],
    *,
    base_dir: Path,
    source_path: Path | None = None,
    options: ParseOptions | None = None,
) -> Manifest:
    options = options or ParseOptions()
    raw_version = data.get("schema_version", data.get("version", 1))
    if isinstance(raw_version, bool):
        raise PipelineError(E_UNSUPPORTED_SCHEMA, "Boolean schema versions are invalid")
    try:
        schema_version = int(raw_version)
    except (TypeError, ValueError) as exc:
        raise PipelineError(
            E_UNSUPPORTED_SCHEMA,
            "Manifest schema version must be an integer",
            {"value": raw_version, "supported": list(SUPPORTED_SCHEMA_VERSIONS)},
        ) from exc
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        raise PipelineError(
            E_UNSUPPORTED_SCHEMA,
            "Unsupported manifest schema version",
            {"version": schema_version, "supported": list(SUPPORTED_SCHEMA_VERSIONS)},
        )

    if "shards" not in data:
        raise PipelineError(E_INVALID_CONFIG, "Manifest is missing shards")
    defaults = data.get("defaults") if isinstance(data.get("defaults"), Mapping) else {}
    global_categories = data.get("categories")
    global_things = _collect_thing_categories(data.get("thing_categories"))
    global_confidences = data.get("category_confidences")
    if not isinstance(global_confidences, Mapping):
        global_confidences = None

    shards: list[ShardSpec] = []
    for shard_id, item in _shard_items(data["shards"]):
        if str(item.get("id", shard_id)) != shard_id:
            raise PipelineError(E_INVALID_CONFIG, "Shard id mismatch in mapping", {"shard_id": shard_id})
        categories_value = item.get("categories", global_categories)
        if categories_value is None:
            raise PipelineError(E_INVALID_CONFIG, "Manifest category mapping cannot be empty", {"shard_id": shard_id})
        shard_things = _collect_thing_categories(item.get("thing_categories"))
        shard_confidences = item.get("category_confidences")
        if not isinstance(shard_confidences, Mapping):
            shard_confidences = global_confidences
        elif global_confidences:
            merged_confidences = dict(global_confidences)
            merged_confidences.update(shard_confidences)
            shard_confidences = merged_confidences
        categories = _parse_categories(
            categories_value,
            schema_version=schema_version,
            options=options,
            thing_categories=global_things + shard_things,
            category_confidences=shard_confidences,
        )
        max_category_id = max(category.category_id for category in categories)
        raw_start = item.get("instance_start_id", item.get("instance_start", defaults.get("instance_start_id")))
        if raw_start is None:
            instance_start_id = max(int(options.default_instance_start_id), max_category_id + 1)
        else:
            try:
                instance_start_id = int(raw_start)
            except (TypeError, ValueError) as exc:
                raise PipelineError(
                    E_INVALID_CONFIG,
                    "instance_start_id must be an integer",
                    {"shard_id": shard_id, "value": raw_start},
                ) from exc
        raw_min_area = item.get("min_instance_area", defaults.get("min_instance_area", options.min_instance_area))
        try:
            min_area = int(raw_min_area)
        except (TypeError, ValueError) as exc:
            raise PipelineError(E_INVALID_CONFIG, "min_instance_area must be an integer") from exc
        threshold = _parse_confidence(
            item.get("confidence_threshold", defaults.get("confidence_threshold", options.confidence_threshold)),
            schema_version=schema_version,
            default=options.confidence_threshold,
        )
        mask_format = _normalize_mask_format(item.get("mask_format", defaults.get("mask_format", options.mask_format)))
        image_ref = item.get("image", item.get("image_path"))
        if not isinstance(image_ref, str) or not image_ref:
            raise PipelineError(E_INVALID_CONFIG, "Shard is missing an image path", {"shard_id": shard_id})
        image_path = Path(image_ref)
        if not image_path.is_absolute():
            image_path = base_dir / image_path
        width = item.get("width")
        height = item.get("height")
        shard = ShardSpec(
            shard_id=shard_id,
            image_path=image_path.resolve(),
            image_ref=image_ref,
            categories=categories,
            instance_start_id=instance_start_id,
            min_instance_area=min_area,
            confidence_threshold=threshold,
            mask_format=mask_format,
            source_encoding=str(item.get("encoding", "auto")),
            width=int(width) if width is not None else None,
            height=int(height) if height is not None else None,
        )
        shard.validate()
        shards.append(shard)

    project = str(data.get("project", "panopipe"))
    return Manifest(project=project, schema_version=schema_version, shards=tuple(shards), source_path=source_path)


def load_manifest(path: Path, *, options: ParseOptions | None = None) -> Manifest:
    path = path.resolve()
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError as exc:
        raise PipelineError(E_NOT_FOUND, "Manifest file not found", {"path": str(path)}) from exc
    except json.JSONDecodeError as exc:
        raise PipelineError(E_INVALID_CONFIG, "Manifest is not valid JSON", {"path": str(path), "error": str(exc)}) from exc
    if not isinstance(data, Mapping):
        raise PipelineError(E_INVALID_CONFIG, "Manifest root must be an object")
    return parse_manifest(data, base_dir=path.parent, source_path=path, options=options)
