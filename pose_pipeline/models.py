"""Typed, validated view of the local sample manifest."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .constants import COORDINATE_CONVENTION, ERROR_INVALID_MANIFEST, SCHEMA_VERSION
from .errors import PipelineError
from .util import digest_json, read_json_file, safe_name


@dataclass(frozen=True)
class ConfidenceInterval:
    minimum: float
    maximum: float

    def contains(self, value: float) -> bool:
        return self.minimum <= value <= self.maximum

    def to_mapping(self) -> dict[str, float]:
        return {"min": self.minimum, "max": self.maximum}


@dataclass(frozen=True)
class KeypointTruth:
    name: str
    x: float
    y: float
    visible: bool

    def to_mapping(self) -> dict[str, Any]:
        return {"x": self.x, "y": self.y, "visible": self.visible}


@dataclass(frozen=True)
class PoseTruth:
    instance_id: str
    keypoints: dict[str, KeypointTruth]
    expected_confidence: ConfidenceInterval | None = None

    def ordered_keypoints(self, names: Iterable[str]) -> list[KeypointTruth]:
        return [self.keypoints[name] for name in names if name in self.keypoints]

    def truth_digest(self) -> str:
        return digest_json(
            {
                "instance_id": self.instance_id,
                "keypoints": {
                    name: {"x": point.x, "y": point.y, "visible": point.visible}
                    for name, point in sorted(self.keypoints.items())
                },
            }
        )

    def coordinate_digest(self) -> str:
        return digest_json(
            {
                name: {"x": point.x, "y": point.y}
                for name, point in sorted(self.keypoints.items())
            }
        )

    def visibility_digest(self) -> str:
        return digest_json(
            {name: point.visible for name, point in sorted(self.keypoints.items())}
        )

    def expected_confidence_mapping(self) -> dict[str, float] | None:
        if self.expected_confidence is None:
            return None
        return self.expected_confidence.to_mapping()


@dataclass(frozen=True)
class ShardSpec:
    shard_id: str
    image: str
    width: int
    height: int
    source_label: str
    keypoints: tuple[str, ...]
    skeleton: tuple[tuple[str, str], ...]
    instances: tuple[PoseTruth, ...]
    expected_confidence: ConfidenceInterval
    generation: dict[str, Any]
    base_dir: Path

    @property
    def image_path(self) -> Path:
        candidate = Path(self.image)
        if candidate.is_absolute():
            return candidate
        return (self.base_dir / candidate).resolve()

    def truth_digest(self) -> str:
        return digest_json(
            [instance.truth_digest() for instance in self.instances]
        )

    def coordinate_digest(self) -> str:
        return digest_json(
            [instance.coordinate_digest() for instance in self.instances]
        )

    def visibility_digest(self) -> str:
        return digest_json(
            [instance.visibility_digest() for instance in self.instances]
        )

    def generation_digest(self) -> str:
        return digest_json(self.generation)

    def skeleton_digest(self) -> str:
        return digest_json([list(edge) for edge in self.skeleton])

    def expected_confidence_digest(self) -> str:
        return digest_json(
            {
                "shard": self.expected_confidence.to_mapping(),
                "instances": {
                    instance.instance_id: instance.expected_confidence_mapping()
                    for instance in self.instances
                },
            }
        )

    def truth_core_mapping(self) -> list[dict[str, Any]]:
        return [
            {
                "instance_id": instance.instance_id,
                "keypoints": {
                    name: {
                        "x": point.x,
                        "y": point.y,
                        "visible": point.visible,
                    }
                    for name, point in sorted(instance.keypoints.items())
                },
            }
            for instance in self.instances
        ]

    def input_index(self, image_sha256: str) -> dict[str, Any]:
        return {
            "image_sha256": image_sha256,
            "width": self.width,
            "height": self.height,
            "truth_digest": self.truth_digest(),
            "coordinate_digest": self.coordinate_digest(),
            "visibility_digest": self.visibility_digest(),
            "generation_digest": self.generation_digest(),
            "skeleton_digest": self.skeleton_digest(),
            "source_label": self.source_label,
            "expected_confidence_digest": self.expected_confidence_digest(),
            "keypoints": list(self.keypoints),
        }


@dataclass(frozen=True)
class Manifest:
    schema_version: int
    coordinate_convention: str
    keypoints: tuple[str, ...]
    skeleton: tuple[tuple[str, str], ...]
    shards: tuple[ShardSpec, ...]
    generation: dict[str, Any]
    base_dir: Path
    raw: dict[str, Any]

    @classmethod
    def from_file(cls, path: str | Path) -> "Manifest":
        manifest_path = Path(path).resolve()
        try:
            mapping = read_json_file(manifest_path)
        except (OSError, ValueError) as exc:
            raise _manifest_error(f"cannot read manifest: {exc}") from exc
        return cls.from_mapping(mapping, base_dir=manifest_path.parent)

    @classmethod
    def from_mapping(cls, mapping: Any, *, base_dir: str | Path) -> "Manifest":
        if not isinstance(mapping, dict):
            raise _manifest_error("manifest must be a JSON object")
        schema_version = mapping.get("schema_version", SCHEMA_VERSION)
        if schema_version != SCHEMA_VERSION:
            raise _manifest_error(f"unsupported manifest schema_version: {schema_version}")
        coordinate_convention = str(mapping.get("coordinate_convention", COORDINATE_CONVENTION))
        if coordinate_convention != COORDINATE_CONVENTION:
            raise _manifest_error(
                f"unsupported coordinate_convention: {coordinate_convention!r}; "
                f"expected {COORDINATE_CONVENTION!r}"
            )
        generation = mapping.get("generation") or {}
        if not isinstance(generation, dict):
            raise _manifest_error("generation must be an object")

        top_keypoints = _parse_keypoints(mapping.get("keypoints"))
        top_skeleton = _parse_skeleton(mapping.get("skeleton"))
        shards_raw = mapping.get("shards")
        if not isinstance(shards_raw, list) or not shards_raw:
            raise _manifest_error("manifest.shards must be a non-empty array")

        seen_ids: set[str] = set()
        shards: list[ShardSpec] = []
        inferred_keypoints: list[str] = list(top_keypoints)
        for shard_index, raw_shard in enumerate(shards_raw):
            if not isinstance(raw_shard, dict):
                raise _manifest_error(f"shards[{shard_index}] must be an object")
            shard_id = str(raw_shard.get("shard_id") or raw_shard.get("id") or "")
            try:
                safe_name(shard_id)
            except ValueError as exc:
                raise _manifest_error(f"invalid shard_id at index {shard_index}: {shard_id!r}") from exc
            if shard_id in seen_ids:
                raise _manifest_error(f"duplicate shard_id: {shard_id}")
            seen_ids.add(shard_id)
            width = _positive_int(raw_shard.get("width"), f"{shard_id}.width")
            height = _positive_int(raw_shard.get("height"), f"{shard_id}.height")
            image = raw_shard.get("image") or raw_shard.get("image_path")
            if not isinstance(image, str) or not image:
                raise _manifest_error(f"{shard_id}.image must be a non-empty string")
            source_label = str(raw_shard.get("source_label") or raw_shard.get("source") or "")
            if not source_label:
                raise _manifest_error(f"{shard_id}.source_label is required")
            shard_keypoints_raw = raw_shard.get("keypoints")
            shard_keypoints = _parse_keypoints(shard_keypoints_raw)
            instances_raw = raw_shard.get("instances")
            if not isinstance(instances_raw, list) or not instances_raw:
                raise _manifest_error(f"{shard_id}.instances must be a non-empty array")
            instances: list[PoseTruth] = []
            seen_instance_ids: set[str] = set()
            for instance_index, raw_instance in enumerate(instances_raw):
                if not isinstance(raw_instance, dict):
                    raise _manifest_error(
                        f"{shard_id}.instances[{instance_index}] must be an object"
                    )
                instance_id = str(
                    raw_instance.get("instance_id")
                    or raw_instance.get("id")
                    or f"pose_{instance_index + 1:04d}"
                )
                try:
                    safe_name(instance_id)
                except ValueError as exc:
                    raise _manifest_error(
                        f"{shard_id} has invalid instance_id: {instance_id!r}"
                    ) from exc
                if instance_id in seen_instance_ids:
                    raise _manifest_error(
                        f"{shard_id} has duplicate instance_id: {instance_id}"
                    )
                seen_instance_ids.add(instance_id)
                keypoint_values = _parse_instance_keypoints(
                    raw_instance.get("keypoints"), raw_instance.get("visibility")
                )
                for name in keypoint_values:
                    if name not in inferred_keypoints:
                        inferred_keypoints.append(name)
                expected = _parse_confidence_interval(
                    raw_instance.get("expected_pose_confidence")
                    or raw_instance.get("expected_confidence")
                )
                instances.append(
                    PoseTruth(
                        instance_id=instance_id,
                        keypoints=keypoint_values,
                        expected_confidence=expected,
                    )
                )
            effective_keypoints = tuple(shard_keypoints or inferred_keypoints)
            for instance in instances:
                missing = [name for name in effective_keypoints if name not in instance.keypoints]
                if missing:
                    raise _manifest_error(
                        f"{shard_id}/{instance.instance_id} is missing keypoints: {', '.join(missing)}"
                    )
            shard_skeleton = _parse_skeleton(raw_shard.get("skeleton")) or top_skeleton
            if not shard_skeleton:
                raise _manifest_error(f"{shard_id} has no skeleton definition")
            expected_confidence = _parse_confidence_interval(
                raw_shard.get("expected_pose_confidence")
                or raw_shard.get("expected_confidence")
                or {"min": 0.0, "max": 1.0}
            )
            shard_generation = raw_shard.get("generation") or generation
            if not isinstance(shard_generation, dict):
                raise _manifest_error(f"{shard_id}.generation must be an object")
            shards.append(
                ShardSpec(
                    shard_id=shard_id,
                    image=image,
                    width=width,
                    height=height,
                    source_label=source_label,
                    keypoints=effective_keypoints,
                    skeleton=shard_skeleton,
                    instances=tuple(instances),
                    expected_confidence=expected_confidence,
                    generation=dict(shard_generation),
                    base_dir=Path(base_dir).resolve(),
                )
            )

        final_keypoints = tuple(top_keypoints or inferred_keypoints)
        return cls(
            schema_version=schema_version,
            coordinate_convention=coordinate_convention,
            keypoints=final_keypoints,
            skeleton=top_skeleton,
            shards=tuple(shards),
            generation=dict(generation),
            base_dir=Path(base_dir).resolve(),
            raw=dict(mapping),
        )

    def shard_map(self) -> dict[str, ShardSpec]:
        return {shard.shard_id: shard for shard in self.shards}

    def input_fingerprint(self) -> str:
        return digest_json(
            {
                "coordinate_convention": self.coordinate_convention,
                "generation": self.generation,
                "shards": {
                    shard.shard_id: {
                        "image": shard.image,
                        "width": shard.width,
                        "height": shard.height,
                        "source_label": shard.source_label,
                        "keypoints": list(shard.keypoints),
                        "skeleton": [list(edge) for edge in shard.skeleton],
                        "instances": shard.truth_core_mapping(),
                        "expected_confidence": shard.expected_confidence.to_mapping(),
                    }
                    for shard in self.shards
                },
            }
        )


def coordinate_to_pixel(value: float, size: int) -> float:
    return float(value) * (size - 1)


def pixel_to_coordinate(value: float, size: int) -> float:
    if size <= 1:
        return 0.0
    return float(value) / (size - 1)


def _parse_keypoints(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise _manifest_error("keypoints must be an array")
    names: list[str] = []
    seen: set[str] = set()
    for item in value:
        if isinstance(item, str):
            name = item
        elif isinstance(item, dict) and "name" in item:
            name = str(item["name"])
        else:
            raise _manifest_error("keypoints entries must be strings or {name: ...}")
        if not name or name in seen:
            raise _manifest_error(f"invalid or duplicate keypoint name: {name!r}")
        seen.add(name)
        names.append(name)
    return tuple(names)


def _parse_skeleton(value: Any) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise _manifest_error("skeleton must be an array of pairs")
    edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in value:
        try:
            if isinstance(item, dict):
                left = str(item.get("a") or item.get("from") or item.get("parent"))
                right = str(item.get("b") or item.get("to") or item.get("child"))
            else:
                left, right = (str(part) for part in item)
        except (TypeError, ValueError) as exc:
            raise _manifest_error("skeleton entries must be two-element pairs") from exc
        if not left or not right or left == right:
            raise _manifest_error(f"invalid skeleton pair: {item!r}")
        edge = (left, right)
        if edge not in seen:
            seen.add(edge)
            edges.append(edge)
    return tuple(edges)


def _parse_instance_keypoints(
    value: Any, visibility: Any = None
) -> dict[str, KeypointTruth]:
    if isinstance(value, dict):
        raw_items = []
        for name, point in value.items():
            if not isinstance(point, dict):
                raise _manifest_error(f"keypoint {name!r} must be an object")
            raw_items.append((str(name), point))
    elif isinstance(value, list):
        raw_items = []
        for item in value:
            if not isinstance(item, dict) or "name" not in item:
                raise _manifest_error("keypoint list entries must contain name")
            raw_items.append((str(item["name"]), item))
    else:
        raise _manifest_error("instance.keypoints must be an object or array")
    visibility_map = visibility if isinstance(visibility, dict) else {}
    result: dict[str, KeypointTruth] = {}
    for name, point in raw_items:
        if not name or name in result:
            raise _manifest_error(f"invalid or duplicate keypoint: {name!r}")
        try:
            x = float(point["x"])
            y = float(point["y"])
        except (KeyError, TypeError, ValueError) as exc:
            raise _manifest_error(f"keypoint {name!r} requires numeric x and y") from exc
        if not (0.0 <= x <= 1.0 and 0.0 <= y <= 1.0):
            raise _manifest_error(f"keypoint {name!r} coordinates must be normalized to [0,1]")
        visible = point.get("visible", visibility_map.get(name, True))
        if not isinstance(visible, bool):
            raise _manifest_error(f"keypoint {name!r}.visible must be a boolean")
        result[name] = KeypointTruth(name=name, x=x, y=y, visible=visible)
    return result


def _parse_confidence_interval(value: Any) -> ConfidenceInterval | None:
    if value is None:
        return None
    try:
        if isinstance(value, dict):
            minimum = float(value.get("min", value.get("minimum")))
            maximum = float(value.get("max", value.get("maximum")))
        elif isinstance(value, (list, tuple)) and len(value) == 2:
            minimum = float(value[0])
            maximum = float(value[1])
        else:
            raise TypeError
    except (TypeError, ValueError) as exc:
        raise _manifest_error("expected confidence interval must be [min, max] or {min,max}") from exc
    if not (0.0 <= minimum <= maximum <= 1.0):
        raise _manifest_error("expected confidence interval must satisfy 0 <= min <= max <= 1")
    return ConfidenceInterval(minimum=minimum, maximum=maximum)


def _positive_int(value: Any, field_name: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise _manifest_error(f"{field_name} must be a positive integer") from exc
    if number <= 0:
        raise _manifest_error(f"{field_name} must be a positive integer")
    return number


def _manifest_error(message: str) -> PipelineError:
    return PipelineError(ERROR_INVALID_MANIFEST, message)
