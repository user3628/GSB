"""YAML configuration parsing and validation."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .errors import BusinessError, ErrorCode
from .util import stable_hash


STAGE_NAMES = ("encode", "schedule", "sample", "quantize")
SHARD_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


@dataclass(frozen=True)
class EncoderConfig:
    name: str = "sha256-xof-16"
    version: int = 1
    dimension: int = 16
    domain: str = "detdiff.local.v1"


@dataclass(frozen=True)
class ImageConfig:
    width: int = 32
    height: int = 32
    channels: int = 3


@dataclass(frozen=True)
class QuantizeConfig:
    version: int = 1
    levels: int = 256
    png_compression: int = 9


@dataclass(frozen=True)
class GlobalConfig:
    encoder: EncoderConfig = field(default_factory=EncoderConfig)
    image: ImageConfig = field(default_factory=ImageConfig)
    quantize: QuantizeConfig = field(default_factory=QuantizeConfig)
    trace_version: int = 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "encoder": self.encoder.__dict__,
            "image": self.image.__dict__,
            "quantize": self.quantize.__dict__,
            "trace_version": self.trace_version,
        }


@dataclass(frozen=True)
class ShardConfig:
    id: str
    prompt: str
    guidance_scale: float
    steps: int
    seed: int
    reference: str | None = None
    blend_strength: float = 0.35
    source_image: str | None = None

    @property
    def has_reference(self) -> bool:
        return self.reference is not None

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": self.id,
            "prompt": self.prompt,
            "guidance_scale": self.guidance_scale,
            "steps": self.steps,
            "seed": self.seed,
            "blend_strength": self.blend_strength,
        }
        if self.reference is not None:
            result["reference"] = self.reference
        if self.source_image is not None:
            result["source_image"] = self.source_image
        return result


@dataclass(frozen=True)
class WorkbenchConfig:
    schema_version: int
    global_config: GlobalConfig
    shards: tuple[ShardConfig, ...]
    root: Path
    raw: dict[str, Any]

    def shard(self, shard_id: str) -> ShardConfig:
        for item in self.shards:
            if item.id == shard_id:
                return item
        raise BusinessError(
            ErrorCode.E_INVALID_CONFIG,
            f"unknown shard: {shard_id}",
            shard=shard_id,
            field="id",
        )

    @property
    def config_version(self) -> str:
        payload = {
            "schema_version": self.schema_version,
            "global": self.global_config.as_dict(),
            "shards": [item.as_dict() for item in self.shards],
        }
        return stable_hash(payload)

    def resolve(self, relative_path: str) -> Path:
        path = Path(relative_path)
        return path if path.is_absolute() else (self.root / path).resolve()


def load_config(path: Path) -> WorkbenchConfig:
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
    except FileNotFoundError as exc:
        raise BusinessError(
            ErrorCode.E_INVALID_CONFIG,
            f"configuration file not found: {path}",
            field="config",
        ) from exc
    except yaml.YAMLError as exc:
        raise BusinessError(
            ErrorCode.E_INVALID_CONFIG,
            f"invalid YAML configuration: {exc}",
            field="config",
        ) from exc
    return parse_config(raw, root=path.parent.resolve())


def parse_config(raw: Any, root: Path) -> WorkbenchConfig:
    if not isinstance(raw, dict):
        raise _invalid("configuration root must be a mapping", field="root")
    schema_version = raw.get("schema_version", 1)
    if schema_version != 1:
        raise _invalid("unsupported schema_version", field="schema_version")

    global_raw = raw.get("global", {})
    if not isinstance(global_raw, dict):
        raise _invalid("global must be a mapping", field="global")
    global_config = _parse_global(global_raw)

    shards_raw = raw.get("shards")
    if not isinstance(shards_raw, list) or not shards_raw:
        raise _invalid("shards must be a non-empty list", field="shards")
    shards: list[ShardConfig] = []
    seen: set[str] = set()
    for index, item in enumerate(shards_raw):
        shard = _parse_shard(item, index)
        if shard.id in seen:
            raise _invalid("duplicate shard id", shard=shard.id, field="shards.id")
        seen.add(shard.id)
        shards.append(shard)

    normalized_raw = {
        "schema_version": schema_version,
        "global": global_config.as_dict(),
        "shards": [item.as_dict() for item in shards],
    }
    return WorkbenchConfig(
        schema_version=schema_version,
        global_config=global_config,
        shards=tuple(shards),
        root=root.resolve(),
        raw=normalized_raw,
    )


def _parse_global(raw: dict[str, Any]) -> GlobalConfig:
    encoder_raw = raw.get("encoder", {})
    image_raw = raw.get("image", {})
    quantize_raw = raw.get("quantize", {})
    if not all(isinstance(item, dict) for item in (encoder_raw, image_raw, quantize_raw)):
        raise _invalid("global sections must be mappings", field="global")

    encoder = EncoderConfig(
        name=str(encoder_raw.get("name", EncoderConfig.name)),
        version=_positive_int(encoder_raw.get("version", EncoderConfig.version), None, "global.encoder.version"),
        dimension=_positive_int(encoder_raw.get("dimension", EncoderConfig.dimension), None, "global.encoder.dimension"),
        domain=str(encoder_raw.get("domain", EncoderConfig.domain)),
    )
    if encoder.dimension != 16:
        raise _invalid("encoder.dimension must be 16", field="global.encoder.dimension")
    if encoder.name != "sha256-xof-16":
        raise _invalid("encoder.name must be sha256-xof-16", field="global.encoder.name")

    width = _positive_int(image_raw.get("width", ImageConfig.width), None, "global.image.width")
    height = _positive_int(image_raw.get("height", ImageConfig.height), None, "global.image.height")
    channels = _positive_int(image_raw.get("channels", ImageConfig.channels), None, "global.image.channels")
    if channels != 3:
        raise _invalid("global.image.channels must be 3", field="global.image.channels")
    if width < 8 or height < 8 or width > 512 or height > 512:
        raise _invalid("image dimensions must be between 8 and 512", field="global.image")

    levels = _positive_int(quantize_raw.get("levels", QuantizeConfig.levels), None, "global.quantize.levels")
    compression = _positive_int(
        quantize_raw.get("png_compression", QuantizeConfig.png_compression),
        None,
        "global.quantize.png_compression",
    )
    if levels != 256:
        raise _invalid("global.quantize.levels must be 256", field="global.quantize.levels")
    if compression > 9:
        raise _invalid("global.quantize.png_compression must be <= 9", field="global.quantize.png_compression")
    quantize = QuantizeConfig(
        version=_positive_int(quantize_raw.get("version", QuantizeConfig.version), None, "global.quantize.version"),
        levels=levels,
        png_compression=compression,
    )
    trace_version = _positive_int(raw.get("trace_version", 1), None, "global.trace_version")
    return GlobalConfig(encoder=encoder, image=ImageConfig(width, height, channels), quantize=quantize, trace_version=trace_version)


def _parse_shard(raw: Any, index: int) -> ShardConfig:
    if not isinstance(raw, dict):
        raise _invalid("shard must be a mapping", field=f"shards[{index}]")
    shard_id = raw.get("id", f"shard-{index}")
    if not isinstance(shard_id, str) or not shard_id.strip():
        raise _invalid("shard id must be a non-empty string", field=f"shards[{index}].id")
    shard_id = shard_id.strip()
    if not SHARD_ID_PATTERN.fullmatch(shard_id):
        raise _invalid(
            "shard id must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}",
            shard=shard_id,
            field="id",
        )

    prompt = raw.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip():
        raise _invalid("prompt must be a non-empty string", shard=shard_id, field="prompt")

    guidance = raw.get("guidance_scale")
    if isinstance(guidance, bool) or not isinstance(guidance, (int, float)):
        raise _invalid("guidance_scale must be a finite non-negative number", shard=shard_id, field="guidance_scale")
    guidance = float(guidance)
    if not math.isfinite(guidance) or guidance < 0:
        raise _invalid("guidance_scale must be a finite non-negative number", shard=shard_id, field="guidance_scale")

    steps = _positive_int(raw.get("steps"), shard_id, "steps")
    if steps > 512:
        raise _invalid("steps must be <= 512", shard=shard_id, field="steps")

    seed = raw.get("seed")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise _invalid("seed must be an integer", shard=shard_id, field="seed")
    if seed < 0:
        raise _invalid("seed must be non-negative", shard=shard_id, field="seed")

    blend = raw.get("blend_strength", 0.35)
    if isinstance(blend, bool) or not isinstance(blend, (int, float)):
        raise _invalid("blend_strength must be a finite number in [0, 1]", shard=shard_id, field="blend_strength")
    blend = float(blend)
    if not math.isfinite(blend) or blend < 0.0 or blend > 1.0:
        raise _invalid("blend_strength must be a finite number in [0, 1]", shard=shard_id, field="blend_strength")

    reference = raw.get("reference")
    if reference is not None and (not isinstance(reference, str) or not reference.strip()):
        raise _invalid("reference must be a non-empty path or null", shard=shard_id, field="reference")
    source_image = raw.get("source_image")
    if source_image is not None and (not isinstance(source_image, str) or not source_image.strip()):
        raise _invalid("source_image must be a non-empty path or null", shard=shard_id, field="source_image")

    return ShardConfig(
        id=shard_id,
        prompt=prompt,
        guidance_scale=guidance,
        steps=steps,
        seed=seed,
        reference=reference.strip() if isinstance(reference, str) else None,
        blend_strength=blend,
        source_image=source_image.strip() if isinstance(source_image, str) else None,
    )


def _positive_int(value: Any, shard: str | None, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise _invalid(f"{field} must be a positive integer", shard=shard, field=field)
    return value


def _invalid(message: str, *, shard: str | None = None, field: str | None = None) -> BusinessError:
    return BusinessError(ErrorCode.E_INVALID_CONFIG, message, shard=shard, field=field)

