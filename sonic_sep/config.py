"""Separation configuration loading and validation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from .errors import invalid_config
from .util import digest_json, json_safe


@dataclass(frozen=True)
class SeparationConfig:
    sample_rate: int = 16_000
    frame_length: int = 512
    hop_length: int = 128
    band_count: int = 16
    mask_iterations: int = 4
    max_sources: int = 3
    block_overlap_percent: int = 25
    block_frames: int = 16
    projection_seed: int = 20261123

    @property
    def nyquist_hz(self) -> int:
        return self.sample_rate // 2

    @property
    def band_width_hz(self) -> int:
        return self.nyquist_hz // self.band_count

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def fingerprint(self) -> str:
        return digest_json(self.to_dict())

    def stage_fingerprint(self, stage: str) -> str:
        data = self.to_dict()
        if stage == "stft":
            keys = ("sample_rate", "frame_length", "hop_length")
        elif stage == "features":
            keys = ("sample_rate", "frame_length", "hop_length", "band_count")
        elif stage == "mask_init":
            keys = ("sample_rate", "frame_length", "hop_length", "band_count", "projection_seed", "max_sources")
        elif stage == "mask_refine":
            keys = (
                "sample_rate",
                "frame_length",
                "hop_length",
                "band_count",
                "projection_seed",
                "max_sources",
                "mask_iterations",
            )
        elif stage == "reconstruct":
            keys = tuple(data)
        else:
            raise ValueError(f"unknown stage: {stage}")
        return digest_json({"algorithm_version": 1, **{key: data[key] for key in keys}})


_INT_FIELDS = (
    "sample_rate",
    "frame_length",
    "hop_length",
    "band_count",
    "mask_iterations",
    "max_sources",
    "block_overlap_percent",
    "block_frames",
    "projection_seed",
)


def validate_config_data(data: Mapping[str, Any], *, slice_id: str | None = None) -> SeparationConfig:
    if not isinstance(data, Mapping):
        raise invalid_config("configuration must be a mapping", slice_id=slice_id, param="config")
    normalized: dict[str, int] = {}
    defaults = SeparationConfig().to_dict()
    for field in _INT_FIELDS:
        value = data.get(field, defaults[field])
        if isinstance(value, bool) or not isinstance(value, int):
            raise invalid_config(
                f"{field} must be an integer sample/bin value",
                slice_id=slice_id,
                param=field,
            )
        normalized[field] = value
    if normalized["sample_rate"] <= 0 or normalized["sample_rate"] % 2:
        raise invalid_config("sample_rate must be a positive even integer", slice_id=slice_id, param="sample_rate")
    if normalized["frame_length"] <= 0:
        raise invalid_config("frame_length must be positive", slice_id=slice_id, param="frame_length")
    if normalized["hop_length"] <= 0:
        raise invalid_config("hop_length must be positive", slice_id=slice_id, param="hop_length")
    if normalized["hop_length"] > normalized["frame_length"]:
        raise invalid_config(
            "hop_length must not exceed frame_length",
            slice_id=slice_id,
            param="hop_length",
        )
    if normalized["band_count"] <= 0:
        raise invalid_config("band_count must be positive", slice_id=slice_id, param="band_count")
    nyquist = normalized["sample_rate"] // 2
    if nyquist % normalized["band_count"] != 0:
        raise invalid_config(
            "band_count must divide the Nyquist frequency range exactly",
            slice_id=slice_id,
            param="band_count",
        )
    if normalized["mask_iterations"] < 0:
        raise invalid_config("mask_iterations must be non-negative", slice_id=slice_id, param="mask_iterations")
    if normalized["max_sources"] <= 0:
        raise invalid_config("max_sources must be positive", slice_id=slice_id, param="max_sources")
    if not 0 <= normalized["block_overlap_percent"] <= 99:
        raise invalid_config(
            "block_overlap_percent must be an integer in [0, 99]",
            slice_id=slice_id,
            param="block_overlap_percent",
        )
    if normalized["block_frames"] <= 0:
        raise invalid_config("block_frames must be positive", slice_id=slice_id, param="block_frames")
    return SeparationConfig(**normalized)


def load_config(path: str | Path, *, slice_id: str | None = None) -> SeparationConfig:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except OSError as exc:
        raise invalid_config(f"cannot read configuration: {exc}", slice_id=slice_id, param="config") from exc
    except yaml.YAMLError as exc:
        raise invalid_config(f"invalid YAML configuration: {exc}", slice_id=slice_id, param="config") from exc
    return validate_config_data(data, slice_id=slice_id)


def config_from_dict(data: Mapping[str, Any], *, slice_id: str | None = None) -> SeparationConfig:
    return validate_config_data(data, slice_id=slice_id)


def config_json(config: SeparationConfig) -> dict[str, Any]:
    return json_safe(config.to_dict())

