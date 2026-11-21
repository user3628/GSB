"""Pipeline configuration and stage-scoped configuration versions."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .constants import ASSEMBLY, DECODE, ERROR_INVALID_CONFIG, PREPROCESS, SCHEMA_VERSION, VALIDATION
from .errors import PipelineError
from .util import digest_json, read_json_file


DEFAULT_SYMMETRY_EDGE_PAIRS = (
    (("neck", "left_shoulder"), ("neck", "right_shoulder")),
    (("left_shoulder", "left_elbow"), ("right_shoulder", "right_elbow")),
    (("left_elbow", "left_wrist"), ("right_elbow", "right_wrist")),
    (("hip_center", "left_hip"), ("hip_center", "right_hip")),
    (("left_hip", "left_knee"), ("right_hip", "right_knee")),
    (("left_knee", "left_ankle"), ("right_knee", "right_ankle")),
)


@dataclass(frozen=True)
class PipelineConfig:
    schema_version: int = SCHEMA_VERSION
    gaussian_radius: float = 2.2
    contrast_factor: float = 1.10
    heatmap_width: int | None = None
    heatmap_height: int | None = None
    max_candidates: int = 4
    peak_threshold: float = 1e-6
    subpixel_radius: int = 2
    min_valid_keypoints: int = 5
    association_max_edge_fraction: float = 0.23
    symmetry_edge_pairs: tuple[tuple[tuple[str, str], tuple[str, str]], ...] = field(
        default_factory=lambda: DEFAULT_SYMMETRY_EDGE_PAIRS
    )
    symmetry_ratio_tolerance: float = 0.30
    min_joint_angle: float = 5.0
    max_joint_angle: float = 180.0
    min_edge_length: float = 1e-6
    multi_instance_min_separation_fraction: float = 0.01
    hit_error_px: float = 1.5
    round_digits: int = 10

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> "PipelineConfig":
        if mapping is None:
            return cls()
        if not isinstance(mapping, dict):
            raise _config_error("configuration must be a JSON object")
        values = dict(mapping)
        values.pop("$schema", None)
        if "symmetry_edge_pairs" in values:
            raw_pairs = values["symmetry_edge_pairs"]
            pairs: list[tuple[tuple[str, str], tuple[str, str]]] = []
            try:
                for pair in raw_pairs:
                    first, second = pair
                    pairs.append(((str(first[0]), str(first[1])), (str(second[0]), str(second[1]))))
            except (TypeError, ValueError, IndexError) as exc:
                raise _config_error("symmetry_edge_pairs must contain two edge pairs") from exc
            values["symmetry_edge_pairs"] = tuple(pairs)
        allowed = set(cls.__dataclass_fields__)
        unknown = sorted(set(values) - allowed)
        if unknown:
            raise _config_error(f"unknown configuration fields: {', '.join(unknown)}")
        try:
            return cls(**values)
        except TypeError as exc:
            raise _config_error(str(exc)) from exc

    @classmethod
    def from_file(cls, path: str | Path | None) -> "PipelineConfig":
        if path is None:
            return cls()
        try:
            return cls.from_mapping(read_json_file(path))
        except (OSError, ValueError) as exc:
            raise _config_error(f"cannot read configuration: {exc}") from exc

    def to_mapping(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "gaussian_radius": self.gaussian_radius,
            "contrast_factor": self.contrast_factor,
            "heatmap_width": self.heatmap_width,
            "heatmap_height": self.heatmap_height,
            "max_candidates": self.max_candidates,
            "peak_threshold": self.peak_threshold,
            "subpixel_radius": self.subpixel_radius,
            "min_valid_keypoints": self.min_valid_keypoints,
            "association_max_edge_fraction": self.association_max_edge_fraction,
            "symmetry_edge_pairs": [
                [list(first), list(second)] for first, second in self.symmetry_edge_pairs
            ],
            "symmetry_ratio_tolerance": self.symmetry_ratio_tolerance,
            "min_joint_angle": self.min_joint_angle,
            "max_joint_angle": self.max_joint_angle,
            "min_edge_length": self.min_edge_length,
            "multi_instance_min_separation_fraction": self.multi_instance_min_separation_fraction,
            "hit_error_px": self.hit_error_px,
            "round_digits": self.round_digits,
        }

    def stage_mapping(self, stage: str) -> dict[str, Any]:
        if stage == PREPROCESS:
            return {
                "schema_version": self.schema_version,
                "gaussian_radius": self.gaussian_radius,
                "contrast_factor": self.contrast_factor,
                "heatmap_width": self.heatmap_width,
                "heatmap_height": self.heatmap_height,
            }
        if stage == DECODE:
            return {
                "schema_version": self.schema_version,
                "max_candidates": self.max_candidates,
                "peak_threshold": self.peak_threshold,
                "subpixel_radius": self.subpixel_radius,
            }
        if stage == ASSEMBLY:
            return {
                "schema_version": self.schema_version,
                "min_valid_keypoints": self.min_valid_keypoints,
                "association_max_edge_fraction": self.association_max_edge_fraction,
                "symmetry_edge_pairs": [
                    [list(first), list(second)] for first, second in self.symmetry_edge_pairs
                ],
                "symmetry_ratio_tolerance": self.symmetry_ratio_tolerance,
                "min_joint_angle": self.min_joint_angle,
                "max_joint_angle": self.max_joint_angle,
                "min_edge_length": self.min_edge_length,
                "multi_instance_min_separation_fraction": self.multi_instance_min_separation_fraction,
            }
        if stage == VALIDATION:
            return {
                "schema_version": self.schema_version,
                "hit_error_px": self.hit_error_px,
                "round_digits": self.round_digits,
            }
        raise ValueError(f"unknown stage: {stage}")

    def stage_version(self, stage: str) -> str:
        return digest_json(self.stage_mapping(stage))

    def config_version(self) -> str:
        return digest_json(self.to_mapping())

    def validate_numeric(self) -> None:
        if not -1e12 < self.gaussian_radius < 1e12:
            raise _config_error("gaussian_radius must be finite")
        if self.max_candidates < 0:
            raise _config_error("max_candidates must be non-negative")
        if self.subpixel_radius < 0:
            raise _config_error("subpixel_radius must be non-negative")
        if self.min_valid_keypoints < 0:
            raise _config_error("min_valid_keypoints must be non-negative")
        if not 0 <= self.association_max_edge_fraction <= 2:
            raise _config_error("association_max_edge_fraction must be between 0 and 2")
        if not 0 <= self.symmetry_ratio_tolerance < 1:
            raise _config_error("symmetry_ratio_tolerance must be in [0, 1)")
        if not 0 <= self.min_joint_angle <= self.max_joint_angle <= 180:
            raise _config_error("joint angle bounds must satisfy 0 <= min <= max <= 180")
        if self.round_digits < 0 or self.round_digits > 15:
            raise _config_error("round_digits must be between 0 and 15")


def _config_error(message: str) -> PipelineError:
    return PipelineError(ERROR_INVALID_CONFIG, message, stage=PREPROCESS)
