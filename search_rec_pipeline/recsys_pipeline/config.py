"""YAML configuration loading and semantic version fingerprints."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from .errors import E_INVALID_STAGE, PipelineError
from .util import canonical_bytes, hash_json, sha256_bytes


SCALES = ("small", "medium", "large")
STAGES = ("normalize", "decision", "encode", "assemble")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}


class Config:
    """Loaded config plus project/workdir path resolution."""

    def __init__(
        self,
        project_root: Path,
        data: dict[str, Any],
        *,
        config_path: Path | None = None,
        workdir_override: Path | None = None,
        source_label: str = "inline-config",
    ) -> None:
        self.project_root = Path(project_root).resolve()
        self.data = copy.deepcopy(data)
        self.config_path = Path(config_path).resolve() if config_path else None
        self.source_label = source_label
        self.config_version = sha256_bytes(canonical_bytes(self.data))
        try:
            samples_relative = self.data["paths"]["samples_dir"]
            workdir_relative = self.data["paths"]["workdir"]
        except (KeyError, TypeError) as exc:
            raise PipelineError(
                E_INVALID_STAGE,
                "config.paths.samples_dir and config.paths.workdir are required",
                config_path=str(self.config_path) if self.config_path else source_label,
            ) from exc
        self.samples_dir = (self.project_root / samples_relative).resolve()
        self.workdir = (
            Path(workdir_override).resolve()
            if workdir_override is not None
            else (self.project_root / workdir_relative).resolve()
        )

    @classmethod
    def load(
        cls,
        project_root: Path,
        config_path: Path | None = None,
        *,
        workdir_override: Path | None = None,
    ) -> "Config":
        project_root = Path(project_root).resolve()
        config_path = (
            Path(config_path).resolve()
            if config_path is not None
            else project_root / "config" / "pipeline.yaml"
        )
        try:
            text = config_path.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise PipelineError(
                E_INVALID_STAGE,
                "configuration file not found",
                config_path=str(config_path),
            ) from exc
        data = yaml.safe_load(text)
        if not isinstance(data, dict):
            raise PipelineError(
                E_INVALID_STAGE,
                "configuration root must be a YAML mapping",
                config_path=str(config_path),
            )
        return cls(
            project_root,
            data,
            config_path=config_path,
            workdir_override=workdir_override,
            source_label=str(config_path),
        )

    @classmethod
    def from_data(
        cls,
        project_root: Path,
        data: dict[str, Any],
        *,
        workdir_override: Path | None = None,
        source_label: str = "inline-config",
    ) -> "Config":
        return cls(
            project_root,
            data,
            config_path=None,
            workdir_override=workdir_override,
            source_label=source_label,
        )

    @property
    def features(self) -> dict[str, Any]:
        value = self.data.get("features")
        if not isinstance(value, dict):
            raise PipelineError(E_INVALID_STAGE, "config.features must be a mapping")
        return value

    @property
    def numeric_fields(self) -> dict[str, dict[str, Any]]:
        value = self.features.get("numeric")
        if not isinstance(value, dict) or not value:
            raise PipelineError(E_INVALID_STAGE, "config.features.numeric must be a non-empty mapping")
        return value

    @property
    def categorical_fields(self) -> list[str]:
        value = self.features.get("categorical")
        if not isinstance(value, list) or not value:
            raise PipelineError(E_INVALID_STAGE, "config.features.categorical must be a non-empty list")
        return list(value)

    @property
    def cross_candidates(self) -> list[list[str]]:
        value = self.features.get("cross_candidates")
        if not isinstance(value, list):
            raise PipelineError(E_INVALID_STAGE, "config.features.cross_candidates must be a list")
        result: list[list[str]] = []
        for item in value:
            if not isinstance(item, list) or len(item) != 2 or not all(isinstance(x, str) for x in item):
                raise PipelineError(
                    E_INVALID_STAGE,
                    "each cross candidate must be a two-field list",
                    candidate=item,
                )
            result.append(list(item))
        return result

    @property
    def labels(self) -> dict[str, str]:
        value = self.data.get("labels")
        if not isinstance(value, dict) or set(value) != {"exposure", "click", "conversion"}:
            raise PipelineError(
                E_INVALID_STAGE,
                "config.labels must define exposure, click, and conversion",
            )
        return dict(value)

    @property
    def decision(self) -> dict[str, Any]:
        value = self.data.get("decision")
        if not isinstance(value, dict):
            raise PipelineError(E_INVALID_STAGE, "config.decision must be a mapping")
        return value

    def scale_declaration_path(self, scale: str) -> Path:
        scales = self.data.get("scales")
        if not isinstance(scales, dict) or scale not in scales:
            raise PipelineError(E_INVALID_STAGE, "unknown scale", scale=scale, valid_scales=sorted(scales or {}))
        declaration = scales[scale].get("declaration") if isinstance(scales[scale], dict) else None
        if not isinstance(declaration, str):
            raise PipelineError(
                E_INVALID_STAGE,
                "scale declaration path is missing",
                scale=scale,
            )
        return (self.project_root / declaration).resolve()

    def stage_config_slice(self, stage: str) -> dict[str, Any]:
        features = self.features
        if stage == "normalize":
            return {
                "schema_version": self.data.get("schema_version"),
                "regions": features.get("regions"),
                "numeric": features.get("numeric"),
                "categorical": features.get("categorical"),
                "normalization_version": features.get("normalization_version"),
                "labels": self.data.get("labels"),
            }
        if stage == "decision":
            return {
                "decision": self.decision,
                "crossing_limit_hard": features.get("crossing_limit_hard"),
                "cross_candidates": features.get("cross_candidates"),
                "feature_weights": features.get("feature_weights"),
            }
        if stage == "encode":
            return {
                "categorical": features.get("categorical"),
                "cross_candidates": features.get("cross_candidates"),
                "crossing_limit_hard": features.get("crossing_limit_hard"),
                "hash_dim": features.get("hash_dim"),
                "cross_hash_dim": features.get("cross_hash_dim"),
                "vector_length": features.get("vector_length"),
                "hash_salt": features.get("hash_salt"),
            }
        if stage == "assemble":
            return {
                "labels": self.data.get("labels"),
                "stats": self.data.get("stats"),
                "assembly_strategies": self.decision.get("assembly_strategies"),
                "assembly_loss_upper_bound": self.decision.get("assembly_loss_upper_bound"),
                "reservoir_sample_ratio": self.decision.get("reservoir_sample_ratio"),
            }
        raise PipelineError(E_INVALID_STAGE, "unknown stage", stage=stage)

    def stage_config_fingerprint(self, stage: str) -> str:
        return hash_json(self.stage_config_slice(stage))
