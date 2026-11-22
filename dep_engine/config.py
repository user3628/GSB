"""YAML analysis configuration and stage version fingerprints."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .canonical import canonical_bytes, sha256_obj
from .constants import STAGE_CONFIG_FIELDS, STAGES
from .errors import EngineError

_REQUIRED_WEIGHTS = ("shift", "left_reduce", "right_reduce")


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


@dataclass(frozen=True)
class AnalysisConfig:
    schema_version: int
    tokenizer_version: str
    score_weights: dict[str, float]
    max_dependency_distance: int
    unique_root: bool
    smoothing_strength: float
    confidence_threshold: float

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> "AnalysisConfig":
        if not isinstance(raw, dict):
            raise EngineError(
                "E_INVALID_STAGE",
                "analysis configuration must be a YAML mapping",
                {"valid_stages": list(STAGES)},
            )

        required = {
            "score_weights",
            "max_dependency_distance",
            "unique_root",
            "smoothing_strength",
            "confidence_threshold",
        }
        missing = sorted(required - set(raw))
        if missing:
            raise EngineError(
                "E_INVALID_STAGE",
                "analysis configuration is missing required fields",
                {"missing_fields": missing, "valid_stages": list(STAGES)},
            )

        weights_raw = raw["score_weights"]
        if not isinstance(weights_raw, dict):
            raise EngineError(
                "E_INVALID_STAGE",
                "score_weights must be a mapping",
                {"valid_stages": list(STAGES)},
            )
        missing_weights = [name for name in _REQUIRED_WEIGHTS if name not in weights_raw]
        if missing_weights:
            raise EngineError(
                "E_INVALID_STAGE",
                "score_weights is missing action weights",
                {
                    "missing_weights": missing_weights,
                    "valid_stages": list(STAGES),
                },
            )
        weights: dict[str, float] = {}
        for name in _REQUIRED_WEIGHTS:
            value = weights_raw[name]
            if not _is_number(value):
                raise EngineError(
                    "E_INVALID_STAGE",
                    f"score_weights.{name} must be numeric",
                    {"valid_stages": list(STAGES)},
                )
            # Non-finite values are intentionally accepted here.  The decoder
            # reports them per sentence as E_NUMERIC with source and location.
            weights[name] = float(value)

        max_distance = raw["max_dependency_distance"]
        if (
            not isinstance(max_distance, int)
            or isinstance(max_distance, bool)
            or max_distance <= 0
        ):
            raise EngineError(
                "E_INVALID_STAGE",
                "max_dependency_distance must be a positive integer",
                {"valid_stages": list(STAGES)},
            )
        if not isinstance(raw["unique_root"], bool):
            raise EngineError(
                "E_INVALID_STAGE",
                "unique_root must be a boolean",
                {"valid_stages": list(STAGES)},
            )
        for key in ("smoothing_strength", "confidence_threshold"):
            if not _is_number(raw[key]):
                raise EngineError(
                    "E_INVALID_STAGE",
                    f"{key} must be numeric",
                    {"valid_stages": list(STAGES)},
                )

        schema_version = raw.get("schema_version", 1)
        if not isinstance(schema_version, int) or isinstance(schema_version, bool):
            raise EngineError(
                "E_INVALID_STAGE",
                "schema_version must be an integer",
                {"valid_stages": list(STAGES)},
            )
        tokenizer_version = raw.get("tokenizer_version", "zh-mixed-unicode-v1")
        if not isinstance(tokenizer_version, str) or not tokenizer_version:
            raise EngineError(
                "E_INVALID_STAGE",
                "tokenizer_version must be a non-empty string",
                {"valid_stages": list(STAGES)},
            )

        return cls(
            schema_version=schema_version,
            tokenizer_version=tokenizer_version,
            score_weights=weights,
            max_dependency_distance=max_distance,
            unique_root=raw["unique_root"],
            smoothing_strength=float(raw["smoothing_strength"]),
            confidence_threshold=float(raw["confidence_threshold"]),
        )

    @classmethod
    def from_yaml(cls, path: Path) -> "AnalysisConfig":
        try:
            with path.open("r", encoding="utf-8") as handle:
                raw = yaml.safe_load(handle)
        except FileNotFoundError as exc:
            raise EngineError(
                "E_INVALID_STAGE",
                f"configuration file not found: {path}",
                {"valid_stages": list(STAGES)},
            ) from exc
        except yaml.YAMLError as exc:
            raise EngineError(
                "E_INVALID_STAGE",
                "configuration YAML is invalid",
                {"error": str(exc), "valid_stages": list(STAGES)},
            ) from exc
        return cls.from_mapping(raw)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "tokenizer_version": self.tokenizer_version,
            "score_weights": {
                "shift": self.score_weights["shift"],
                "left_reduce": self.score_weights["left_reduce"],
                "right_reduce": self.score_weights["right_reduce"],
            },
            "max_dependency_distance": self.max_dependency_distance,
            "unique_root": self.unique_root,
            "smoothing_strength": self.smoothing_strength,
            "confidence_threshold": self.confidence_threshold,
        }

    def components(self) -> dict[str, Any]:
        return self.to_mapping()

    def fingerprint(self) -> str:
        return sha256_obj(self.components())

    def stage_components(self, stage: str) -> dict[str, Any]:
        if stage not in STAGE_CONFIG_FIELDS:
            raise EngineError(
                "E_INVALID_STAGE",
                f"unknown stage: {stage}",
                {"valid_stages": list(STAGES)},
            )
        whole = self.components()
        return {key: whole[key] for key in STAGE_CONFIG_FIELDS[stage]}

    def stage_fingerprint(self, stage: str) -> str:
        return sha256_obj(self.stage_components(stage))

    def stage_fingerprints(self) -> dict[str, str]:
        return {stage: self.stage_fingerprint(stage) for stage in STAGES}

    def to_yaml_bytes(self) -> bytes:
        return yaml.safe_dump(
            self.to_mapping(),
            allow_unicode=True,
            sort_keys=False,
        ).encode("utf-8")

    def fingerprint_bytes(self) -> bytes:
        return canonical_bytes(self.components())
