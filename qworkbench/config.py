from __future__ import annotations

import copy
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .errors import BusinessError
from .util import read_json, sha256_json


FACTOR_NAMES = ("length_penalty", "lexical_overlap", "template_diversity", "source_prior")


def _config_error(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_CONFIG", message, details)


def _number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _config_error(f"{name} must be a number", field=name, value=str(value))
    result = float(value)
    if not math.isfinite(result):
        raise _config_error(f"{name} must be finite", field=name, value=str(value))
    return result


@dataclass(frozen=True)
class QuantizationConfig:
    fraction_bits: int = 20
    min_factor: float = 0.0001

    @property
    def scale(self) -> int:
        return 1 << self.fraction_bits

    def validate(self) -> None:
        if isinstance(self.fraction_bits, bool) or not isinstance(self.fraction_bits, int):
            raise _config_error("quantization.fraction_bits must be an integer")
        if not 8 <= self.fraction_bits <= 30:
            raise _config_error(
                "quantization.fraction_bits is out of range",
                allowed=[8, 30],
                value=self.fraction_bits,
            )
        if not 1e-9 <= self.min_factor < 0.5:
            raise _config_error(
                "quantization.min_factor is out of range",
                allowed=[1e-9, 0.5],
                value=self.min_factor,
            )


@dataclass(frozen=True)
class DedupConfig:
    hamming_threshold: int = 10
    separator: str = "␟"

    def validate(self) -> None:
        if isinstance(self.hamming_threshold, bool) or not isinstance(self.hamming_threshold, int):
            raise _config_error("dedup.hamming_threshold must be an integer")
        if not 0 <= self.hamming_threshold <= 64:
            raise _config_error(
                "dedup.hamming_threshold is out of range",
                allowed=[0, 64],
                value=self.hamming_threshold,
            )
        if not isinstance(self.separator, str) or not self.separator:
            raise _config_error("dedup.separator must be a non-empty string")


@dataclass(frozen=True)
class ScoringConfig:
    weights: Mapping[str, float]
    source_priors: Mapping[str, float]
    target_tokens: int = 32
    long_output_multiplier: float = 2.0
    unknown_source_prior: float = 0.5

    def validate(self) -> None:
        missing = set(FACTOR_NAMES) - set(self.weights)
        extra = set(self.weights) - set(FACTOR_NAMES)
        if missing or extra:
            raise _config_error(
                "scoring.weights must contain exactly the four quality factors",
                missing=sorted(missing),
                extra=sorted(extra),
            )
        total = 0.0
        for name in FACTOR_NAMES:
            weight = _number(self.weights[name], f"scoring.weights.{name}")
            if not 0.0 < weight <= 1.0:
                raise _config_error(
                    "scoring weight is out of range",
                    field=f"scoring.weights.{name}",
                    allowed=[">0", 1.0],
                    value=weight,
                )
            total += weight
        if total <= 0.0:
            raise _config_error("scoring weights must have a positive sum")
        for source, prior in self.source_priors.items():
            if not isinstance(source, str) or not source:
                raise _config_error("source prior keys must be non-empty strings")
            value = _number(prior, f"scoring.source_priors.{source}")
            if not 0.0 < value <= 1.0:
                raise _config_error(
                    "source prior is out of range",
                    field=f"scoring.source_priors.{source}",
                    allowed=[">0", 1.0],
                    value=value,
                )
        unknown = _number(self.unknown_source_prior, "scoring.unknown_source_prior")
        if not 0.0 < unknown <= 1.0:
            raise _config_error(
                "scoring.unknown_source_prior is out of range",
                allowed=[">0", 1.0],
                value=unknown,
            )
        if isinstance(self.target_tokens, bool) or not isinstance(self.target_tokens, int):
            raise _config_error("scoring.target_tokens must be an integer")
        if not 1 <= self.target_tokens <= 1_000_000:
            raise _config_error("scoring.target_tokens is out of range", value=self.target_tokens)
        multiple = _number(self.long_output_multiplier, "scoring.long_output_multiplier")
        if not 1.0 <= multiple <= 100.0:
            raise _config_error(
                "scoring.long_output_multiplier is out of range",
                allowed=[1.0, 100.0],
                value=multiple,
            )


@dataclass(frozen=True)
class AllocationConfig:
    target_size: int
    source_weights: Mapping[str, float]
    lang_weights: Mapping[str, float]
    default_weight: float = 0.1

    def validate(self) -> None:
        if isinstance(self.target_size, bool) or not isinstance(self.target_size, int):
            raise _config_error("allocation.target_size must be an integer")
        if not 1 <= self.target_size <= 10_000_000:
            raise _config_error("allocation.target_size is out of range", value=self.target_size)
        for group_name, weights in (
            ("source_weights", self.source_weights),
            ("lang_weights", self.lang_weights),
        ):
            if not weights:
                raise _config_error(f"allocation.{group_name} must not be empty")
            positive = False
            for key, raw in weights.items():
                if not isinstance(key, str) or not key:
                    raise _config_error(f"allocation.{group_name} keys must be strings")
                value = _number(raw, f"allocation.{group_name}.{key}")
                if not 0.0 <= value <= 1.0:
                    raise _config_error(
                        "allocation weight is out of range",
                        field=f"allocation.{group_name}.{key}",
                        allowed=[0.0, 1.0],
                        value=value,
                    )
                positive = positive or value > 0.0
            if not positive:
                raise _config_error(f"allocation.{group_name} has no positive weight")
        default = _number(self.default_weight, "allocation.default_weight")
        if not 0.0 <= default <= 1.0:
            raise _config_error(
                "allocation.default_weight is out of range",
                allowed=[0.0, 1.0],
                value=default,
            )


@dataclass(frozen=True)
class ConsistencyConfig:
    buckets: int = 5
    annotation_min: float = 1.0
    annotation_max: float = 5.0
    source_bias_threshold: float = 0.18
    sample_outlier_threshold: float = 0.35
    min_annotated_for_review: int = 1

    def validate(self) -> None:
        if isinstance(self.buckets, bool) or not isinstance(self.buckets, int):
            raise _config_error("consistency.buckets must be an integer")
        if not 2 <= self.buckets <= 100:
            raise _config_error("consistency.buckets is out of range", allowed=[2, 100])
        low = _number(self.annotation_min, "consistency.annotation_min")
        high = _number(self.annotation_max, "consistency.annotation_max")
        if not high > low:
            raise _config_error("consistency annotation range must be increasing")
        for name in ("source_bias_threshold", "sample_outlier_threshold"):
            value = _number(getattr(self, name), f"consistency.{name}")
            if not 0.0 <= value <= 1.0:
                raise _config_error(
                    f"consistency.{name} is out of range",
                    field=name,
                    allowed=[0.0, 1.0],
                    value=value,
                )
        if isinstance(self.min_annotated_for_review, bool) or not isinstance(
            self.min_annotated_for_review, int
        ):
            raise _config_error("consistency.min_annotated_for_review must be an integer")
        if not 1 <= self.min_annotated_for_review <= 1_000_000:
            raise _config_error("consistency.min_annotated_for_review is out of range")


@dataclass(frozen=True)
class WorkbenchConfig:
    version: str
    quantization: QuantizationConfig
    dedup: DedupConfig
    scoring: ScoringConfig
    allocation: AllocationConfig
    consistency: ConsistencyConfig
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WorkbenchConfig":
        if not isinstance(data, Mapping):
            raise _config_error("config root must be an object")
        version = data.get("version")
        if not isinstance(version, str) or not version:
            raise _config_error("config.version must be a non-empty string")
        try:
            quant_raw = dict(data.get("quantization", {}))
            dedup_raw = dict(data.get("dedup", {}))
            scoring_raw = dict(data.get("scoring", {}))
            allocation_raw = dict(data.get("allocation", {}))
            consistency_raw = dict(data.get("consistency", {}))
            config = cls(
                version=version,
                quantization=QuantizationConfig(
                    fraction_bits=quant_raw.get("fraction_bits", 20),
                    min_factor=quant_raw.get("min_factor", 0.0001),
                ),
                dedup=DedupConfig(
                    hamming_threshold=dedup_raw.get("hamming_threshold", 10),
                    separator=dedup_raw.get("separator", "␟"),
                ),
                scoring=ScoringConfig(
                    weights=dict(scoring_raw.get("weights", {})),
                    source_priors=dict(scoring_raw.get("source_priors", {})),
                    target_tokens=scoring_raw.get("target_tokens", 32),
                    long_output_multiplier=scoring_raw.get("long_output_multiplier", 2.0),
                    unknown_source_prior=scoring_raw.get("unknown_source_prior", 0.5),
                ),
                allocation=AllocationConfig(
                    target_size=allocation_raw.get("target_size", 24),
                    source_weights=dict(allocation_raw.get("source_weights", {})),
                    lang_weights=dict(allocation_raw.get("lang_weights", {})),
                    default_weight=allocation_raw.get("default_weight", 0.1),
                ),
                consistency=ConsistencyConfig(
                    buckets=consistency_raw.get("buckets", 5),
                    annotation_min=consistency_raw.get("annotation_min", 1.0),
                    annotation_max=consistency_raw.get("annotation_max", 5.0),
                    source_bias_threshold=consistency_raw.get("source_bias_threshold", 0.18),
                    sample_outlier_threshold=consistency_raw.get(
                        "sample_outlier_threshold", 0.35
                    ),
                    min_annotated_for_review=consistency_raw.get(
                        "min_annotated_for_review", 1
                    ),
                ),
                raw=copy.deepcopy(dict(data)),
            )
        except (TypeError, ValueError) as exc:
            raise _config_error("invalid config shape", reason=str(exc)) from exc
        config.validate()
        return config

    @classmethod
    def load(cls, path: Path) -> "WorkbenchConfig":
        try:
            data = read_json(path)
        except (OSError, ValueError) as exc:
            raise BusinessError("E_CONFIG", "unable to read config", {"path": str(path), "reason": str(exc)})
        return cls.from_dict(data)

    def validate(self) -> None:
        self.quantization.validate()
        self.dedup.validate()
        self.scoring.validate()
        self.allocation.validate()
        self.consistency.validate()

    @property
    def fingerprint(self) -> str:
        return sha256_json(self.raw)

    def stage_payload(self, stage: str) -> dict[str, Any]:
        common = {"config_version": self.version, "quantization": self.quantization.__dict__}
        if stage == "normalize":
            return {"config_version": self.version, "dedup": self.dedup.__dict__}
        if stage == "score":
            return {**common, "scoring": dict(self.scoring.weights), "source_priors": dict(self.scoring.source_priors), "target_tokens": self.scoring.target_tokens, "long_output_multiplier": self.scoring.long_output_multiplier, "unknown_source_prior": self.scoring.unknown_source_prior}
        if stage == "allocate":
            return {"config_version": self.version, "allocation": self.allocation.__dict__}
        if stage == "mix":
            return {"config_version": self.version, "consistency": self.consistency.__dict__}
        raise _config_error("unknown pipeline stage", stage=stage)

    def stage_config_fingerprint(self, stage: str) -> str:
        return sha256_json(self.stage_payload(stage))
