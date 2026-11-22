"""Typed pipeline configuration with per-stage versioning and content digests."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from fractions import Fraction

from .codec import digest, fraction_from_text


@dataclass(frozen=True)
class DetectConfig:
    iou_threshold: str = "0.5"
    offset_sigma: str = "0.02"
    candidates_per_region: int = 2
    background_candidates_per_page: int = 2
    confidence_precision: int = 6

    @property
    def threshold(self) -> Fraction:
        return fraction_from_text(self.iou_threshold)

    @property
    def sigma(self) -> float:
        return float(self.offset_sigma)


@dataclass(frozen=True)
class ParseConfig:
    location_priority: tuple = ("cn_article", "cn_section", "en_section", "en_article")


@dataclass(frozen=True)
class NormalizeConfig:
    duration_precision: int = 2
    penalty_precision: int = 2
    month_precision: int = 0
    duration_factors: dict = field(
        default_factory=lambda: {
            "年": "12",
            "周年": "12",
            "月": "1",
            "个月": "1",
            "日": "1/30",
            "天": "1/30",
            "year": "12",
            "years": "12",
            "month": "1",
            "months": "1",
            "day": "1/30",
            "days": "1/30",
        }
    )
    penalty_factors: dict = field(
        default_factory=lambda: {
            "%": "100",
            "％": "100",
            "percent": "100",
        }
    )

    def duration_factor(self, unit: str) -> Fraction:
        return fraction_from_text(self.duration_factors[unit])

    def penalty_factor(self, unit: str) -> Fraction:
        return fraction_from_text(self.penalty_factors[unit])


@dataclass(frozen=True)
class ResolveConfig:
    source_prior: dict = field(
        default_factory=lambda: {
            "official": "0.95",
            "broker": "0.7",
            "archive": "0.6",
            "web": "0.4",
        }
    )
    language_weight: dict = field(default_factory=lambda: {"zh": "1.0", "en": "0.95"})
    freshness_half_life_months: int = 12
    compare_precision: dict = field(
        default_factory=lambda: {"duration": 0, "penalty": 0, "month": 0}
    )
    arbitration_order: tuple = (
        "source_prior_desc",
        "confidence_desc",
        "freshness_desc",
        "language_weight_desc",
        "ref_id_asc",
    )

    def prior(self, source: str) -> Fraction:
        return fraction_from_text(self.source_prior.get(source, "0.5"))

    def lang_weight(self, lang: str) -> Fraction:
        return fraction_from_text(self.language_weight.get(lang, "0.9"))


@dataclass(frozen=True)
class PipelineConfig:
    schema_version: int = 1
    detect: DetectConfig = field(default_factory=DetectConfig)
    parse: ParseConfig = field(default_factory=ParseConfig)
    normalize: NormalizeConfig = field(default_factory=NormalizeConfig)
    resolve: ResolveConfig = field(default_factory=ResolveConfig)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineConfig":
        data = dict(data)
        data.pop("schema_version", None)
        return cls(
            detect=DetectConfig(**data.get("detect", {})),
            parse=ParseConfig(**data.get("parse", {})),
            normalize=NormalizeConfig(**data.get("normalize", {})),
            resolve=ResolveConfig(**data.get("resolve", {})),
        )

    def stage_digest(self, stage: str) -> str:
        """Content digest of the configuration slice governing one stage."""
        if stage == "detect":
            return digest(asdict(self.detect))
        if stage == "parse":
            return digest(asdict(self.parse))
        if stage == "normalize":
            return digest(asdict(self.normalize))
        if stage == "resolve":
            return digest(asdict(self.resolve))
        raise KeyError(stage)

    def full_digest(self) -> str:
        return digest(self.to_dict())


STAGE_ORDER = ("detect", "parse", "normalize", "resolve")
CONFIG_STAGE_OF = {
    "iou_threshold": "detect",
    "offset_sigma": "detect",
    "candidates_per_region": "detect",
    "background_candidates_per_page": "detect",
    "confidence_precision": "detect",
    "location_priority": "parse",
    "duration_factors": "normalize",
    "penalty_factors": "normalize",
    "duration_precision": "normalize",
    "penalty_precision": "normalize",
    "month_precision": "normalize",
    "source_prior": "resolve",
    "language_weight": "resolve",
    "freshness_half_life_months": "resolve",
    "compare_precision": "resolve",
    "arbitration_order": "resolve",
}
