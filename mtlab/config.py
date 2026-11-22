from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from . import ENGINE_VERSION


TIERS: tuple[str, ...] = ("small", "medium", "large")
STAGE_ORDER: tuple[str, ...] = ("split", "align", "policy", "decode", "evaluate")
STAGE_INDEX = {stage: idx + 1 for idx, stage in enumerate(STAGE_ORDER)}


@dataclass(frozen=True)
class ExperimentConfig:
    generator_seed: int = 20261123
    bpe_merges: int = 40
    bpe_min_count: int = 2
    smoothing: float = 0.25
    discard_threshold: float = -12.0
    em_iterations: int = 4
    policy_budget: int = 20_000_000
    beam_width_override: int | None = None
    phrase_limit_override: int | None = None
    phrase_max_len: int = 3
    metric_max_order: int = 4
    confidence_level: float = 0.95
    tiers: tuple[str, ...] = TIERS
    engine_version: str = ENGINE_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "generator_seed": self.generator_seed,
            "bpe_merges": self.bpe_merges,
            "bpe_min_count": self.bpe_min_count,
            "smoothing": self.smoothing,
            "discard_threshold": self.discard_threshold,
            "em_iterations": self.em_iterations,
            "policy_budget": self.policy_budget,
            "beam_width_override": self.beam_width_override,
            "phrase_limit_override": self.phrase_limit_override,
            "phrase_max_len": self.phrase_max_len,
            "metric_max_order": self.metric_max_order,
            "confidence_level": self.confidence_level,
            "tiers": list(self.tiers),
            "engine_version": self.engine_version,
        }

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "ExperimentConfig":
        known = {
            "generator_seed",
            "bpe_merges",
            "bpe_min_count",
            "smoothing",
            "discard_threshold",
            "em_iterations",
            "policy_budget",
            "beam_width_override",
            "phrase_limit_override",
            "phrase_max_len",
            "metric_max_order",
            "confidence_level",
            "tiers",
            "engine_version",
        }
        data = {key: val for key, val in value.items() if key in known}
        if "tiers" in data:
            data["tiers"] = tuple(data["tiers"])
        return cls(**data)


@dataclass
class RunOptions:
    resume: bool = True
    stop_after_tier: str | None = None
    data_dir: str = "data"
    quiet: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

