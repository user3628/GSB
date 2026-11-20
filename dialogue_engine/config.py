"""Versioned, serializable configuration for every engine stage."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .serialization import digest_json


STAGES = ("features", "emotion", "behavior", "state")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}
EMOTIONS = ("平静", "高兴", "愤怒", "悲伤")
DIALOGUE_ACTS = ("陈述", "提问", "确认", "打断")


def _default_transition_weights() -> dict[str, dict[str, float]]:
    return {
        "陈述": {"陈述": 0.35, "提问": 0.10, "确认": 0.08, "打断": 0.00},
        "提问": {"陈述": 0.05, "提问": 0.10, "确认": 0.30, "打断": 0.02},
        "确认": {"陈述": 0.20, "提问": 0.10, "确认": 0.15, "打断": 0.02},
        "打断": {"陈述": 0.10, "提问": 0.08, "确认": 0.08, "打断": 0.08},
    }


@dataclass(frozen=True)
class FeatureConfig:
    sample_rate: int = 16_000
    frame_ms: float = 25.0
    hop_ms: float = 10.0
    mel_bands: int = 26
    pause_db_below_peak: float = 25.0
    energy_floor: float = 1e-16
    window: str = "hann"
    decimals: int = 10

    @property
    def frame_samples(self) -> int:
        return int(round(self.sample_rate * self.frame_ms / 1000.0))

    @property
    def hop_samples(self) -> int:
        return int(round(self.sample_rate * self.hop_ms / 1000.0))

    @property
    def fft_size(self) -> int:
        frame_samples = self.frame_samples
        return max(256, 1 << (frame_samples - 1).bit_length())

    @property
    def dimension(self) -> int:
        return 16 + self.mel_bands

    def validate(self) -> None:
        if self.sample_rate <= 0:
            raise ValueError("sample_rate must be positive")
        if self.frame_samples <= 1 or self.hop_samples <= 0:
            raise ValueError("frame_ms and hop_ms must produce non-empty frames")
        if self.hop_samples > self.frame_samples:
            raise ValueError("hop_ms cannot exceed frame_ms")
        if self.mel_bands < 1:
            raise ValueError("mel_bands must be positive")
        if self.pause_db_below_peak < 0:
            raise ValueError("pause_db_below_peak must be non-negative")
        if self.energy_floor <= 0:
            raise ValueError("energy_floor must be positive")
        if self.window != "hann":
            raise ValueError("only the deterministic hann window is supported")
        if self.decimals < 4 or self.decimals > 15:
            raise ValueError("decimals must be between 4 and 15")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def version(self) -> str:
        return digest_json({"stage": "features", "config": self.to_dict()})

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "FeatureConfig":
        return cls(**value)


@dataclass(frozen=True)
class ClassifierConfig:
    projection_seed: int = 20_261_121
    projection_dim: int = 64
    score_temperature: float = 5.0
    anchor_version: str = "acoustic-anchors-v1"
    decimals: int = 10

    def validate(self) -> None:
        if self.projection_dim < 8:
            raise ValueError("projection_dim must be at least 8")
        if self.score_temperature <= 0:
            raise ValueError("score_temperature must be positive")
        if self.decimals < 4 or self.decimals > 15:
            raise ValueError("decimals must be between 4 and 15")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def version(self) -> str:
        return digest_json({"stage": "emotion", "config": self.to_dict()})

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ClassifierConfig":
        return cls(**value)


@dataclass(frozen=True)
class BehaviorConfig:
    statement_prior: float = 0.85
    question_energy_slope_gain: float = 2.80
    question_end_energy_gain: float = 0.45
    confirmation_max_duration_ms: float = 760.0
    confirmation_low_pause_gain: float = 0.75
    interruption_max_gap_ms: float = 90.0
    interruption_max_duration_ms: float = 900.0
    interruption_energy_gain: float = 0.55
    prior_act_weight: float = 1.0
    prior_emotion_weight: float = 0.35
    transition_weights: dict[str, dict[str, float]] = field(
        default_factory=_default_transition_weights
    )
    score_temperature: float = 2.0
    decimals: int = 10

    def validate(self) -> None:
        for previous in DIALOGUE_ACTS:
            row = self.transition_weights.get(previous)
            if not isinstance(row, dict):
                raise ValueError(f"transition_weights missing row {previous}")
            for current in DIALOGUE_ACTS:
                if not isinstance(row.get(current), (int, float)):
                    raise ValueError(
                        f"transition_weights missing {previous}->{current}"
                    )
        if self.score_temperature <= 0:
            raise ValueError("score_temperature must be positive")
        if self.decimals < 4 or self.decimals > 15:
            raise ValueError("decimals must be between 4 and 15")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def version(self) -> str:
        return digest_json({"stage": "behavior", "config": self.to_dict()})

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "BehaviorConfig":
        return cls(**value)


@dataclass(frozen=True)
class StateConfig:
    decay: float = 0.62
    trend_scale: float = 6.0
    decimals: int = 10

    def validate(self) -> None:
        if not 0.0 <= self.decay <= 1.0:
            raise ValueError("decay must be between 0 and 1")
        if self.trend_scale <= 0:
            raise ValueError("trend_scale must be positive")
        if self.decimals < 4 or self.decimals > 15:
            raise ValueError("decimals must be between 4 and 15")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def version(self) -> str:
        return digest_json({"stage": "state", "config": self.to_dict()})

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "StateConfig":
        return cls(**value)


@dataclass(frozen=True)
class MetricsConfig:
    label_score_tolerance: float = 0.02
    trace_tolerance: float = 0.15
    decimals: int = 10

    def validate(self) -> None:
        if self.label_score_tolerance < 0:
            raise ValueError("label_score_tolerance must be non-negative")
        if self.trace_tolerance < 0:
            raise ValueError("trace_tolerance must be non-negative")
        if self.decimals < 4 or self.decimals > 15:
            raise ValueError("decimals must be between 4 and 15")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def version(self) -> str:
        return digest_json({"stage": "metrics", "config": self.to_dict()})

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MetricsConfig":
        return cls(**value)


@dataclass(frozen=True)
class EngineConfig:
    schema_version: int = 1
    features: FeatureConfig = field(default_factory=FeatureConfig)
    classifier: ClassifierConfig = field(default_factory=ClassifierConfig)
    behavior: BehaviorConfig = field(default_factory=BehaviorConfig)
    state: StateConfig = field(default_factory=StateConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)

    def validate(self) -> None:
        self.features.validate()
        self.classifier.validate()
        self.behavior.validate()
        self.state.validate()
        self.metrics.validate()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "features": self.features.to_dict(),
            "classifier": self.classifier.to_dict(),
            "behavior": self.behavior.to_dict(),
            "state": self.state.to_dict(),
            "metrics": self.metrics.to_dict(),
        }

    @property
    def version(self) -> str:
        return digest_json({"stage": "engine", "config": self.to_dict()})

    def stage_version(self, stage: str) -> str:
        if stage == "features":
            return self.features.version
        if stage == "emotion":
            return self.classifier.version
        if stage == "behavior":
            return self.behavior.version
        if stage == "state":
            return digest_json(
                {
                    "stage": "state",
                    "state": self.state.to_dict(),
                    "metrics": self.metrics.to_dict(),
                }
            )
        raise ValueError(f"unknown stage: {stage}")

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "EngineConfig":
        return cls(
            schema_version=int(value.get("schema_version", 1)),
            features=FeatureConfig.from_dict(value.get("features", {})),
            classifier=ClassifierConfig.from_dict(value.get("classifier", {})),
            behavior=BehaviorConfig.from_dict(value.get("behavior", {})),
            state=StateConfig.from_dict(value.get("state", {})),
            metrics=MetricsConfig.from_dict(value.get("metrics", {})),
        )

