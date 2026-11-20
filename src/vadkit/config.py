"""Validated, fingerprintable VAD configuration."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .errors import invalid_config
from .util import sha256_canonical


@dataclass(frozen=True)
class VADConfig:
    sample_rate: int = 16_000
    frame_ms: float = 20.0
    hop_ms: float = 10.0
    window: str = "hann"
    energy_weight: float = 0.62
    flatness_weight: float = 0.38
    energy_floor_db: float = -70.0
    energy_ceiling_db: float = -10.0
    high_threshold: float = 0.55
    low_threshold: float = 0.46
    min_speech_ms: float = 60.0
    min_silence_ms: float = 160.0
    clip_peak_abs: int = 32_760
    clip_run_samples: int = 8

    def validate(self) -> VADConfig:
        if self.sample_rate <= 0:
            raise invalid_config("sample_rate must be positive", sample_rate=self.sample_rate)
        if self.frame_ms <= 0 or self.hop_ms <= 0:
            raise invalid_config(
                "frame_ms and hop_ms must be positive",
                frame_ms=self.frame_ms,
                hop_ms=self.hop_ms,
            )
        if self.frame_ms < self.hop_ms:
            raise invalid_config(
                "frame_ms must be greater than or equal to hop_ms",
                frame_ms=self.frame_ms,
                hop_ms=self.hop_ms,
            )
        if self.window != "hann":
            raise invalid_config("only the hann window is supported", window=self.window)
        if self.energy_weight < 0 or self.flatness_weight < 0:
            raise invalid_config(
                "feature weights must be non-negative",
                energy_weight=self.energy_weight,
                flatness_weight=self.flatness_weight,
            )
        if abs(self.energy_weight + self.flatness_weight - 1.0) > 1e-9:
            raise invalid_config(
                "feature weights must sum to 1",
                energy_weight=self.energy_weight,
                flatness_weight=self.flatness_weight,
            )
        if self.energy_floor_db >= self.energy_ceiling_db:
            raise invalid_config(
                "energy_floor_db must be lower than energy_ceiling_db",
                energy_floor_db=self.energy_floor_db,
                energy_ceiling_db=self.energy_ceiling_db,
            )
        if not 0.0 <= self.low_threshold <= 1.0:
            raise invalid_config(
                "low_threshold must be in [0, 1]", low_threshold=self.low_threshold
            )
        if not 0.0 <= self.high_threshold <= 1.0:
            raise invalid_config(
                "high_threshold must be in [0, 1]", high_threshold=self.high_threshold
            )
        if self.high_threshold < self.low_threshold:
            raise invalid_config(
                "high_threshold must not be lower than low_threshold",
                high_threshold=self.high_threshold,
                low_threshold=self.low_threshold,
            )
        if self.min_speech_ms < 0 or self.min_silence_ms < 0:
            raise invalid_config(
                "minimum segment lengths must be non-negative",
                min_speech_ms=self.min_speech_ms,
                min_silence_ms=self.min_silence_ms,
            )
        if self.clip_peak_abs <= 0 or self.clip_peak_abs > 32_768:
            raise invalid_config(
                "clip_peak_abs must be in (0, 32768]", clip_peak_abs=self.clip_peak_abs
            )
        if self.clip_run_samples <= 0:
            raise invalid_config(
                "clip_run_samples must be positive", clip_run_samples=self.clip_run_samples
            )
        return self

    @property
    def frame_samples(self) -> int:
        return max(1, int(round(self.sample_rate * self.frame_ms / 1000.0)))

    @property
    def hop_samples(self) -> int:
        return max(1, int(round(self.sample_rate * self.hop_ms / 1000.0)))

    @property
    def min_speech_samples(self) -> int:
        return int(round(self.sample_rate * self.min_speech_ms / 1000.0))

    @property
    def min_silence_samples(self) -> int:
        return int(round(self.sample_rate * self.min_silence_ms / 1000.0))

    @property
    def fingerprint(self) -> str:
        data = asdict(self)
        data["schema"] = "vadkit.vad-config.v1"
        data["frame_samples"] = self.frame_samples
        data["hop_samples"] = self.hop_samples
        return sha256_canonical(data)

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["frame_samples"] = self.frame_samples
        data["hop_samples"] = self.hop_samples
        data["fingerprint"] = self.fingerprint
        return data


def config_from_dict(data: dict[str, Any]) -> VADConfig:
    allowed = set(VADConfig.__dataclass_fields__)
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise invalid_config("unknown configuration fields", fields=unknown)
    return VADConfig(**data).validate()

