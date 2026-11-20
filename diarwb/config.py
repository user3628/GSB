"""Pipeline configuration and canonical, fingerprintable serialization.

Floating-point values are rendered as fixed-point decimals (6 places) in the
canonical form so that the configuration fingerprint is byte-stable across
Python builds and platforms -- nothing relies on ``repr(float)``.
"""
from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict

from .errors import EInvalidConfig

CANON_DECIMALS = 6
CONFIG_VERSION_PREFIX = "cfg"


def _canon(value: Any) -> Any:
    if isinstance(value, float):
        return f"{value:.{CANON_DECIMALS}}f"
    if isinstance(value, dict):
        return {k: _canon(value[k]) for k in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canon(v) for v in value]
    return value


@dataclass
class PipelineConfig:
    """All tunable parameters; every field participates in the fingerprint."""

    sample_rate: int = 16000
    frame_size: int = 512          # samples (32 ms at 16 kHz)
    hop_size: int = 256            # samples (16 ms)
    n_mels: int = 26
    embedding_dim: int = 32
    mask_iterations: int = 12
    mask_floor: float = 0.05
    energy_floor: float = 1e-6
    recognition_margin: float = 0.08
    overlap_energy_quantile: float = 0.35
    low_confidence_quantile: float = 0.25
    base_seed: int = 20261120
    latency_budget_ms: int = 4000

    # Per-shard speaker count override: {shard_id: count}.  Empty means the
    # manifest-declared speaker count is used.
    speaker_overrides: Dict[str, int] = field(default_factory=dict)

    def validate(self) -> None:
        problems = []
        if self.sample_rate <= 0:
            problems.append("sample_rate must be positive")
        if self.frame_size <= 0 or self.hop_size <= 0:
            problems.append("frame/hop size must be positive")
        if self.hop_size > self.frame_size:
            problems.append("hop_size must not exceed frame_size")
        if self.n_mels <= 0 or self.embedding_dim <= 0:
            problems.append("n_mels/embedding_dim must be positive")
        if self.mask_iterations < 1:
            problems.append("mask_iterations must be >= 1")
        if not 0.0 <= self.mask_floor <= 1.0:
            problems.append("mask_floor must lie in [0, 1]")
        if self.energy_floor <= 0:
            problems.append("energy_floor must be > 0")
        if not 0.0 <= self.recognition_margin <= 1.0:
            problems.append("recognition_margin must lie in [0, 1]")
        if not 0.0 <= self.overlap_energy_quantile <= 1.0:
            problems.append("overlap_energy_quantile must lie in [0, 1]")
        if not 0.0 <= self.low_confidence_quantile <= 1.0:
            problems.append("low_confidence_quantile must lie in [0, 1]")
        if self.base_seed < 0:
            problems.append("base_seed must be non-negative")
        if self.latency_budget_ms < 1:
            problems.append("latency_budget_ms must be >= 1")
        for shard_id, count in self.speaker_overrides.items():
            if not isinstance(count, int) or count < 1 or count > 8:
                problems.append(f"speaker override for {shard_id!r} out of range")
        if problems:
            raise EInvalidValidationConfig(problems)

    def canonical(self) -> Dict[str, Any]:
        return _canon(asdict(self))

    def canonical_json(self) -> bytes:
        return json.dumps(self.canonical(), sort_keys=True,
                          separators=(",", ":")).encode("utf-8")

    def fingerprint(self) -> str:
        return f"{CONFIG_VERSION_PREFIX}-" + hashlib.sha256(
            self.canonical_json()).hexdigest()[:16]

    def clone(self, **changes: Any) -> "PipelineConfig":
        data = asdict(self)
        data.update(changes)
        cfg = PipelineConfig(**data)
        cfg.validate()
        return cfg


class EInvalidValidationConfig(EInvalidConfig):
    def __init__(self, problems):
        super().__init__("invalid pipeline configuration",
                         {"problems": list(problems)})


def default_config() -> PipelineConfig:
    cfg = PipelineConfig()
    cfg.validate()
    return cfg
