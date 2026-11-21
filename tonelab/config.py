"""Configuration loading and versioning."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from .util import canonical_json_bytes, deep_merge, read_json, sha256_bytes


DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "audio": {
        "sample_rate": 16000,
        "channels": 1,
        "bit_depth": 16,
        "wav_subtype": "PCM_16",
    },
    "manifest": {
        "exact_total_samples": False,
        "allow_declared_total_upper_bound": True,
        "require_shard_sample_count": True,
    },
    "features": {
        "frame_length": 512,
        "hop_length": 256,
        "fft_size": 1024,
        "window": "hann",
        "envelope_bins": 64,
        "envelope_smoothing_bins": 9,
        "harmonic_count": 12,
        "round_digits": 12,
    },
    "model": {
        "hop_length": 256,
        "formant_count": 3,
        "pitch_min": 0.5,
        "pitch_max": 2.0,
        "f0_min_hz": 60.0,
        "f0_max_hz": 500.0,
        "transition_ms": 24.0,
        "round_digits": 12,
    },
    "quantization": {
        "pitch_bits": 16,
        "formant_bits": 18,
        "f0_bits": 16,
        "pitch_scale": 8192,
        "formant_scale": 16,
        "f0_scale": 16,
    },
    "v1_compat": {
        "pitch_percent_mode": "reference_ratio",
        "pitch_percent_scale": 0.01,
        "unit_separator": "|",
        "stable_id_prefix": "v1-",
        "stable_id_length": 12,
        "equal_unit_duration": True,
        "default_formants": {
            "vowel": [700.0, 1500.0, 2600.0],
            "front_vowel": [400.0, 2200.0, 3000.0],
            "consonant": [500.0, 1800.0, 2800.0],
            "pause": [300.0, 900.0, 1800.0],
            "fallback": [500.0, 1500.0, 2500.0],
        },
        "pause_symbols": ["pause", "sil", "sp", "停顿", "静音"],
        "front_vowel_symbols": ["i", "e", "u", "ü", "衣", "一", "语", "音"],
    },
    "export": {
        "json_embed_pcm": True,
        "sidecar_suffix": ".pitch.json",
        "units_suffix": ".units.json",
    },
}


class Config:
    def __init__(self, data: dict[str, Any] | None = None, *, source: str = "default") -> None:
        self.source = source
        self.data = deep_merge(copy.deepcopy(DEFAULT_CONFIG), data or {})
        self._validate()
        self.version = sha256_bytes(canonical_json_bytes(self.data))[:24]

    @classmethod
    def load(cls, path: str | Path | None = None, overrides: dict[str, Any] | None = None) -> "Config":
        data: dict[str, Any] = {}
        source = "default"
        if path:
            data = read_json(path)
            source = str(path)
        if overrides:
            data = deep_merge(data, overrides)
        return cls(data, source=source)

    def get(self, dotted: str, default: Any = None) -> Any:
        current: Any = self.data
        for part in dotted.split("."):
            if not isinstance(current, dict) or part not in current:
                return default
            current = current[part]
        return current

    def stage_config(self, stage: str) -> dict[str, Any]:
        mapping = {
            "feature": ("features", "audio"),
            "model": ("model",),
            "quantize": ("quantization",),
            "export": ("export", "audio"),
        }
        selected: dict[str, Any] = {"schema_version": self.data["schema_version"]}
        for key in mapping[stage]:
            selected[key] = self.data[key]
        return selected

    @property
    def audio(self) -> dict[str, Any]:
        return self.data["audio"]

    def _validate(self) -> None:
        audio = self.data["audio"]
        if int(audio["sample_rate"]) <= 0:
            raise ValueError("audio.sample_rate must be positive")
        if int(audio["channels"]) != 1:
            raise ValueError("this workbench currently supports mono PCM")
        if int(audio["bit_depth"]) != 16:
            raise ValueError("this workbench's deterministic PCM path requires 16-bit")
        features = self.data["features"]
        if int(features["frame_length"]) <= 0 or int(features["hop_length"]) <= 0:
            raise ValueError("feature frame/hop lengths must be positive")
        if int(features["fft_size"]) < int(features["frame_length"]):
            raise ValueError("features.fft_size must be >= frame_length")
        model = self.data["model"]
        if float(model["pitch_min"]) >= float(model["pitch_max"]):
            raise ValueError("model.pitch_min must be < pitch_max")
        quantization = self.data["quantization"]
        for key in ("pitch_bits", "formant_bits", "f0_bits"):
            if int(quantization[key]) < 2:
                raise ValueError(f"quantization.{key} must be >= 2")
        for key in ("pitch_scale", "formant_scale", "f0_scale"):
            if float(quantization[key]) <= 0:
                raise ValueError(f"quantization.{key} must be positive")
        if str(self.data["v1_compat"]["unit_separator"]) == "":
            raise ValueError("v1_compat.unit_separator must not be empty")

