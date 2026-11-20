"""YAML configuration loading and validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .errors import TTSError
from .utils import canonical_json, sha256_json, sha256_text


SUPPORTED_ACTIVATIONS = {"tanh"}


@dataclass(frozen=True)
class ModelConfig:
    seed: int
    text_embedding_dim: int
    frame_shift_ms: int
    n_bands: int
    hidden_dims: tuple[int, ...]
    activation: str
    prosody_dim: int

    @property
    def input_dim(self) -> int:
        return self.text_embedding_dim + self.prosody_dim + 4

    @property
    def output_dim(self) -> int:
        return self.n_bands + 2


@dataclass(frozen=True)
class SplitConfig:
    first_chunk_min_chars: int
    subsequent_chunk_chars: int
    punctuation_forced: bool
    max_budget_splits: int


@dataclass(frozen=True)
class BudgetCostUnits:
    base: float
    per_visible_char: float
    per_frame: float
    per_complexity: float
    per_depth: float


@dataclass(frozen=True)
class BudgetConfig:
    first_packet_ms: float
    continuation_ms: float
    max_workers: int
    max_preprocess_depth: int
    cost_units: BudgetCostUnits


@dataclass(frozen=True)
class SynthesisConfig:
    phase_seed: int
    harmonic_rolloff: float
    noise_level: float


@dataclass(frozen=True)
class OutputConfig:
    sample_rate: int
    bit_depth: int
    channels: int

    @property
    def bytes_per_sample(self) -> int:
        return self.bit_depth // 8

    @property
    def frame_bytes(self) -> int:
        return self.bytes_per_sample * self.channels


@dataclass(frozen=True)
class ValidationConfig:
    mode: str
    reference_tolerance: float


@dataclass(frozen=True)
class EngineConfig:
    schema_version: int
    workspace: Path
    config_path: Path
    paths: dict[str, str]
    model: ModelConfig
    split: SplitConfig
    budget: BudgetConfig
    synthesis: SynthesisConfig
    output: OutputConfig
    validation: ValidationConfig
    raw: dict[str, Any] = field(repr=False)

    def resolved_path(self, key: str) -> Path:
        value = self.paths[key]
        path = Path(value)
        return path if path.is_absolute() else self.workspace / path

    @property
    def hop_samples(self) -> int:
        numerator = self.output.sample_rate * self.model.frame_shift_ms
        if numerator % 1000:
            raise TTSError(
                "E_INVALID_CONFIG",
                "sample_rate * frame_shift_ms must be an integer number of samples",
                {"sample_rate": self.output.sample_rate, "frame_shift_ms": self.model.frame_shift_ms},
            )
        return numerator // 1000

    def model_structure_digest(self) -> str:
        return sha256_json(
            {
                "seed": self.model.seed,
                "text_embedding_dim": self.model.text_embedding_dim,
                "frame_shift_ms": self.model.frame_shift_ms,
                "n_bands": self.model.n_bands,
                "hidden_dims": list(self.model.hidden_dims),
                "activation": self.model.activation,
                "prosody_dim": self.model.prosody_dim,
            }
        )

    def synthesis_digest(self) -> str:
        return sha256_json(
            {
                "phase_seed": self.synthesis.phase_seed,
                "harmonic_rolloff": self.synthesis.harmonic_rolloff,
                "noise_level": self.synthesis.noise_level,
                "sample_rate": self.output.sample_rate,
                "bit_depth": self.output.bit_depth,
                "channels": self.output.channels,
                "frame_shift_ms": self.model.frame_shift_ms,
            }
        )

    def budget_digest(self) -> str:
        return sha256_json(
            {
                "first_packet_ms": self.budget.first_packet_ms,
                "continuation_ms": self.budget.continuation_ms,
                "max_workers": self.budget.max_workers,
                "max_preprocess_depth": self.budget.max_preprocess_depth,
                "cost_units": self.budget.cost_units.__dict__,
            }
        )

    def split_digest(self) -> str:
        return sha256_json(
            {
                "first_chunk_min_chars": self.split.first_chunk_min_chars,
                "subsequent_chunk_chars": self.split.subsequent_chunk_chars,
                "punctuation_forced": self.split.punctuation_forced,
                "max_budget_splits": self.split.max_budget_splits,
            }
        )

    def public_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "paths": dict(self.paths),
            "model": {
                "seed": self.model.seed,
                "text_embedding_dim": self.model.text_embedding_dim,
                "frame_shift_ms": self.model.frame_shift_ms,
                "n_bands": self.model.n_bands,
                "hidden_dims": list(self.model.hidden_dims),
                "activation": self.model.activation,
                "prosody_dim": self.model.prosody_dim,
            },
            "split": self.split.__dict__,
            "budget": {
                "first_packet_ms": self.budget.first_packet_ms,
                "continuation_ms": self.budget.continuation_ms,
                "max_workers": self.budget.max_workers,
                "max_preprocess_depth": self.budget.max_preprocess_depth,
                "cost_units": self.budget.cost_units.__dict__,
            },
            "synthesis": self.synthesis.__dict__,
            "output": self.output.__dict__,
            "validation": self.validation.__dict__,
        }

    def version_fingerprint(self, weights_digest: str, phoneme_digest: str, calibration_digest: str) -> str:
        return sha256_json(
            {
                "config": self.public_dict(),
                "weights": weights_digest,
                "phoneme_table": phoneme_digest,
                "calibration": calibration_digest,
            }
        )


def _require(mapping: dict[str, Any], key: str, typ: type, label: str) -> Any:
    if key not in mapping:
        raise TTSError("E_INVALID_CONFIG", f"missing configuration key {label}.{key}")
    value = mapping[key]
    if typ is float and isinstance(value, int):
        value = float(value)
    if not isinstance(value, typ) or (typ is int and isinstance(value, bool)):
        raise TTSError(
            "E_INVALID_CONFIG",
            f"configuration key {label}.{key} has wrong type",
            {"expected": typ.__name__, "actual": type(value).__name__},
        )
    return value


def _validate_config(raw: dict[str, Any]) -> None:
    split = raw.get("split", {})
    if not isinstance(split, dict):
        raise TTSError("E_INVALID_CONFIG", "split must be a mapping")

    budget = raw.get("budget", {})
    for key in ("first_packet_ms", "continuation_ms"):
        value = budget.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or float(value) <= 0:
            raise TTSError("E_INVALID_CONFIG", f"budget.{key} must be a positive number")
    for key in ("max_workers", "max_preprocess_depth"):
        value = budget.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise TTSError("E_INVALID_CONFIG", f"budget.{key} must be a positive integer")
    units = budget.get("cost_units", {})
    for key in ("base", "per_visible_char", "per_frame", "per_complexity", "per_depth"):
        value = units.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or float(value) < 0:
            raise TTSError("E_INVALID_CONFIG", f"budget.cost_units.{key} must be a non-negative number")

    model = raw.get("model", {})
    if model.get("activation", "tanh") not in SUPPORTED_ACTIVATIONS:
        raise TTSError("E_INVALID_CONFIG", "model.activation must be 'tanh'")
    if not isinstance(model.get("hidden_dims"), list) or not model["hidden_dims"]:
        raise TTSError("E_INVALID_CONFIG", "model.hidden_dims must be a non-empty list")
    if any(not isinstance(x, int) or isinstance(x, bool) or x <= 0 for x in model["hidden_dims"]):
        raise TTSError("E_INVALID_CONFIG", "every model.hidden_dims entry must be a positive integer")
    for key in ("text_embedding_dim", "frame_shift_ms", "n_bands", "prosody_dim"):
        value = model.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise TTSError("E_INVALID_CONFIG", f"model.{key} must be a positive integer")
    if not isinstance(model.get("seed"), int) or isinstance(model.get("seed"), bool):
        raise TTSError("E_INVALID_CONFIG", "model.seed must be an integer")

    output = raw.get("output")
    if not isinstance(output, dict):
        raise TTSError("E_INVALID_CONFIG", "output must be a mapping")
    if output.get("bit_depth") != 16:
        raise TTSError("E_INVALID_CONFIG", "output.bit_depth must be 16")
    if output.get("channels") not in (1, 2):
        raise TTSError("E_INVALID_CONFIG", "output.channels must be 1 or 2")
    if not isinstance(output.get("sample_rate"), int) or output["sample_rate"] <= 0:
        raise TTSError("E_INVALID_CONFIG", "output.sample_rate must be a positive integer")

    synthesis = raw.get("synthesis", {})
    noise = synthesis.get("noise_level", 0.0)
    if not isinstance(noise, (int, float)) or not 0 <= float(noise) <= 1:
        raise TTSError("E_INVALID_CONFIG", "synthesis.noise_level must be in [0, 1]")
    rolloff = synthesis.get("harmonic_rolloff", 0.62)
    if not isinstance(rolloff, (int, float)) or not 0 < float(rolloff) <= 1:
        raise TTSError("E_INVALID_CONFIG", "synthesis.harmonic_rolloff must be in (0, 1]")
    if not isinstance(synthesis.get("phase_seed"), int):
        raise TTSError("E_INVALID_CONFIG", "synthesis.phase_seed must be an integer")


def load_config(path: str | Path, workspace: str | Path | None = None) -> EngineConfig:
    config_path = Path(path).resolve()
    workspace_path = Path(workspace).resolve() if workspace else config_path.parent.parent.resolve()
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise TTSError("E_INVALID_CONFIG", f"configuration file not found: {config_path}") from exc
    except yaml.YAMLError as exc:
        raise TTSError("E_INVALID_CONFIG", f"invalid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise TTSError("E_INVALID_CONFIG", "configuration root must be a mapping")
    _validate_config(raw)

    paths = raw.get("paths", {})
    required_paths = (
        "samples",
        "phoneme_table",
        "acoustic_weights",
        "calibration",
        "reference_dir",
    )
    for key in required_paths:
        if key not in paths or not isinstance(paths[key], str):
            raise TTSError("E_INVALID_CONFIG", f"paths.{key} must be a string")

    model_raw = raw["model"]
    output_raw = raw["output"]
    synth_raw = raw.get("synthesis", {})
    budget_raw = raw["budget"]
    units = budget_raw["cost_units"]
    split_raw = raw["split"]
    validation_raw = raw.get("validation", {})

    return EngineConfig(
        schema_version=int(raw.get("schema_version", 1)),
        workspace=workspace_path,
        config_path=config_path,
        paths=dict(paths),
        model=ModelConfig(
            seed=int(model_raw["seed"]),
            text_embedding_dim=int(model_raw["text_embedding_dim"]),
            frame_shift_ms=int(model_raw["frame_shift_ms"]),
            n_bands=int(model_raw["n_bands"]),
            hidden_dims=tuple(int(x) for x in model_raw["hidden_dims"]),
            activation=str(model_raw.get("activation", "tanh")),
            prosody_dim=int(model_raw["prosody_dim"]),
        ),
        split=SplitConfig(
            first_chunk_min_chars=split_raw.get("first_chunk_min_chars"),
            subsequent_chunk_chars=split_raw.get("subsequent_chunk_chars"),
            punctuation_forced=split_raw.get("punctuation_forced"),
            max_budget_splits=split_raw.get("max_budget_splits"),
        ),
        budget=BudgetConfig(
            first_packet_ms=float(budget_raw["first_packet_ms"]),
            continuation_ms=float(budget_raw["continuation_ms"]),
            max_workers=int(budget_raw["max_workers"]),
            max_preprocess_depth=int(budget_raw["max_preprocess_depth"]),
            cost_units=BudgetCostUnits(
                base=float(units["base"]),
                per_visible_char=float(units["per_visible_char"]),
                per_frame=float(units["per_frame"]),
                per_complexity=float(units["per_complexity"]),
                per_depth=float(units["per_depth"]),
            ),
        ),
        synthesis=SynthesisConfig(
            phase_seed=int(synth_raw.get("phase_seed", 911382323)),
            harmonic_rolloff=float(synth_raw.get("harmonic_rolloff", 0.62)),
            noise_level=float(synth_raw.get("noise_level", 0.012)),
        ),
        output=OutputConfig(
            sample_rate=int(output_raw["sample_rate"]),
            bit_depth=int(output_raw["bit_depth"]),
            channels=int(output_raw["channels"]),
        ),
        validation=ValidationConfig(
            mode=str(validation_raw.get("mode", "strict")),
            reference_tolerance=float(validation_raw.get("reference_tolerance", 0)),
        ),
        raw=raw,
    )


def config_snapshot_digest(raw: dict[str, Any]) -> str:
    return sha256_text(canonical_json(raw))
