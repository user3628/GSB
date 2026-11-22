from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .canonical import atomic_write_json, canonical_bytes, digest_json, read_json, round_float


DEFAULT_WEIGHTS = {
    "similarity": 0.50,
    "operator_confidence": 0.25,
    "language_consistency": 0.25,
}

DEFAULT_THRESHOLDS = {"consistent": 0.72, "weak": 0.45}

DEFAULT_CONFIDENCE = {"vision": 0.92, "caption": 0.78, "ocr": 0.64}


def deterministic_matrix(salt: str, seed: int, input_dim: int, output_dim: int) -> list[list[float]]:
    rows: list[list[float]] = []
    for row in range(output_dim):
        values: list[float] = []
        for column in range(input_dim):
            digest = hashlib.sha256(f"{salt}|matrix|{seed}|{row}|{column}".encode("utf-8")).digest()
            integer = int.from_bytes(digest[:8], "big")
            values.append((integer / float(1 << 64) - 0.5) / 8.0)
        rows.append(values)
    return rows


@dataclass(frozen=True)
class Config:
    path: Path
    raw: dict[str, Any]
    matrix: list[list[float]]
    digest: str

    @property
    def structure_version(self) -> str:
        return str(self.raw.get("structure_version", "1"))

    @property
    def text(self) -> dict[str, Any]:
        return dict(self.raw.get("text", {}))

    @property
    def projection(self) -> dict[str, Any]:
        return dict(self.raw.get("projection", {}))

    @property
    def fusion(self) -> dict[str, Any]:
        return dict(self.raw.get("fusion", {}))

    @property
    def aggregation(self) -> dict[str, Any]:
        return dict(self.raw.get("aggregation", {}))

    @property
    def runtime(self) -> dict[str, Any]:
        return dict(self.raw.get("runtime", {}))

    @property
    def embedding_dim(self) -> int:
        return int(self.text.get("embedding_dim", 64))

    @property
    def hash_salt(self) -> str:
        return str(self.text.get("hash_salt", "mmcoop-fixed-v1"))

    @property
    def max_chars(self) -> int:
        return int(self.text.get("max_chars", 256))

    @property
    def projection_input_dim(self) -> int:
        return int(self.projection.get("input_dim", 64))

    @property
    def projection_output_dim(self) -> int:
        return int(self.projection.get("output_dim", 64))

    @property
    def weights(self) -> dict[str, float]:
        source = self.fusion.get("weights", DEFAULT_WEIGHTS)
        return {key: float(source.get(key, DEFAULT_WEIGHTS[key])) for key in DEFAULT_WEIGHTS}

    @property
    def thresholds(self) -> dict[str, float]:
        source = self.fusion.get("thresholds", DEFAULT_THRESHOLDS)
        return {
            "consistent": float(source.get("consistent", DEFAULT_THRESHOLDS["consistent"])),
            "weak": float(source.get("weak", DEFAULT_THRESHOLDS["weak"])),
        }

    @property
    def operator_confidence(self) -> dict[str, float | None]:
        source = self.fusion.get("operator_confidence", DEFAULT_CONFIDENCE)
        result: dict[str, float | None] = {}
        for key, value in source.items():
            result[str(key)] = None if value is None else float(value)
        return result

    @property
    def language_config(self) -> dict[str, Any]:
        return dict(self.fusion.get("language", {}))

    @property
    def unit_delay_seconds(self) -> float:
        milliseconds = self.runtime.get("unit_delay_ms", 0)
        return max(0.0, float(milliseconds)) / 1000.0

    def component_config(self) -> dict[str, Any]:
        return {
            "structure_version": self.structure_version,
            "text": self.text,
            "projection": {
                "input_dim": self.projection_input_dim,
                "output_dim": self.projection_output_dim,
                "matrix": self.matrix,
            },
            "fusion": {
                "weights": self.weights,
                "thresholds": self.thresholds,
                "operator_confidence": self.operator_confidence,
                "language": self.language_config,
            },
            "aggregation": self.aggregation,
        }

    def public_summary(self) -> dict[str, Any]:
        return {
            "structure_version": self.structure_version,
            "config_version": self.digest,
            "embedding_dim": self.embedding_dim,
            "projection_input_dim": self.projection_input_dim,
            "projection_output_dim": self.projection_output_dim,
            "projection_matrix_sha256": hashlib.sha256(canonical_bytes(self.matrix)).hexdigest(),
            "weights": self.weights,
            "thresholds": self.thresholds,
            "operator_confidence": self.operator_confidence,
            "max_chars": self.max_chars,
        }


def _resolve_matrix(raw: dict[str, Any], path: Path) -> list[list[float]]:
    projection = dict(raw.get("projection", {}))
    if "matrix" in projection:
        matrix = projection["matrix"]
        return [[round_float(float(item), 12) for item in row] for row in matrix]

    matrix_file = projection.get("matrix_file")
    if matrix_file:
        candidate = Path(str(matrix_file))
        if not candidate.is_absolute():
            candidate = path.parent / candidate
        if candidate.exists():
            matrix = read_json(candidate)
            return [[round_float(float(item), 12) for item in row] for row in matrix]

    salt = str(projection.get("matrix_salt", "mmcoop-projection-v1"))
    seed = int(projection.get("matrix_seed", 20261123))
    input_dim = int(projection.get("input_dim", 64))
    output_dim = int(projection.get("output_dim", 64))
    return [
        [round_float(value, 12) for value in row]
        for row in deterministic_matrix(salt, seed, input_dim, output_dim)
    ]


def load_config(path: str | Path) -> Config:
    config_path = Path(path).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"config file not found: {config_path}")
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a mapping")
    matrix = _resolve_matrix(raw, config_path)
    digest_input = {"raw": raw, "projection_matrix": matrix}
    digest = digest_json(digest_input)
    return Config(path=config_path, raw=raw, matrix=matrix, digest=digest)


def write_projection_matrix(config_path: str | Path, destination: str | Path | None = None) -> Path:
    config = load_config(config_path)
    target = Path(destination) if destination is not None else config.path.parent / "projection_matrix.json"
    atomic_write_json(target, config.matrix, indent=2)
    return target


def validate_weights_for_error_detection(config: Config) -> None:
    # Weight validity is deliberately handled by the fusion stage so the stable
    # E_WEIGHT_ZERO diagnostic can include per-sample context.
    for name, value in config.weights.items():
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"invalid weight {name}: {value}")
