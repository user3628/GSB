"""Validated, deterministic configuration objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .canonical import digest_obj
from .errors import CMIRError, E_INVALID_BITWIDTH


SCHEMA_VERSION = 1
FEATURE_CONFIG_VERSION = "cmir.feature-config.v1"
PROJECTION_CONFIG_VERSION = "cmir.projection-config.v1"
QUANT_CONFIG_VERSION = "cmir.quant-config.v1"
RANKING_CONFIG_VERSION = "cmir.ranking-config.v1"
RECALL_CONFIG_VERSION = "cmir.recall-config.v1"


def _as_int(value: Any, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CMIRError("E_CONFIG_INVALID", f"{name} must be an integer", field=name)
    if value < minimum:
        raise CMIRError(
            "E_CONFIG_INVALID", f"{name} must be >= {minimum}", field=name, value=value
        )
    return value


def _as_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise CMIRError("E_CONFIG_INVALID", f"{name} must be numeric", field=name)
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise CMIRError(
            "E_CONFIG_INVALID", f"{name} must be numeric", field=name
        ) from exc


@dataclass(frozen=True)
class FeatureConfig:
    grid_rows: int = 4
    grid_cols: int = 4
    grad_bins: int = 8
    text_hash_dim: int = 512
    char_ngram_min: int = 2
    char_ngram_max: int = 4
    word_weight: float = 1.0
    char_weight: float = 0.35
    normalization: str = "NFKC+lower+collapse"
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in ("grid_rows", "grid_cols", "grad_bins", "text_hash_dim"):
            _as_int(getattr(self, name), name, 1)
        _as_int(self.char_ngram_min, "char_ngram_min", 1)
        _as_int(self.char_ngram_max, "char_ngram_max", 1)
        if self.char_ngram_max < self.char_ngram_min:
            raise CMIRError(
                "E_CONFIG_INVALID",
                "char_ngram_max must be >= char_ngram_min",
                field="char_ngram_max",
            )
        if not _as_float(self.word_weight, "word_weight") > 0:
            raise CMIRError("E_CONFIG_INVALID", "word_weight must be > 0")
        if not _as_float(self.char_weight, "char_weight") > 0:
            raise CMIRError("E_CONFIG_INVALID", "char_weight must be > 0")

    @property
    def visual_dim(self) -> int:
        return self.grid_rows * self.grid_cols * (3 + 3 + self.grad_bins)

    @property
    def text_dim(self) -> int:
        return self.text_hash_dim

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "grid_rows": self.grid_rows,
            "grid_cols": self.grid_cols,
            "grad_bins": self.grad_bins,
            "text_hash_dim": self.text_hash_dim,
            "char_ngram_min": self.char_ngram_min,
            "char_ngram_max": self.char_ngram_max,
            "word_weight": self.word_weight,
            "char_weight": self.char_weight,
            "normalization": self.normalization,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "FeatureConfig":
        return cls(**{key: value[key] for key in cls.__dataclass_fields__ if key in value})

    def digest(self) -> str:
        return digest_obj({"version": FEATURE_CONFIG_VERSION, **self.to_dict()})


@dataclass(frozen=True)
class ProjectionConfig:
    seed: int = 20261122
    alignment_dim: int = 64
    visual_matrix: list[list[float]] | None = None
    text_matrix: list[list[float]] | None = None
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        _as_int(self.seed, "seed", 0)
        _as_int(self.alignment_dim, "alignment_dim", 1)
        if self.alignment_dim != 64:
            raise CMIRError(
                "E_PROJECTION_INVALID",
                "alignment_dim must be exactly 64",
                item_id="__global__",
                dimensions={"alignment_dim": self.alignment_dim, "required": 64},
            )
        for name in ("visual_matrix", "text_matrix"):
            matrix = getattr(self, name)
            if matrix is None:
                continue
            if not isinstance(matrix, list) or any(not isinstance(row, list) for row in matrix):
                raise CMIRError(
                    "E_PROJECTION_INVALID",
                    f"{name} must be a 2-D numeric array",
                    item_id="__global__",
                    dimensions={"matrix": name},
                )
            if any(len(row) != len(matrix[0]) for row in matrix):
                raise CMIRError(
                    "E_PROJECTION_INVALID",
                    f"{name} rows have inconsistent dimensions",
                    item_id="__global__",
                    dimensions={
                        "rows": len(matrix),
                        "row_widths": sorted({len(row) for row in matrix}),
                    },
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "seed": self.seed,
            "alignment_dim": self.alignment_dim,
            "visual_matrix": self.visual_matrix,
            "text_matrix": self.text_matrix,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ProjectionConfig":
        return cls(**{key: value[key] for key in cls.__dataclass_fields__ if key in value})

    def digest(self, feature_config: FeatureConfig) -> str:
        return digest_obj(
            {
                "version": PROJECTION_CONFIG_VERSION,
                **self.to_dict(),
                "visual_input_dim": feature_config.visual_dim,
                "text_input_dim": feature_config.text_dim,
            }
        )


@dataclass(frozen=True)
class QuantizationConfig:
    bits: int = 8
    granularity: str = "per_dim"
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if isinstance(self.bits, bool) or self.bits not in (4, 8):
            raise CMIRError(
                E_INVALID_BITWIDTH,
                "quantization bits must be 4 or 8",
                item_id="__global__",
                bitwidth=self.bits,
                allowed=[4, 8],
                dimensions={"bitwidth": self.bits, "alignment_dim": 64},
                stage="quantization_config",
            )
        if self.granularity not in ("per_dim", "per_vector"):
            raise CMIRError(
                "E_CONFIG_INVALID",
                "quantization granularity must be per_dim or per_vector",
                field="granularity",
                value=self.granularity,
            )

    @property
    def levels(self) -> int:
        return (1 << self.bits) - 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "bits": self.bits,
            "granularity": self.granularity,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "QuantizationConfig":
        return cls(**{key: value[key] for key in cls.__dataclass_fields__ if key in value})

    def digest(self) -> str:
        return digest_obj({"version": QUANT_CONFIG_VERSION, **self.to_dict()})


@dataclass(frozen=True)
class RankingConfig:
    cosine_weight: float = 1.0
    inverted_hit_weight: float = 0.12
    t2i_visual_weight: float = 0.68
    t2i_text_weight: float = 0.32
    i2t_visual_weight: float = 0.32
    i2t_text_weight: float = 0.68
    index_dims: int = 10
    default_k: int = 5
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in (
            "cosine_weight",
            "inverted_hit_weight",
            "t2i_visual_weight",
            "t2i_text_weight",
            "i2t_visual_weight",
            "i2t_text_weight",
        ):
            _as_float(getattr(self, name), name)
        _as_int(self.index_dims, "index_dims", 1)
        _as_int(self.default_k, "default_k", 1)

    def route_weights(self, mode: str) -> tuple[float, float]:
        if mode == "text2image":
            return self.t2i_visual_weight, self.t2i_text_weight
        return self.i2t_visual_weight, self.i2t_text_weight

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "cosine_weight": self.cosine_weight,
            "inverted_hit_weight": self.inverted_hit_weight,
            "t2i_visual_weight": self.t2i_visual_weight,
            "t2i_text_weight": self.t2i_text_weight,
            "i2t_visual_weight": self.i2t_visual_weight,
            "i2t_text_weight": self.i2t_text_weight,
            "index_dims": self.index_dims,
            "default_k": self.default_k,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RankingConfig":
        return cls(**{key: value[key] for key in cls.__dataclass_fields__ if key in value})

    def digest(self) -> str:
        return digest_obj({"version": RANKING_CONFIG_VERSION, **self.to_dict()})


@dataclass(frozen=True)
class RecallConfig:
    max_error_bound: float = 0.35
    probe_count: int = 3
    probe_k: int = 3
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not _as_float(self.max_error_bound, "max_error_bound") > 0:
            raise CMIRError("E_CONFIG_INVALID", "max_error_bound must be > 0")
        _as_int(self.probe_count, "probe_count", 1)
        _as_int(self.probe_k, "probe_k", 1)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "max_error_bound": self.max_error_bound,
            "probe_count": self.probe_count,
            "probe_k": self.probe_k,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RecallConfig":
        return cls(**{key: value[key] for key in cls.__dataclass_fields__ if key in value})

    def digest(self) -> str:
        return digest_obj({"version": RECALL_CONFIG_VERSION, **self.to_dict()})


@dataclass(frozen=True)
class AlignmentConfig:
    feature: FeatureConfig = field(default_factory=FeatureConfig)
    projection: ProjectionConfig = field(default_factory=ProjectionConfig)
    quantization: QuantizationConfig = field(default_factory=QuantizationConfig)
    ranking: RankingConfig = field(default_factory=RankingConfig)
    recall: RecallConfig = field(default_factory=RecallConfig)
    schema_version: int = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "feature": self.feature.to_dict(),
            "projection": self.projection.to_dict(),
            "quantization": self.quantization.to_dict(),
            "ranking": self.ranking.to_dict(),
            "recall": self.recall.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "AlignmentConfig":
        return cls(
            feature=FeatureConfig.from_dict(value.get("feature", {})),
            projection=ProjectionConfig.from_dict(value.get("projection", {})),
            quantization=QuantizationConfig.from_dict(value.get("quantization", {})),
            ranking=RankingConfig.from_dict(value.get("ranking", {})),
            recall=RecallConfig.from_dict(value.get("recall", {})),
            schema_version=int(value.get("schema_version", SCHEMA_VERSION)),
        )

    @property
    def feature_digest(self) -> str:
        return self.feature.digest()

    @property
    def projection_digest(self) -> str:
        return self.projection.digest(self.feature)

    @property
    def quantization_digest(self) -> str:
        return self.quantization.digest()

    @property
    def ranking_digest(self) -> str:
        return self.ranking.digest()

    @property
    def recall_digest(self) -> str:
        return self.recall.digest()

    @property
    def digest(self) -> str:
        return digest_obj(self.to_dict())

    def stage_digests(self) -> dict[str, str]:
        return {
            "feature": self.feature_digest,
            "projection": self.projection_digest,
            "quantization": self.quantization_digest,
            "index": self.ranking_digest,
            "recall": self.recall_digest,
        }


def default_config(bits: int = 8, granularity: str = "per_dim") -> AlignmentConfig:
    return AlignmentConfig(
        quantization=QuantizationConfig(bits=bits, granularity=granularity)
    )


def merge_config_patch(current: AlignmentConfig, patch: dict[str, Any]) -> AlignmentConfig:
    merged = current.to_dict()
    for key, value in patch.items():
        if key in ("feature", "projection", "quantization", "ranking", "recall"):
            if not isinstance(value, dict):
                raise CMIRError(
                    "E_CONFIG_INVALID", f"{key} must be an object", field=key
                )
            merged[key].update(value)
        elif key == "schema_version":
            merged[key] = value
        elif key == "bits" or key == "granularity":
            merged["quantization"][key] = value
        else:
            raise CMIRError(
                "E_CONFIG_INVALID", f"unknown configuration field: {key}", field=key
            )
    return AlignmentConfig.from_dict(merged)
