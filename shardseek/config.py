"""Configuration loading, validation, deterministic matrices, and fingerprints."""

from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any

import numpy as np

from .errors import BusinessError, E_DIM_MISMATCH, E_INVALID_CONFIG, E_NUMERIC, error
from .util import digest_obj, read_json, write_json_atomic


CONFIG_FORMAT = "shardseek-config-v1"
VISUAL_RAW_DIM = 56
TEXT_RAW_DIM = 128
RETRIEVAL_DIM = 64

BASE_FEATURE_CONFIG: dict[str, Any] = {
    "grid_rows": 2,
    "grid_cols": 2,
    "color_channels": 3,
    "orientation_bins": 8,
    "text_bow_dim": 64,
    "text_ngram_dim": 64,
    "char_ngram_sizes": [2, 3],
    "normalization": "NFKC_whitespace_lower",
}

BASE_RANKING_CONFIG: dict[str, Any] = {
    "dense_weight": 0.72,
    "inverted_weight": 0.28,
    "visual_token_count": 8,
    "tie_break": "item_id_ascending",
}


def visual_raw_dim(feature_config: dict[str, Any]) -> int:
    return int(
        feature_config["grid_rows"]
        * feature_config["grid_cols"]
        * (6 + int(feature_config["orientation_bins"]))
    )


def text_raw_dim(feature_config: dict[str, Any]) -> int:
    return int(feature_config["text_bow_dim"] + feature_config["text_ngram_dim"])


def _matrix(seed: int, rows: int, cols: int) -> list[list[float]]:
    rng = np.random.default_rng(seed)
    scale = 1.0 / math.sqrt(rows)
    matrix = rng.normal(0.0, scale, size=(rows, cols))
    # Round before serialization so loading a config and re-generating it are byte stable.
    return [[round(float(v), 12) for v in row] for row in matrix.astype(np.float64)]


def build_config(
    *,
    name: str,
    bits: int,
    granularity: str = "per_dim",
    seed: int = 20261122,
    dense_weight: float = 0.72,
    inverted_weight: float = 0.28,
    visual_projection: list[list[float]] | None = None,
    text_projection: list[list[float]] | None = None,
) -> dict[str, Any]:
    feature = copy.deepcopy(BASE_FEATURE_CONFIG)
    ranking = copy.deepcopy(BASE_RANKING_CONFIG)
    ranking["dense_weight"] = dense_weight
    ranking["inverted_weight"] = inverted_weight
    return {
        "format": CONFIG_FORMAT,
        "name": name,
        "feature": feature,
        "projection": {
            "retrieval_dim": RETRIEVAL_DIM,
            "visual": visual_projection
            if visual_projection is not None
            else _matrix(seed + 101, visual_raw_dim(feature), RETRIEVAL_DIM),
            "text": text_projection
            if text_projection is not None
            else _matrix(seed + 202, text_raw_dim(feature), RETRIEVAL_DIM),
        },
        "quantization": {
            "bits": int(bits),
            "granularity": granularity,
            "rounding": "nearest_even",
            "clamp": True,
        },
        "ranking": ranking,
        "index": {
            "visual_token_count": 8,
            "text_analyzer": "nfkc_whitespace_lower",
        },
    }


def validate_config(config: dict[str, Any]) -> None:
    try:
        if config.get("format") != CONFIG_FORMAT:
            raise error(
                E_INVALID_CONFIG,
                "Unsupported configuration format",
                stage="config",
                details={"expected": CONFIG_FORMAT, "actual": config.get("format")},
            )
        feature = config["feature"]
        rows = int(feature["grid_rows"])
        cols = int(feature["grid_cols"])
        if rows <= 0 or cols <= 0:
            raise error(
                E_INVALID_CONFIG,
                "Grid dimensions must be positive",
                stage="config",
                details={"grid_rows": rows, "grid_cols": cols},
            )
        if int(feature["orientation_bins"]) <= 0:
            raise error(
                E_INVALID_CONFIG,
                "orientation_bins must be positive",
                stage="config",
                details={"orientation_bins": feature["orientation_bins"]},
            )
        expected_visual = visual_raw_dim(feature)
        expected_text = text_raw_dim(feature)
        projection = config["projection"]
        if int(projection["retrieval_dim"]) != RETRIEVAL_DIM:
            raise error(
                E_DIM_MISMATCH,
                "Retrieval dimension must be 64",
                stage="config",
                details={
                    "dimension": "retrieval",
                    "expected": RETRIEVAL_DIM,
                    "actual": projection["retrieval_dim"],
                },
            )
        for route, expected in (("visual", expected_visual), ("text", expected_text)):
            matrix = np.asarray(projection[route], dtype=np.float64)
            if matrix.ndim != 2 or matrix.shape != (expected, RETRIEVAL_DIM):
                raise error(
                    E_DIM_MISMATCH,
                    "Projection matrix shape does not match raw feature dimension",
                    stage="projection",
                    details={
                        "route": route,
                        "expected_shape": [expected, RETRIEVAL_DIM],
                        "actual_shape": list(matrix.shape),
                    },
                )
            non_finite = np.argwhere(~np.isfinite(matrix))
            if non_finite.size:
                position = non_finite[0].tolist()
                raise error(
                    E_NUMERIC,
                    "Projection matrix contains NaN or Inf",
                    stage="projection",
                    details={"route": route, "dimension_position": position, "value": str(matrix[tuple(position)])},
                )
        bits = int(config["quantization"]["bits"])
        if bits not in (4, 8):
            raise error(
                E_INVALID_CONFIG,
                "Only 4-bit and 8-bit fixed-point quantization are supported",
                stage="config",
                details={"bits": bits},
            )
        if config["quantization"]["granularity"] not in ("per_dim", "per_vector"):
            raise error(
                E_INVALID_CONFIG,
                "Unknown quantization granularity",
                stage="config",
                details={"granularity": config["quantization"]["granularity"]},
            )
        ranking = config["ranking"]
        for key in ("dense_weight", "inverted_weight"):
            value = float(ranking[key])
            if not math.isfinite(value) or value < 0:
                raise error(
                    E_NUMERIC,
                    "Ranking weight must be finite and non-negative",
                    stage="index",
                    details={"field": key, "value": str(ranking[key])},
                )
        if float(ranking["dense_weight"]) + float(ranking["inverted_weight"]) <= 0:
            raise error(
                E_INVALID_CONFIG,
                "At least one ranking weight must be positive",
                stage="config",
                details={"ranking": ranking},
            )
    except KeyError as exc:
        raise error(
            E_INVALID_CONFIG,
            "Missing configuration field",
            stage="config",
            details={"field": str(exc)},
        ) from exc


def config_fingerprint(config: dict[str, Any]) -> str:
    return digest_obj(config)


def feature_fingerprint(config: dict[str, Any]) -> str:
    return digest_obj({"feature": config["feature"]})


def projection_quantization_fingerprint(config: dict[str, Any]) -> str:
    return digest_obj(
        {
            "feature_dimensions": {
                "visual": visual_raw_dim(config["feature"]),
                "text": text_raw_dim(config["feature"]),
            },
            "projection": config["projection"],
            "quantization": config["quantization"],
        }
    )


def index_fingerprint(config: dict[str, Any]) -> str:
    return digest_obj({"ranking": config["ranking"], "index": config["index"]})


def quantization_fingerprint(config: dict[str, Any]) -> str:
    return digest_obj(
        {
            "quantization": config["quantization"],
            "projection": config["projection"],
        }
    )


def config_resolution_order(name_or_path: str, root: Path) -> list[Path]:
    candidate = Path(name_or_path)
    return [
        candidate,
        root / "configs" / f"{name_or_path}.json",
        root / "configs" / name_or_path,
        Path.cwd() / "configs" / f"{name_or_path}.json",
    ]


def load_config(name_or_path: str | Path, root: str | Path = ".") -> dict[str, Any]:
    path: Path | None = None
    for candidate in config_resolution_order(str(name_or_path), Path(root)):
        if candidate.is_file():
            path = candidate
            break
    if path is None:
        raise error(
            E_INVALID_CONFIG,
            "Configuration file not found",
            stage="config",
            details={"requested": str(name_or_path)},
        )
    try:
        config = read_json(path)
    except Exception as exc:
        raise error(
            E_INVALID_CONFIG,
            "Configuration is not valid JSON",
            stage="config",
            details={"path": str(path), "reason": type(exc).__name__},
        ) from exc
    validate_config(config)
    return config


def _inverse_sqrt(matrix: np.ndarray) -> np.ndarray:
    eigenvalues, eigenvectors = np.linalg.eigh(matrix)
    eigenvalues = np.maximum(eigenvalues, 1e-9)
    return (eigenvectors * (1.0 / np.sqrt(eigenvalues))) @ eigenvectors.T


def calibrated_projection_matrices(
    visual_vectors: np.ndarray,
    text_vectors: np.ndarray,
    *,
    regularization: float = 1e-2,
    seed: int = 20261122,
) -> tuple[list[list[float]], list[list[float]]]:
    """Deterministic regularized CCA alignment for the local paired corpus.

    This is linear algebraic calibration, not a learned vision model. The first
    six latent directions maximize paired visual/text correlation; the remaining
    dimensions receive tiny deterministic projections so every dimension has a
    non-zero scale and remains measurable.
    """
    visual = np.asarray(visual_vectors, dtype=np.float64)
    text = np.asarray(text_vectors, dtype=np.float64)
    if visual.ndim != 2 or text.ndim != 2 or visual.shape[0] != text.shape[0]:
        raise error(
            E_DIM_MISMATCH,
            "Calibration matrices require paired feature rows",
            stage="config",
            details={"visual_shape": list(visual.shape), "text_shape": list(text.shape)},
        )
    visual_norm = np.linalg.norm(visual, axis=1, keepdims=True)
    text_norm = np.linalg.norm(text, axis=1, keepdims=True)
    if np.any(visual_norm == 0) or np.any(text_norm == 0):
        raise error(E_NUMERIC, "Calibration feature row has zero norm", stage="config")
    visual_unit = visual / visual_norm
    text_unit = text / text_norm
    visual_dim = visual.shape[1]
    text_dim = text.shape[1]
    covariance_vv = visual_unit.T @ visual_unit + regularization * np.eye(visual_dim)
    covariance_tt = text_unit.T @ text_unit + regularization * np.eye(text_dim)
    inv_vv = _inverse_sqrt(covariance_vv)
    inv_tt = _inverse_sqrt(covariance_tt)
    cross = visual_unit.T @ text_unit
    kernel = inv_vv @ cross @ inv_tt
    left, singular_values, right_t = np.linalg.svd(kernel, full_matrices=False)
    rank = min(int(np.sum(singular_values > 1e-10)), len(visual), RETRIEVAL_DIM)
    if rank == 0:
        raise error(E_NUMERIC, "CCA calibration produced no usable components", stage="config")
    visual_weights = inv_vv @ left[:, :rank]
    text_weights = inv_tt @ right_t[:rank, :].T
    # Canonicalize SVD sign ambiguity without changing projected similarity.
    for column in range(rank):
        v_anchor = int(np.argmax(np.abs(visual_weights[:, column])))
        if visual_weights[v_anchor, column] < 0:
            visual_weights[:, column] *= -1
            text_weights[:, column] *= -1
    rng = np.random.default_rng(seed + 707)
    remaining = RETRIEVAL_DIM - rank
    visual_padding = rng.normal(0.0, 1e-4 / math.sqrt(visual_dim), size=(visual_dim, remaining))
    text_padding = rng.normal(0.0, 1e-4 / math.sqrt(text_dim), size=(text_dim, remaining))
    visual_matrix = np.concatenate([visual_weights, visual_padding], axis=1)
    text_matrix = np.concatenate([text_weights, text_padding], axis=1)
    return (
        [[round(float(value), 12) for value in row] for row in visual_matrix],
        [[round(float(value), 12) for value in row] for row in text_matrix],
    )


def write_configs(
    config_dir: str | Path,
    *,
    calibration: tuple[list[list[float]], list[list[float]]] | None = None,
) -> dict[str, str]:
    target = Path(config_dir)
    visual_projection, text_projection = calibration if calibration else (None, None)
    configs = {
        "baseline_8bit.json": build_config(
            name="baseline-8bit-per-dim",
            bits=8,
            visual_projection=visual_projection,
            text_projection=text_projection,
        ),
        "compact_4bit.json": build_config(
            name="compact-4bit-per-dim",
            bits=4,
            visual_projection=visual_projection,
            text_projection=text_projection,
        ),
        "projection_alt_8bit.json": build_config(
            name="projection-alt-8bit-per-dim", bits=8, seed=20261123
        ),
    }
    hashes: dict[str, str] = {}
    for filename, config in configs.items():
        validate_config(config)
        payload = write_json_atomic(target / filename, config, pretty=True)
        hashes[filename] = digest_obj(config)
    return hashes

