from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from .errors import invalid_config
from .util import apply_overrides, canonical_json_bytes, deep_merge, read_json, sha256_bytes


DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": "1.0",
    "seed": 20261120,
    "sample_manifest": "samples/manifest.json",
    "tiers": ["small", "medium", "large"],
    "preprocess": {
        "numeric_impute": "median",
        "categorical_impute": "most_frequent",
        "add_missing_indicators": True,
        "drop_exact_duplicate_features": False,
        "max_onehot_cardinality": 16,
    },
    "reduction": {
        "mode": "auto",
        "candidate": None,
        "target_dimension": None,
        "compute_budget": "balanced",
        "variance_target": 0.85,
        "reconstruction_error_limit": 0.30,
        "random_state": 20261120,
        "svd_n_iter": 7,
        "projection_eps": 0.20,
    },
    "clustering": {
        "algorithm": "auto",
        "n_clusters": None,
        "auto_k": True,
        "k_min": 2,
        "k_max": 6,
        "random_state": 20261120,
        "max_iter": 100,
        "tol": 0.0001,
        "empty_cluster_policy": "relocate_farthest",
    },
    "diagnostics": {
        "silhouette_floor": 0.10,
        "dominant_cluster_ratio": 0.72,
        "silhouette_sample_size": 3000,
        "confidence": 0.95,
    },
}


def load_config(
    path: Path | None = None,
    overrides: list[str] | None = None,
) -> dict[str, Any]:
    config = copy.deepcopy(DEFAULT_CONFIG)
    if path is not None:
        if not path.exists():
            raise invalid_config("Config file does not exist", {"path": str(path)})
        loaded = read_json(path)
        if not isinstance(loaded, dict):
            raise invalid_config("Config root must be an object")
        config = deep_merge(config, loaded)
    if overrides:
        config = apply_overrides(config, overrides)
    validate_config(config)
    return config


def validate_config(config: dict[str, Any]) -> None:
    if not isinstance(config, dict):
        raise invalid_config("Config must be an object")
    seed = config.get("seed")
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise invalid_config("seed must be an integer")
    tiers = config.get("tiers")
    if not isinstance(tiers, list) or not tiers or not all(isinstance(t, str) for t in tiers):
        raise invalid_config("tiers must be a non-empty string list")
    if len(set(tiers)) != len(tiers):
        raise invalid_config("tiers must not contain duplicates")

    pre = config.get("preprocess", {})
    if pre.get("numeric_impute") not in {"median", "mean", "zero"}:
        raise invalid_config("preprocess.numeric_impute must be median, mean, or zero")
    if pre.get("categorical_impute") not in {"most_frequent", "constant"}:
        raise invalid_config(
            "preprocess.categorical_impute must be most_frequent or constant"
        )
    if not isinstance(pre.get("add_missing_indicators"), bool):
        raise invalid_config("preprocess.add_missing_indicators must be boolean")
    if not isinstance(pre.get("drop_exact_duplicate_features"), bool):
        raise invalid_config("preprocess.drop_exact_duplicate_features must be boolean")
    max_card = pre.get("max_onehot_cardinality")
    if not isinstance(max_card, int) or max_card < 2:
        raise invalid_config("preprocess.max_onehot_cardinality must be an integer >= 2")

    red = config.get("reduction", {})
    if red.get("mode") not in {"auto", "manual"}:
        raise invalid_config("reduction.mode must be auto or manual")
    candidate = red.get("candidate")
    if candidate not in {None, "pca", "truncated_svd", "random_projection"}:
        raise invalid_config(
            "reduction.candidate must be pca, truncated_svd, random_projection, or null"
        )
    target = red.get("target_dimension")
    if target is not None:
        if not isinstance(target, int) or isinstance(target, bool) or target < 2:
            raise invalid_config("reduction.target_dimension must be an integer >= 2")
    if red.get("compute_budget") not in {"low", "balanced", "high"}:
        raise invalid_config("reduction.compute_budget must be low, balanced, or high")
    variance_target = red.get("variance_target")
    if not isinstance(variance_target, (int, float)) or not 0.0 < variance_target <= 1.0:
        raise invalid_config("reduction.variance_target must be in (0, 1]")
    recon_limit = red.get("reconstruction_error_limit")
    if not isinstance(recon_limit, (int, float)) or not 0.0 <= recon_limit <= 10.0:
        raise invalid_config("reduction.reconstruction_error_limit must be in [0, 10]")
    eps = red.get("projection_eps")
    if not isinstance(eps, (int, float)) or not 0.0 < eps <= 2.0:
        raise invalid_config("reduction.projection_eps must be in (0, 2]")
    svd_iter = red.get("svd_n_iter")
    if not isinstance(svd_iter, int) or svd_iter < 1:
        raise invalid_config("reduction.svd_n_iter must be an integer >= 1")
    if not isinstance(red.get("random_state"), int):
        raise invalid_config("reduction.random_state must be an integer")

    clu = config.get("clustering", {})
    if clu.get("algorithm") not in {"auto", "kmeans", "hierarchical"}:
        raise invalid_config("clustering.algorithm must be auto, kmeans, or hierarchical")
    n_clusters = clu.get("n_clusters")
    if n_clusters is not None:
        if (
            not isinstance(n_clusters, int)
            or isinstance(n_clusters, bool)
            or n_clusters < 2
        ):
            raise invalid_config("clustering.n_clusters must be an integer >= 2")
    if not isinstance(clu.get("auto_k"), bool):
        raise invalid_config("clustering.auto_k must be boolean")
    k_min = clu.get("k_min")
    k_max = clu.get("k_max")
    for name, value in (("k_min", k_min), ("k_max", k_max)):
        if not isinstance(value, int) or isinstance(value, bool) or value < 2:
            raise invalid_config(f"clustering.{name} must be an integer >= 2")
    if k_min > k_max:
        raise invalid_config("clustering.k_min must be <= clustering.k_max")
    max_iter = clu.get("max_iter")
    if not isinstance(max_iter, int) or max_iter < 1:
        raise invalid_config("clustering.max_iter must be an integer >= 1")
    tol = clu.get("tol")
    if not isinstance(tol, (int, float)) or tol < 0:
        raise invalid_config("clustering.tol must be >= 0")
    if clu.get("empty_cluster_policy") != "relocate_farthest":
        raise invalid_config(
            "Only the explicit empty_cluster_policy=relocate_farthest is supported"
        )
    if not isinstance(clu.get("random_state"), int):
        raise invalid_config("clustering.random_state must be an integer")

    diag = config.get("diagnostics", {})
    for name in ("silhouette_floor", "dominant_cluster_ratio"):
        value = diag.get(name)
        if not isinstance(value, (int, float)) or not 0.0 <= value <= 1.0:
            raise invalid_config(f"diagnostics.{name} must be in [0, 1]")
    sample_size = diag.get("silhouette_sample_size")
    if not isinstance(sample_size, int) or sample_size < 3:
        raise invalid_config("diagnostics.silhouette_sample_size must be an integer >= 3")
    confidence = diag.get("confidence")
    if not isinstance(confidence, (int, float)) or not 0.5 <= confidence < 1.0:
        raise invalid_config("diagnostics.confidence must be in [0.5, 1)")


def config_fingerprint(config: dict[str, Any]) -> str:
    return sha256_bytes(canonical_json_bytes(config))


def stage_config(config: dict[str, Any], stage: str) -> dict[str, Any]:
    common = {
        "schema_version": config["schema_version"],
        "seed": config["seed"],
    }
    if stage == "preprocess":
        return {**common, "preprocess": config["preprocess"]}
    if stage == "reduce":
        return {
            **common,
            "preprocess": config["preprocess"],
            "reduction": config["reduction"],
        }
    if stage == "cluster":
        return {
            **common,
            "reduction": {
                "random_state": config["reduction"]["random_state"],
                "projection_eps": config["reduction"]["projection_eps"],
            },
            "clustering": config["clustering"],
        }
    if stage in {"diagnose", "report"}:
        return {
            **common,
            "diagnostics": config["diagnostics"],
            "clustering": config["clustering"],
        }
    raise invalid_config(f"Unknown stage: {stage}")


def stage_config_fingerprint(config: dict[str, Any], stage: str) -> str:
    return sha256_bytes(canonical_json_bytes(stage_config(config, stage)))


def resolve_manifest_path(config: dict[str, Any], root: Path) -> Path:
    path = Path(config["sample_manifest"])
    if not path.is_absolute():
        path = root / path
    return path.resolve()
