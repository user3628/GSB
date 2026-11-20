"""JSON config: defaults, validation, monotonic versioning, section hashes."""

from __future__ import annotations

import copy
import hashlib
import numbers

from .errors import ConfigError
from .util import canonical_json

DEFAULT_CONFIG = {
    "label_column": "label",
    "inference": {"numeric_ratio_threshold": 0.9},
    "missing": {
        "strategy_numeric": "mean",
        "strategy_categorical": "constant",
        "constant": "__MISSING__",
        "numeric_constant": 0.0,
    },
    "encoding": {
        "categorical_default": "onehot",
        "per_column": {"city": "hash", "loyalty": "target"},
        "onehot": {"max_categories": 64},
        "hash": {"n_buckets": 8},
        "target": {"n_folds": 5, "smoothing": 10.0, "seed": 20261120},
    },
    "selection": {
        "method": "mutual_info",
        "variance": {"threshold": 0.0001},
        "mutual_info": {"threshold": 0.0005, "top_k": 40, "n_bins": 8},
        "rfe": {"n_features_to_select": 15, "step": 1, "ridge_lambda": 1.0},
    },
    "runtime": {"latency_budget_ms": 10000},
}

SECTIONS = ("label_column", "inference", "missing", "encoding", "selection", "runtime")

STAGE_SECTIONS = {
    "infer": ("label_column", "inference"),
    "encode": ("label_column", "inference", "missing", "encoding"),
    "select": ("label_column", "inference", "missing", "encoding", "selection"),
    "summarize": ("label_column", "inference", "missing", "encoding", "selection"),
}

_ENCODERS = ("onehot", "target", "hash")
_SELECTORS = ("variance", "mutual_info", "rfe")

_ALLOWED_NESTED = {
    "inference": {"numeric_ratio_threshold"},
    "missing": {"strategy_numeric", "strategy_categorical", "constant", "numeric_constant"},
    "encoding": {"categorical_default", "per_column", "onehot", "hash", "target"},
    "selection": {"method", "variance", "mutual_info", "rfe"},
    "runtime": {"latency_budget_ms"},
}

_ALLOWED_SUB = {
    "encoding.onehot": {"max_categories"},
    "encoding.hash": {"n_buckets"},
    "encoding.target": {"n_folds", "smoothing", "seed"},
    "selection.variance": {"threshold"},
    "selection.mutual_info": {"threshold", "top_k", "n_bins"},
    "selection.rfe": {"n_features_to_select", "step", "ridge_lambda"},
}


def _is_number(v) -> bool:
    return isinstance(v, numbers.Real) and not isinstance(v, bool)


def _require(cond, message, details=None):
    if not cond:
        raise ConfigError(message, details or {})


def validate_config(cfg: dict) -> None:
    _require(isinstance(cfg, dict), "config must be a JSON object")
    unknown = sorted(set(cfg) - set(SECTIONS))
    _require(not unknown, "unknown top-level config sections", {"unknown": unknown})
    for section in SECTIONS:
        _require(section in cfg, "missing config section", {"section": section})
    _require(isinstance(cfg["label_column"], str) and cfg["label_column"],
             "label_column must be a non-empty string")
    for section, allowed in _ALLOWED_NESTED.items():
        body = cfg[section]
        _require(isinstance(body, dict), "config section must be an object", {"section": section})
        unknown = sorted(set(body) - allowed)
        _require(not unknown, "unknown keys in config section",
                 {"section": section, "unknown": unknown})
    for path, allowed in _ALLOWED_SUB.items():
        body = cfg
        for part in path.split("."):
            body = body[part]
        _require(isinstance(body, dict), "config subsection must be an object", {"path": path})
        unknown = sorted(set(body) - allowed)
        _require(not unknown, "unknown keys in config subsection",
                 {"path": path, "unknown": unknown})

    thr = cfg["inference"]["numeric_ratio_threshold"]
    _require(_is_number(thr) and 0.0 <= thr <= 1.0, "numeric_ratio_threshold must be in [0, 1]")

    missing = cfg["missing"]
    _require(missing["strategy_numeric"] in ("mean", "median", "constant"),
             "missing.strategy_numeric must be mean|median|constant")
    _require(missing["strategy_categorical"] in ("constant", "mode"),
             "missing.strategy_categorical must be constant|mode")
    _require(isinstance(missing["constant"], str) and missing["constant"],
             "missing.constant must be a non-empty string")
    _require(_is_number(missing["numeric_constant"]), "missing.numeric_constant must be numeric")

    enc = cfg["encoding"]
    _require(enc["categorical_default"] in _ENCODERS,
             "encoding.categorical_default must be onehot|target|hash")
    _require(isinstance(enc["per_column"], dict), "encoding.per_column must be an object")
    for col, encoder in enc["per_column"].items():
        _require(isinstance(col, str) and col, "encoding.per_column keys must be column names")
        _require(encoder in _ENCODERS, "unknown encoder for column",
                 {"column": col, "encoder": encoder})
    _require(isinstance(enc["onehot"]["max_categories"], int)
             and enc["onehot"]["max_categories"] >= 1, "onehot.max_categories must be >= 1")
    _require(isinstance(enc["hash"]["n_buckets"], int) and enc["hash"]["n_buckets"] >= 1,
             "hash.n_buckets must be an integer >= 1")
    _require(isinstance(enc["target"]["n_folds"], int) and enc["target"]["n_folds"] >= 2,
             "target.n_folds must be an integer >= 2")
    _require(_is_number(enc["target"]["smoothing"]) and enc["target"]["smoothing"] >= 0.0,
             "target.smoothing must be >= 0")
    _require(isinstance(enc["target"]["seed"], int), "target.seed must be an integer")

    sel = cfg["selection"]
    _require(sel["method"] in _SELECTORS, "selection.method must be variance|mutual_info|rfe")
    _require(_is_number(sel["variance"]["threshold"]) and sel["variance"]["threshold"] >= 0.0,
             "variance.threshold must be >= 0")
    mi = sel["mutual_info"]
    _require(_is_number(mi["threshold"]) and mi["threshold"] >= 0.0, "mi threshold must be >= 0")
    _require(isinstance(mi["top_k"], int) and mi["top_k"] >= 1, "mutual_info.top_k must be >= 1")
    _require(isinstance(mi["n_bins"], int) and mi["n_bins"] >= 2, "mutual_info.n_bins must be >= 2")
    rfe = sel["rfe"]
    _require(isinstance(rfe["n_features_to_select"], int) and rfe["n_features_to_select"] >= 1,
             "rfe.n_features_to_select must be >= 1")
    _require(isinstance(rfe["step"], int) and rfe["step"] >= 1, "rfe.step must be >= 1")
    _require(_is_number(rfe["ridge_lambda"]) and rfe["ridge_lambda"] > 0.0,
             "rfe.ridge_lambda must be > 0")

    budget = cfg["runtime"]["latency_budget_ms"]
    _require(budget is None or (_is_number(budget) and budget > 0),
             "runtime.latency_budget_ms must be positive or null")


def default_config_store() -> dict:
    return {
        "version": 1,
        "section_versions": {s: 1 for s in SECTIONS},
        "config": copy.deepcopy(DEFAULT_CONFIG),
    }


def bump_config(store: dict, new_config: dict) -> dict:
    """Return a new store with bumped version; per-section versions advance
    only for sections whose canonical content actually changed."""
    validate_config(new_config)
    new_version = store["version"] + 1
    section_versions = dict(store["section_versions"])
    for section in SECTIONS:
        if canonical_json(store["config"][section]) != canonical_json(new_config[section]):
            section_versions[section] = new_version
    return {"version": new_version, "section_versions": section_versions, "config": new_config}


def stage_sections_hash(config: dict, stage: str) -> str:
    """Fingerprint of exactly the config sections a stage depends on."""
    h = hashlib.sha256()
    for section in STAGE_SECTIONS[stage]:
        h.update(section.encode("utf-8"))
        h.update(b"\0")
        h.update(canonical_json(config[section]))
        h.update(b"\0")
    return h.hexdigest()


def effective_version(store: dict, stage: str) -> int:
    """Last config version that touched any section this stage depends on."""
    return max(store["section_versions"][s] for s in STAGE_SECTIONS[stage])
