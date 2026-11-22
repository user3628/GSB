"""Config loading, validation and stage-scoped version fingerprints.

The config version is a hash of the canonical JSON.  Each pipeline stage
additionally gets a *scoped* digest over only the config sections that can
influence it, which is what allows the incremental scheduler to find the
earliest affected stage when the config changes.
"""
from __future__ import annotations

import json
import os

from .errors import E_INVALID_CONFIG, PipelineError
from .fingerprint import hash_obj

STAGE_ORDER = ["infer", "cross", "reduce", "summary"]

# Config sections that directly influence each stage (used for scoped
# digests).  Indirect influence propagates through the upstream chain in
# the stage fingerprint, so scopes stay minimal: only genuinely affected
# stages are rebuilt when the config changes.
STAGE_CONFIG_SCOPES: dict[str, tuple[str, ...]] = {
    "infer": ("inference",),
    "cross": ("crossing",),
    "reduce": ("reduction", "quantization"),
    "summary": (),
}

_VALID_METHODS = {"pca", "truncated_svd", "random_projection"}


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    validate_config(cfg)
    return cfg


def validate_config(cfg: dict) -> None:
    problems: dict[str, str] = {}
    crossing = cfg.get("crossing", {})
    max_order = crossing.get("max_order")
    if not isinstance(max_order, int) or max_order < 1:
        problems["crossing.max_order"] = (
            f"combination order cap must be a positive integer, got {max_order!r}"
        )
    max_crosses = crossing.get("max_crosses")
    if not isinstance(max_crosses, int) or max_crosses < 1:
        problems["crossing.max_crosses"] = (
            f"cross combination cap must be a positive integer, got {max_crosses!r}"
        )
    candidates = cfg.get("reduction", {}).get("candidates", [])
    if not candidates:
        problems["reduction.candidates"] = "at least one reduction candidate is required"
    for idx, cand in enumerate(candidates):
        method = cand.get("method")
        if method not in _VALID_METHODS:
            problems[f"reduction.candidates[{idx}].method"] = (
                f"unknown reduction method {method!r}"
            )
        dim = cand.get("target_dim")
        if not isinstance(dim, int) or dim < 1:
            problems[f"reduction.candidates[{idx}].target_dim"] = (
                f"target dimension must be a positive integer, got {dim!r}"
            )
    capacity = cfg.get("cache", {}).get("capacity")
    if not isinstance(capacity, int) or capacity < 1:
        problems["cache.capacity"] = f"cache capacity must be a positive integer, got {capacity!r}"
    budget = cfg.get("query", {}).get("latency_budget_ms")
    if not isinstance(budget, (int, float)) or budget <= 0:
        problems["query.latency_budget_ms"] = (
            f"latency budget must be positive milliseconds, got {budget!r}"
        )
    if problems:
        raise PipelineError(
            E_INVALID_CONFIG, "invalid pipeline configuration", problems=problems
        )


def config_version(cfg: dict) -> str:
    return hash_obj(cfg)


def stage_config_digest(cfg: dict, stage: str) -> str:
    """Digest over only the config sections that can influence ``stage``."""
    scope = STAGE_CONFIG_SCOPES[stage]
    subset = {key: cfg.get(key) for key in sorted(scope)}
    return hash_obj({"stage": stage, "config": subset})


def config_path_default(workdir: str) -> str:
    return os.path.join(workdir, "data", "config.json")
