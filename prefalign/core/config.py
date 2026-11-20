"""Configuration loading, validation and content-addressed versioning."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from .errors import ConfigError
from .hashing import canonical_json, stable_hash64
from .paths import resolve

STAGES = ["normalize", "quality", "features", "sampling"]

# Which config subtree each stage depends on (used for config-change invalidation).
STAGE_CONFIG_KEYS = {
    "normalize": ["version"],
    "quality": ["version", "quality"],
    "features": ["version", "folds", "features"],
    "sampling": ["version", "sampling"],
}


class Config:
    def __init__(self, raw: dict[str, Any], source_path: str | None, project_root: Path):
        self.raw = raw
        self.source_path = source_path
        self.project_root = project_root

    @classmethod
    def load(cls, path: str | Path, project_root: str | Path | None = None) -> "Config":
        path = Path(path)
        root = Path(project_root) if project_root else path.parent.parent
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ConfigError(f"config not found: {path}") from exc
        except yaml.YAMLError as exc:
            raise ConfigError(f"invalid YAML config: {exc}") from exc
        cfg = cls(raw or {}, str(path), root)
        cfg.validate()
        return cfg

    def validate(self) -> None:
        raw = self.raw
        for key in ("quality", "folds", "features", "sampling", "storage"):
            if key not in raw:
                raise ConfigError(f"missing config section: {key}")
        weights = raw["quality"].get("weights", {})
        for name, value in weights.items():
            if not isinstance(value, (int, float)) or value < 0:
                raise ConfigError(f"quality weight must be non-negative number: {name}")
        if not weights:
            raise ConfigError("quality.weights must not be empty")
        if int(raw["folds"].get("n_folds", 0)) < 2:
            raise ConfigError("folds.n_folds must be >= 2")
        if raw["features"].get("rule") not in ("concat", "diff", "normalized"):
            raise ConfigError("features.rule must be one of concat/diff/normalized")
        if int(raw["features"].get("embedding_dim", 0)) <= 0:
            raise ConfigError("features.embedding_dim must be positive")
        targets = raw["sampling"].get("target_weights", {})
        for key, value in targets.items():
            if "::" not in key:
                raise ConfigError(f"sampling target key must be '<source>::<lang>': {key}")
            if not isinstance(value, (int, float)) or value < 0:
                raise ConfigError(f"sampling target weight must be >= 0: {key}")

    def path(self, key: str) -> Path:
        return resolve(self.raw["storage"][key], self.project_root)

    @property
    def config_version(self) -> int:
        return int(self.raw.get("version", 1))

    def subtree(self, stage: str) -> dict[str, Any]:
        return {key: self.raw.get(key) for key in STAGE_CONFIG_KEYS[stage]}

    def subtree_fingerprint(self, stage: str) -> str:
        return stable_hash64(canonical_json(self.subtree(stage)))

    def full_fingerprint(self) -> str:
        return stable_hash64(canonical_json(self.raw))


def config_version_chain(cfg: Config) -> dict[str, str]:
    """Per-stage config fingerprint, used by incremental invalidation."""
    return {stage: cfg.subtree_fingerprint(stage) for stage in STAGES}
