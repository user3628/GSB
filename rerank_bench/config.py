"""YAML scheduling configuration: loading, validation, version fingerprints."""

from __future__ import annotations

import yaml

from .errors import PipelineError
from .hashing import content_digest


class Config:
    """Typed view over the YAML scheduling configuration."""

    def __init__(self, raw: dict):
        self.raw = raw
        self._validate()

    def _validate(self):
        for section in ("features", "precision", "rerank", "parallel"):
            if section not in self.raw:
                raise PipelineError(
                    "E_CONFIG", f"missing config section: {section}", section=section
                )
        if not isinstance(self.raw["features"], list) or not self.raw["features"]:
            raise PipelineError("E_CONFIG", "features must be a non-empty list")
        weights = self.raw["precision"].get("weights", {})
        for name in self.raw["features"]:
            if name not in weights:
                raise PipelineError(
                    "E_CONFIG", f"missing weight for feature {name}", feature=name
                )
        par = self.raw["parallel"]
        for key in ("max_workers", "global_eval_budget", "per_worker_quota",
                    "shard_latency_budget_ms"):
            if key not in par:
                raise PipelineError("E_CONFIG", f"missing parallel.{key}", key=key)

    @property
    def config_version(self) -> str:
        return str(self.raw.get("config_version", "0"))

    @property
    def features(self) -> list[str]:
        return list(self.raw["features"])

    @property
    def weights_micro(self) -> dict:
        from .hashing import to_micro

        return {k: to_micro(v) for k, v in self.raw["precision"]["weights"].items()}

    @property
    def embedding_dim(self) -> int:
        return int(self.raw["precision"].get("embedding_dim", 8))

    @property
    def embedding_weight_micro(self) -> int:
        from .hashing import to_micro

        return to_micro(self.raw["precision"].get("embedding_weight", 0))

    @property
    def window_size(self) -> int:
        return int(self.raw["rerank"]["window_size"])

    @property
    def dedup_penalty_micro(self) -> int:
        from .hashing import to_micro

        return to_micro(self.raw["rerank"].get("dedup_penalty", 0))

    @property
    def category_penalty_micro(self) -> int:
        from .hashing import to_micro

        return to_micro(self.raw["rerank"].get("category_penalty", 0))

    @property
    def language_penalty_micro(self) -> int:
        from .hashing import to_micro

        return to_micro(self.raw["rerank"].get("language_penalty", 0))

    @property
    def max_workers(self) -> int:
        return int(self.raw["parallel"]["max_workers"])

    @property
    def global_eval_budget(self) -> int:
        return int(self.raw["parallel"]["global_eval_budget"])

    @property
    def per_worker_quota(self) -> int:
        return int(self.raw["parallel"]["per_worker_quota"])

    @property
    def shard_latency_budget_ms(self) -> int:
        return int(self.raw["parallel"]["shard_latency_budget_ms"])

    def stage_config_digest(self, stage: str) -> str:
        """Digest of the config subset a stage depends on (drives invalidation)."""
        if stage == "assemble":
            return content_digest({
                "features": self.features,
                "embedding_dim": self.embedding_dim,
            })
        if stage == "score":
            return content_digest({
                "weights": self.raw["precision"]["weights"],
                "embedding_weight": self.raw["precision"].get("embedding_weight", 0),
                "embedding_dim": self.embedding_dim,
            })
        if stage == "rerank":
            return content_digest({
                "window_size": self.window_size,
                "dedup_penalty": self.raw["rerank"].get("dedup_penalty", 0),
                "category_penalty": self.raw["rerank"].get("category_penalty", 0),
                "language_penalty": self.raw["rerank"].get("language_penalty", 0),
            })
        if stage == "aggregate":
            return content_digest({
                "max_workers": self.max_workers,
                "global_eval_budget": self.global_eval_budget,
                "per_worker_quota": self.per_worker_quota,
            })
        raise PipelineError("E_CONFIG", f"unknown stage {stage}", stage=stage)


def load_config(path) -> Config:
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict):
        raise PipelineError("E_CONFIG", f"config at {path} is not a mapping")
    return Config(raw)
