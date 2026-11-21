"""Configuration parsing, validation and behavior-version fingerprints."""

from __future__ import annotations

import copy
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .errors import ErrorCode, PipelineError
from .util import canonical_hash, canonical_json_bytes, ensure_relative_path, sha256_bytes


DEFAULT_CONFIG: dict[str, Any] = {
    "experiment": {"name": "local-ctr-cvr", "seed": 20261122},
    "paths": {
        "manifest": "data/shards/manifest.json",
        "checkpoints": "checkpoints",
        "outputs": "outputs",
        "state": "state",
    },
    "features": {
        "dimension": 256,
        "numeric_bins": {
            "user_features.age": [18, 25, 35, 45, 55],
            "user_features.activity_score": [0.15, 0.35, 0.6, 0.8],
            "item_features.price": [20, 50, 100, 200, 500],
            "context_features.hour": [6, 12, 18, 22],
        },
        "categorical_fields": [
            "user_features.city",
            "user_features.device",
            "user_features.member_tier",
            "item_features.category",
            "item_features.brand_tier",
            "context_features.network",
            "source",
            "lang",
        ],
    },
    "training": {
        "epochs": 4,
        "learning_rate": 0.15,
        "combo_weight": 0.35,
        "quantization_scale": 1000000,
    },
    "scheduler": {
        "max_global_sample_rounds": 6000,
        "per_shard_sample_quota": 240,
        "max_workers": 3,
    },
    "funnel": {"confidence_threshold": 0.5},
    "inference": {"score_format": "ctr-cvr-log-score-v1"},
    "server": {"host": "127.0.0.1", "port": 8765},
}


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


@dataclass
class Config:
    raw: dict[str, Any]
    source_path: str
    source_digest: str
    base_dir: Path

    @classmethod
    def load(cls, path: str | Path, base_dir: str | Path) -> "Config":
        base = Path(base_dir).resolve()
        config_path = Path(path)
        if not config_path.is_absolute():
            config_path = (base / config_path).resolve()
        try:
            payload = config_path.read_bytes()
            parsed = yaml.safe_load(payload.decode("utf-8"))
        except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
            raise PipelineError(
                ErrorCode.E_INVALID_PARAM,
                "cannot load YAML configuration",
                stage="config",
                details={"path": str(config_path), "reason": str(exc)},
            ) from exc
        if not isinstance(parsed, dict):
            raise PipelineError(
                ErrorCode.E_INVALID_PARAM,
                "configuration root must be a mapping",
                stage="config",
            )
        config = cls(
            raw=deep_merge(DEFAULT_CONFIG, parsed),
            source_path=config_path.as_posix(),
            source_digest=sha256_bytes(payload),
            base_dir=base,
        )
        config.validate()
        return config

    @property
    def experiment_name(self) -> str:
        return str(self.raw["experiment"]["name"])

    @property
    def seed(self) -> int:
        return int(self.raw["experiment"]["seed"])

    @property
    def manifest_path(self) -> str:
        return str(self.raw["paths"]["manifest"])

    @property
    def checkpoints_root(self) -> Path:
        return self.base_dir / str(self.raw["paths"]["checkpoints"])

    @property
    def outputs_root(self) -> Path:
        return self.base_dir / str(self.raw["paths"]["outputs"])

    @property
    def state_root(self) -> Path:
        return self.base_dir / str(self.raw["paths"]["state"])

    @property
    def feature_dimension(self) -> int:
        return int(self.raw["features"]["dimension"])

    @property
    def numeric_bins(self) -> dict[str, list[float]]:
        return {
            str(key): [float(item) for item in values]
            for key, values in self.raw["features"]["numeric_bins"].items()
        }

    @property
    def categorical_fields(self) -> list[str]:
        return [str(item) for item in self.raw["features"]["categorical_fields"]]

    @property
    def epochs(self) -> int:
        return int(self.raw["training"]["epochs"])

    @property
    def learning_rate(self) -> float:
        return float(self.raw["training"]["learning_rate"])

    @property
    def combo_weight(self) -> float:
        return float(self.raw["training"]["combo_weight"])

    @property
    def quantization_scale(self) -> int:
        return int(self.raw["training"]["quantization_scale"])

    @property
    def global_round_budget(self) -> int:
        return int(self.raw["scheduler"]["max_global_sample_rounds"])

    @property
    def shard_quota(self) -> int:
        return int(self.raw["scheduler"]["per_shard_sample_quota"])

    @property
    def max_workers(self) -> int:
        return int(self.raw["scheduler"]["max_workers"])

    @property
    def confidence_threshold(self) -> float:
        return float(self.raw["funnel"]["confidence_threshold"])

    @property
    def score_format(self) -> str:
        return str(self.raw["inference"]["score_format"])

    @property
    def server_host(self) -> str:
        return str(self.raw["server"]["host"])

    @property
    def server_port(self) -> int:
        return int(self.raw["server"]["port"])

    def validate(
        self,
        *,
        stage: str = "feature",
        shard_id: str | None = None,
        row_number: int | None = 1,
    ) -> None:
        def fail(message: str, field: str, value: Any) -> None:
            raise PipelineError(
                ErrorCode.E_INVALID_PARAM,
                message,
                stage=stage,
                shard_id=shard_id,
                row_number=row_number,
                details={"field": field, "value": value},
            )

        for field_name, value in self.raw["paths"].items():
            try:
                ensure_relative_path(str(value), f"paths.{field_name}")
            except PipelineError as exc:
                fail(exc.message, f"paths.{field_name}", value)

        dimension = self.raw["features"].get("dimension")
        if not isinstance(dimension, int) or isinstance(dimension, bool) or dimension <= 0:
            fail("feature dimension must be a positive integer", "features.dimension", dimension)

        bins = self.raw["features"].get("numeric_bins")
        if not isinstance(bins, dict):
            fail("numeric_bins must be a mapping", "features.numeric_bins", bins)
        for path, edges in bins.items():
            if not isinstance(path, str) or not path:
                fail("numeric bin field path must be non-empty", "features.numeric_bins", path)
            if not isinstance(edges, list) or not edges:
                fail("numeric bin edges must be a non-empty list", f"features.numeric_bins.{path}", edges)
            numbers = []
            for edge in edges:
                if isinstance(edge, bool) or not isinstance(edge, (int, float)):
                    fail("numeric bin edge must be numeric", f"features.numeric_bins.{path}", edge)
                number = float(edge)
                if not math.isfinite(number):
                    fail("numeric bin edge must be finite", f"features.numeric_bins.{path}", edge)
                numbers.append(number)
            if numbers != sorted(numbers) or len(numbers) != len(set(numbers)):
                fail("numeric bin edges must be strictly increasing", f"features.numeric_bins.{path}", edges)

        categorical = self.raw["features"].get("categorical_fields")
        if not isinstance(categorical, list) or any(not isinstance(item, str) or not item for item in categorical):
            fail("categorical_fields must be a list of non-empty strings", "features.categorical_fields", categorical)

        epochs = self.raw["training"].get("epochs")
        if not isinstance(epochs, int) or isinstance(epochs, bool) or epochs <= 0:
            fail("iterations must be a positive integer", "training.epochs", epochs)
        lr = self.raw["training"].get("learning_rate")
        if isinstance(lr, bool) or not isinstance(lr, (int, float)) or not math.isfinite(float(lr)) or float(lr) <= 0:
            fail("learning rate must be positive and finite", "training.learning_rate", lr)
        combo = self.raw["training"].get("combo_weight")
        if isinstance(combo, bool) or not isinstance(combo, (int, float)) or not math.isfinite(float(combo)) or float(combo) < 0:
            fail("combo weight must be non-negative and finite", "training.combo_weight", combo)
        scale = self.raw["training"].get("quantization_scale")
        if not isinstance(scale, int) or isinstance(scale, bool) or scale <= 0:
            fail("quantization scale must be a positive integer", "training.quantization_scale", scale)

        budget = self.raw["scheduler"].get("max_global_sample_rounds")
        if not isinstance(budget, int) or isinstance(budget, bool) or budget < 0:
            fail("global round budget must be a non-negative integer", "scheduler.max_global_sample_rounds", budget)
        quota = self.raw["scheduler"].get("per_shard_sample_quota")
        if not isinstance(quota, int) or isinstance(quota, bool) or quota <= 0:
            fail("per-shard sample quota must be a positive integer", "scheduler.per_shard_sample_quota", quota)
        workers = self.raw["scheduler"].get("max_workers")
        if not isinstance(workers, int) or isinstance(workers, bool) or workers <= 0:
            fail("max workers must be a positive integer", "scheduler.max_workers", workers)

        threshold = self.raw["funnel"].get("confidence_threshold")
        if (
            isinstance(threshold, bool)
            or not isinstance(threshold, (int, float))
            or not math.isfinite(float(threshold))
            or not 0.0 < float(threshold) < 1.0
        ):
            fail("confidence threshold must be in (0, 1)", "funnel.confidence_threshold", threshold)

        host = self.raw["server"].get("host")
        if host != "127.0.0.1":
            fail("HTTP workbench must bind to localhost only", "server.host", host)
        port = self.raw["server"].get("port")
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            fail("server port is invalid", "server.port", port)

    def injected(self, injection: str | None) -> "Config":
        if not injection:
            return self
        allowed = {
            "zero_feature_dim",
            "manifest_mismatch",
            "invalid_ctr_label",
            "cvr_without_ctr",
            "negative_lr",
            "negative_epochs",
        }
        if injection not in allowed:
            raise PipelineError(
                ErrorCode.E_INVALID_PARAM,
                "unknown fault injection",
                stage="config",
                details={"injection": injection, "allowed": sorted(allowed)},
            )
        raw = copy.deepcopy(self.raw)
        if injection == "zero_feature_dim":
            raw["features"]["dimension"] = 0
        elif injection == "negative_lr":
            raw["training"]["learning_rate"] = -abs(float(raw["training"]["learning_rate"]))
        elif injection == "negative_epochs":
            raw["training"]["epochs"] = -abs(int(raw["training"]["epochs"]))
        return Config(
            raw=raw,
            source_path=self.source_path,
            source_digest=self.source_digest,
            base_dir=self.base_dir,
        )

    def validation_injection(self) -> str | None:
        # Data-level injections are applied by the pipeline after rows are loaded.
        return None

    def behavior_dict(self) -> dict[str, Any]:
        behavior = copy.deepcopy(self.raw)
        behavior.pop("paths", None)
        behavior.pop("server", None)
        behavior.setdefault("scheduler", {}).pop("max_workers", None)
        return behavior

    @property
    def version(self) -> str:
        return canonical_hash(self.behavior_dict())

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.behavior_dict())

    def with_outputs(self, outputs: str, checkpoints: str | None = None, state: str | None = None) -> "Config":
        raw = copy.deepcopy(self.raw)
        raw["paths"]["outputs"] = outputs
        if checkpoints is not None:
            raw["paths"]["checkpoints"] = checkpoints
        if state is not None:
            raw["paths"]["state"] = state
        return Config(
            raw=raw,
            source_path=self.source_path,
            source_digest=self.source_digest,
            base_dir=self.base_dir,
        )
