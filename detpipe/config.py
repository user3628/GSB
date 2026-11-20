"""Pipeline configuration with strict validation."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from pathlib import Path

from .errors import PipeError, E_INVALID_CONFIG
from .versioning import budget_version, threshold_version


@dataclass(frozen=True)
class PipelineConfig:
    iou_threshold: float = 0.5
    score_threshold: float = 0.5
    max_evaluations: int = 1_000_000
    worker_quota: int = 1_000_000
    workers: int = 2
    shard_size: int = 2

    def validate(self) -> None:
        if not isinstance(self.iou_threshold, (int, float)) or not (
            0.0 <= float(self.iou_threshold) <= 1.0
        ):
            raise PipeError(
                E_INVALID_CONFIG,
                "iou_threshold must be a number within [0.0, 1.0]",
                field="iou_threshold",
                details={"value": self.iou_threshold},
            )
        if not isinstance(self.score_threshold, (int, float)) or not (
            0.0 <= float(self.score_threshold) <= 1.0
        ):
            raise PipeError(
                E_INVALID_CONFIG,
                "score_threshold must be a number within [0.0, 1.0]",
                field="score_threshold",
                details={"value": self.score_threshold},
            )
        if not isinstance(self.max_evaluations, int) or self.max_evaluations < 0:
            raise PipeError(
                E_INVALID_CONFIG,
                "max_evaluations must be a non-negative integer",
                field="max_evaluations",
                details={"value": self.max_evaluations},
            )
        if not isinstance(self.worker_quota, int) or self.worker_quota < 0:
            raise PipeError(
                E_INVALID_CONFIG,
                "worker_quota must be a non-negative integer",
                field="worker_quota",
                details={"value": self.worker_quota},
            )
        if not isinstance(self.workers, int) or self.workers < 1:
            raise PipeError(
                E_INVALID_CONFIG,
                "workers must be a positive integer",
                field="workers",
                details={"value": self.workers},
            )
        if not isinstance(self.shard_size, int) or self.shard_size < 1:
            raise PipeError(
                E_INVALID_CONFIG,
                "shard_size must be a positive integer",
                field="shard_size",
                details={"value": self.shard_size},
            )

    @property
    def threshold_version(self) -> str:
        return threshold_version(self.iou_threshold, self.score_threshold)

    @property
    def budget_version(self) -> str:
        return budget_version(
            self.max_evaluations, self.worker_quota, self.workers
        )

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineConfig":
        allowed = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        unknown = set(data) - allowed
        if unknown:
            raise PipeError(
                E_INVALID_CONFIG,
                f"unknown configuration fields: {sorted(unknown)}",
                field=sorted(unknown)[0],
            )
        cfg = cls(**{k: data[k] for k in allowed if k in data})
        cfg.validate()
        return cfg

    @classmethod
    def from_file(cls, path: str | Path) -> "PipelineConfig":
        with open(path, "r", encoding="utf-8") as fh:
            return cls.from_dict(json.load(fh))
