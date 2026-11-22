"""Workspace configuration, stage-specific version fingerprints, and limits."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Mapping

from .canonical import canonical_json_text, sha256_json
from .errors import E_INVALID_INPUT, PipelineError


DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "pipeline_version": "gspipe-v1",
    "parse": {
        "max_chain_length": 5,
        "op_spec_version": "ops-canonical-v1",
    },
    "execution": {
        "kernel_version": "kernel-fixedpoint-v1",
        "scheduler": {
            "total_step_budget": 20000,
            "worker_step_quota": 8000,
            "max_workers": 3,
        },
        "cache_enabled": True,
    },
    "verify": {
        "version": "verify-summary-v1",
    },
    "export": {
        "version": "export-bundle-v1",
        "include_report": True,
    },
    "limits": {
        "max_image_side": 2048,
        "max_image_pixels": 4194304,
        "max_tasks": 256,
        "max_import_bytes": 16777216,
    },
}


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = deep_merge(dict(result[key]), value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def _require_int(value: Any, path: str, minimum: int, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PipelineError(
            E_INVALID_INPUT,
            f"Configuration value {path} must be an integer",
            details={"path": path, "value": value},
        )
    if value < minimum or (maximum is not None and value > maximum):
        raise PipelineError(
            E_INVALID_INPUT,
            f"Configuration value {path} is out of range",
            details={"path": path, "value": value, "minimum": minimum, "maximum": maximum},
        )
    return value


class PipelineConfig:
    def __init__(self, raw: Mapping[str, Any] | None = None) -> None:
        self.raw = deep_merge(DEFAULT_CONFIG, raw or {})
        self._validate()
        self.parse_version = sha256_json(
            {
                "schema_version": self.raw["schema_version"],
                "parse": self.raw["parse"],
                "limits": self.raw["limits"],
            }
        )
        self.kernel_version = sha256_json(
            {
                "pipeline_version": self.raw["pipeline_version"],
                "execution": {
                    "kernel_version": self.raw["execution"]["kernel_version"],
                },
            }
        )
        self.scheduler_version = sha256_json(self.raw["execution"]["scheduler"])
        self.execute_version = sha256_json(
            {
                "kernel_version": self.kernel_version,
                "scheduler_version": self.scheduler_version,
            }
        )
        self.verify_version = sha256_json(self.raw["verify"])
        self.export_version = sha256_json(self.raw["export"])
        self.config_version = sha256_json(self.raw)

    def _validate(self) -> None:
        parse = self.raw.get("parse")
        execution = self.raw.get("execution")
        limits = self.raw.get("limits")
        if not isinstance(parse, Mapping) or not isinstance(execution, Mapping) or not isinstance(limits, Mapping):
            raise PipelineError(E_INVALID_INPUT, "Configuration sections parse/execution/limits must be objects")
        _require_int(parse.get("max_chain_length"), "parse.max_chain_length", 1, 64)
        scheduler = execution.get("scheduler")
        if not isinstance(scheduler, Mapping):
            raise PipelineError(E_INVALID_INPUT, "execution.scheduler must be an object")
        _require_int(scheduler.get("total_step_budget"), "execution.scheduler.total_step_budget", 1)
        _require_int(scheduler.get("worker_step_quota"), "execution.scheduler.worker_step_quota", 1)
        _require_int(scheduler.get("max_workers"), "execution.scheduler.max_workers", 1, 64)
        cache_enabled = execution.get("cache_enabled")
        if not isinstance(cache_enabled, bool):
            raise PipelineError(E_INVALID_INPUT, "execution.cache_enabled must be a boolean")
        _require_int(limits.get("max_image_side"), "limits.max_image_side", 1)
        _require_int(limits.get("max_image_pixels"), "limits.max_image_pixels", 1)
        _require_int(limits.get("max_tasks"), "limits.max_tasks", 1)
        _require_int(limits.get("max_import_bytes"), "limits.max_import_bytes", 1)

    @property
    def parse(self) -> Mapping[str, Any]:
        return self.raw["parse"]

    @property
    def execution(self) -> Mapping[str, Any]:
        return self.raw["execution"]

    @property
    def scheduler(self) -> Mapping[str, Any]:
        return self.raw["execution"]["scheduler"]

    @property
    def cache_enabled(self) -> bool:
        return bool(self.raw["execution"]["cache_enabled"])

    @property
    def limits(self) -> Mapping[str, Any]:
        return self.raw["limits"]

    def to_dict(self) -> dict[str, Any]:
        return copy.deepcopy(self.raw)

    def pretty(self) -> str:
        return json.dumps(self.raw, ensure_ascii=False, indent=2, sort_keys=True)


def load_config(path: str | Path | None = None, override: Mapping[str, Any] | None = None) -> PipelineConfig:
    raw: dict[str, Any] = {}
    if path is not None:
        config_path = Path(path)
        if config_path.exists():
            try:
                loaded = json.loads(config_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise PipelineError(
                    E_INVALID_INPUT,
                    f"Unable to read configuration {config_path}: {exc}",
                    details={"path": str(config_path)},
                ) from exc
            if not isinstance(loaded, dict):
                raise PipelineError(E_INVALID_INPUT, "Configuration root must be a JSON object")
            raw = loaded
    if override:
        raw = deep_merge(raw, override)
    return PipelineConfig(raw)


def config_text(config: PipelineConfig) -> str:
    return canonical_json_text(config.to_dict())
